use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow_array::builder::*;
use arrow_schema::{DataType, SchemaRef, TimeUnit};
use tracing::{info, warn};

use crate::source::types::{CdcEvent, ColumnValue, TableId};

/// Per-table buffer that accumulates CdcEvents and flushes them as Arrow RecordBatches.
/// Number of CDC metadata columns appended to every table's Arrow schema.
const CDC_META_COLS: usize = 4; // _cdc_op, _cdc_ts, _cdc_seq, _cdc_schema_version

pub struct TableBuffer {
    table_id: TableId,
    schema: SchemaRef,
    /// One builder per column (user columns + CDC metadata columns)
    builders: Vec<ColumnBuilder>,
    /// Cached column names for user columns (avoids schema lookup on hot path)
    col_names: Vec<String>,
    row_count: usize,
    schema_version: u64,
}

/// Generates the ColumnBuilder enum and its mechanical methods (type_name,
/// append_null_untyped, finish) from a declaration list. Decimal128 is handled
/// separately because it carries extra state (scale).
macro_rules! column_builder {
    ( $( $variant:ident($builder_ty:ty) ),+ $(,)? ) => {
        enum ColumnBuilder {
            $( $variant($builder_ty), )+
            Decimal128 { builder: Decimal128Builder, scale: i8 },
        }

        impl ColumnBuilder {
            fn type_name(&self) -> &'static str {
                match self {
                    $( Self::$variant(_) => stringify!($variant), )+
                    Self::Decimal128 { .. } => "Decimal128",
                }
            }

            fn append_null_untyped(&mut self) {
                match self {
                    $( Self::$variant(b) => b.append_null(), )+
                    Self::Decimal128 { builder: b, .. } => b.append_null(),
                }
            }

            fn finish(&mut self) -> arrow::array::ArrayRef {
                match self {
                    $( Self::$variant(b) => Arc::new(b.finish()), )+
                    Self::Decimal128 { builder: b, .. } => Arc::new(b.finish()),
                }
            }
        }
    };
}

column_builder! {
    Boolean(BooleanBuilder),
    Int8(Int8Builder),
    Int16(Int16Builder),
    Int32(Int32Builder),
    Int64(Int64Builder),
    UInt8(UInt8Builder),
    UInt16(UInt16Builder),
    UInt32(UInt32Builder),
    UInt64(UInt64Builder),
    Float32(Float32Builder),
    Float64(Float64Builder),
    Date32(Date32Builder),
    TimestampMicro(TimestampMicrosecondBuilder),
    DurationMicro(DurationMicrosecondBuilder),
    Utf8(StringBuilder),
    Binary(BinaryBuilder),
}

/// Rescale a decimal mantissa from `source_scale` to `target_scale`.
/// E.g. mantissa=1234, source_scale=2 (12.34), target_scale=4 → 12340000? No:
/// 1234 at scale 2 = 12.34, at scale 4 needs mantissa 123400.
/// Rescale a decimal mantissa from `source_scale` to `target_scale`.
/// Returns `None` on overflow (e.g. large mantissa × large power of 10).
fn rescale_decimal(mantissa: i128, source_scale: i8, target_scale: i8) -> Option<i128> {
    let diff = target_scale as i32 - source_scale as i32;
    if diff > 0 {
        let factor = 10i128.checked_pow(diff as u32)?;
        mantissa.checked_mul(factor)
    } else if diff < 0 {
        let divisor = 10i128.checked_pow((-diff) as u32)?;
        if mantissa % divisor != 0 {
            warn!(
                source_scale, target_scale, mantissa,
                "Decimal scale-down truncation: losing fractional digits"
            );
        }
        Some(mantissa / divisor)
    } else {
        Some(mantissa)
    }
}

impl ColumnBuilder {
    fn new_for_type(dt: &DataType) -> Self {
        Self::new_for_type_with_capacity(dt, 0)
    }

    fn new_for_type_with_capacity(dt: &DataType, capacity: usize) -> Self {
        match dt {
            DataType::Boolean => Self::Boolean(BooleanBuilder::with_capacity(capacity)),
            DataType::Int8 => Self::Int8(Int8Builder::with_capacity(capacity)),
            DataType::Int16 => Self::Int16(Int16Builder::with_capacity(capacity)),
            DataType::Int32 => Self::Int32(Int32Builder::with_capacity(capacity)),
            DataType::Int64 => Self::Int64(Int64Builder::with_capacity(capacity)),
            DataType::UInt8 => Self::UInt8(UInt8Builder::with_capacity(capacity)),
            DataType::UInt16 => Self::UInt16(UInt16Builder::with_capacity(capacity)),
            DataType::UInt32 => Self::UInt32(UInt32Builder::with_capacity(capacity)),
            DataType::UInt64 => Self::UInt64(UInt64Builder::with_capacity(capacity)),
            DataType::Float32 => Self::Float32(Float32Builder::with_capacity(capacity)),
            DataType::Float64 => Self::Float64(Float64Builder::with_capacity(capacity)),
            DataType::Decimal128(p, s) => {
                Self::Decimal128 {
                    builder: Decimal128Builder::with_capacity(capacity)
                        .with_precision_and_scale(*p, *s).unwrap(),
                    scale: *s,
                }
            }
            DataType::Date32 => Self::Date32(Date32Builder::with_capacity(capacity)),
            DataType::Timestamp(TimeUnit::Microsecond, tz) => {
                Self::TimestampMicro(
                    TimestampMicrosecondBuilder::with_capacity(capacity)
                        .with_timezone(tz.clone().unwrap_or("UTC".into())),
                )
            }
            DataType::Duration(TimeUnit::Microsecond) => {
                Self::DurationMicro(DurationMicrosecondBuilder::with_capacity(capacity))
            }
            DataType::Utf8 => Self::Utf8(StringBuilder::with_capacity(capacity, capacity * 32)),
            DataType::Binary => Self::Binary(BinaryBuilder::with_capacity(capacity, capacity * 64)),
            other => {
                warn!(data_type = ?other, "Unsupported Arrow type, falling back to Utf8");
                Self::Utf8(StringBuilder::with_capacity(capacity, capacity * 32))
            }
        }
    }

    /// Append a ColumnValue to this builder. On type mismatch, append null.
    fn append(&mut self, value: &ColumnValue, col_name: &str) {
        match (self, value) {
            // --- NULL or conversion failed ---
            (builder, ColumnValue::Null) | (builder, ColumnValue::ConversionFailed(_)) => {
                builder.append_null_untyped();
            }

            // --- Boolean ---
            (Self::Boolean(b), ColumnValue::Int(v)) => b.append_value(*v != 0),
            (Self::Boolean(b), ColumnValue::UInt(v)) => b.append_value(*v != 0),
            (Self::Boolean(b), ColumnValue::Bool(v)) => b.append_value(*v),
            (Self::Boolean(b), ColumnValue::String(s)) => {
                b.append_value(s == "1" || s.eq_ignore_ascii_case("true"));
            }

            // --- Integers with bounds checking ---
            // Int16 from String (YEAR type arrives as string from binlog)
            (Self::Int16(b), ColumnValue::String(s)) => {
                match s.parse::<i16>() {
                    Ok(v) => b.append_value(v),
                    Err(_) => {
                        warn!(column = col_name, value = %s, "Failed to parse Int16 from string");
                        b.append_null();
                    }
                }
            }
            // Signed: check fits in target range, NULL on overflow
            (Self::Int8(b), ColumnValue::Int(v)) => {
                if *v >= i8::MIN as i64 && *v <= i8::MAX as i64 {
                    b.append_value(*v as i8);
                } else {
                    warn!(column = col_name, value = v, "Int overflow for Int8, writing NULL");
                    b.append_null();
                }
            }
            (Self::Int16(b), ColumnValue::Int(v)) => {
                if *v >= i16::MIN as i64 && *v <= i16::MAX as i64 {
                    b.append_value(*v as i16);
                } else {
                    warn!(column = col_name, value = v, "Int overflow for Int16, writing NULL");
                    b.append_null();
                }
            }
            // Unsigned→signed widening: UInt8→Int16, UInt16→Int32, UInt32→Int64
            (Self::Int16(b), ColumnValue::UInt(v)) => {
                if *v <= i16::MAX as u64 {
                    b.append_value(*v as i16);
                } else {
                    warn!(column = col_name, value = v, "UInt overflow for Int16, writing NULL");
                    b.append_null();
                }
            }
            (Self::Int32(b), ColumnValue::Int(v)) => {
                if *v >= i32::MIN as i64 && *v <= i32::MAX as i64 {
                    b.append_value(*v as i32);
                } else {
                    warn!(column = col_name, value = v, "Int overflow for Int32, writing NULL");
                    b.append_null();
                }
            }
            (Self::Int32(b), ColumnValue::UInt(v)) => {
                if *v <= i32::MAX as u64 {
                    b.append_value(*v as i32);
                } else {
                    warn!(column = col_name, value = v, "UInt overflow for Int32, writing NULL");
                    b.append_null();
                }
            }
            (Self::Int64(b), ColumnValue::Int(v)) => b.append_value(*v),
            (Self::Int64(b), ColumnValue::UInt(v)) => {
                // BIGINT UNSIGNED > i64::MAX: store as raw bit pattern (negative in i64)
                b.append_value(*v as i64);
            }

            // --- Floats ---
            (Self::Float32(b), ColumnValue::Float(v)) => {
                let f = *v as f32;
                if f.is_infinite() && !v.is_infinite() {
                    warn!(column = col_name, value = v, "Float64→Float32 overflow to infinity");
                }
                b.append_value(f);
            }
            (Self::Float64(b), ColumnValue::Float(v)) => b.append_value(*v),

            // --- Decimal ---
            (Self::Decimal128 { builder: b, scale }, ColumnValue::Decimal(d)) => {
                match rescale_decimal(d.mantissa(), d.scale() as i8, *scale) {
                    Some(v) => b.append_value(v),
                    None => {
                        warn!(column = col_name, "Decimal overflow during rescale, writing NULL");
                        b.append_null();
                    }
                }
            }
            (Self::Decimal128 { builder: b, scale }, ColumnValue::String(s)) => {
                match s.parse::<rust_decimal::Decimal>() {
                    Ok(d) => match rescale_decimal(d.mantissa(), d.scale() as i8, *scale) {
                        Some(v) => b.append_value(v),
                        None => {
                            warn!(column = col_name, "Decimal overflow during rescale, writing NULL");
                            b.append_null();
                        }
                    },
                    Err(_) => {
                        warn!(column = col_name, value = %s, "Failed to parse decimal");
                        b.append_null();
                    }
                }
            }
            (Self::Decimal128 { builder: b, scale }, ColumnValue::Int(v)) => {
                match 10i128.checked_pow(*scale as u32).and_then(|f| (*v as i128).checked_mul(f)) {
                    Some(val) => b.append_value(val),
                    None => {
                        warn!(column = col_name, "Decimal overflow, writing NULL");
                        b.append_null();
                    }
                }
            }
            (Self::Decimal128 { builder: b, scale }, ColumnValue::UInt(v)) => {
                match 10i128.checked_pow(*scale as u32).and_then(|f| (*v as i128).checked_mul(f)) {
                    Some(val) => b.append_value(val),
                    None => {
                        warn!(column = col_name, "Decimal overflow, writing NULL");
                        b.append_null();
                    }
                }
            }

            // --- Date ---
            (Self::Date32(b), ColumnValue::Date(d)) => {
                let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                let days = (*d - epoch).num_days();
                if days >= i32::MIN as i64 && days <= i32::MAX as i64 {
                    b.append_value(days as i32);
                } else {
                    warn!(column = col_name, "Date out of i32 range, writing NULL");
                    b.append_null();
                }
            }

            // --- Timestamp ---
            (Self::TimestampMicro(b), ColumnValue::DateTime(dt)) => {
                b.append_value(dt.and_utc().timestamp_micros());
            }
            (Self::TimestampMicro(b), ColumnValue::Int(epoch_secs)) => {
                b.append_value(epoch_secs.saturating_mul(1_000_000));
            }
            (Self::TimestampMicro(b), ColumnValue::UInt(epoch_secs)) => {
                b.append_value((*epoch_secs as i64).saturating_mul(1_000_000));
            }
            (Self::TimestampMicro(b), ColumnValue::String(s)) => {
                if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
                    b.append_value(dt.and_utc().timestamp_micros());
                } else if let Ok(secs) = s.parse::<i64>() {
                    b.append_value(secs.saturating_mul(1_000_000));
                } else {
                    warn!(column = col_name, value = %s, "Failed to parse timestamp");
                    b.append_null();
                }
            }

            // --- Duration / TIME ---
            // TIME maps to Int64 (signed microseconds) since Iceberg doesn't support Arrow Duration
            (Self::Int64(b), ColumnValue::TimeMicros(micros)) => {
                b.append_value(micros.0);
            }
            (Self::DurationMicro(b), ColumnValue::TimeMicros(micros)) => {
                b.append_value(micros.0);
            }

            // --- String/Utf8 ---
            (Self::Utf8(b), ColumnValue::String(s)) => b.append_value(s),
            (Self::Utf8(b), ColumnValue::Json(j)) => {
                b.append_value(serde_json::to_string(j).unwrap_or_else(|_| "null".to_string()));
            }
            (Self::Utf8(b), ColumnValue::Int(v)) => {
                // ENUM/SET values from binlog arrive as integers
                b.append_value(v.to_string());
            }
            (Self::Utf8(b), ColumnValue::UInt(v)) => {
                b.append_value(v.to_string());
            }
            (Self::Utf8(b), ColumnValue::Decimal(d)) => {
                // JSON column with decimal, or fallback
                b.append_value(d.to_string());
            }

            // --- Binary ---
            (Self::Binary(b), ColumnValue::Bytes(v)) => b.append_value(v),
            (Self::Binary(b), ColumnValue::String(s)) => b.append_value(s.as_bytes()),

            // --- Type mismatch fallback ---
            (builder, value) => {
                warn!(
                    column = col_name,
                    value = ?value,
                    builder_type = builder.type_name(),
                    "Type mismatch, writing NULL"
                );
                builder.append_null_untyped();
            }
        }
    }
}

impl TableBuffer {
    pub fn new(table_id: TableId, schema: SchemaRef, schema_version: u64) -> Self {
        let cdc_cols = schema.fields().iter().filter(|f| f.name().starts_with("_cdc_")).count();
        debug_assert_eq!(
            cdc_cols, CDC_META_COLS,
            "CDC_META_COLS ({}) doesn't match actual _cdc_ columns in schema ({})",
            CDC_META_COLS, cdc_cols
        );

        let num_user_cols = schema.fields().len() - CDC_META_COLS;
        let col_names: Vec<String> = schema.fields().iter()
            .take(num_user_cols)
            .map(|f| f.name().clone())
            .collect();

        let builders = schema
            .fields()
            .iter()
            .map(|f| ColumnBuilder::new_for_type(f.data_type()))
            .collect();

        Self {
            table_id,
            schema,
            builders,
            col_names,
            row_count: 0,
            schema_version,
        }
    }

    /// Append a CdcEvent to the buffer. The event's columns must match the user columns.
    pub fn append(&mut self, event: &CdcEvent) {
        let num_user_cols = self.schema.fields().len() - CDC_META_COLS;

        // Append user columns (positional access — both event values and schema
        // columns are ordered by INFORMATION_SCHEMA.ordinal_position)
        for (i, builder) in self.builders.iter_mut().enumerate().take(num_user_cols) {
            let col_name = &self.col_names[i];
            let value = event
                .values
                .get(i)
                .unwrap_or(&ColumnValue::Null);
            builder.append(value, col_name);
        }

        // Append CDC metadata columns
        // _cdc_op
        if let ColumnBuilder::Utf8(b) = &mut self.builders[num_user_cols] {
            b.append_value(event.op.as_str());
        }
        // _cdc_ts
        if let ColumnBuilder::TimestampMicro(b) = &mut self.builders[num_user_cols + 1] {
            b.append_value(event.timestamp.timestamp_micros());
        }
        // _cdc_seq
        if let ColumnBuilder::Int64(b) = &mut self.builders[num_user_cols + 2] {
            b.append_value(event.sequence);
        }
        // _cdc_schema_version
        if let ColumnBuilder::Int64(b) = &mut self.builders[num_user_cols + 3] {
            b.append_value(self.schema_version as i64);
        }

        self.row_count += 1;
    }

    /// Drain the buffer into a RecordBatch and reset.
    pub fn flush(&mut self) -> anyhow::Result<RecordBatch> {
        let count = self.row_count;
        let arrays: Vec<arrow::array::ArrayRef> =
            self.builders.iter_mut().map(|b| b.finish()).collect();

        let batch = RecordBatch::try_new(self.schema.clone(), arrays)?;

        // Reset builders with capacity hint from previous batch size
        self.builders = self
            .schema
            .fields()
            .iter()
            .map(|f| ColumnBuilder::new_for_type_with_capacity(f.data_type(), count))
            .collect();
        self.row_count = 0;

        info!(
            table = %self.table_id,
            rows = count,
            "Flushed buffer to RecordBatch"
        );

        Ok(batch)
    }

    pub fn len(&self) -> usize {
        self.row_count
    }

    pub fn is_empty(&self) -> bool {
        self.row_count == 0
    }
}

/// Pipeline orchestrates per-table buffering and flush triggers.
pub struct Pipeline {
    buffers: HashMap<TableId, TableBuffer>,
    schemas: HashMap<TableId, (SchemaRef, u64)>, // (schema, version)
    batch_size: usize,
}

impl Pipeline {
    pub fn new(
        tables: &[crate::source::schema::TableMeta],
        batch_size: usize,
    ) -> Self {
        let mut buffers = HashMap::new();
        let mut schemas = HashMap::new();

        for table in tables {
            let tid = table.table_id.clone();
            schemas.insert(tid.clone(), (table.arrow_schema.clone(), table.schema_version));
            buffers.insert(
                tid.clone(),
                TableBuffer::new(tid, table.arrow_schema.clone(), table.schema_version),
            );
        }

        Self {
            buffers,
            schemas,
            batch_size,
        }
    }

    /// Handle a schema change: flush old buffer with old schema, update schema.
    /// Returns the flushed batch (if any) so the caller can write it.
    pub fn handle_schema_change(
        &mut self,
        tid: &TableId,
        new_schema: SchemaRef,
    ) -> anyhow::Result<Option<(TableId, RecordBatch)>> {
        info!(table = %tid, "Schema change — flushing old buffer");
        if let Some(buffer) = self.buffers.get_mut(tid) {
            if !buffer.is_empty() {
                let batch = buffer.flush()?;
                self.buffers.remove(tid);
                self.update_schema(tid, new_schema);
                return Ok(Some((tid.clone(), batch)));
            } else {
                self.buffers.remove(tid);
            }
        }
        self.update_schema(tid, new_schema);
        Ok(None)
    }

    /// Ingest a single CDC event. Returns a RecordBatch if a flush was triggered.
    pub fn ingest(&mut self, event: &CdcEvent) -> Option<(TableId, RecordBatch)> {
        let tid = &event.table;

        // Ensure buffer exists
        if !self.buffers.contains_key(tid) {
            let (schema, version) = match self.schemas.get(tid) {
                Some(sv) => sv.clone(),
                None => {
                    warn!(table = %tid, "No schema for table, skipping event");
                    return None;
                }
            };
            self.buffers.insert(tid.clone(), TableBuffer::new(tid.clone(), schema, version));
        }

        let buffer = self.buffers.get_mut(tid).unwrap();
        buffer.append(event);

        // Check flush trigger
        if buffer.len() >= self.batch_size {
            let batch = buffer.flush().ok()?;
            Some((tid.clone(), batch))
        } else {
            None
        }
    }

    /// Update the schema for a table (called after schema evolution).
    pub fn update_schema(&mut self, tid: &TableId, schema: SchemaRef) {
        let version = self.schemas.get(tid).map(|(_, v)| v + 1).unwrap_or(1);
        self.schemas.insert(tid.clone(), (schema, version));
        // Remove existing buffer so it gets recreated with new schema
        self.buffers.remove(tid);
    }

    /// Flush all non-empty buffers. Returns vec of (table, batch) pairs.
    pub fn flush_all(&mut self) -> Vec<(TableId, RecordBatch)> {
        let mut results = Vec::new();
        for (tid, buffer) in self.buffers.iter_mut() {
            if !buffer.is_empty() {
                match buffer.flush() {
                    Ok(batch) => results.push((tid.clone(), batch)),
                    Err(e) => {
                        warn!(table = %tid, error = %e, "Failed to flush buffer");
                    }
                }
            }
        }
        results
    }

    /// Total buffered rows across all tables.
    #[allow(dead_code)]
    pub fn total_buffered(&self) -> usize {
        self.buffers.values().map(|b| b.len()).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Array;
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use chrono::{DateTime, Utc};
    use crate::source::types::{CdcEvent, CdcOp, TableId};

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("amount", DataType::Decimal128(10, 2), true),
            Field::new("active", DataType::Boolean, true),
            // CDC metadata
            Field::new("_cdc_op", DataType::Utf8, false),
            Field::new("_cdc_ts", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false),
            Field::new("_cdc_seq", DataType::Int64, false),
            Field::new("_cdc_schema_version", DataType::Int64, false),
        ]))
    }

    fn test_table_id() -> TableId {
        TableId {
            database: "testdb".to_string(),
            table: "test_table".to_string(),
        }
    }

    fn make_event(id: i64, name: &str, amount: rust_decimal::Decimal, active: bool, seq: i64) -> CdcEvent {
        CdcEvent {
            table: test_table_id(),
            op: CdcOp::Insert,
            timestamp: DateTime::<Utc>::from(std::time::SystemTime::UNIX_EPOCH),
            sequence: seq,
            values: vec![
                ColumnValue::Int(id),
                ColumnValue::String(name.to_string()),
                ColumnValue::Decimal(amount),
                ColumnValue::Bool(active),
            ],
        }
    }

    // --- Decimal scale alignment ---

    #[test]
    fn decimal_scale_alignment() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("val", DataType::Decimal128(10, 4), true),
            Field::new("_cdc_op", DataType::Utf8, false),
            Field::new("_cdc_ts", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false),
            Field::new("_cdc_seq", DataType::Int64, false),
            Field::new("_cdc_schema_version", DataType::Int64, false),
        ]));

        let mut buffer = TableBuffer::new(test_table_id(), schema, 1);

        // Decimal with scale 2 going into scale 4 column
        let d = rust_decimal::Decimal::new(1234, 2); // 12.34
        let event = CdcEvent {
            table: test_table_id(),
            op: CdcOp::Insert,
            timestamp: DateTime::<Utc>::from(std::time::SystemTime::UNIX_EPOCH),
            sequence: 0,
            values: vec![ColumnValue::Decimal(d)],
        };
        buffer.append(&event);

        let batch = buffer.flush().unwrap();
        let arr = batch.column(0).as_any().downcast_ref::<arrow_array::Decimal128Array>().unwrap();
        // 12.34 at scale 4 = mantissa 123400
        assert_eq!(arr.value(0), 123400);
    }

    #[test]
    fn decimal_int_scaled() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("val", DataType::Decimal128(10, 2), true),
            Field::new("_cdc_op", DataType::Utf8, false),
            Field::new("_cdc_ts", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false),
            Field::new("_cdc_seq", DataType::Int64, false),
            Field::new("_cdc_schema_version", DataType::Int64, false),
        ]));

        let mut buffer = TableBuffer::new(test_table_id(), schema, 1);
        let event = CdcEvent {
            table: test_table_id(),
            op: CdcOp::Insert,
            timestamp: DateTime::<Utc>::from(std::time::SystemTime::UNIX_EPOCH),
            sequence: 0,
            values: vec![ColumnValue::Int(42)],
        };
        buffer.append(&event);

        let batch = buffer.flush().unwrap();
        let arr = batch.column(0).as_any().downcast_ref::<arrow_array::Decimal128Array>().unwrap();
        // 42 at scale 2 = mantissa 4200
        assert_eq!(arr.value(0), 4200);
    }

    // --- Boolean coercion ---

    #[test]
    fn int_to_boolean() {
        let mut builder = ColumnBuilder::new_for_type(&DataType::Boolean);
        builder.append(&ColumnValue::Int(1), "test");
        builder.append(&ColumnValue::Int(0), "test");
        let arr = builder.finish();
        let bools = arr.as_any().downcast_ref::<arrow_array::BooleanArray>().unwrap();
        assert!(bools.value(0));
        assert!(!bools.value(1));
    }

    // --- Unsigned widening ---

    #[test]
    fn uint_to_int16() {
        let mut builder = ColumnBuilder::new_for_type(&DataType::Int16);
        builder.append(&ColumnValue::UInt(255), "test");
        let arr = builder.finish();
        let ints = arr.as_any().downcast_ref::<arrow_array::Int16Array>().unwrap();
        assert_eq!(ints.value(0), 255);
    }

    #[test]
    fn uint_overflow_to_null() {
        let mut builder = ColumnBuilder::new_for_type(&DataType::Int16);
        builder.append(&ColumnValue::UInt(40000), "test"); // > i16::MAX
        let arr = builder.finish();
        let ints = arr.as_any().downcast_ref::<arrow_array::Int16Array>().unwrap();
        assert!(ints.is_null(0));
    }

    // --- TimeMicros ---

    #[test]
    fn time_micros_to_int64() {
        let mut builder = ColumnBuilder::new_for_type(&DataType::Int64);
        let micros = -5_400_000_000i64; // -1.5 hours
        builder.append(&ColumnValue::TimeMicros(crate::source::types::TimeMicroseconds(micros)), "test");
        let arr = builder.finish();
        let ints = arr.as_any().downcast_ref::<arrow_array::Int64Array>().unwrap();
        assert_eq!(ints.value(0), micros);
    }

    // --- TableBuffer round-trip ---

    #[test]
    fn table_buffer_flush() {
        let schema = test_schema();
        let mut buffer = TableBuffer::new(test_table_id(), schema.clone(), 1);

        let d = rust_decimal::Decimal::new(9999, 2); // 99.99
        buffer.append(&make_event(1, "alice", d, true, 0));
        buffer.append(&make_event(2, "bob", d, false, 1));

        assert_eq!(buffer.len(), 2);
        let batch = buffer.flush().unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 8); // 4 user + 4 cdc
        assert_eq!(buffer.len(), 0);
    }

    // --- Pipeline batch trigger ---

    #[test]
    fn pipeline_batch_trigger() {
        let table_meta = crate::source::schema::TableMeta {
            table_id: test_table_id(),
            columns: vec![
                crate::source::types::ColumnMeta {
                    name: "id".to_string(),
                    ordinal: 1,
                    col_type: crate::source::types::MysqlType::Int { unsigned: false },
                    mysql_type_raw: "int".to_string(),
                    nullable: false,
                },
                crate::source::types::ColumnMeta {
                    name: "name".to_string(),
                    ordinal: 2,
                    col_type: crate::source::types::MysqlType::Varchar,
                    mysql_type_raw: "varchar(255)".to_string(),
                    nullable: true,
                },
                crate::source::types::ColumnMeta {
                    name: "amount".to_string(),
                    ordinal: 3,
                    col_type: crate::source::types::MysqlType::Decimal { precision: 10, scale: 2 },
                    mysql_type_raw: "decimal(10,2)".to_string(),
                    nullable: true,
                },
                crate::source::types::ColumnMeta {
                    name: "active".to_string(),
                    ordinal: 4,
                    col_type: crate::source::types::MysqlType::TinyintBool,
                    mysql_type_raw: "tinyint(1)".to_string(),
                    nullable: true,
                },
            ],
            primary_key_columns: vec!["id".to_string()],
            arrow_schema: test_schema(),
            schema_version: 1,
        };

        let batch_size = 3;
        let mut pipeline = Pipeline::new(&[table_meta], batch_size);

        let d = rust_decimal::Decimal::new(100, 2);
        // Insert 2 events — no flush
        assert!(pipeline.ingest(&make_event(1, "a", d, true, 0)).is_none());
        assert!(pipeline.ingest(&make_event(2, "b", d, true, 1)).is_none());

        // 3rd event triggers flush
        let result = pipeline.ingest(&make_event(3, "c", d, true, 2));
        assert!(result.is_some());
        let (tid, batch) = result.unwrap();
        assert_eq!(tid.table, "test_table");
        assert_eq!(batch.num_rows(), 3);
    }

    // --- String to timestamp ---

    #[test]
    fn string_to_timestamp() {
        let mut builder = ColumnBuilder::new_for_type(&DataType::Timestamp(
            TimeUnit::Microsecond,
            Some("UTC".into()),
        ));
        builder.append(
            &ColumnValue::String("2024-06-15 10:30:45".to_string()),
            "test",
        );
        let arr = builder.finish();
        let ts = arr.as_any().downcast_ref::<arrow_array::TimestampMicrosecondArray>().unwrap();
        assert!(ts.value(0) > 0);
    }

    // --- Null handling ---

    #[test]
    fn null_appended_to_any_builder() {
        let types = vec![
            DataType::Boolean,
            DataType::Int8,
            DataType::Int32,
            DataType::Int64,
            DataType::Float64,
            DataType::Utf8,
            DataType::Binary,
            DataType::Decimal128(10, 2),
            DataType::Date32,
        ];
        for dt in types {
            let mut builder = ColumnBuilder::new_for_type(&dt);
            builder.append(&ColumnValue::Null, "test");
            let arr = builder.finish();
            assert!(arr.is_null(0), "NULL not handled for {:?}", dt);
        }
    }

    // --- rescale_decimal edge cases ---

    #[test]
    fn rescale_same_scale() {
        assert_eq!(rescale_decimal(1234, 2, 2), Some(1234));
    }

    #[test]
    fn rescale_scale_down() {
        assert_eq!(rescale_decimal(12345, 4, 2), Some(123));
    }

    #[test]
    fn rescale_negative_mantissa() {
        assert_eq!(rescale_decimal(-1234, 2, 4), Some(-123400));
    }

    #[test]
    fn rescale_zero() {
        assert_eq!(rescale_decimal(0, 2, 4), Some(0));
        assert_eq!(rescale_decimal(0, 4, 2), Some(0));
    }

    #[test]
    fn rescale_overflow() {
        // i128::MAX with scale-up should overflow
        assert_eq!(rescale_decimal(i128::MAX, 0, 10), None);
    }

    // --- Event with fewer columns than schema ---

    #[test]
    fn ingest_sparse_columns() {
        let schema = test_schema(); // 4 user columns + 3 CDC
        let mut buffer = TableBuffer::new(test_table_id(), schema, 1);

        // Event with only 2 of the 4 user columns
        let event = CdcEvent {
            table: test_table_id(),
            op: CdcOp::Insert,
            timestamp: DateTime::<Utc>::from(std::time::SystemTime::UNIX_EPOCH),
            sequence: 0,
            values: vec![
                ColumnValue::Int(1),
                ColumnValue::String("alice".to_string()),
            ],
        };
        buffer.append(&event);

        let batch = buffer.flush().unwrap();
        assert_eq!(batch.num_rows(), 1);
        // Missing columns should be NULL
        let amount = batch.column(2); // amount column
        assert!(amount.is_null(0));
        let active = batch.column(3); // active column
        assert!(active.is_null(0));
    }

    // --- SinkMode config parsing ---

    #[test]
    fn sink_mode_rejects_invalid() {
        let toml_str = r#"
            [source]
            host = "localhost"
            port = 3306
            user = "root"
            password = "root"
            server_id = 1
            database = "test"
            tables = ["t"]

            [sink]
            mode = "bogus"
            [sink.writer]

            [state]
            [observability]
        "#;
        let result: Result<crate::config::Config, _> = toml::from_str(toml_str);
        assert!(result.is_err(), "Should reject invalid sink mode");
    }
}
