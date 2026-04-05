use std::collections::HashMap;

use arrow::array::RecordBatch;
use arrow_array::builder::*;
use arrow_schema::{DataType, SchemaRef, TimeUnit};
use tracing::{info, warn};

use crate::source::types::{CdcEvent, ColumnValue, TableId};

/// Per-table buffer that accumulates CdcEvents and flushes them as Arrow RecordBatches.
pub struct TableBuffer {
    table_id: TableId,
    schema: SchemaRef,
    /// One builder per column (user columns + 3 CDC metadata columns)
    builders: Vec<ColumnBuilder>,
    row_count: usize,
}

/// A typed column builder wrapping Arrow's builder API.
enum ColumnBuilder {
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
    Decimal128 {
        builder: Decimal128Builder,
        scale: i8,
    },
    Date32(Date32Builder),
    TimestampMicro(TimestampMicrosecondBuilder),
    DurationMicro(DurationMicrosecondBuilder),
    Utf8(StringBuilder),
    Binary(BinaryBuilder),
}

/// Rescale a decimal mantissa from `source_scale` to `target_scale`.
/// E.g. mantissa=1234, source_scale=2 (12.34), target_scale=4 → 12340000? No:
/// 1234 at scale 2 = 12.34, at scale 4 needs mantissa 123400.
fn rescale_decimal(mantissa: i128, source_scale: i8, target_scale: i8) -> i128 {
    let diff = target_scale as i32 - source_scale as i32;
    if diff > 0 {
        mantissa * 10i128.pow(diff as u32)
    } else if diff < 0 {
        mantissa / 10i128.pow((-diff) as u32)
    } else {
        mantissa
    }
}

impl ColumnBuilder {
    fn new_for_type(dt: &DataType) -> Self {
        match dt {
            DataType::Boolean => Self::Boolean(BooleanBuilder::new()),
            DataType::Int8 => Self::Int8(Int8Builder::new()),
            DataType::Int16 => Self::Int16(Int16Builder::new()),
            DataType::Int32 => Self::Int32(Int32Builder::new()),
            DataType::Int64 => Self::Int64(Int64Builder::new()),
            DataType::UInt8 => Self::UInt8(UInt8Builder::new()),
            DataType::UInt16 => Self::UInt16(UInt16Builder::new()),
            DataType::UInt32 => Self::UInt32(UInt32Builder::new()),
            DataType::UInt64 => Self::UInt64(UInt64Builder::new()),
            DataType::Float32 => Self::Float32(Float32Builder::new()),
            DataType::Float64 => Self::Float64(Float64Builder::new()),
            DataType::Decimal128(p, s) => {
                Self::Decimal128 {
                    builder: Decimal128Builder::new().with_precision_and_scale(*p, *s).unwrap(),
                    scale: *s,
                }
            }
            DataType::Date32 => Self::Date32(Date32Builder::new()),
            DataType::Timestamp(TimeUnit::Microsecond, tz) => {
                Self::TimestampMicro(
                    TimestampMicrosecondBuilder::new()
                        .with_timezone(tz.clone().unwrap_or("UTC".into())),
                )
            }
            DataType::Duration(TimeUnit::Microsecond) => {
                Self::DurationMicro(DurationMicrosecondBuilder::new())
            }
            DataType::Utf8 => Self::Utf8(StringBuilder::new()),
            DataType::Binary => Self::Binary(BinaryBuilder::new()),
            other => {
                warn!(data_type = ?other, "Unsupported Arrow type, falling back to Utf8");
                Self::Utf8(StringBuilder::new())
            }
        }
    }

    /// Append a ColumnValue to this builder. On type mismatch, append null.
    fn append(&mut self, value: &ColumnValue, col_name: &str) {
        match (self, value) {
            // --- NULL (any builder) ---
            (builder, ColumnValue::Null) => builder.append_null_untyped(),

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
                let rescaled = rescale_decimal(d.mantissa(), d.scale() as i8, *scale);
                b.append_value(rescaled);
            }
            (Self::Decimal128 { builder: b, scale }, ColumnValue::String(s)) => {
                match s.parse::<rust_decimal::Decimal>() {
                    Ok(d) => {
                        let rescaled = rescale_decimal(d.mantissa(), d.scale() as i8, *scale);
                        b.append_value(rescaled);
                    }
                    Err(_) => {
                        warn!(column = col_name, value = %s, "Failed to parse decimal");
                        b.append_null();
                    }
                }
            }
            (Self::Decimal128 { builder: b, scale }, ColumnValue::Int(v)) => {
                let factor = 10i128.pow(*scale as u32);
                b.append_value(*v as i128 * factor);
            }
            (Self::Decimal128 { builder: b, scale }, ColumnValue::UInt(v)) => {
                let factor = 10i128.pow(*scale as u32);
                b.append_value(*v as i128 * factor);
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
                b.append_value(*micros);
            }
            (Self::DurationMicro(b), ColumnValue::TimeMicros(micros)) => {
                b.append_value(*micros);
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

    fn type_name(&self) -> &'static str {
        match self {
            Self::Boolean(_) => "Boolean",
            Self::Int8(_) => "Int8",
            Self::Int16(_) => "Int16",
            Self::Int32(_) => "Int32",
            Self::Int64(_) => "Int64",
            Self::UInt8(_) => "UInt8",
            Self::UInt16(_) => "UInt16",
            Self::UInt32(_) => "UInt32",
            Self::UInt64(_) => "UInt64",
            Self::Float32(_) => "Float32",
            Self::Float64(_) => "Float64",
            Self::Decimal128 { .. } => "Decimal128",
            Self::Date32(_) => "Date32",
            Self::TimestampMicro(_) => "TimestampMicro",
            Self::DurationMicro(_) => "DurationMicro",
            Self::Utf8(_) => "Utf8",
            Self::Binary(_) => "Binary",
        }
    }

    fn append_null_untyped(&mut self) {
        match self {
            Self::Boolean(b) => b.append_null(),
            Self::Int8(b) => b.append_null(),
            Self::Int16(b) => b.append_null(),
            Self::Int32(b) => b.append_null(),
            Self::Int64(b) => b.append_null(),
            Self::UInt8(b) => b.append_null(),
            Self::UInt16(b) => b.append_null(),
            Self::UInt32(b) => b.append_null(),
            Self::UInt64(b) => b.append_null(),
            Self::Float32(b) => b.append_null(),
            Self::Float64(b) => b.append_null(),
            Self::Decimal128 { builder: b, .. } => b.append_null(),
            Self::Date32(b) => b.append_null(),
            Self::TimestampMicro(b) => b.append_null(),
            Self::DurationMicro(b) => b.append_null(),
            Self::Utf8(b) => b.append_null(),
            Self::Binary(b) => b.append_null(),
        }
    }

    fn finish(&mut self) -> arrow::array::ArrayRef {
        match self {
            Self::Boolean(b) => Arc::new(b.finish()),
            Self::Int8(b) => Arc::new(b.finish()),
            Self::Int16(b) => Arc::new(b.finish()),
            Self::Int32(b) => Arc::new(b.finish()),
            Self::Int64(b) => Arc::new(b.finish()),
            Self::UInt8(b) => Arc::new(b.finish()),
            Self::UInt16(b) => Arc::new(b.finish()),
            Self::UInt32(b) => Arc::new(b.finish()),
            Self::UInt64(b) => Arc::new(b.finish()),
            Self::Float32(b) => Arc::new(b.finish()),
            Self::Float64(b) => Arc::new(b.finish()),
            Self::Decimal128 { builder: b, .. } => Arc::new(b.finish()),
            Self::Date32(b) => Arc::new(b.finish()),
            Self::TimestampMicro(b) => Arc::new(b.finish()),
            Self::DurationMicro(b) => Arc::new(b.finish()),
            Self::Utf8(b) => Arc::new(b.finish()),
            Self::Binary(b) => Arc::new(b.finish()),
        }
    }
}

use std::sync::Arc;

impl TableBuffer {
    pub fn new(table_id: TableId, schema: SchemaRef) -> Self {
        let builders = schema
            .fields()
            .iter()
            .map(|f| ColumnBuilder::new_for_type(f.data_type()))
            .collect();

        Self {
            table_id,
            schema,
            builders,
            row_count: 0,
        }
    }

    /// Append a CdcEvent to the buffer. The event's columns must match the user columns.
    pub fn append(&mut self, event: &CdcEvent) {
        let num_user_cols = self.schema.fields().len() - 3; // minus CDC metadata

        // Append user columns (positional access — both event columns and schema
        // columns are ordered by INFORMATION_SCHEMA.ordinal_position)
        for (i, builder) in self.builders.iter_mut().enumerate().take(num_user_cols) {
            let col_name = self.schema.field(i).name();
            let value = event
                .columns
                .get(i)
                .map(|(_, v)| v)
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

        self.row_count += 1;
    }

    /// Drain the buffer into a RecordBatch and reset.
    pub fn flush(&mut self) -> anyhow::Result<RecordBatch> {
        let arrays: Vec<arrow::array::ArrayRef> =
            self.builders.iter_mut().map(|b| b.finish()).collect();

        let batch = RecordBatch::try_new(self.schema.clone(), arrays)?;

        // Reset builders
        self.builders = self
            .schema
            .fields()
            .iter()
            .map(|f| ColumnBuilder::new_for_type(f.data_type()))
            .collect();

        let count = self.row_count;
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
    schemas: HashMap<TableId, SchemaRef>,
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
            schemas.insert(tid.clone(), table.arrow_schema.clone());
            buffers.insert(
                tid.clone(),
                TableBuffer::new(tid, table.arrow_schema.clone()),
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
    ) -> Option<(TableId, RecordBatch)> {
        info!(table = %tid, "Schema change — flushing old buffer");
        let result = if let Some(buffer) = self.buffers.get_mut(tid) {
            if !buffer.is_empty() {
                match buffer.flush() {
                    Ok(batch) => Some((tid.clone(), batch)),
                    Err(e) => {
                        warn!(table = %tid, error = %e, "Failed to flush during schema change");
                        None
                    }
                }
            } else {
                None
            }
        } else {
            None
        };
        self.buffers.remove(tid);
        self.update_schema(tid, new_schema);
        result
    }

    /// Ingest a single CDC event. Returns a RecordBatch if a flush was triggered.
    pub fn ingest(&mut self, event: &CdcEvent) -> Option<(TableId, RecordBatch)> {
        let tid = &event.table;

        // Ensure buffer exists
        if !self.buffers.contains_key(tid) {
            if let Some(schema) = self.schemas.get(tid) {
                self.buffers
                    .insert(tid.clone(), TableBuffer::new(tid.clone(), schema.clone()));
            } else {
                warn!(table = %tid, "No schema for table, skipping event");
                return None;
            }
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
        self.schemas.insert(tid.clone(), schema);
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
            columns: vec![
                ("id".to_string(), ColumnValue::Int(id)),
                ("name".to_string(), ColumnValue::String(name.to_string())),
                ("amount".to_string(), ColumnValue::Decimal(amount)),
                ("active".to_string(), ColumnValue::Bool(active)),
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
        ]));

        let mut buffer = TableBuffer::new(test_table_id(), schema);

        // Decimal with scale 2 going into scale 4 column
        let d = rust_decimal::Decimal::new(1234, 2); // 12.34
        let event = CdcEvent {
            table: test_table_id(),
            op: CdcOp::Insert,
            timestamp: DateTime::<Utc>::from(std::time::SystemTime::UNIX_EPOCH),
            sequence: 0,
            columns: vec![("val".to_string(), ColumnValue::Decimal(d))],
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
        ]));

        let mut buffer = TableBuffer::new(test_table_id(), schema);
        let event = CdcEvent {
            table: test_table_id(),
            op: CdcOp::Insert,
            timestamp: DateTime::<Utc>::from(std::time::SystemTime::UNIX_EPOCH),
            sequence: 0,
            columns: vec![("val".to_string(), ColumnValue::Int(42))],
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
        builder.append(&ColumnValue::TimeMicros(micros), "test");
        let arr = builder.finish();
        let ints = arr.as_any().downcast_ref::<arrow_array::Int64Array>().unwrap();
        assert_eq!(ints.value(0), micros);
    }

    // --- TableBuffer round-trip ---

    #[test]
    fn table_buffer_flush() {
        let schema = test_schema();
        let mut buffer = TableBuffer::new(test_table_id(), schema.clone());

        let d = rust_decimal::Decimal::new(9999, 2); // 99.99
        buffer.append(&make_event(1, "alice", d, true, 0));
        buffer.append(&make_event(2, "bob", d, false, 1));

        assert_eq!(buffer.len(), 2);
        let batch = buffer.flush().unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 7); // 4 user + 3 cdc
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
                    mysql_type: "int".to_string(),
                    nullable: false,
                    is_unsigned: false,
                },
                crate::source::types::ColumnMeta {
                    name: "name".to_string(),
                    ordinal: 2,
                    mysql_type: "varchar(255)".to_string(),
                    nullable: true,
                    is_unsigned: false,
                },
                crate::source::types::ColumnMeta {
                    name: "amount".to_string(),
                    ordinal: 3,
                    mysql_type: "decimal(10,2)".to_string(),
                    nullable: true,
                    is_unsigned: false,
                },
                crate::source::types::ColumnMeta {
                    name: "active".to_string(),
                    ordinal: 4,
                    mysql_type: "tinyint(1)".to_string(),
                    nullable: true,
                    is_unsigned: false,
                },
            ],
            primary_key_columns: vec!["id".to_string()],
            arrow_schema: test_schema(),
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
}
