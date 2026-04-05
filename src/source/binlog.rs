use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use futures::StreamExt;
use mysql_async::binlog::events::EventData;
use mysql_async::binlog::jsonb;
use mysql_async::binlog::row::BinlogRow;
use mysql_async::binlog::value::BinlogValue;
use mysql_async::binlog::EventFlags;
use mysql_async::prelude::*;
use mysql_async::Pool;
use tracing::{debug, trace, warn};

use super::schema::TableMeta;
use super::types::{CdcEvent, CdcOp, ColumnMeta, ColumnValue, PipelineMessage, TableId};
use super::SourceError;
use crate::config::SourceConfig;

/// Tracks the current binlog position for offset persistence.
#[derive(Debug, Clone)]
pub struct BinlogPosition {
    pub filename: String,
    pub position: u64,
}

/// The main binlog stream reader. Connects to MySQL, tails the binlog,
/// and emits CdcEvent values through a channel.
pub struct BinlogSource {
    pool: Pool,
    config: SourceConfig,
    /// binlog table_id → TableMeta (populated from TableMapEvent + introspection)
    table_map: HashMap<u64, TableMeta>,
    /// Monotonically increasing sequence counter
    sequence: Arc<AtomicI64>,
    /// Tables we're interested in (from config)
    tracked_tables: HashMap<String, TableMeta>,
    /// Shared current position — read by main for periodic offset saves
    shared_position: Arc<std::sync::Mutex<BinlogPosition>>,
}

impl BinlogSource {
    pub async fn new(
        pool: Pool,
        config: SourceConfig,
        tables: Vec<TableMeta>,
        sequence: Arc<AtomicI64>,
        shared_position: Arc<std::sync::Mutex<BinlogPosition>>,
    ) -> Result<Self, SourceError> {
        let tracked_tables: HashMap<String, TableMeta> = tables
            .into_iter()
            .map(|t| (t.table_id.table.clone(), t))
            .collect();

        Ok(Self {
            pool,
            config,
            table_map: HashMap::new(),
            sequence,
            tracked_tables,
            shared_position,
        })
    }

    /// Start streaming binlog events. Sends CdcEvents through the channel.
    /// Runs until shutdown is signaled or the channel is closed.
    /// Start streaming binlog events from a given position (or current master position).
    pub async fn stream(
        &mut self,
        tx: tokio::sync::mpsc::Sender<PipelineMessage>,
        shutdown: Arc<AtomicBool>,
        start_from: Option<BinlogPosition>,
    ) -> Result<BinlogPosition, SourceError> {
        let (filename, position) = match start_from {
            Some(pos) => {
                debug!(filename = %pos.filename, position = pos.position, "Resuming binlog from saved position");
                (pos.filename, pos.position)
            }
            None => {
                let mut conn = self.pool.get_conn().await?;
                let pos = get_master_status(&mut conn).await?;
                debug!(filename = %pos.0, position = pos.1, "Starting binlog from current position");
                pos
            }
        };

        // Open binlog stream
        let conn = self.pool.get_conn().await?;
        let request = mysql_async::BinlogStreamRequest::new(self.config.server_id)
            .with_filename(filename.as_bytes())
            .with_pos(position);
        let mut binlog_stream = conn.get_binlog_stream(request).await?;

        let mut current_position = BinlogPosition {
            filename: filename.clone(),
            position,
        };

        while let Some(result) = binlog_stream.next().await {
            if shutdown.load(Ordering::Relaxed) {
                debug!("Shutdown signal received, stopping binlog reader");
                break;
            }

            let event = match result {
                Ok(e) => e,
                Err(e) => {
                    warn!(error = %e, "Binlog read error");
                    return Err(SourceError::Connection(e));
                }
            };

            // Skip artificial events
            if event
                .header()
                .flags()
                .contains(EventFlags::LOG_EVENT_ARTIFICIAL_F)
            {
                continue;
            }

            // Update position
            let log_pos: u64 = event.header().log_pos().into();
            if log_pos > 0 {
                current_position.position = log_pos;
                if let Ok(mut sp) = self.shared_position.lock() {
                    sp.position = log_pos;
                }
            }

            let event_type = match event.header().event_type() {
                Ok(et) => et,
                Err(_) => continue,
            };

            use mysql_async::binlog::EventType::*;

            match event_type {
                ROTATE_EVENT => {
                    if let Ok(Some(EventData::RotateEvent(rotate))) = event.read_data() {
                        let new_filename = rotate.name().to_string();
                        let new_position = rotate.position();
                        debug!(
                            filename = %new_filename,
                            position = new_position,
                            "Binlog rotation"
                        );
                        current_position.filename = new_filename.clone();
                        current_position.position = new_position;
                        if let Ok(mut sp) = self.shared_position.lock() {
                            sp.filename = new_filename;
                            sp.position = new_position;
                        }
                        // Clear table_id cache — IDs are not stable across rotations
                        self.table_map.clear();
                    }
                }

                TABLE_MAP_EVENT => {
                    if let Ok(Some(EventData::TableMapEvent(tme))) = event.read_data() {
                        let db = tme.database_name().to_string();
                        let table = tme.table_name().to_string();
                        let table_id = tme.table_id();

                        // Only cache if this is a tracked table
                        if self.tracked_tables.contains_key(&table) {
                            if let Some(meta) = self.tracked_tables.get(&table) {
                                if meta.table_id.database == db {
                                    self.table_map.insert(table_id, meta.clone());
                                }
                            }
                        }

                        trace!(
                            table_id = table_id,
                            table = %format!("{}.{}", db, table),
                            "TableMapEvent"
                        );
                    }
                }

                WRITE_ROWS_EVENT | WRITE_ROWS_EVENT_V1 | UPDATE_ROWS_EVENT
                | UPDATE_ROWS_EVENT_V1 | DELETE_ROWS_EVENT | DELETE_ROWS_EVENT_V1 => {
                    let op = match event_type {
                        WRITE_ROWS_EVENT | WRITE_ROWS_EVENT_V1 => CdcOp::Insert,
                        UPDATE_ROWS_EVENT | UPDATE_ROWS_EVENT_V1 => CdcOp::Update,
                        DELETE_ROWS_EVENT | DELETE_ROWS_EVENT_V1 => CdcOp::Delete,
                        _ => unreachable!(),
                    };

                    let event_data = match event.read_data() {
                        Ok(Some(EventData::RowsEvent(data))) => data,
                        Ok(_) => continue,
                        Err(e) => {
                            warn!(error = %e, "Failed to parse rows event");
                            continue;
                        }
                    };

                    let binlog_table_id = event_data.table_id();

                    // Look up the table metadata from TableMapEvent cache
                    let table_meta = match self.table_map.get(&binlog_table_id) {
                        Some(meta) => meta.clone(),
                        None => {
                            // Not a tracked table, skip
                            trace!(table_id = binlog_table_id, "Skipping untracked table");
                            continue;
                        }
                    };

                    let event_timestamp = DateTime::from_timestamp(
                        event.header().timestamp() as i64,
                        0,
                    )
                    .unwrap_or_else(Utc::now);

                    // Get the TME for row iteration
                    let tme = match binlog_stream.get_tme(binlog_table_id) {
                        Some(tme) => tme,
                        None => {
                            warn!(
                                table_id = binlog_table_id,
                                "Missing TableMapEvent for rows event"
                            );
                            continue;
                        }
                    };

                    let rows = event_data.rows(tme);
                    for row_result in rows {
                        let row = match row_result {
                            Ok(r) => r,
                            Err(e) => {
                                warn!(error = %e, table = %table_meta.table_id, "Row parse error");
                                continue;
                            }
                        };

                        // For INSERT/UPDATE: use after-image (row.1)
                        // For DELETE: use before-image (row.0)
                        let binlog_row = match op {
                            CdcOp::Delete => row.0,
                            _ => row.1,
                        };

                        let binlog_row = match binlog_row {
                            Some(r) => r,
                            None => continue,
                        };

                        let seq = self.sequence.fetch_add(1, Ordering::Relaxed);

                        let columns =
                            convert_binlog_row(binlog_row, &table_meta.columns, &table_meta.table_id);

                        let cdc_event = CdcEvent {
                            table: table_meta.table_id.clone(),
                            op,
                            timestamp: event_timestamp,
                            sequence: seq,
                            columns,
                        };

                        if tx.send(PipelineMessage::Event(cdc_event)).await.is_err() {
                            debug!("Channel closed, stopping binlog reader");
                            return Ok(current_position);
                        }
                    }
                }

                QUERY_EVENT => {
                    if let Ok(Some(EventData::QueryEvent(query))) = event.read_data() {
                        let sql = query.query();
                        let sql_upper = sql.to_uppercase();

                        if sql_upper.contains("ALTER TABLE") {
                            tracing::info!(sql = %sql, "ALTER TABLE detected, evolving schema");

                            if let Some(table_name) = parse_alter_table_name(&sql_upper, &self.config.database) {
                                if self.tracked_tables.contains_key(&table_name) {
                                    match super::schema::introspect_table(
                                        &self.pool, &self.config.database, &table_name
                                    ).await {
                                        Ok(new_meta) => {
                                            tracing::info!(
                                                table = %table_name,
                                                columns = new_meta.columns.len(),
                                                "Schema re-introspected after ALTER TABLE"
                                            );
                                            self.tracked_tables.insert(table_name.clone(), new_meta.clone());
                                            self.table_map.retain(|_, meta| meta.table_id.table != table_name);

                                            // Send typed schema change signal
                                            let msg = PipelineMessage::SchemaChange {
                                                table: new_meta.table_id.clone(),
                                                new_schema: new_meta.arrow_schema.clone(),
                                            };
                                            if tx.send(msg).await.is_err() {
                                                return Ok(current_position);
                                            }
                                        }
                                        Err(e) => {
                                            warn!(table = %table_name, error = %e, "Failed to re-introspect after ALTER TABLE");
                                        }
                                    }
                                }
                            } else {
                                warn!(sql = %sql, "Could not parse table name from ALTER TABLE");
                            }
                        } else if sql_upper.contains("DROP TABLE") || sql_upper.contains("CREATE TABLE") {
                            warn!(sql = %sql, "DDL detected (non-ALTER)");
                        }
                    }
                }

                other => {
                    trace!(event_type = ?other, "Unhandled binlog event type");
                }
            }
        }

        Ok(current_position)
    }
}

/// Convert a BinlogRow into a vector of (column_name, ColumnValue) pairs.
fn convert_binlog_row(
    mut row: BinlogRow,
    columns: &[ColumnMeta],
    table_id: &TableId,
) -> Vec<(String, ColumnValue)> {
    let mut result = Vec::with_capacity(columns.len());

    for (i, col) in columns.iter().enumerate() {
        let value = row.take(i);
        let converted = match convert_binlog_value(value, col) {
            Ok(v) => v,
            Err(e) => {
                warn!(
                    table = %table_id,
                    column = %col.name,
                    error = %e,
                    "Column conversion error, writing NULL"
                );
                ColumnValue::Null
            }
        };
        result.push((col.name.clone(), converted));
    }

    result
}

/// Convert a single BinlogValue to our ColumnValue type.
fn convert_binlog_value(
    value: Option<BinlogValue<'_>>,
    col: &ColumnMeta,
) -> Result<ColumnValue, String> {
    let bv = match value {
        None => return Ok(ColumnValue::Null),
        Some(v) => v,
    };

    match bv {
        BinlogValue::Value(v) => convert_mysql_value(v, col),
        BinlogValue::Jsonb(v) => {
            let json = jsonb_to_serde_json(v)?;
            Ok(ColumnValue::Json(json))
        }
        BinlogValue::JsonDiff(_) => {
            // JsonDiff is a partial update — rare in practice.
            // Log a warning and return null for now.
            Err("JsonDiff not yet supported".to_string())
        }
    }
}

/// Convert a mysql_common::Value to our ColumnValue.
pub fn convert_mysql_value(
    value: mysql_common::value::Value,
    col: &ColumnMeta,
) -> Result<ColumnValue, String> {
    use mysql_common::value::Value;

    match value {
        Value::NULL => Ok(ColumnValue::Null),

        Value::Bytes(bytes) => {
            let base_type = mysql_base_type(&col.mysql_type);
            match base_type {
                "binary" | "varbinary" | "tinyblob" | "blob" | "mediumblob" | "longblob"
                | "bit" | "point" | "linestring" | "polygon" | "geometry" | "multipoint"
                | "multilinestring" | "multipolygon" | "geomcollection"
                | "geometrycollection" => Ok(ColumnValue::Bytes(bytes)),
                "json" => {
                    let s = String::from_utf8(bytes).map_err(|e| e.to_string())?;
                    let json: serde_json::Value =
                        serde_json::from_str(&s).map_err(|e| e.to_string())?;
                    Ok(ColumnValue::Json(json))
                }
                "decimal" | "numeric" => {
                    let s = String::from_utf8(bytes).map_err(|e| e.to_string())?;
                    let d: rust_decimal::Decimal =
                        s.parse().map_err(|e: rust_decimal::Error| e.to_string())?;
                    Ok(ColumnValue::Decimal(d))
                }
                // Integer types arriving as Bytes (common from SELECT queries)
                "tinyint" | "smallint" | "mediumint" | "int" | "integer" | "bigint" => {
                    let s = String::from_utf8(bytes).map_err(|e| e.to_string())?;
                    if col.is_unsigned {
                        let v: u64 = s.parse().map_err(|e: std::num::ParseIntError| e.to_string())?;
                        Ok(ColumnValue::UInt(v))
                    } else {
                        let v: i64 = s.parse().map_err(|e: std::num::ParseIntError| e.to_string())?;
                        Ok(ColumnValue::Int(v))
                    }
                }
                "float" | "double" | "real" => {
                    let s = String::from_utf8(bytes).map_err(|e| e.to_string())?;
                    let v: f64 = s.parse().map_err(|e: std::num::ParseFloatError| e.to_string())?;
                    Ok(ColumnValue::Float(v))
                }
                "year" => {
                    let s = String::from_utf8(bytes).map_err(|e| e.to_string())?;
                    let v: i64 = s.parse().map_err(|e: std::num::ParseIntError| e.to_string())?;
                    Ok(ColumnValue::Int(v))
                }
                _ => {
                    // String-like types (varchar, text, char, enum, set, etc.)
                    match String::from_utf8(bytes) {
                        Ok(s) => Ok(ColumnValue::String(s)),
                        Err(e) => {
                            warn!(
                                column = %col.name,
                                mysql_type = %col.mysql_type,
                                "Non-UTF-8 bytes in string column, storing as binary"
                            );
                            Ok(ColumnValue::Bytes(e.into_bytes()))
                        }
                    }
                }
            }
        }

        Value::Int(i) => {
            if col.is_unsigned {
                // Value::Int stores i64; reinterpret as unsigned
                Ok(ColumnValue::UInt(i as u64))
            } else {
                Ok(ColumnValue::Int(i))
            }
        }

        Value::UInt(u) => {
            // BIGINT UNSIGNED: values > i64::MAX will be truncated when stored in Int64 Arrow.
            // Log a warning for values that exceed i64::MAX.
            if u > i64::MAX as u64 {
                warn!(
                    column = %col.name,
                    value = u,
                    "BIGINT UNSIGNED value exceeds i64::MAX, will be stored as negative in Int64"
                );
            }
            Ok(ColumnValue::UInt(u))
        }

        Value::Float(f) => Ok(ColumnValue::Float(f as f64)),
        Value::Double(f) => Ok(ColumnValue::Float(f)),

        Value::Date(year, month, day, hour, min, sec, micro) => {
            // Handle zero dates: MySQL allows 0000-00-00 which is invalid in chrono
            if year == 0 && month == 0 && day == 0 {
                return Ok(ColumnValue::Null);
            }

            let base_type = mysql_base_type(&col.mysql_type);
            match base_type {
                "date" => {
                    match chrono::NaiveDate::from_ymd_opt(year as i32, month as u32, day as u32) {
                        Some(date) => Ok(ColumnValue::Date(date)),
                        None => {
                            // Invalid date (e.g., Feb 30) → NULL
                            Err(format!("Invalid date: {}-{:02}-{:02}", year, month, day))
                        }
                    }
                }
                _ => {
                    // datetime, timestamp, or unknown date-like type
                    match chrono::NaiveDate::from_ymd_opt(year as i32, month as u32, day as u32) {
                        Some(date) => {
                            let time = chrono::NaiveTime::from_hms_micro_opt(
                                hour as u32,
                                min as u32,
                                sec as u32,
                                micro,
                            )
                            .unwrap_or_default();
                            Ok(ColumnValue::DateTime(chrono::NaiveDateTime::new(date, time)))
                        }
                        None => {
                            // Invalid date → NULL
                            Err(format!(
                                "Invalid datetime: {}-{:02}-{:02} {:02}:{:02}:{:02}",
                                year, month, day, hour, min, sec
                            ))
                        }
                    }
                }
            }
        }

        Value::Time(is_neg, days, hours, mins, secs, micros) => {
            // MySQL TIME range: -838:59:59.000000 to 838:59:59.000000
            // Arrow Duration(Microsecond) is signed i64, so we CAN represent negative values.
            let total_micros = ((days as i64) * 86_400 + (hours as i64) * 3_600
                + (mins as i64) * 60
                + (secs as i64))
                * 1_000_000
                + (micros as i64);
            let signed_micros = if is_neg { -total_micros } else { total_micros };
            Ok(ColumnValue::TimeMicros(signed_micros))
        }
    }
}

/// Extract the base MySQL type name (before parentheses or "unsigned").
fn mysql_base_type(mysql_type: &str) -> &str {
    let s = mysql_type.trim();
    // Find first '(' or ' '
    let end = s
        .find(|c: char| c == '(' || c == ' ')
        .unwrap_or(s.len());
    &s[..end]
}

/// Recursively convert mysql_common jsonb::Value to serde_json::Value.
fn jsonb_to_serde_json(value: jsonb::Value<'_>) -> Result<serde_json::Value, String> {
    use jsonb::Value::*;
    match value {
        Null => Ok(serde_json::Value::Null),
        Bool(b) => Ok(serde_json::Value::Bool(b)),
        I16(v) => Ok(serde_json::json!(v)),
        U16(v) => Ok(serde_json::json!(v)),
        I32(v) => Ok(serde_json::json!(v)),
        U32(v) => Ok(serde_json::json!(v)),
        I64(v) => Ok(serde_json::json!(v)),
        U64(v) => Ok(serde_json::json!(v)),
        F64(v) => Ok(serde_json::json!(v)),
        String(v) => Ok(serde_json::Value::String(v.str().to_string())),
        SmallArray(arr) => {
            let items: Result<Vec<serde_json::Value>, std::string::String> = arr
                .iter()
                .map(|item| {
                    let v = item.map_err(|e| format!("JSON array parse error: {e}"))?;
                    jsonb_to_serde_json(v)
                })
                .collect();
            Ok(serde_json::Value::Array(items?))
        }
        LargeArray(arr) => {
            let items: Result<Vec<serde_json::Value>, std::string::String> = arr
                .iter()
                .map(|item| {
                    let v = item.map_err(|e| format!("JSON array parse error: {e}"))?;
                    jsonb_to_serde_json(v)
                })
                .collect();
            Ok(serde_json::Value::Array(items?))
        }
        SmallObject(obj) => jsonb_object_to_serde_json(obj),
        LargeObject(obj) => jsonb_object_to_serde_json(obj),
        Opaque(v) => {
            // Opaque values: encode as an object with type and data
            Ok(serde_json::json!({
                "value_type": v.value_type() as u8,
                "data": format!("{:?}", v.data()),
            }))
        }
    }
}

/// Convert a JSONB object (SmallObject or LargeObject) to serde_json::Value.
fn jsonb_object_to_serde_json<T: jsonb::StorageFormat>(
    obj: jsonb::ComplexValue<'_, T, jsonb::Object>,
) -> Result<serde_json::Value, String> {
    let mut map = serde_json::Map::new();
    for entry in obj.iter() {
        let (key, value) = entry.map_err(|e| format!("JSON object parse error: {e}"))?;
        let key_str = key.value().into_owned();
        let json_value = jsonb_to_serde_json(value)?;
        map.insert(key_str, json_value);
    }
    Ok(serde_json::Value::Object(map))
}

/// Get the current master binlog position.
pub async fn get_master_status(conn: &mut mysql_async::Conn) -> Result<(String, u64), SourceError> {
    let row: Option<mysql_async::Row> = conn
        .query_first("SHOW MASTER STATUS")
        .await?;

    let row = row.ok_or_else(|| {
        SourceError::BinlogParse("SHOW MASTER STATUS returned no rows — is binlog enabled?".into())
    })?;

    let filename: String = row.get(0).ok_or_else(|| {
        SourceError::BinlogParse("Missing filename in SHOW MASTER STATUS".into())
    })?;

    let position: u64 = row.get(1).ok_or_else(|| {
        SourceError::BinlogParse("Missing position in SHOW MASTER STATUS".into())
    })?;

    Ok((filename, position))
}

/// Parse the table name from an ALTER TABLE SQL statement (uppercased).
/// Handles: ALTER TABLE tablename, ALTER TABLE `tablename`, ALTER TABLE db.tablename
fn parse_alter_table_name(sql_upper: &str, default_db: &str) -> Option<String> {
    let idx = sql_upper.find("ALTER TABLE")?;
    let after = &sql_upper[idx + "ALTER TABLE".len()..];
    let trimmed = after.trim_start();

    // Extract the identifier (possibly backtick-quoted, possibly db.table)
    let ident = trimmed
        .split(|c: char| c.is_whitespace() || c == '(')
        .next()?;

    if ident.is_empty() {
        return None;
    }

    // Remove backticks
    let clean = ident.replace('`', "");

    // Handle db.table format
    let table_name = if let Some((_db, table)) = clean.split_once('.') {
        table.to_lowercase()
    } else {
        clean.to_lowercase()
    };

    if table_name.is_empty() {
        None
    } else {
        Some(table_name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::types::ColumnMeta;

    fn col(name: &str, mysql_type: &str, unsigned: bool) -> ColumnMeta {
        ColumnMeta {
            name: name.to_string(),
            ordinal: 0,
            mysql_type: mysql_type.to_string(),
            nullable: true,
            is_unsigned: unsigned,
        }
    }

    // --- parse_alter_table_name ---

    #[test]
    fn parse_alter_plain() {
        assert_eq!(
            parse_alter_table_name("ALTER TABLE ORDERS ADD COLUMN FOO INT", "testdb"),
            Some("orders".to_string())
        );
    }

    #[test]
    fn parse_alter_backtick() {
        assert_eq!(
            parse_alter_table_name("ALTER TABLE `ORDERS` ADD COLUMN FOO INT", "testdb"),
            Some("orders".to_string())
        );
    }

    #[test]
    fn parse_alter_db_dot_table() {
        assert_eq!(
            parse_alter_table_name("ALTER TABLE TESTDB.ORDERS ADD COLUMN FOO INT", "testdb"),
            Some("orders".to_string())
        );
    }

    #[test]
    fn parse_alter_db_dot_backtick_table() {
        assert_eq!(
            parse_alter_table_name("ALTER TABLE `TESTDB`.`ORDERS` ADD COLUMN FOO INT", "testdb"),
            Some("orders".to_string())
        );
    }

    #[test]
    fn parse_alter_empty() {
        assert_eq!(parse_alter_table_name("SELECT 1", "testdb"), None);
    }

    // --- mysql_base_type ---

    #[test]
    fn base_type_simple() {
        assert_eq!(mysql_base_type("int"), "int");
    }

    #[test]
    fn base_type_with_parens() {
        assert_eq!(mysql_base_type("varchar(255)"), "varchar");
    }

    #[test]
    fn base_type_unsigned() {
        assert_eq!(mysql_base_type("bigint unsigned"), "bigint");
    }

    #[test]
    fn base_type_decimal() {
        assert_eq!(mysql_base_type("decimal(10,2)"), "decimal");
    }

    // --- convert_mysql_value ---

    #[test]
    fn convert_null() {
        let c = col("id", "int", false);
        let result = convert_mysql_value(mysql_common::value::Value::NULL, &c).unwrap();
        assert!(matches!(result, ColumnValue::Null));
    }

    #[test]
    fn convert_int_signed() {
        let c = col("id", "int", false);
        let result = convert_mysql_value(mysql_common::value::Value::Int(-42), &c).unwrap();
        assert!(matches!(result, ColumnValue::Int(-42)));
    }

    #[test]
    fn convert_int_unsigned_flag() {
        let c = col("id", "int unsigned", true);
        let result = convert_mysql_value(mysql_common::value::Value::Int(42), &c).unwrap();
        assert!(matches!(result, ColumnValue::UInt(42)));
    }

    #[test]
    fn convert_uint_large() {
        let c = col("id", "bigint unsigned", true);
        let big = u64::MAX;
        let result = convert_mysql_value(mysql_common::value::Value::UInt(big), &c).unwrap();
        match result {
            ColumnValue::UInt(v) => assert_eq!(v, big),
            _ => panic!("Expected UInt"),
        }
    }

    #[test]
    fn convert_bytes_integer() {
        let c = col("id", "int", false);
        let result =
            convert_mysql_value(mysql_common::value::Value::Bytes(b"12345".to_vec()), &c)
                .unwrap();
        assert!(matches!(result, ColumnValue::Int(12345)));
    }

    #[test]
    fn convert_bytes_decimal() {
        let c = col("amount", "decimal(10,2)", false);
        let result =
            convert_mysql_value(mysql_common::value::Value::Bytes(b"123.45".to_vec()), &c)
                .unwrap();
        match result {
            ColumnValue::Decimal(d) => assert_eq!(d.to_string(), "123.45"),
            _ => panic!("Expected Decimal, got {:?}", result),
        }
    }

    #[test]
    fn convert_bytes_json() {
        let c = col("data", "json", false);
        let result = convert_mysql_value(
            mysql_common::value::Value::Bytes(br#"{"a":1}"#.to_vec()),
            &c,
        )
        .unwrap();
        match result {
            ColumnValue::Json(v) => assert_eq!(v["a"], 1),
            _ => panic!("Expected Json"),
        }
    }

    #[test]
    fn convert_zero_date() {
        let c = col("d", "date", false);
        let result =
            convert_mysql_value(mysql_common::value::Value::Date(0, 0, 0, 0, 0, 0, 0), &c)
                .unwrap();
        assert!(matches!(result, ColumnValue::Null));
    }

    #[test]
    fn convert_valid_date() {
        let c = col("d", "date", false);
        let result =
            convert_mysql_value(mysql_common::value::Value::Date(2024, 3, 15, 0, 0, 0, 0), &c)
                .unwrap();
        match result {
            ColumnValue::Date(d) => {
                assert_eq!(d.to_string(), "2024-03-15");
            }
            _ => panic!("Expected Date"),
        }
    }

    #[test]
    fn convert_datetime() {
        let c = col("ts", "datetime", false);
        let result = convert_mysql_value(
            mysql_common::value::Value::Date(2024, 6, 15, 10, 30, 45, 123456),
            &c,
        )
        .unwrap();
        match result {
            ColumnValue::DateTime(dt) => {
                assert_eq!(dt.to_string(), "2024-06-15 10:30:45.123456");
            }
            _ => panic!("Expected DateTime"),
        }
    }

    #[test]
    fn convert_negative_time() {
        let c = col("t", "time", false);
        // -01:30:00 = negative, 0 days, 1 hour, 30 min, 0 sec, 0 micro
        let result =
            convert_mysql_value(mysql_common::value::Value::Time(true, 0, 1, 30, 0, 0), &c)
                .unwrap();
        match result {
            ColumnValue::TimeMicros(us) => {
                assert_eq!(us, -(90 * 60 * 1_000_000)); // -1h30m in micros
            }
            _ => panic!("Expected TimeMicros"),
        }
    }

    #[test]
    fn convert_year_bytes() {
        let c = col("y", "year", false);
        let result =
            convert_mysql_value(mysql_common::value::Value::Bytes(b"2024".to_vec()), &c)
                .unwrap();
        assert!(matches!(result, ColumnValue::Int(2024)));
    }

    #[test]
    fn convert_bytes_float() {
        let c = col("f", "double", false);
        let result =
            convert_mysql_value(mysql_common::value::Value::Bytes(b"3.14".to_vec()), &c)
                .unwrap();
        match result {
            ColumnValue::Float(v) => assert!((v - 3.14).abs() < 0.001),
            _ => panic!("Expected Float"),
        }
    }

    #[test]
    fn convert_binary_bytes() {
        let c = col("b", "blob", false);
        let data = vec![0xDE, 0xAD, 0xBE, 0xEF];
        let result =
            convert_mysql_value(mysql_common::value::Value::Bytes(data.clone()), &c).unwrap();
        match result {
            ColumnValue::Bytes(v) => assert_eq!(v, data),
            _ => panic!("Expected Bytes"),
        }
    }
}
