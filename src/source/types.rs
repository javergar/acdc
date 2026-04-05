use chrono::{DateTime, Utc};
use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum CdcOp {
    #[serde(rename = "I")]
    Insert,
    #[serde(rename = "U")]
    Update,
    #[serde(rename = "D")]
    Delete,
    #[serde(rename = "S")]
    Snapshot,
}

impl CdcOp {
    pub fn as_str(&self) -> &'static str {
        match self {
            CdcOp::Insert => "I",
            CdcOp::Update => "U",
            CdcOp::Delete => "D",
            CdcOp::Snapshot => "S",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
pub struct TableId {
    pub database: String,
    pub table: String,
}

impl std::fmt::Display for TableId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.database, self.table)
    }
}

/// Structured error for value conversion failures.
#[derive(Debug)]
pub enum ConversionError {
    InvalidUtf8 { column: String },
    InvalidDate { column: String, raw: String },
    ParseError { column: String, target: &'static str, raw: String },
    UnsupportedJsonDiff,
    #[allow(dead_code)]
    Overflow { column: String, value: String },
}

impl std::fmt::Display for ConversionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidUtf8 { column } => write!(f, "Invalid UTF-8 in column '{}'", column),
            Self::InvalidDate { column, raw } => {
                write!(f, "Invalid date '{}' in column '{}'", raw, column)
            }
            Self::ParseError { column, target, raw } => {
                write!(f, "Failed to parse '{}' as {} in column '{}'", raw, target, column)
            }
            Self::UnsupportedJsonDiff => write!(f, "JsonDiff not yet supported"),
            Self::Overflow { column, value } => {
                write!(f, "Overflow converting '{}' in column '{}'", value, column)
            }
        }
    }
}

impl std::error::Error for ConversionError {}

/// Signed microseconds for MySQL TIME values (can be negative: -838:59:59 to 838:59:59).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct TimeMicroseconds(pub i64);

/// A single column value extracted from a binlog row or snapshot query.
#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub enum ColumnValue {
    Null,
    /// Conversion failed — treated as NULL in Arrow, but reason preserved for logging.
    ConversionFailed(String),
    #[allow(dead_code)]
    Bool(bool),
    Int(i64),
    UInt(u64),
    Float(f64),
    String(String),
    Bytes(#[serde(serialize_with = "serialize_bytes")] Vec<u8>),
    Decimal(#[serde(serialize_with = "serialize_decimal")] rust_decimal::Decimal),
    Date(chrono::NaiveDate),
    DateTime(chrono::NaiveDateTime),
    /// TIME as signed microseconds (MySQL TIME can be negative)
    TimeMicros(TimeMicroseconds),
    Json(serde_json::Value),
}

fn serialize_bytes<S: serde::Serializer>(bytes: &[u8], s: S) -> Result<S::Ok, S::Error> {
    use serde::ser::SerializeSeq;
    let mut seq = s.serialize_seq(Some(bytes.len()))?;
    for b in bytes {
        seq.serialize_element(b)?;
    }
    seq.end()
}

fn serialize_decimal<S: serde::Serializer>(
    d: &rust_decimal::Decimal,
    s: S,
) -> Result<S::Ok, S::Error> {
    s.serialize_str(&d.to_string())
}

/// Structured representation of a MySQL column type, parsed once at introspection time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MysqlType {
    TinyintBool,
    Tinyint { unsigned: bool },
    Smallint { unsigned: bool },
    Mediumint { unsigned: bool },
    Int { unsigned: bool },
    Bigint { unsigned: bool },
    Float,
    Double,
    Decimal { precision: u8, scale: i8 },
    Date,
    DateTime,
    Timestamp,
    Time,
    Year,
    Char,
    Varchar,
    Text,
    Enum,
    Set,
    Json,
    Binary,
    Varbinary,
    Blob,
    Bit,
    Spatial,
}

impl MysqlType {
    /// Whether this type is unsigned (only applies to integer types).
    pub fn is_unsigned(&self) -> bool {
        matches!(
            self,
            Self::Tinyint { unsigned: true }
                | Self::Smallint { unsigned: true }
                | Self::Mediumint { unsigned: true }
                | Self::Int { unsigned: true }
                | Self::Bigint { unsigned: true }
        )
    }

    /// Whether this is a binary/blob type (raw bytes, not text).
    #[allow(dead_code)]
    pub fn is_binary(&self) -> bool {
        matches!(
            self,
            Self::Binary | Self::Varbinary | Self::Blob | Self::Bit | Self::Spatial
        )
    }
}

/// Metadata about a single column in a MySQL table.
#[derive(Debug, Clone)]
pub struct ColumnMeta {
    pub name: String,
    #[allow(dead_code)]
    pub ordinal: usize,
    pub col_type: MysqlType,
    /// Raw MySQL type string for logging (e.g. "bigint unsigned", "decimal(10,2)")
    pub mysql_type_raw: String,
    pub nullable: bool,
}

/// A CDC event representing a single row change.
#[derive(Debug, Clone, Serialize)]
pub struct CdcEvent {
    pub table: TableId,
    pub op: CdcOp,
    pub timestamp: DateTime<Utc>,
    pub sequence: i64,
    /// Column values in ordinal order (names come from the table schema, not repeated per row)
    pub values: Vec<ColumnValue>,
}

/// Message sent through the pipeline channel.
#[derive(Debug, Clone)]
pub enum PipelineMessage {
    /// A CDC row change event.
    Event(CdcEvent),
    /// Schema change detected — flush old buffers and adopt new schema.
    SchemaChange {
        table: TableId,
        new_schema: std::sync::Arc<arrow_schema::Schema>,
    },
}
