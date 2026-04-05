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

/// A single column value extracted from a binlog row or snapshot query.
#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub enum ColumnValue {
    Null,
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
    TimeMicros(i64),
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

/// Metadata about a single column in a MySQL table.
#[derive(Debug, Clone)]
pub struct ColumnMeta {
    pub name: String,
    pub ordinal: usize,
    pub mysql_type: String,
    pub nullable: bool,
    pub is_unsigned: bool,
}

/// A CDC event representing a single row change.
#[derive(Debug, Clone, Serialize)]
pub struct CdcEvent {
    pub table: TableId,
    pub op: CdcOp,
    pub timestamp: DateTime<Utc>,
    pub sequence: i64,
    /// Column name → value pairs (after-image for INSERT/UPDATE, full row for DELETE)
    pub columns: Vec<(String, ColumnValue)>,
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
