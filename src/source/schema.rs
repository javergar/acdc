use arrow_schema::{DataType, Field, Schema, TimeUnit};
use mysql_async::prelude::*;
use mysql_async::Pool;
use std::sync::Arc;
use tracing::debug;

use super::types::{ColumnMeta, MysqlType, TableId};
use super::SourceError;

/// Schema metadata for a single MySQL table.
#[derive(Debug, Clone)]
pub struct TableMeta {
    pub table_id: TableId,
    pub columns: Vec<ColumnMeta>,
    pub primary_key_columns: Vec<String>,
    /// Arrow schema including user columns + CDC metadata columns
    pub arrow_schema: Arc<Schema>,
    /// Incremented on each schema change (ALTER TABLE)
    pub schema_version: u64,
}

/// CDC metadata column names appended to every table's Arrow schema.
pub const CDC_OP_COL: &str = "_cdc_op";
pub const CDC_TS_COL: &str = "_cdc_ts";
pub const CDC_SEQ_COL: &str = "_cdc_seq";
pub const CDC_SCHEMA_VERSION_COL: &str = "_cdc_schema_version";

/// Parse a raw MySQL column_type string into a structured MysqlType.
pub fn parse_mysql_type(raw: &str) -> MysqlType {
    let s = raw.trim().to_lowercase();
    let base = mysql_base_type(&s);
    let unsigned = s.contains(" unsigned");

    match base {
        "tinyint" => {
            if s == "tinyint(1)" {
                MysqlType::TinyintBool
            } else {
                MysqlType::Tinyint { unsigned }
            }
        }
        "smallint" => MysqlType::Smallint { unsigned },
        "mediumint" => MysqlType::Mediumint { unsigned },
        "int" | "integer" => MysqlType::Int { unsigned },
        "bigint" => MysqlType::Bigint { unsigned },
        "float" => MysqlType::Float,
        "double" | "real" => MysqlType::Double,
        "decimal" | "numeric" => {
            let (p, sc) = parse_precision_scale(&s);
            MysqlType::Decimal { precision: p, scale: sc }
        }
        "date" => MysqlType::Date,
        "datetime" => MysqlType::DateTime,
        "timestamp" => MysqlType::Timestamp,
        "time" => MysqlType::Time,
        "year" => MysqlType::Year,
        "char" => MysqlType::Char,
        "varchar" => MysqlType::Varchar,
        "text" | "tinytext" | "mediumtext" | "longtext" => MysqlType::Text,
        "enum" => MysqlType::Enum,
        "set" => MysqlType::Set,
        "json" => MysqlType::Json,
        "binary" => MysqlType::Binary,
        "varbinary" => MysqlType::Varbinary,
        "blob" | "tinyblob" | "mediumblob" | "longblob" => MysqlType::Blob,
        "bit" => MysqlType::Bit,
        "point" | "linestring" | "polygon" | "geometry" | "multipoint"
        | "multilinestring" | "multipolygon" | "geomcollection" | "geometrycollection" => {
            MysqlType::Spatial
        }
        _ => {
            tracing::warn!(mysql_type = %raw, "Unknown MySQL type, mapping to Text");
            MysqlType::Text
        }
    }
}

/// Map a structured MysqlType to an Arrow DataType.
pub fn mysql_type_to_arrow(col: &ColumnMeta) -> DataType {
    match &col.col_type {
        MysqlType::TinyintBool => DataType::Boolean,
        MysqlType::Tinyint { unsigned: true } => DataType::Int16,
        MysqlType::Tinyint { unsigned: false } => DataType::Int8,
        MysqlType::Smallint { unsigned: true } => DataType::Int32,
        MysqlType::Smallint { unsigned: false } => DataType::Int16,
        MysqlType::Mediumint { unsigned: true } | MysqlType::Int { unsigned: true } => DataType::Int64,
        MysqlType::Mediumint { unsigned: false } | MysqlType::Int { unsigned: false } => DataType::Int32,
        MysqlType::Bigint { .. } => DataType::Int64,
        MysqlType::Float => DataType::Float32,
        MysqlType::Double => DataType::Float64,
        MysqlType::Decimal { precision, scale } => DataType::Decimal128(*precision, *scale),
        MysqlType::Date => DataType::Date32,
        MysqlType::DateTime | MysqlType::Timestamp => {
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        }
        MysqlType::Time => DataType::Int64,
        MysqlType::Year => DataType::Int16,
        MysqlType::Char | MysqlType::Varchar | MysqlType::Text
        | MysqlType::Enum | MysqlType::Set | MysqlType::Json => DataType::Utf8,
        MysqlType::Binary | MysqlType::Varbinary | MysqlType::Blob
        | MysqlType::Bit | MysqlType::Spatial => DataType::Binary,
    }
}

/// Build an Arrow Schema for a table: user columns + CDC metadata columns.
/// Each field gets a PARQUET_FIELD_ID metadata entry (required for Iceberg schema conversion).
pub fn build_arrow_schema(columns: &[ColumnMeta]) -> Arc<Schema> {
    let mut fields: Vec<Field> = columns
        .iter()
        .enumerate()
        .map(|(i, col)| {
            let dt = mysql_type_to_arrow(col);
            let field_id = (i + 1) as i32; // Iceberg field IDs start at 1
            Field::new(&col.name, dt, col.nullable).with_metadata(
                std::collections::HashMap::from([
                    (parquet::arrow::PARQUET_FIELD_ID_META_KEY.to_string(), field_id.to_string()),
                ]),
            )
        })
        .collect();

    let next_id = (columns.len() + 1) as i32;

    // Append CDC metadata columns (always non-null)
    fields.push(
        Field::new(CDC_OP_COL, DataType::Utf8, false).with_metadata(
            std::collections::HashMap::from([
                (parquet::arrow::PARQUET_FIELD_ID_META_KEY.to_string(), next_id.to_string()),
            ]),
        ),
    );
    fields.push(
        Field::new(
            CDC_TS_COL,
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        )
        .with_metadata(std::collections::HashMap::from([
            (parquet::arrow::PARQUET_FIELD_ID_META_KEY.to_string(), (next_id + 1).to_string()),
        ])),
    );
    fields.push(
        Field::new(CDC_SEQ_COL, DataType::Int64, false).with_metadata(
            std::collections::HashMap::from([
                (parquet::arrow::PARQUET_FIELD_ID_META_KEY.to_string(), (next_id + 2).to_string()),
            ]),
        ),
    );
    fields.push(
        Field::new(CDC_SCHEMA_VERSION_COL, DataType::Int64, false).with_metadata(
            std::collections::HashMap::from([
                (parquet::arrow::PARQUET_FIELD_ID_META_KEY.to_string(), (next_id + 3).to_string()),
            ]),
        ),
    );

    Arc::new(Schema::new(fields))
}

/// Extract base type name before '(' or ' '.
fn mysql_base_type(mysql_type: &str) -> &str {
    let s = mysql_type.trim();
    let end = s.find(|c: char| c == '(' || c == ' ').unwrap_or(s.len());
    &s[..end]
}

/// Parse precision and scale from "decimal(p,s)". Defaults to (10,0).
/// Arrow Decimal128 supports precision up to 38.
fn parse_precision_scale(mysql_type: &str) -> (u8, i8) {
    let (p, s) = parse_precision_scale_inner(mysql_type);
    // Clamp precision to Arrow Decimal128 max (38)
    let p = p.min(38).max(1);
    let s = s.min(p as i8);
    (p, s)
}

fn parse_precision_scale_inner(mysql_type: &str) -> (u8, i8) {
    if let Some(start) = mysql_type.find('(') {
        if let Some(end) = mysql_type.find(')') {
            let inner = &mysql_type[start + 1..end];
            let parts: Vec<&str> = inner.split(',').collect();
            if parts.len() == 2 {
                if let (Ok(p), Ok(s)) = (
                    parts[0].trim().parse::<u8>(),
                    parts[1].trim().parse::<i8>(),
                ) {
                    return (p, s);
                }
            } else if parts.len() == 1 {
                if let Ok(p) = parts[0].trim().parse::<u8>() {
                    return (p, 0);
                }
            }
            tracing::warn!(mysql_type, "Failed to parse decimal precision/scale, using defaults");
        }
    }
    (10, 0)
}

/// Query INFORMATION_SCHEMA to discover the schema of a single table.
pub async fn introspect_table(
    pool: &Pool,
    database: &str,
    table: &str,
) -> Result<TableMeta, SourceError> {
    let mut conn = pool.get_conn().await?;

    // Discover columns
    let columns: Vec<(String, u64, String, String)> = conn
        .exec(
            r"SELECT column_name, ordinal_position, column_type, is_nullable
              FROM information_schema.columns
              WHERE table_schema = ? AND table_name = ?
              ORDER BY ordinal_position",
            (database, table),
        )
        .await?;

    if columns.is_empty() {
        return Err(SourceError::Schema(format!(
            "Table {}.{} not found or has no columns",
            database, table
        )));
    }

    let column_metas: Vec<ColumnMeta> = columns
        .into_iter()
        .map(|(name, ordinal, col_type_raw, nullable)| {
            let col_type = parse_mysql_type(&col_type_raw);
            ColumnMeta {
                name,
                ordinal: ordinal as usize,
                col_type,
                mysql_type_raw: col_type_raw,
                nullable: nullable == "YES",
            }
        })
        .collect();

    // Discover primary key columns
    let pk_columns: Vec<String> = conn
        .exec(
            r"SELECT column_name
              FROM information_schema.key_column_usage
              WHERE table_schema = ? AND table_name = ? AND constraint_name = 'PRIMARY'
              ORDER BY ordinal_position",
            (database, table),
        )
        .await?;

    debug!(
        table = %format!("{}.{}", database, table),
        columns = column_metas.len(),
        pk_columns = ?pk_columns,
        "Introspected table schema"
    );

    let arrow_schema = build_arrow_schema(&column_metas);

    Ok(TableMeta {
        table_id: TableId {
            database: database.to_string(),
            table: table.to_string(),
        },
        columns: column_metas,
        primary_key_columns: pk_columns,
        arrow_schema,
        schema_version: 1,
    })
}

/// Discover all configured tables in the database.
pub async fn introspect_tables(
    pool: &Pool,
    database: &str,
    table_names: &[String],
) -> Result<Vec<TableMeta>, SourceError> {
    let mut tables = Vec::with_capacity(table_names.len());
    for table_name in table_names {
        let meta = introspect_table(pool, database, table_name).await?;
        tables.push(meta);
    }
    Ok(tables)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::types::ColumnMeta;

    fn col(mysql_type_raw: &str) -> ColumnMeta {
        ColumnMeta {
            name: "test".to_string(),
            ordinal: 0,
            col_type: parse_mysql_type(mysql_type_raw),
            mysql_type_raw: mysql_type_raw.to_string(),
            nullable: true,
        }
    }

    #[test]
    fn tinyint1_is_boolean() {
        assert_eq!(mysql_type_to_arrow(&col("tinyint(1)")), DataType::Boolean);
    }

    #[test]
    fn tinyint_signed() {
        assert_eq!(mysql_type_to_arrow(&col("tinyint")), DataType::Int8);
    }

    #[test]
    fn tinyint_unsigned_widens() {
        assert_eq!(mysql_type_to_arrow(&col("tinyint unsigned")), DataType::Int16);
    }

    #[test]
    fn smallint_unsigned_widens() {
        assert_eq!(mysql_type_to_arrow(&col("smallint unsigned")), DataType::Int32);
    }

    #[test]
    fn int_unsigned_widens() {
        assert_eq!(mysql_type_to_arrow(&col("int unsigned")), DataType::Int64);
    }

    #[test]
    fn bigint_always_int64() {
        assert_eq!(mysql_type_to_arrow(&col("bigint")), DataType::Int64);
        assert_eq!(mysql_type_to_arrow(&col("bigint unsigned")), DataType::Int64);
    }

    #[test]
    fn decimal_with_precision() {
        assert_eq!(
            mysql_type_to_arrow(&col("decimal(10,2)")),
            DataType::Decimal128(10, 2)
        );
    }

    #[test]
    fn decimal_clamped_precision() {
        // MySQL allows DECIMAL(65,30), Arrow caps at 38
        assert_eq!(
            mysql_type_to_arrow(&col("decimal(65,30)")),
            DataType::Decimal128(38, 30)
        );
    }

    #[test]
    fn time_is_int64() {
        assert_eq!(mysql_type_to_arrow(&col("time")), DataType::Int64);
    }

    #[test]
    fn json_is_utf8() {
        assert_eq!(mysql_type_to_arrow(&col("json")), DataType::Utf8);
    }

    #[test]
    fn year_is_int16() {
        assert_eq!(mysql_type_to_arrow(&col("year")), DataType::Int16);
    }

    #[test]
    fn datetime_is_timestamp() {
        assert_eq!(
            mysql_type_to_arrow(&col("datetime")),
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
    }

    #[test]
    fn blob_is_binary() {
        assert_eq!(mysql_type_to_arrow(&col("blob")), DataType::Binary);
    }

    #[test]
    fn enum_is_utf8() {
        assert_eq!(mysql_type_to_arrow(&col("enum")), DataType::Utf8);
    }

    // --- parse_precision_scale ---

    #[test]
    fn parse_ps_normal() {
        assert_eq!(parse_precision_scale("decimal(10,2)"), (10, 2));
    }

    #[test]
    fn parse_ps_clamped() {
        let (p, s) = parse_precision_scale("decimal(65,30)");
        assert_eq!(p, 38);
        assert_eq!(s, 30);
    }

    #[test]
    fn parse_ps_no_parens() {
        assert_eq!(parse_precision_scale("decimal"), (10, 0));
    }

    #[test]
    fn parse_ps_precision_only() {
        assert_eq!(parse_precision_scale("numeric(5)"), (5, 0));
    }

    #[test]
    fn parse_ps_scale_clamped_to_precision() {
        // scale can't exceed precision
        let (p, s) = parse_precision_scale("decimal(5,10)");
        assert_eq!(p, 5);
        assert_eq!(s, 5); // clamped
    }
}
