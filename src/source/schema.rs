use arrow_schema::{DataType, Field, Schema, TimeUnit};
use mysql_async::prelude::*;
use mysql_async::Pool;
use std::sync::Arc;
use tracing::debug;

use super::types::{ColumnMeta, TableId};
use super::SourceError;

/// Schema metadata for a single MySQL table.
#[derive(Debug, Clone)]
pub struct TableMeta {
    pub table_id: TableId,
    pub columns: Vec<ColumnMeta>,
    pub primary_key_columns: Vec<String>,
    /// Arrow schema including user columns + CDC metadata columns
    pub arrow_schema: Arc<Schema>,
}

/// CDC metadata column names appended to every table's Arrow schema.
pub const CDC_OP_COL: &str = "_cdc_op";
pub const CDC_TS_COL: &str = "_cdc_ts";
pub const CDC_SEQ_COL: &str = "_cdc_seq";

/// Map a MySQL column_type string to an Arrow DataType.
///
/// The column_type comes from INFORMATION_SCHEMA.COLUMNS (e.g. "int", "varchar(255)",
/// "bigint unsigned", "decimal(10,2)").
pub fn mysql_type_to_arrow(col: &ColumnMeta) -> DataType {
    let base = mysql_base_type(&col.mysql_type);
    match base {
        // Iceberg doesn't support unsigned types. Widen to next signed type.
        // tinyint(1) = BOOLEAN, otherwise signed fits in same type, unsigned widens.
        "tinyint" => {
            if col.mysql_type == "tinyint(1)" {
                DataType::Boolean
            } else if col.is_unsigned {
                DataType::Int16 // UInt8 max 255 fits in Int16
            } else {
                DataType::Int8
            }
        }
        "smallint" => {
            if col.is_unsigned {
                DataType::Int32 // UInt16 max 65535 fits in Int32
            } else {
                DataType::Int16
            }
        }
        "mediumint" | "int" | "integer" => {
            if col.is_unsigned {
                DataType::Int64 // UInt32 max ~4B fits in Int64
            } else {
                DataType::Int32
            }
        }
        "bigint" => {
            // UInt64 doesn't fit in Int64 but Iceberg has no UInt64.
            // Use Int64 and accept potential overflow for values > i64::MAX.
            DataType::Int64
        }
        "float" => DataType::Float32,
        "double" | "real" => DataType::Float64,
        "decimal" | "numeric" => {
            let (p, s) = parse_precision_scale(&col.mysql_type);
            DataType::Decimal128(p, s)
        }
        "date" => DataType::Date32,
        "datetime" | "timestamp" => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        // TIME stored as Int64 microseconds (signed, supports negative TIME values).
        // Arrow Duration is not supported by Iceberg, so use Int64 directly.
        "time" => DataType::Int64,
        "year" => DataType::Int16,
        "char" | "varchar" | "text" | "tinytext" | "mediumtext" | "longtext" | "enum" | "set"
        | "json" => DataType::Utf8,
        "binary" | "varbinary" | "blob" | "tinyblob" | "mediumblob" | "longblob" | "bit" => {
            DataType::Binary
        }
        // Spatial types → WKB binary
        "point" | "linestring" | "polygon" | "geometry" | "multipoint" | "multilinestring"
        | "multipolygon" | "geomcollection" | "geometrycollection" => DataType::Binary,
        // Fallback
        _ => {
            tracing::warn!(mysql_type = %col.mysql_type, "Unknown MySQL type, mapping to Utf8");
            DataType::Utf8
        }
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
        .map(|(name, ordinal, col_type, nullable)| {
            let is_unsigned = col_type.contains(" unsigned");
            ColumnMeta {
                name,
                ordinal: ordinal as usize,
                mysql_type: col_type,
                nullable: nullable == "YES",
                is_unsigned,
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

    fn col(mysql_type: &str, unsigned: bool) -> ColumnMeta {
        ColumnMeta {
            name: "test".to_string(),
            ordinal: 0,
            mysql_type: mysql_type.to_string(),
            nullable: true,
            is_unsigned: unsigned,
        }
    }

    #[test]
    fn tinyint1_is_boolean() {
        assert_eq!(mysql_type_to_arrow(&col("tinyint(1)", false)), DataType::Boolean);
    }

    #[test]
    fn tinyint_signed() {
        assert_eq!(mysql_type_to_arrow(&col("tinyint", false)), DataType::Int8);
    }

    #[test]
    fn tinyint_unsigned_widens() {
        assert_eq!(mysql_type_to_arrow(&col("tinyint unsigned", true)), DataType::Int16);
    }

    #[test]
    fn smallint_unsigned_widens() {
        assert_eq!(mysql_type_to_arrow(&col("smallint unsigned", true)), DataType::Int32);
    }

    #[test]
    fn int_unsigned_widens() {
        assert_eq!(mysql_type_to_arrow(&col("int unsigned", true)), DataType::Int64);
    }

    #[test]
    fn bigint_always_int64() {
        assert_eq!(mysql_type_to_arrow(&col("bigint", false)), DataType::Int64);
        assert_eq!(mysql_type_to_arrow(&col("bigint unsigned", true)), DataType::Int64);
    }

    #[test]
    fn decimal_with_precision() {
        assert_eq!(
            mysql_type_to_arrow(&col("decimal(10,2)", false)),
            DataType::Decimal128(10, 2)
        );
    }

    #[test]
    fn decimal_clamped_precision() {
        // MySQL allows DECIMAL(65,30), Arrow caps at 38
        assert_eq!(
            mysql_type_to_arrow(&col("decimal(65,30)", false)),
            DataType::Decimal128(38, 30)
        );
    }

    #[test]
    fn time_is_int64() {
        assert_eq!(mysql_type_to_arrow(&col("time", false)), DataType::Int64);
    }

    #[test]
    fn json_is_utf8() {
        assert_eq!(mysql_type_to_arrow(&col("json", false)), DataType::Utf8);
    }

    #[test]
    fn year_is_int16() {
        assert_eq!(mysql_type_to_arrow(&col("year", false)), DataType::Int16);
    }

    #[test]
    fn datetime_is_timestamp() {
        assert_eq!(
            mysql_type_to_arrow(&col("datetime", false)),
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
    }

    #[test]
    fn blob_is_binary() {
        assert_eq!(mysql_type_to_arrow(&col("blob", false)), DataType::Binary);
    }

    #[test]
    fn enum_is_utf8() {
        assert_eq!(mysql_type_to_arrow(&col("enum", false)), DataType::Utf8);
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
