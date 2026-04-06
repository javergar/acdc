use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;

use mysql_async::prelude::*;
use mysql_async::Pool;
use tracing::{debug, info, warn};

use super::binlog::{get_master_status, BinlogPosition};
use super::schema::TableMeta;
use super::types::{CdcEvent, CdcOp, ColumnValue, PipelineMessage};
use super::SourceError;
use crate::config::SourceConfig;

/// Performs an initial full-table snapshot before CDC begins.
///
/// Uses START TRANSACTION WITH CONSISTENT SNAPSHOT to get a point-in-time
/// view of all InnoDB tables, captures the binlog position within that
/// snapshot, then streams all rows as Snapshot ("S") events.
pub struct SnapshotReader {
    pool: Pool,
    config: SourceConfig,
    tables: Vec<TableMeta>,
    sequence: Arc<AtomicI64>,
}

impl SnapshotReader {
    pub fn new(
        pool: Pool,
        config: SourceConfig,
        tables: Vec<TableMeta>,
        sequence: Arc<AtomicI64>,
    ) -> Self {
        Self {
            pool,
            config,
            tables,
            sequence,
        }
    }

    /// Run the snapshot. Returns the binlog position captured at snapshot time.
    ///
    /// All rows are sent through `tx` as CdcEvent with op=Snapshot.
    /// The returned BinlogPosition should be used to start the binlog stream.
    /// `on_table_complete` is called after each table finishes so the caller
    /// can persist per-table progress for crash recovery.
    pub async fn run(
        &self,
        tx: &tokio::sync::mpsc::Sender<PipelineMessage>,
        shutdown: &AtomicBool,
        mut on_table_complete: impl FnMut(&str),
    ) -> Result<BinlogPosition, SourceError> {
        let mut conn = self.pool.get_conn().await?;

        // 1. Start a consistent snapshot transaction
        conn.query_drop("START TRANSACTION WITH CONSISTENT SNAPSHOT")
            .await?;
        let snapshot_ts = chrono::Utc::now();
        info!("Started consistent snapshot transaction");

        // 2. Capture binlog position inside the snapshot
        let (binlog_file, binlog_position) = get_master_status(&mut conn).await?;
        info!(
            file = %binlog_file,
            position = binlog_position,
            "Captured binlog position for snapshot"
        );

        // 3. Snapshot each table
        for table_meta in &self.tables {
            if shutdown.load(Ordering::Acquire) {
                warn!("Shutdown during snapshot, aborting");
                conn.query_drop("ROLLBACK").await?;
                return Ok(BinlogPosition {
                    filename: binlog_file,
                    position: binlog_position,
                });
            }

            self.snapshot_table(&mut conn, table_meta, tx, shutdown, snapshot_ts)
                .await?;
            on_table_complete(&table_meta.table_id.table);
        }

        // 4. Commit and release the connection
        conn.query_drop("COMMIT").await?;
        drop(conn);
        info!("Snapshot transaction committed");

        Ok(BinlogPosition {
            filename: binlog_file,
            position: binlog_position,
        })
    }

    /// Snapshot a single table using chunked SELECT by primary key.
    async fn snapshot_table(
        &self,
        conn: &mut mysql_async::Conn,
        table_meta: &TableMeta,
        tx: &tokio::sync::mpsc::Sender<PipelineMessage>,
        shutdown: &AtomicBool,
        snapshot_ts: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), SourceError> {
        let db = &self.config.database;
        let table = &table_meta.table_id.table;
        let select_cols = table_meta.columns.iter()
            .map(|c| format!("`{}`", c.name))
            .collect::<Vec<_>>()
            .join(", ");

        // Determine chunking strategy based on primary key
        let pk_cols = &table_meta.primary_key_columns;

        let mut total_rows: u64 = 0;
        let chunk_size = self.config.snapshot_chunk_size;

        info!(table, pk_columns = ?pk_cols, chunk_size, "Starting snapshot for table");

        if pk_cols.is_empty() {
            // No primary key — paginate with LIMIT/OFFSET to avoid loading entire table into memory.
            // Without a PK there is no reliable ORDER BY key, so row ordering is not guaranteed
            // across chunks; this is acceptable for an append-only snapshot.
            warn!(table, "No primary key, using LIMIT/OFFSET pagination");
            let mut page_offset: u64 = 0;

            loop {
                if shutdown.load(Ordering::Acquire) {
                    return Ok(());
                }

                let query = format!(
                    "SELECT {} FROM `{}`.`{}` LIMIT {} OFFSET {}",
                    select_cols, db, table, chunk_size, page_offset
                );
                let rows: Vec<mysql_async::Row> = conn.query(&query).await?;
                if rows.is_empty() {
                    break;
                }

                let chunk_len = rows.len();
                for row in rows {
                    let event = self.row_to_event(row, table_meta, snapshot_ts);
                    if tx.send(PipelineMessage::Event(event)).await.is_err() {
                        return Err(SourceError::ChannelClosed);
                    }
                    total_rows += 1;
                }

                debug!(
                    table,
                    chunk_rows = chunk_len,
                    total_rows,
                    "Snapshot chunk (no PK)"
                );
                if total_rows % 100_000 < chunk_size as u64 && total_rows >= 100_000 {
                    info!(table, rows_so_far = total_rows, "Snapshot progress");
                }

                page_offset += chunk_len as u64;
                if chunk_len < chunk_size {
                    break;
                }
            }
        } else if pk_cols.len() == 1 {
            // Single-column PK — simple pagination
            let pk = &pk_cols[0];
            let mut last_pk_value: Option<mysql_async::Value> = None;

            loop {
                if shutdown.load(Ordering::Acquire) {
                    return Ok(());
                }

                let query = match &last_pk_value {
                    None => format!(
                        "SELECT {} FROM `{}`.`{}` ORDER BY `{}` LIMIT {}",
                        select_cols, db, table, pk, chunk_size
                    ),
                    Some(_) => format!(
                        "SELECT {} FROM `{}`.`{}` WHERE `{}` > :last_pk ORDER BY `{}` LIMIT {}",
                        select_cols, db, table, pk, pk, chunk_size
                    ),
                };

                let rows: Vec<mysql_async::Row> = match &last_pk_value {
                    None => conn.query(&query).await?,
                    Some(val) => {
                        conn.exec(&query, params! { "last_pk" => val.clone() })
                            .await?
                    }
                };

                if rows.is_empty() {
                    break;
                }

                let chunk_len = rows.len();

                // Get the last PK value for next chunk
                let pk_idx = table_meta
                    .columns
                    .iter()
                    .position(|c| c.name == *pk)
                    .unwrap_or(0);

                let last_row = &rows[rows.len() - 1];
                last_pk_value = last_row.get::<mysql_async::Value, _>(pk_idx);

                for row in rows {
                    let event = self.row_to_event(row, table_meta, snapshot_ts);
                    if tx.send(PipelineMessage::Event(event)).await.is_err() {
                        return Err(SourceError::ChannelClosed);
                    }
                    total_rows += 1;
                }

                debug!(
                    table,
                    chunk_rows = chunk_len,
                    total_rows,
                    "Snapshot chunk"
                );
                if total_rows % 100_000 < chunk_size as u64 && total_rows >= 100_000 {
                    info!(table, rows_so_far = total_rows, "Snapshot progress");
                }

                if chunk_len < chunk_size {
                    break; // Last chunk
                }
            }
        } else {
            // Composite PK — cursor-based pagination using tuple comparison
            // MySQL supports: WHERE (pk1, pk2) > (:v1, :v2)
            let pk_order = pk_cols
                .iter()
                .map(|c| format!("`{}`", c))
                .collect::<Vec<_>>()
                .join(", ");
            let pk_indices: Vec<usize> = pk_cols
                .iter()
                .map(|pk| table_meta.columns.iter().position(|c| c.name == *pk).unwrap_or(0))
                .collect();
            let mut last_pk_values: Option<Vec<mysql_async::Value>> = None;

            loop {
                if shutdown.load(Ordering::Acquire) {
                    return Ok(());
                }

                let rows: Vec<mysql_async::Row> = match &last_pk_values {
                    None => {
                        let query = format!(
                            "SELECT {} FROM `{}`.`{}` ORDER BY {} LIMIT {}",
                            select_cols, db, table, pk_order, chunk_size
                        );
                        conn.query(&query).await?
                    }
                    Some(vals) => {
                        // Build WHERE (pk1, pk2, ...) > (:pk0, :pk1, ...)
                        let pk_tuple = pk_cols.iter().map(|c| format!("`{}`", c)).collect::<Vec<_>>().join(", ");
                        let param_list: Vec<String> = (0..pk_cols.len()).map(|i| format!(":pk{}", i)).collect();
                        let query = format!(
                            "SELECT {} FROM `{}`.`{}` WHERE ({}) > ({}) ORDER BY {} LIMIT {}",
                            select_cols, db, table, pk_tuple, param_list.join(", "), pk_order, chunk_size
                        );
                        let params: Vec<(Vec<u8>, mysql_async::Value)> = vals.iter().enumerate()
                            .map(|(i, v)| (format!("pk{}", i).into_bytes(), v.clone()))
                            .collect();
                        conn.exec(&query, mysql_async::Params::Named(params.into_iter().collect())).await?
                    }
                };

                if rows.is_empty() {
                    break;
                }

                let chunk_len = rows.len();

                // Capture last row's PK values for next cursor
                let last_row = &rows[rows.len() - 1];
                last_pk_values = Some(
                    pk_indices.iter().map(|&idx| {
                        last_row.get::<mysql_async::Value, _>(idx).unwrap_or(mysql_async::Value::NULL)
                    }).collect()
                );

                for row in rows {
                    let event = self.row_to_event(row, table_meta, snapshot_ts);
                    if tx.send(PipelineMessage::Event(event)).await.is_err() {
                        return Err(SourceError::ChannelClosed);
                    }
                    total_rows += 1;
                }

                debug!(
                    table,
                    chunk_rows = chunk_len,
                    total_rows,
                    "Snapshot chunk (composite PK)"
                );
                if total_rows % 100_000 < chunk_size as u64 && total_rows >= 100_000 {
                    info!(table, rows_so_far = total_rows, "Snapshot progress");
                }

                if chunk_len < chunk_size {
                    break;
                }
            }
        }

        info!(table, rows = total_rows, "Snapshot complete for table");
        Ok(())
    }

    /// Convert a mysql_async::Row (from SELECT) to a CdcEvent.
    fn row_to_event(&self, row: mysql_async::Row, table_meta: &TableMeta, snapshot_ts: chrono::DateTime<chrono::Utc>) -> CdcEvent {
        let values: Vec<ColumnValue> = table_meta
            .columns
            .iter()
            .enumerate()
            .map(|(i, col)| {
                let value: Option<mysql_async::Value> = row.get(i);
                match value {
                    None | Some(mysql_async::Value::NULL) => ColumnValue::Null,
                    Some(v) => {
                        match super::binlog::convert_mysql_value(v, col) {
                            Ok(cv) => cv,
                            Err(e) => {
                                warn!(
                                    table = %table_meta.table_id,
                                    column = %col.name,
                                    error = %e,
                                    "Snapshot row conversion error"
                                );
                                ColumnValue::ConversionFailed(e.to_string())
                            }
                        }
                    }
                }
            })
            .collect();

        let seq = self.sequence.fetch_add(1, Ordering::Relaxed);

        CdcEvent {
            table: table_meta.table_id.clone(),
            op: CdcOp::Snapshot,
            timestamp: snapshot_ts,
            sequence: seq,
            values,
        }
    }
}
