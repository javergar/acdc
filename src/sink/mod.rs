pub mod iceberg;
pub mod parquet;

use arrow::array::RecordBatch;
use iceberg::IcebergSink;
use parquet::LocalParquetWriter;
use parquet::S3ParquetWriter;

/// Unified sink writer enum.
pub enum SinkWriter {
    Local(LocalParquetWriter),
    S3(S3ParquetWriter),
    Iceberg(Box<IcebergSink>),
}

impl SinkWriter {
    pub async fn write_batch(
        &mut self,
        table: &str,
        batch: &RecordBatch,
    ) -> anyhow::Result<String> {
        match self {
            Self::Local(w) => {
                let w = w.clone();
                let table = table.to_string();
                let batch = batch.clone();
                tokio::task::spawn_blocking(move || w.write_batch(&table, &batch))
                    .await
                    .map_err(|e| anyhow::anyhow!("spawn_blocking join error: {}", e))?
            }
            Self::S3(w) => w.write_batch(table, batch).await,
            Self::Iceberg(w) => w.write_batch(table, batch).await,
        }
    }
}
