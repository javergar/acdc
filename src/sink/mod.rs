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
    Iceberg(IcebergSink),
}

impl SinkWriter {
    pub async fn write_batch(
        &mut self,
        table: &str,
        batch: &RecordBatch,
    ) -> anyhow::Result<String> {
        match self {
            // Note: LocalParquetWriter::write_batch is sync — blocks the tokio runtime.
            // For production use with large files, wrap in spawn_blocking.
            Self::Local(w) => w.write_batch(table, batch),
            Self::S3(w) => w.write_batch(table, batch).await,
            Self::Iceberg(w) => w.write_batch(table, batch).await,
        }
    }
}
