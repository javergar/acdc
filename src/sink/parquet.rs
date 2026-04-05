use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::RecordBatch;
use bytes::Bytes;
use object_store::ObjectStore;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::time::Instant;

use tracing::info;

use crate::config::SinkConfig;
use crate::error::retry;

/// Writes RecordBatches as local Parquet files.
pub struct LocalParquetWriter {
    output_dir: PathBuf,
}

impl LocalParquetWriter {
    pub fn new(output_dir: impl AsRef<Path>) -> anyhow::Result<Self> {
        let output_dir = output_dir.as_ref().to_path_buf();
        fs::create_dir_all(&output_dir)?;
        Ok(Self { output_dir })
    }

    pub fn write_batch(&mut self, table: &str, batch: &RecordBatch) -> anyhow::Result<String> {
        let write_start = Instant::now();
        let table_dir = self.output_dir.join(table);
        fs::create_dir_all(&table_dir)?;

        let timestamp = chrono::Utc::now().format("%Y%m%d_%H%M%S");
        let id = uuid::Uuid::new_v4();
        let filename = format!("{}-{}.parquet", timestamp, id);
        let path = table_dir.join(&filename);

        let file = File::create(&path)?;
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))?;
        writer.write(batch)?;
        writer.close()?;

        let path_str = path.to_string_lossy().to_string();
        info!(table, rows = batch.num_rows(), elapsed_ms = write_start.elapsed().as_millis() as u64, path = %path_str, "Wrote Parquet file");
        Ok(path_str)
    }
}

/// Writes RecordBatches as Parquet files to S3 via object_store.
pub struct S3ParquetWriter {
    store: Arc<dyn ObjectStore>,
    prefix: String,
}

impl S3ParquetWriter {
    pub fn new(config: &SinkConfig) -> anyhow::Result<Self> {
        let warehouse = config
            .warehouse
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("sink.warehouse is required for S3 mode"))?;

        let warehouse = warehouse
            .strip_prefix("s3://")
            .ok_or_else(|| anyhow::anyhow!("warehouse must start with s3://"))?;
        let (bucket, prefix) = warehouse.split_once('/').unwrap_or((warehouse, ""));

        let store = config.build_s3_store(bucket)?;

        info!(bucket, prefix, "S3 writer initialized");

        Ok(Self {
            store,
            prefix: prefix.to_string(),
        })
    }

    pub async fn write_batch(
        &mut self,
        table: &str,
        batch: &RecordBatch,
    ) -> anyhow::Result<String> {
        let write_start = Instant::now();
        // Write Parquet to in-memory buffer
        let mut buf = Vec::with_capacity(1024 * 1024);
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), Some(props))?;
        writer.write(batch)?;
        writer.close()?;

        let file_size = buf.len();

        // Build S3 path
        let timestamp = chrono::Utc::now().format("%Y%m%d_%H%M%S");
        let id = uuid::Uuid::new_v4();
        let s3_key = if self.prefix.is_empty() {
            format!("{}/data/{}-{}.parquet", table, timestamp, id)
        } else {
            format!(
                "{}/{}/data/{}-{}.parquet",
                self.prefix, table, timestamp, id
            )
        };

        let path = object_store::path::Path::from(s3_key.clone());
        let payload = Bytes::from(buf);
        let store = self.store.clone();
        retry("s3_put", 3, || {
            let store = store.clone();
            let path = path.clone();
            let payload = payload.clone();
            async move {
                store.put(&path, payload.into()).await?;
                Ok(())
            }
        })
        .await?;

        info!(table, rows = batch.num_rows(), elapsed_ms = write_start.elapsed().as_millis() as u64, path = %s3_key, size_bytes = file_size, "Wrote Parquet to S3");
        Ok(s3_key)
    }
}
