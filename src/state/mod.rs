use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use object_store::path::Path;
use object_store::ObjectStore;
use serde::{Deserialize, Serialize};
use tracing::{debug, info};

use crate::config::SinkConfig;

/// Persisted binlog offset for crash recovery.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinlogOffset {
    pub version: u32,
    pub updated_at: String,
    pub binlog_file: String,
    pub binlog_position: u64,
    pub gtid_set: Option<String>,
    pub snapshot_complete: HashMap<String, bool>,
    pub snapshot_in_progress: Option<String>,
    pub cdc_seq: i64,
}

impl BinlogOffset {
    pub fn new(binlog_file: String, binlog_position: u64, cdc_seq: i64) -> Self {
        Self {
            version: 1,
            updated_at: chrono::Utc::now().to_rfc3339(),
            binlog_file,
            binlog_position,
            gtid_set: None,
            snapshot_complete: HashMap::new(),
            snapshot_in_progress: None,
            cdc_seq,
        }
    }
}

/// Stores and retrieves binlog offsets from S3.
pub struct OffsetStore {
    store: Arc<dyn ObjectStore>,
    path: Path,
}

impl OffsetStore {
    /// Create an OffsetStore backed by S3.
    pub fn new_s3(config: &SinkConfig, bucket: &str, key: &str) -> anyhow::Result<Self> {
        let store = config.build_s3_store(bucket)?;
        let path = Path::from(key);

        Ok(Self { store, path })
    }

    /// Load the saved offset. Returns None if not found.
    pub async fn load(&self) -> anyhow::Result<Option<BinlogOffset>> {
        match self.store.get(&self.path).await {
            Ok(result) => {
                let bytes = result.bytes().await?;
                let offset: BinlogOffset = serde_json::from_slice(&bytes)?;
                debug!(
                    file = %offset.binlog_file,
                    position = offset.binlog_position,
                    seq = offset.cdc_seq,
                    "Loaded offset"
                );
                Ok(Some(offset))
            }
            Err(object_store::Error::NotFound { .. }) => {
                debug!("No saved offset found");
                Ok(None)
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Save the offset. Called AFTER successful Iceberg commit.
    pub async fn save(&self, offset: &BinlogOffset) -> anyhow::Result<()> {
        let json = serde_json::to_string_pretty(offset)?;
        self.store
            .put(&self.path, Bytes::from(json).into())
            .await?;
        info!(
            file = %offset.binlog_file,
            position = offset.binlog_position,
            seq = offset.cdc_seq,
            "Saved offset"
        );
        Ok(())
    }
}
