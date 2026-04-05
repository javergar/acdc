use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use object_store::path::Path;
use object_store::ObjectStore;
use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::config::SinkConfig;
use crate::error::retry;

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
        let payload = Bytes::from(json);
        let store = self.store.clone();
        let path = self.path.clone();
        retry("offset_save", 3, || {
            let store = store.clone();
            let path = path.clone();
            let payload = payload.clone();
            async move {
                store.put(&path, payload.into()).await?;
                Ok(())
            }
        })
        .await?;
        debug!(
            file = %offset.binlog_file,
            position = offset.binlog_position,
            seq = offset.cdc_seq,
            "Saved offset"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn offset_roundtrip() {
        let mut offset = BinlogOffset::new(
            "mysql-bin.000003".to_string(),
            12345,
            42,
        );
        offset.snapshot_complete.insert("orders".to_string(), true);
        offset.snapshot_complete.insert("customers".to_string(), true);
        offset.gtid_set = Some("abc-123:1-10".to_string());

        let json = serde_json::to_string(&offset).unwrap();
        let deserialized: BinlogOffset = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.version, 1);
        assert_eq!(deserialized.binlog_file, "mysql-bin.000003");
        assert_eq!(deserialized.binlog_position, 12345);
        assert_eq!(deserialized.cdc_seq, 42);
        assert_eq!(deserialized.gtid_set.as_deref(), Some("abc-123:1-10"));
        assert_eq!(deserialized.snapshot_complete.len(), 2);
        assert_eq!(deserialized.snapshot_complete.get("orders"), Some(&true));
    }
}
