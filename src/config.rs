use std::fmt;

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub source: SourceConfig,
    pub sink: SinkConfig,
    pub state: StateConfig,
    pub observability: ObservabilityConfig,
}

impl fmt::Display for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "source={}:{}/{} tables=[{}] mode={} sink={} batch_size={} flush_interval={}s",
            self.source.host,
            self.source.port,
            self.source.database,
            self.source.tables.join(", "),
            self.source.mode,
            self.sink.mode,
            self.sink.writer.batch_size,
            self.sink.writer.flush_interval_secs,
        )
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct SourceConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub server_id: u32,
    pub database: String,
    pub tables: Vec<String>,
    #[serde(default = "default_source_mode")]
    pub mode: SourceMode,
    #[serde(default = "default_snapshot_chunk_size")]
    pub snapshot_chunk_size: usize,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SourceMode {
    #[default]
    CdcOnly,
    FullLoadThenCdc,
}

impl fmt::Display for SourceMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CdcOnly => write!(f, "cdc_only"),
            Self::FullLoadThenCdc => write!(f, "full_load_then_cdc"),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SinkMode {
    #[default]
    Local,
    S3,
    Iceberg,
}

impl fmt::Display for SinkMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Local => write!(f, "local"),
            Self::S3 => write!(f, "s3"),
            Self::Iceberg => write!(f, "iceberg"),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct SinkConfig {
    #[serde(default)]
    pub mode: SinkMode,
    /// Local output directory (mode = "local")
    #[serde(default = "default_output_dir")]
    pub output_dir: String,
    /// S3 warehouse path, e.g. s3://bucket/prefix
    pub warehouse: Option<String>,
    pub catalog_uri: Option<String>,
    pub catalog_name: Option<String>,
    pub namespace: Option<String>,
    pub region: Option<String>,
    pub endpoint: Option<String>,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    /// OAuth credential for Iceberg REST catalog (client_id:client_secret)
    pub credential: Option<String>,
    #[serde(default)]
    pub allow_http: bool,
    pub writer: WriterConfig,
}

fn default_output_dir() -> String {
    "./output".to_string()
}

#[derive(Debug, Clone, Deserialize)]
pub struct WriterConfig {
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    #[serde(default = "default_flush_interval")]
    pub flush_interval_secs: u64,
    #[serde(default = "default_file_size")]
    #[allow(dead_code)]
    pub target_file_size_mb: usize,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct StateConfig {
    pub bucket: Option<String>,
    pub prefix: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ObservabilityConfig {
    #[serde(default = "default_log_level")]
    pub log_level: String,
    #[serde(default)]
    pub json_logs: bool,
    #[serde(default = "default_health_bind")]
    pub health_check_bind: String,
    #[serde(default = "default_stats_interval")]
    pub stats_interval_secs: u64,
}

fn default_source_mode() -> SourceMode {
    SourceMode::CdcOnly
}

fn default_snapshot_chunk_size() -> usize {
    5000
}

fn default_batch_size() -> usize {
    10_000
}

fn default_flush_interval() -> u64 {
    30
}

fn default_file_size() -> usize {
    128
}

fn default_stats_interval() -> u64 {
    30
}

fn default_log_level() -> String {
    "info".to_string()
}

fn default_health_bind() -> String {
    "0.0.0.0:8080".to_string()
}

impl Config {
    pub fn load(path: &str) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let config: Config = toml::from_str(&content)?;
        Ok(config)
    }
}

impl SinkConfig {
    /// Build an S3 ObjectStore for the given bucket, using this config's credentials.
    pub fn build_s3_store(
        &self,
        bucket: &str,
    ) -> anyhow::Result<std::sync::Arc<dyn object_store::ObjectStore>> {
        use object_store::aws::AmazonS3Builder;

        let mut builder = AmazonS3Builder::new()
            .with_bucket_name(bucket)
            .with_region(self.region.as_deref().unwrap_or("us-east-1"));
        if let Some(ref endpoint) = self.endpoint {
            builder = builder.with_endpoint(endpoint);
        }
        if let Some(ref key) = self.access_key_id {
            builder = builder.with_access_key_id(key);
        }
        if let Some(ref secret) = self.secret_access_key {
            builder = builder.with_secret_access_key(secret);
        }
        if self.allow_http {
            builder = builder.with_allow_http(true);
        }
        builder = builder.with_virtual_hosted_style_request(false);
        Ok(std::sync::Arc::new(builder.build()?))
    }
}
