use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow_schema::SchemaRef;
use bytes::Bytes;
use iceberg::spec::{DataContentType, DataFileBuilder, DataFileFormat, Struct as IcebergStruct};
use iceberg::table::Table;
use iceberg::transaction::Transaction;
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};
use object_store::path::Path;
use object_store::ObjectStore;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::time::Instant;

use tracing::{debug, info};

use crate::config::SinkConfig;
use crate::error::retry;
use crate::source::schema::TableMeta;

/// Iceberg sink: writes Parquet to S3, then commits data files to the Iceberg catalog.
pub struct IcebergSink {
    catalog: RestCatalog,
    namespace: NamespaceIdent,
    s3_store: Arc<dyn ObjectStore>,
    warehouse_prefix: String,
    bucket: String,
    /// Cached table handles per table name
    tables: HashMap<String, Table>,
    /// Arrow schemas per table name
    schemas: HashMap<String, SchemaRef>,
}

impl IcebergSink {
    pub async fn new(
        config: &SinkConfig,
        table_metas: &[TableMeta],
    ) -> anyhow::Result<Self> {
        let catalog_uri = config
            .catalog_uri
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("sink.catalog_uri required for iceberg mode"))?;
        let warehouse = config
            .warehouse
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("sink.warehouse required for iceberg mode"))?;
        let namespace_str = config
            .namespace
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("sink.namespace required for iceberg mode"))?;

        // Build REST catalog config
        let mut props = HashMap::new();
        // Pass S3 credentials to the catalog so it can configure FileIO
        if let Some(ref region) = config.region {
            props.insert("s3.region".to_string(), region.clone());
        }
        if let Some(ref endpoint) = config.endpoint {
            props.insert("s3.endpoint".to_string(), endpoint.clone());
        }
        if let Some(ref key) = config.access_key_id {
            props.insert("s3.access-key-id".to_string(), key.clone());
        }
        if let Some(ref secret) = config.secret_access_key {
            props.insert("s3.secret-access-key".to_string(), secret.clone());
        }
        if config.allow_http {
            props.insert("s3.allow-http".to_string(), "true".to_string());
        }
        // WORKAROUND: iceberg-rust 0.4 has an inverted bug in storage_s3.rs:96
        // where s3.path-style-access=true ENABLES virtual-host-style.
        // Set to "false" to get actual path-style access for RustFS/MinIO.
        props.insert(
            "s3.path-style-access".to_string(),
            "false".to_string(),
        );
        // Credential for OAuth (client_id:client_secret)
        if let Some(ref cred) = config.credential {
            props.insert("credential".to_string(), cred.clone());
        }
        // OAuth scope for Polaris
        props.insert("scope".to_string(), "PRINCIPAL_ROLE:ALL".to_string());

        // For Polaris, the "warehouse" sent to the REST config endpoint should be
        // the catalog name, not the S3 path. The S3 path is returned by the server.
        let rest_warehouse = config
            .catalog_name
            .clone()
            .unwrap_or_else(|| warehouse.to_string());

        let catalog_config = RestCatalogConfig::builder()
            .uri(catalog_uri.to_string())
            .warehouse(rest_warehouse)
            .props(props)
            .build();
        let catalog = RestCatalog::new(catalog_config);

        let namespace = NamespaceIdent::from_strs([namespace_str])?;

        // Build S3 object store for writing Parquet files
        let warehouse_stripped = warehouse
            .strip_prefix("s3://")
            .ok_or_else(|| anyhow::anyhow!("warehouse must start with s3://"))?;
        let (bucket, prefix) = warehouse_stripped
            .split_once('/')
            .unwrap_or((warehouse_stripped, ""));

        let s3_store = config.build_s3_store(bucket)?;

        // Cache Arrow schemas
        let schemas: HashMap<String, SchemaRef> = table_metas
            .iter()
            .map(|m| (m.table_id.table.clone(), m.arrow_schema.clone()))
            .collect();

        info!(
            catalog_uri,
            namespace = namespace_str,
            warehouse,
            tables = table_metas.len(),
            "Iceberg sink initialized"
        );

        Ok(Self {
            catalog,
            namespace,
            s3_store,
            warehouse_prefix: prefix.to_string(),
            bucket: bucket.to_string(),
            tables: HashMap::new(),
            schemas,
        })
    }

    /// Ensure an Iceberg table exists, creating it if needed. Returns the table handle.
    async fn ensure_table(&mut self, table_name: &str) -> anyhow::Result<&Table> {
        if self.tables.contains_key(table_name) {
            return Ok(self.tables.get(table_name).unwrap());
        }

        let ident = TableIdent::from_strs([
            self.namespace.as_ref().first().unwrap().as_str(),
            table_name,
        ])?;

        // Try to load existing table
        match self.catalog.load_table(&ident).await {
            Ok(table) => {
                info!(table = table_name, "Loaded existing Iceberg table");
                self.tables.insert(table_name.to_string(), table);
            }
            Err(e) => {
                // Only create if the table genuinely doesn't exist.
                // iceberg-rust 0.4 doesn't expose typed error variants, so match on message.
                let err_msg = e.to_string().to_lowercase();
                if !err_msg.contains("not found")
                    && !err_msg.contains("no such")
                    && !err_msg.contains("does not exist")
                    && !err_msg.contains("404")
                {
                    return Err(anyhow::anyhow!(
                        "Failed to load Iceberg table '{}': {}",
                        table_name,
                        e
                    ));
                }

                let arrow_schema = self
                    .schemas
                    .get(table_name)
                    .ok_or_else(|| anyhow::anyhow!("No schema for table {}", table_name))?;

                let iceberg_schema =
                    iceberg::arrow::arrow_schema_to_schema(arrow_schema.as_ref())?;

                let ns_name = self.namespace.as_ref().first().unwrap();
                let creation = TableCreation::builder()
                    .name(table_name.to_string())
                    .schema(iceberg_schema)
                    .location(format!(
                        "s3://{}/{}/{}/{}",
                        self.bucket, self.warehouse_prefix, ns_name, table_name
                    ))
                    .build();

                let table = self
                    .catalog
                    .create_table(&self.namespace, creation)
                    .await?;
                info!(table = table_name, "Created new Iceberg table");
                self.tables.insert(table_name.to_string(), table);
            }
        }

        Ok(self.tables.get(table_name).unwrap())
    }

    /// Write a RecordBatch to S3 as Parquet, then commit to Iceberg catalog.
    pub async fn write_batch(
        &mut self,
        table_name: &str,
        batch: &RecordBatch,
    ) -> anyhow::Result<String> {
        let write_start = Instant::now();

        // 1. Ensure table exists in catalog
        self.ensure_table(table_name).await?;

        // 2. Write Parquet to in-memory buffer
        let mut buf = Vec::with_capacity(1024 * 1024);
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), Some(props))?;
        writer.write(batch)?;
        writer.close()?;

        let file_size = buf.len() as u64;
        let record_count = batch.num_rows() as u64;

        // 3. PUT Parquet file to S3
        let timestamp = chrono::Utc::now().format("%Y%m%d_%H%M%S");
        let id = uuid::Uuid::new_v4();
        let ns_name = self.namespace.as_ref().first().unwrap();
        let relative_path = format!(
            "{}/{}/{}/data/{}-{}.parquet",
            self.warehouse_prefix, ns_name, table_name, timestamp, id
        );
        let s3_path = Path::from(relative_path.clone());
        let full_s3_uri = format!("s3://{}/{}", self.bucket, relative_path);

        let payload = Bytes::from(buf);
        let store = self.s3_store.clone();
        let path = s3_path.clone();
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

        debug!(
            table = table_name,
            path = %full_s3_uri,
            size = file_size,
            rows = record_count,
            "Parquet file written to S3"
        );

        // 4. Build DataFile for Iceberg commit
        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(full_s3_uri.clone())
            .file_format(DataFileFormat::Parquet)
            .partition(IcebergStruct::empty())
            .record_count(record_count)
            .file_size_in_bytes(file_size)
            .build()
            .map_err(|e| anyhow::anyhow!("Failed to build DataFile: {}", e))?;

        // 5. Commit to Iceberg catalog via transaction
        let table = self.tables.get(table_name).unwrap();
        let tx = Transaction::new(table);
        let mut action = tx.fast_append(None, vec![])?;
        action.add_data_files(vec![data_file])?;
        let tx = action.apply().await?;
        let updated_table = tx.commit(&self.catalog).await?;

        // Update cached table with new snapshot
        self.tables.insert(table_name.to_string(), updated_table);

        info!(
            table = table_name,
            rows = record_count,
            size_bytes = file_size,
            elapsed_ms = write_start.elapsed().as_millis() as u64,
            path = %full_s3_uri,
            "Committed to Iceberg"
        );

        Ok(full_s3_uri)
    }
}
