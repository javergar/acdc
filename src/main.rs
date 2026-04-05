mod config;
mod error;
mod health;
mod pipeline;
mod sink;
mod source;
mod state;

use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::Context;
use tracing::{error, info, warn};

use crate::config::{Config, SourceMode};
use crate::health::{serve_health, HealthState};
use crate::pipeline::Pipeline;
use crate::sink::iceberg::IcebergSink;
use crate::sink::parquet::{LocalParquetWriter, S3ParquetWriter};
use crate::sink::SinkWriter;
use crate::source::binlog::BinlogSource;
use crate::source::schema::introspect_tables;
use crate::source::snapshot::SnapshotReader;
use crate::source::types::PipelineMessage;
use crate::state::{BinlogOffset, OffsetStore};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "acdc.toml".to_string());

    let config = Config::load(&config_path)
        .with_context(|| format!("Failed to load config from {}", config_path))?;

    init_tracing(&config.observability);
    info!(config_path = %config_path, "Starting ACDC");

    // Connect to MySQL
    let mysql_url = format!(
        "mysql://{}:{}@{}:{}/{}",
        config.source.user,
        config.source.password,
        config.source.host,
        config.source.port,
        config.source.database,
    );
    let pool = mysql_async::Pool::new(mysql_url.as_str());

    // Introspect table schemas
    info!("Introspecting table schemas...");
    let tables = introspect_tables(&pool, &config.source.database, &config.source.tables).await?;
    for table in &tables {
        info!(
            table = %table.table_id,
            columns = table.columns.len(),
            arrow_fields = table.arrow_schema.fields().len(),
            pk = ?table.primary_key_columns,
            "Discovered table"
        );
    }

    // Setup shutdown signal
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_clone = shutdown.clone();
    tokio::spawn(async move {
        let ctrl_c = tokio::signal::ctrl_c();
        #[cfg(unix)]
        {
            let mut sigterm =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("failed to register SIGTERM handler");
            tokio::select! {
                _ = sigterm.recv() => {},
                _ = ctrl_c => {},
            }
        }
        #[cfg(not(unix))]
        {
            ctrl_c.await.ok();
        }
        info!("Shutdown signal received");
        shutdown_clone.store(true, Ordering::Relaxed);
    });

    // Shared sequence counter
    let sequence = Arc::new(AtomicI64::new(0));

    // Health state shared between main loop and health endpoint
    let health_state = Arc::new(Mutex::new(HealthState::new(
        config.source.tables.clone(),
    )));

    // Spawn health check server
    let health_shutdown = shutdown.clone();
    let health_state_clone = health_state.clone();
    let health_bind = config.observability.health_check_bind.clone();
    let health_flush_interval = config.sink.writer.flush_interval_secs;
    tokio::spawn(async move {
        if let Err(e) = serve_health(
            &health_bind,
            health_state_clone,
            health_shutdown,
            health_flush_interval,
        )
        .await
        {
            warn!(error = %e, "Health check server error");
        }
    });

    // Initialize offset store (if S3 config available)
    let offset_store = if config.state.bucket.is_some() {
        let bucket = config.state.bucket.as_deref().unwrap();
        let prefix = config.state.prefix.as_deref().unwrap_or("acdc/offsets");
        let key = format!("{}/acdc_offset.json", prefix);
        match OffsetStore::new_s3(&config.sink, bucket, &key) {
            Ok(store) => {
                info!(bucket, key = %key, "Offset store initialized");
                Some(store)
            }
            Err(e) => {
                warn!(error = %e, "Failed to initialize offset store, continuing without persistence");
                None
            }
        }
    } else {
        None
    };

    // Load saved offset
    let saved_offset = if let Some(ref store) = offset_store {
        match store.load().await {
            Ok(Some(offset)) => {
                info!(
                    file = %offset.binlog_file,
                    position = offset.binlog_position,
                    seq = offset.cdc_seq,
                    "Loaded saved offset"
                );
                sequence.store(offset.cdc_seq, Ordering::Relaxed);
                Some(offset)
            }
            Ok(None) => None,
            Err(e) => {
                warn!(error = %e, "Failed to load offset, starting fresh");
                None
            }
        }
    } else {
        None
    };

    // Initialize pipeline and sink
    let mut pipeline = Pipeline::new(&tables, config.sink.writer.batch_size);
    let mut writer = match config.sink.mode.as_str() {
        "iceberg" => {
            info!("Using Iceberg sink writer");
            SinkWriter::Iceberg(IcebergSink::new(&config.sink, &tables).await?)
        }
        "s3" => {
            info!("Using S3 sink writer");
            SinkWriter::S3(S3ParquetWriter::new(&config.sink)?)
        }
        _ => {
            info!(output_dir = %config.sink.output_dir, "Using local sink writer");
            SinkWriter::Local(LocalParquetWriter::new(&config.sink.output_dir)?)
        }
    };
    let flush_interval = Duration::from_secs(config.sink.writer.flush_interval_secs);

    // --- Snapshot phase (if configured and not already done) ---
    let snapshot_already_done = saved_offset
        .as_ref()
        .map(|o| !o.snapshot_complete.is_empty())
        .unwrap_or(false);

    let binlog_start_pos = match config.source.mode {
        SourceMode::FullLoadThenCdc if !snapshot_already_done => {
            info!("Running initial snapshot (full_load_then_cdc mode)...");

            let snapshot_reader = SnapshotReader::new(
                pool.clone(),
                config.source.clone(),
                tables.clone(),
                sequence.clone(),
            );

            let (snap_tx, mut snap_rx) = tokio::sync::mpsc::channel::<PipelineMessage>(1024);
            let shutdown_snap = shutdown.clone();
            let snap_handle = tokio::spawn(async move {
                snapshot_reader.run(&snap_tx, &shutdown_snap).await
            });

            while let Some(msg) = snap_rx.recv().await {
                match msg {
                    PipelineMessage::Event(event) => {
                        if let Some((table_id, batch)) = pipeline.ingest(&event) {
                            writer.write_batch(&table_id.table, &batch).await?;
                        }
                    }
                    PipelineMessage::SchemaChange { table, new_schema } => {
                        pipeline.update_schema(&table, new_schema);
                    }
                }
            }

            let batches = pipeline.flush_all();
            for (table_id, batch) in &batches {
                writer.write_batch(&table_id.table, batch).await?;
            }

            let binlog_pos = snap_handle.await??;
            let snap_rows = sequence.load(Ordering::Relaxed);
            info!(
                file = %binlog_pos.filename,
                position = binlog_pos.position,
                rows = snap_rows,
                "Snapshot complete, switching to CDC"
            );

            // Save offset after snapshot
            if let Some(ref store) = offset_store {
                let mut offset = BinlogOffset::new(
                    binlog_pos.filename.clone(),
                    binlog_pos.position,
                    snap_rows,
                );
                for t in &config.source.tables {
                    offset.snapshot_complete.insert(t.clone(), true);
                }
                store.save(&offset).await?;
            }

            Some(binlog_pos)
        }
        SourceMode::FullLoadThenCdc => {
            // Snapshot already done, resume from saved offset
            info!("Snapshot already complete, resuming CDC from saved offset");
            saved_offset.map(|o| source::binlog::BinlogPosition {
                filename: o.binlog_file,
                position: o.binlog_position,
            })
        }
        SourceMode::CdcOnly => {
            // Resume from saved offset if available
            saved_offset.map(|o| source::binlog::BinlogPosition {
                filename: o.binlog_file,
                position: o.binlog_position,
            })
        }
    };

    // --- CDC phase ---
    let (tx, mut rx) = tokio::sync::mpsc::channel::<PipelineMessage>(1024);

    let shared_binlog_pos = Arc::new(Mutex::new(source::binlog::BinlogPosition {
        filename: String::new(),
        position: 0,
    }));

    let mut binlog_source =
        BinlogSource::new(pool.clone(), config.source.clone(), tables.clone(), sequence.clone(), shared_binlog_pos.clone())
            .await?;
    let shutdown_for_source = shutdown.clone();
    let source_handle = tokio::spawn(async move {
        binlog_source
            .stream(tx, shutdown_for_source, binlog_start_pos)
            .await
    });

    let mut last_flush = Instant::now();
    let mut last_offset_save = Instant::now();
    let offset_save_interval = Duration::from_secs(60);
    info!(
        batch_size = config.sink.writer.batch_size,
        flush_interval_secs = config.sink.writer.flush_interval_secs,
        "Listening for binlog events..."
    );

    // Main event loop
    loop {
        let timeout = flush_interval
            .checked_sub(last_flush.elapsed())
            .unwrap_or(Duration::ZERO);

        match tokio::time::timeout(timeout, rx.recv()).await {
            Ok(Some(msg)) => {
                match msg {
                    PipelineMessage::Event(event) => {
                        if let Some((table_id, batch)) = pipeline.ingest(&event) {
                            let rows = batch.num_rows() as u64;
                            writer.write_batch(&table_id.table, &batch).await?;
                            last_flush = Instant::now();
                            let mut hs = health_state.lock().unwrap();
                            hs.events_processed += rows;
                            hs.last_flush_at = Some(chrono::Utc::now());
                        }
                    }
                    PipelineMessage::SchemaChange { ref table, ref new_schema } => {
                        if let Some((table_id, batch)) = pipeline.handle_schema_change(table, new_schema.clone()) {
                            writer.write_batch(&table_id.table, &batch).await?;
                        }
                    }
                }
            }
            Ok(None) => {
                info!("Source channel closed");
                break;
            }
            Err(_) => {
                // Timer flush
                let batches = pipeline.flush_all();
                if !batches.is_empty() {
                    let mut total_rows = 0u64;
                    for (table_id, batch) in &batches {
                        total_rows += batch.num_rows() as u64;
                        writer.write_batch(&table_id.table, batch).await?;
                    }
                    last_flush = Instant::now();
                    let mut hs = health_state.lock().unwrap();
                    hs.events_processed += total_rows;
                    hs.last_flush_at = Some(chrono::Utc::now());
                }
            }
        }

        // Periodic offset persistence (every 60s)
        if last_offset_save.elapsed() >= offset_save_interval {
            if let Some(ref store) = offset_store {
                let pos = shared_binlog_pos.lock().unwrap().clone();
                if !pos.filename.is_empty() {
                    let offset = BinlogOffset::new(
                        pos.filename,
                        pos.position,
                        sequence.load(Ordering::Relaxed),
                    );
                    if let Err(e) = store.save(&offset).await {
                        warn!(error = %e, "Failed to save periodic offset");
                    }
                }
            }
            last_offset_save = Instant::now();
        }

        if shutdown.load(Ordering::Relaxed) {
            info!("Shutting down — flushing remaining buffers...");
            break;
        }
    }

    // --- Graceful shutdown ---

    // 1. Final flush of all buffers
    let batches = pipeline.flush_all();
    for (table_id, batch) in &batches {
        writer.write_batch(&table_id.table, batch).await?;
    }
    if !batches.is_empty() {
        info!(tables = batches.len(), "Final flush complete");
    }

    // 2. Wait for source to finish and get final position
    drop(rx);
    let final_position = match source_handle.await? {
        Ok(pos) => {
            info!(
                filename = %pos.filename,
                position = pos.position,
                "Binlog reader stopped"
            );
            Some(pos)
        }
        Err(e) => {
            error!(error = %e, "Binlog reader error");
            None
        }
    };

    // 3. Save final offset
    if let (Some(ref store), Some(pos)) = (&offset_store, final_position) {
        let offset = BinlogOffset::new(
            pos.filename,
            pos.position,
            sequence.load(Ordering::Relaxed),
        );
        store.save(&offset).await?;
        info!("Final offset saved");
    }

    pool.disconnect().await?;
    info!("Shutdown complete");
    Ok(())
}

fn init_tracing(config: &config::ObservabilityConfig) {
    use tracing_subscriber::EnvFilter;

    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(format!("acdc={}", config.log_level)));

    if config.json_logs {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .json()
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .init();
    }
}
