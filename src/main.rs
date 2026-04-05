mod config;
mod error;
mod health;
mod pipeline;
mod sink;
mod source;
mod state;

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::Context;
use tracing::{debug, error, info, info_span, warn};

use crate::config::{Config, SinkMode, SourceMode};
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

    print_banner(&config);
    init_tracing(&config.observability);
    info!(config = %config, "Starting ACDC");

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
    let mut writer = match config.sink.mode {
        SinkMode::Iceberg => {
            info!("Using Iceberg sink writer");
            SinkWriter::Iceberg(IcebergSink::new(&config.sink, &tables).await?)
        }
        SinkMode::S3 => {
            info!("Using S3 sink writer");
            SinkWriter::S3(S3ParquetWriter::new(&config.sink)?)
        }
        SinkMode::Local => {
            info!(output_dir = %config.sink.output_dir, "Using local sink writer");
            SinkWriter::Local(LocalParquetWriter::new(&config.sink.output_dir)?)
        }
    };
    let flush_interval = Duration::from_secs(config.sink.writer.flush_interval_secs);

    // --- Snapshot phase (if configured and not already done) ---
    let _snapshot_span = info_span!("snapshot").entered();
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

    // Track snapshot completion state for periodic offset saves
    let snapshot_state: HashMap<String, bool> = if (snapshot_already_done
        || matches!(binlog_start_pos, Some(_)))
        && matches!(config.source.mode, SourceMode::FullLoadThenCdc)
    {
        config.source.tables.iter().map(|t| (t.clone(), true)).collect()
    } else {
        HashMap::new()
    };

    // --- CDC phase ---
    drop(_snapshot_span);
    let _cdc_span = info_span!("cdc").entered();

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

    let offset_save_interval = Duration::from_secs(60);
    info!(
        batch_size = config.sink.writer.batch_size,
        flush_interval_secs = config.sink.writer.flush_interval_secs,
        "Listening for binlog events..."
    );

    // Main event loop using tokio::select! for composable signal handling
    let start_time = Instant::now();
    let mut last_stats_seq: i64 = sequence.load(Ordering::Relaxed);

    let mut flush_timer = tokio::time::interval(flush_interval);
    flush_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut offset_timer = tokio::time::interval(offset_save_interval);
    offset_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let stats_interval_secs = config.observability.stats_interval_secs;
    let stats_duration = Duration::from_secs(stats_interval_secs);
    let mut stats_timer = tokio::time::interval_at(
        tokio::time::Instant::now() + stats_duration,
        stats_duration,
    );
    stats_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            msg = rx.recv() => {
                match msg {
                    Some(PipelineMessage::Event(event)) => {
                        if let Some((table_id, batch)) = pipeline.ingest(&event) {
                            let rows = batch.num_rows() as u64;
                            writer.write_batch(&table_id.table, &batch).await?;
                            flush_timer.reset();
                            let mut hs = health_state.lock().unwrap_or_else(|e| e.into_inner());
                            hs.events_processed += rows;
                            hs.last_flush_at = Some(chrono::Utc::now());
                        }
                    }
                    Some(PipelineMessage::SchemaChange { ref table, ref new_schema }) => {
                        if let Some((table_id, batch)) = pipeline.handle_schema_change(table, new_schema.clone())? {
                            writer.write_batch(&table_id.table, &batch).await?;
                        }
                    }
                    None => {
                        info!("Source channel closed");
                        break;
                    }
                }
            }
            _ = flush_timer.tick() => {
                let batches = pipeline.flush_all();
                if !batches.is_empty() {
                    let mut total_rows = 0u64;
                    for (table_id, batch) in &batches {
                        total_rows += batch.num_rows() as u64;
                        writer.write_batch(&table_id.table, batch).await?;
                    }
                    info!(tables = batches.len(), rows = total_rows, "Timer flush");
                    let mut hs = health_state.lock().unwrap_or_else(|e| e.into_inner());
                    hs.events_processed += total_rows;
                    hs.last_flush_at = Some(chrono::Utc::now());
                } else {
                    debug!("Flush tick — no buffered data");
                }
            }
            _ = stats_timer.tick() => {
                let current_seq = sequence.load(Ordering::Relaxed);
                let delta = current_seq - last_stats_seq;
                let rate = if stats_interval_secs > 0 {
                    delta as f64 / stats_interval_secs as f64
                } else {
                    0.0
                };
                let pos = shared_binlog_pos.lock().unwrap_or_else(|e| e.into_inner()).clone();
                info!(
                    events_total = current_seq,
                    events_interval = delta,
                    events_per_sec = format!("{:.1}", rate),
                    buffered_rows = pipeline.total_buffered(),
                    binlog_file = %pos.filename,
                    binlog_position = pos.position,
                    uptime_secs = start_time.elapsed().as_secs(),
                    "Stats"
                );
                // Update health state with current binlog position
                {
                    let mut hs = health_state.lock().unwrap_or_else(|e| e.into_inner());
                    hs.binlog_file = pos.filename;
                    hs.binlog_position = pos.position;
                }
                last_stats_seq = current_seq;
            }
            _ = offset_timer.tick() => {
                if let Some(ref store) = offset_store {
                    let pos = shared_binlog_pos.lock().unwrap_or_else(|e| e.into_inner()).clone();
                    if !pos.filename.is_empty() {
                        let mut offset = BinlogOffset::new(
                            pos.filename,
                            pos.position,
                            sequence.load(Ordering::Relaxed),
                        );
                        offset.snapshot_complete = snapshot_state.clone();
                        if let Err(e) = store.save(&offset).await {
                            warn!(error = %e, "Failed to save periodic offset");
                        }
                    }
                }
            }
        }

        if shutdown.load(Ordering::Relaxed) {
            info!(
                total_events = sequence.load(Ordering::Relaxed),
                uptime_secs = start_time.elapsed().as_secs(),
                "Shutting down — flushing remaining buffers..."
            );
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
        let total: usize = batches.iter().map(|(_, b)| b.num_rows()).sum();
        info!(tables = batches.len(), total_rows = total, "Final flush complete");
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

    // 3. Save final offset (use shared_binlog_pos as fallback if source errored)
    if let Some(ref store) = offset_store {
        let pos = final_position.unwrap_or_else(|| {
            shared_binlog_pos.lock().unwrap_or_else(|e| e.into_inner()).clone()
        });
        if !pos.filename.is_empty() {
            let mut offset = BinlogOffset::new(
                pos.filename,
                pos.position,
                sequence.load(Ordering::Relaxed),
            );
            offset.snapshot_complete = snapshot_state.clone();
            if let Err(e) = store.save(&offset).await {
                error!(error = %e, "Failed to save final offset");
            } else {
                info!("Final offset saved");
            }
        }
    }

    match tokio::time::timeout(Duration::from_secs(5), pool.disconnect()).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => warn!(error = %e, "MySQL pool disconnect error"),
        Err(_) => warn!("MySQL pool disconnect timed out"),
    }
    let final_pos = shared_binlog_pos.lock().unwrap_or_else(|e| e.into_inner()).clone();
    info!(
        binlog_file = %final_pos.filename,
        binlog_position = final_pos.position,
        total_events = sequence.load(Ordering::Relaxed),
        uptime_secs = start_time.elapsed().as_secs(),
        "Shutdown complete"
    );
    Ok(())
}

fn print_banner(config: &Config) {
    const BANNER: &str = r#"
    ___   ____________ ______
   /   | / ____/ __ \/ ____/
  / /| |/ /   / / / / /
 / ___ / /___/ /_/ / /___
/_/  |_\____/_____/\____/
"#;
    eprintln!("{}", BANNER);
    eprintln!(
        "  Alberto's CDC  v{}",
        env!("CARGO_PKG_VERSION")
    );
    eprintln!("  MySQL binlog -> Apache Iceberg");
    eprintln!();
    eprintln!(
        "  source: {}:{}/{} tables=[{}]",
        config.source.host,
        config.source.port,
        config.source.database,
        config.source.tables.join(", "),
    );
    eprintln!(
        "  sink:   {} | batch={} flush={}s",
        config.sink.mode,
        config.sink.writer.batch_size,
        config.sink.writer.flush_interval_secs,
    );
    eprintln!();
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
