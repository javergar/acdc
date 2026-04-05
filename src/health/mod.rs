use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tracing::{debug, info, warn};

/// Shared health state updated by the pipeline and read by the health endpoint.
#[derive(Debug, Clone)]
pub struct HealthState {
    pub binlog_file: String,
    pub binlog_position: u64,
    pub last_flush_at: Option<chrono::DateTime<chrono::Utc>>,
    pub tables: Vec<String>,
    pub buffer_sizes: HashMap<String, usize>,
    pub started_at: Instant,
    pub events_processed: u64,
}

impl HealthState {
    pub fn new(tables: Vec<String>) -> Self {
        Self {
            binlog_file: String::new(),
            binlog_position: 0,
            last_flush_at: None,
            tables,
            buffer_sizes: HashMap::new(),
            started_at: Instant::now(),
            events_processed: 0,
        }
    }
}

/// Run the health check HTTP server. Blocks until shutdown.
pub async fn serve_health(
    bind_addr: &str,
    state: Arc<Mutex<HealthState>>,
    shutdown: Arc<AtomicBool>,
    flush_interval_secs: u64,
) -> anyhow::Result<()> {
    let listener = TcpListener::bind(bind_addr).await?;
    info!(bind = bind_addr, "Health check listening");

    loop {
        if shutdown.load(Ordering::Relaxed) {
            break;
        }

        // Accept with timeout so we can check shutdown
        let accept = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            listener.accept(),
        )
        .await;

        let (mut stream, _addr) = match accept {
            Ok(Ok(conn)) => conn,
            Ok(Err(e)) => {
                warn!(error = %e, "Health check accept error");
                continue;
            }
            Err(_) => continue, // timeout, check shutdown
        };

        // Read the request (we only care about GET /health)
        let mut buf = [0u8; 1024];
        let _ = tokio::io::AsyncReadExt::read(&mut stream, &mut buf).await;

        let state = state.lock().unwrap().clone();
        let uptime = state.started_at.elapsed().as_secs();

        // Determine health status
        let stale_threshold = std::time::Duration::from_secs(flush_interval_secs * 3);
        let is_healthy = match state.last_flush_at {
            Some(last) => {
                let since_flush = chrono::Utc::now() - last;
                since_flush.to_std().unwrap_or_default() < stale_threshold
            }
            None => {
                // No flush yet — healthy if we just started (< stale_threshold)
                state.started_at.elapsed() < stale_threshold
            }
        };

        let status_code = if is_healthy { 200 } else { 503 };
        let status_text = if is_healthy { "ok" } else { "stale" };

        let body = serde_json::json!({
            "status": status_text,
            "binlog_file": state.binlog_file,
            "binlog_position": state.binlog_position,
            "last_flush_at": state.last_flush_at.map(|t| t.to_rfc3339()),
            "tables": state.tables,
            "buffer_sizes": state.buffer_sizes,
            "uptime_secs": uptime,
            "events_processed": state.events_processed,
        });
        let body_str = serde_json::to_string(&body).unwrap_or_default();

        let response = format!(
            "HTTP/1.1 {} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            status_code,
            if is_healthy { "OK" } else { "Service Unavailable" },
            body_str.len(),
            body_str,
        );

        let _ = stream.write_all(response.as_bytes()).await;
        debug!(status = status_code, "Health check response");
    }

    Ok(())
}
