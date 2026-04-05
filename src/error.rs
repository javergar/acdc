use std::future::Future;
use std::time::Duration;

use thiserror::Error;
use tracing::warn;

#[derive(Debug, Error)]
#[allow(dead_code)]
pub enum AcdcError {
    #[error("Source error: {0}")]
    Source(#[from] crate::source::SourceError),

    #[error("Config error: {0}")]
    Config(String),
}

/// Retry an async operation with exponential backoff.
/// Starts at 100ms, doubles each attempt, caps at 30s.
pub async fn retry<F, Fut, T>(op_name: &str, max_attempts: u32, f: F) -> anyhow::Result<T>
where
    F: Fn() -> Fut,
    Fut: Future<Output = anyhow::Result<T>>,
{
    let mut delay = Duration::from_millis(100);
    for attempt in 1..=max_attempts {
        match f().await {
            Ok(v) => return Ok(v),
            Err(e) if attempt < max_attempts => {
                warn!(
                    op = op_name,
                    attempt,
                    max_attempts,
                    error = %e,
                    delay_ms = delay.as_millis() as u64,
                    "Transient error, retrying"
                );
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(Duration::from_secs(30));
            }
            Err(e) => return Err(e),
        }
    }
    unreachable!()
}
