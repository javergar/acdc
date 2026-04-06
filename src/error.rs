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

/// Check if an error looks retryable (transient network/server errors).
/// Non-retryable: auth failures, bad requests, not-found, client errors.
fn is_retryable(err: &anyhow::Error) -> bool {
    // Check typed errors first (more reliable than string matching)
    for cause in err.chain() {
        if let Some(store_err) = cause.downcast_ref::<object_store::Error>() {
            return !matches!(
                store_err,
                object_store::Error::NotFound { .. }
                    | object_store::Error::Precondition { .. }
            );
        }
    }
    // Fallback: string matching for errors without typed variants
    let msg = err.to_string().to_lowercase();
    if msg.contains("access denied") || msg.contains("forbidden") || msg.contains("unauthorized") {
        return false;
    }
    true
}

/// Check if an error looks like an authentication/authorization failure.
pub fn is_auth_error(err: &anyhow::Error) -> bool {
    let msg = err.to_string().to_lowercase();
    msg.contains("unauthorized") || msg.contains("forbidden")
        || msg.contains("access denied") || msg.contains("401") || msg.contains("403")
}

/// Retry an async operation with exponential backoff.
/// Starts at 100ms, doubles each attempt, caps at 30s.
/// Non-retryable errors (auth, client errors) fail immediately.
pub async fn retry<F, Fut, T>(op_name: &str, max_attempts: u32, f: F) -> anyhow::Result<T>
where
    F: Fn() -> Fut,
    Fut: Future<Output = anyhow::Result<T>>,
{
    let mut delay = Duration::from_millis(100);
    for attempt in 1..=max_attempts {
        match f().await {
            Ok(v) => return Ok(v),
            Err(e) if attempt < max_attempts && is_retryable(&e) => {
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
