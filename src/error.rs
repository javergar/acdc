use thiserror::Error;

#[derive(Debug, Error)]
pub enum AcdcError {
    #[error("Source error: {0}")]
    Source(#[from] crate::source::SourceError),

    #[error("Config error: {0}")]
    Config(String),
}
