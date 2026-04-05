pub mod binlog;
pub mod schema;
pub mod snapshot;
pub mod types;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum SourceError {
    #[error("MySQL connection failed: {0}")]
    Connection(#[from] mysql_async::Error),

    #[error("Binlog parse error: {0}")]
    BinlogParse(String),

    #[error("Schema introspection failed: {0}")]
    Schema(String),

    #[error("Channel send error")]
    ChannelClosed,
}
