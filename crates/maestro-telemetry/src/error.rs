use thiserror::Error;

#[derive(Debug, Error)]
pub enum TelemetryError {
    #[error(transparent)]
    Database(#[from] duckdb::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("telemetry task failed: {0}")]
    Task(#[from] tokio::task::JoinError),
    #[error("invalid log query: {0}")]
    Query(String),
    #[error("{0}")]
    Storage(String),
}

pub type Result<T> = std::result::Result<T, TelemetryError>;
