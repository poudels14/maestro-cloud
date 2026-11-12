use thiserror::Error;

#[derive(Debug, Error)]
pub enum TelemetryError {
    #[error(transparent)]
    Database(duckdb::Error),
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

impl From<duckdb::Error> for TelemetryError {
    /// DuckDB invalidates the database after a fatal error and rejects everything until it is
    /// reopened, so exit and let systemd restart telemetry instead of running without storage.
    fn from(error: duckdb::Error) -> Self {
        if is_fatal(&error) {
            tracing::error!(%error, "the log database hit a fatal error; exiting to reopen it");
            std::process::exit(1);
        }
        Self::Database(error)
    }
}

fn is_fatal(error: &duckdb::Error) -> bool {
    let message = error.to_string();
    [
        "FATAL Error",
        "INTERNAL Error",
        "database has been invalidated",
    ]
    .iter()
    .any(|marker| message.contains(marker))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recognizes_fatal_database_errors() {
        let failure = |message: &str| {
            duckdb::Error::DuckDBFailure(
                duckdb::ffi::Error::new(duckdb::ffi::DuckDBError),
                Some(message.to_string()),
            )
        };
        assert!(is_fatal(&failure(
            "FATAL Error: Failed: database has been invalidated because of a previous fatal error."
        )));
        assert!(is_fatal(&failure(
            "FATAL Error: could not write to the WAL"
        )));
        assert!(is_fatal(&failure(
            "INTERNAL Error: Invalid bitmask for FixedSizeAllocator"
        )));
        assert!(!is_fatal(&failure(
            "Binder Error: column \"id\" does not exist"
        )));
    }
}
