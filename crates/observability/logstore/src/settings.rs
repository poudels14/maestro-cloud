use std::path::PathBuf;

use crate::DuckStoreError;

/// Host persistence and bounded ingestion settings for one DuckDB writer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DuckStoreSettings {
    /// Absolute database path owned by this daemon node.
    pub path: PathBuf,
    /// Maximum append or shutdown commands awaiting the single writer thread.
    pub queue_capacity: usize,
}

impl DuckStoreSettings {
    /// Validates an absolute database path and a positive backpressure bound.
    pub fn new(path: PathBuf, queue_capacity: usize) -> Result<Self, DuckStoreError> {
        if !path.is_absolute() {
            Err(DuckStoreError::InvalidConfiguration {
                message: "DuckDB path must be absolute".to_owned(),
            })
        } else if queue_capacity == 0 {
            Err(DuckStoreError::InvalidConfiguration {
                message: "DuckDB queue capacity must be positive".to_owned(),
            })
        } else {
            Ok(Self {
                path,
                queue_capacity,
            })
        }
    }
}
