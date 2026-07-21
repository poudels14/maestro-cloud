use std::path::PathBuf;

use crate::DuckLogStoreError;

/// Host persistence and bounded ingestion settings for one DuckDB writer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DuckLogStoreSettings {
    /// Absolute database path owned by this daemon node.
    pub path: PathBuf,
    /// Maximum append or shutdown commands awaiting the single writer thread.
    pub queue_capacity: usize,
}

impl DuckLogStoreSettings {
    /// Validates an absolute database path and a positive backpressure bound.
    pub fn new(path: PathBuf, queue_capacity: usize) -> Result<Self, DuckLogStoreError> {
        if !path.is_absolute() {
            Err(DuckLogStoreError::InvalidConfiguration {
                message: "DuckDB log path must be absolute".to_owned(),
            })
        } else if queue_capacity == 0 {
            Err(DuckLogStoreError::InvalidConfiguration {
                message: "DuckDB log queue capacity must be positive".to_owned(),
            })
        } else {
            Ok(Self {
                path,
                queue_capacity,
            })
        }
    }
}
