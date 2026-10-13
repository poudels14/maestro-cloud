use maestro_core::{DeploymentId, ServiceId};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum StoreError {
    #[error(transparent)]
    Database(#[from] redb::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("failed to encode or decode a stored value: {0}")]
    Codec(#[from] serde_json::Error),
    #[error("deployment {service_id}/{deployment_id} does not exist")]
    DeploymentNotFound {
        service_id: ServiceId,
        deployment_id: DeploymentId,
    },
    #[error("deployment {service_id}/{deployment_id} already exists")]
    DeploymentExists {
        service_id: ServiceId,
        deployment_id: DeploymentId,
    },
    #[error("service {0} does not exist")]
    ServiceNotFound(ServiceId),
}

impl StoreError {
    pub fn is_rejection(&self) -> bool {
        matches!(
            self,
            Self::DeploymentNotFound { .. }
                | Self::DeploymentExists { .. }
                | Self::ServiceNotFound(_)
        )
    }
}

macro_rules! from_redb_error {
    ($($source:ty),*) => {
        $(impl From<$source> for StoreError {
            fn from(error: $source) -> Self {
                Self::Database(error.into())
            }
        })*
    };
}

from_redb_error!(
    redb::DatabaseError,
    redb::TransactionError,
    redb::TableError,
    redb::StorageError,
    redb::CommitError
);
