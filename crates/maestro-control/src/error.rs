use maestro_core::SpecError;
use maestro_store::{ConsensusError, SecretError, StoreError};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ControlError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Consensus(#[from] ConsensusError),
    #[error(transparent)]
    Spec(#[from] SpecError),
    #[error(transparent)]
    Secret(#[from] SecretError),
    #[error("{0} not found")]
    NotFound(String),
}
