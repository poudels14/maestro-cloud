use maestro_runtime::RuntimeError;
use maestro_store::{PlanError, SecretError, StoreError};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum AgentError {
    #[error(transparent)]
    Runtime(#[from] RuntimeError),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Secret(#[from] SecretError),
    #[error(transparent)]
    Plan(#[from] PlanError),
    #[error("{0}")]
    Host(String),
}
