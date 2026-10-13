use maestro_runtime::RuntimeError;
use maestro_store::StoreError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum AgentError {
    #[error(transparent)]
    Runtime(#[from] RuntimeError),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("{0}")]
    Host(String),
}
