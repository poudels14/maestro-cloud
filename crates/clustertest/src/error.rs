/// A failure produced while exercising a shared acceptance scenario.
#[derive(Debug, thiserror::Error)]
pub enum ScenarioError {
    /// The system-specific driver rejected an operation.
    #[error("acceptance driver failed during {operation}: {message}")]
    Driver {
        /// The operation that failed.
        operation: &'static str,
        /// The driver's error rendered without erasing the operation context.
        message: String,
    },

    /// The converged state did not satisfy the behavioral contract.
    #[error("acceptance assertion failed: {0}")]
    Assertion(String),
}
