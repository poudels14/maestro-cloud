//! Durable DuckDB hot-tier storage for normalized Maestro logs.
//!
//! This crate implements observability storage contracts. It may depend on `logs`, but must not
//! depend on runtimes, node agents, operators, cluster provisioning, or application composition.

mod duck;
mod error;
mod schema;
mod settings;

pub use duck::{DuckLogStore, DuckLogStoreRuntime};
pub use error::DuckLogStoreError;
pub use settings::DuckLogStoreSettings;

#[cfg(test)]
mod tests;
