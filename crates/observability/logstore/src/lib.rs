//! Durable DuckDB hot-tier storage for normalized Maestro observability data.
//!
//! This crate implements log and metric storage contracts. It must not depend on operators,
//! cluster provisioning, or application composition.

mod delivery_schema;
mod duck;
mod error;
mod metric_delivery_schema;
mod metric_duck;
mod metric_schema;
mod schema;
mod settings;

pub use duck::{DuckLogStore, DuckLogStoreRuntime};
pub use error::DuckStoreError;
pub use metric_duck::{DuckMetricStore, DuckMetricStoreRuntime};
pub use settings::DuckStoreSettings;

#[cfg(test)]
mod tests;
