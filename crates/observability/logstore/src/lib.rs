//! Durable DuckDB hot-tier storage for normalized Maestro observability data.
//!
//! This crate implements log and metric storage contracts. It must not depend on operators,
//! cluster provisioning, or application composition.

mod delivery_schema;
mod duck;
mod duck_worker;
mod error;
mod host_metric_delivery_schema;
mod host_metric_duck;
mod host_metric_schema;
mod metric_delivery_schema;
mod metric_duck;
mod metric_schema;
mod schema;
mod settings;
mod workload_metric_schema;

pub use duck::{DuckLogStore, DuckLogStoreRuntime};
pub use error::DuckStoreError;
pub use metric_duck::{DuckMetricStore, DuckMetricStoreRuntime};
pub use settings::DuckStoreSettings;

#[cfg(test)]
mod tests;
