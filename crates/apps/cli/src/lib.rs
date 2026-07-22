//! Operator CLI for the rewritten Maestro API.
//!
//! Operational commands use only the public HTTP API. Local configuration and
//! formation commands may use public cluster contracts, but this crate must not
//! reach into daemon internals, cluster storage, or operator implementation
//! modules.

mod api_client;
mod archive;
mod cluster;
mod cluster_command;
mod cluster_config;
mod cluster_formation;
mod cluster_join;
mod command;
mod config;
mod config_source;
mod contexts;
mod deployments;
mod error;
mod exec_command;
mod log_command;
mod login;
mod private_document;
mod rollout;
mod service_config;
mod service_config_convert;
mod services;
mod services_command;
mod up;
mod upgrades;

pub use command::{Cli, run};
pub use error::CliError;

#[cfg(test)]
mod tests;
