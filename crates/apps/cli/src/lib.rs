//! Operator CLI for the rewritten Maestro API.
//!
//! Operational commands use only the public HTTP API. Local config validation
//! may use public cluster contracts, but this crate must not reach into daemon
//! internals, cluster storage, or operator implementation modules.

mod api_client;
mod cluster_config;
mod command;
mod config;
mod config_source;
mod contexts;
mod error;
mod login;
mod rollout;
mod service_config;
mod service_config_convert;
mod services;

pub use command::{Cli, run};
pub use error::CliError;

#[cfg(test)]
mod tests;
