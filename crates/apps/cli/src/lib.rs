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
mod cluster_config_document;
mod cluster_config_launch;
mod cluster_cutover;
mod cluster_formation;
mod cluster_join;
mod cluster_prepare;
mod cluster_restart;
mod cluster_tailscale;
mod command;
mod config;
mod config_source;
mod contexts;
mod daemon_command;
mod deployments;
mod error;
mod exec_command;
mod launch_document;
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

pub use cluster_config::LoadedClusterConfig;
pub use cluster_prepare::{NodeLaunchOptions, prepare_node_launch};
pub use command::{Cli, run};
pub use config::{
    ClusterConfigFallbacks, load_cluster, load_cluster_for_node,
    load_cluster_for_node_with_fallbacks, load_cluster_with_fallbacks,
};
pub use config_source::{ConfigSourceReader, SystemConfigSourceReader};
pub use error::CliError;

#[cfg(test)]
mod tests;
