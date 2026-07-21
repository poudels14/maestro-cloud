//! Operator CLI for the rewritten Maestro API.
//!
//! The CLI is an API client only. It must not reach into daemon internals,
//! cluster storage, or operator implementation modules.

mod api_client;
mod command;
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
