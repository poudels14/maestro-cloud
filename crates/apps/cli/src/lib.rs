//! Operator CLI for the rewritten Maestro API.
//!
//! The CLI is an API client only. It must not reach into daemon internals,
//! cluster storage, or operator implementation modules.

mod api_client;
mod command;
mod contexts;
mod error;
mod login;
mod services;

pub use command::{Cli, run};
pub use error::CliError;

#[cfg(test)]
mod tests;
