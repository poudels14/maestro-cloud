pub mod auth;
pub mod cancel;
pub mod config;
pub mod confirm;
pub mod contexts;
pub mod info;
pub mod logs;
pub mod redeploy;
pub mod restart;
pub mod rollout;
pub mod services;
pub mod upgrade;

#[cfg(test)]
#[path = "../tests/cli/mod.rs"]
mod tests;
