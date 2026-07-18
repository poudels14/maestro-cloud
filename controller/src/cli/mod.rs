pub mod auth;
pub mod cancel;
pub mod cluster_lifecycle;
pub mod config;
pub mod confirm;
pub mod contexts;
pub mod dead_letters;
pub mod info;
pub mod logs;
pub mod nodes;
pub mod redeploy;
pub mod restart;
pub mod rollout;
pub mod services;
pub mod ssh;
pub mod up;
pub mod upgrade;

pub(crate) fn idempotent(request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
    request.header("Idempotency-Key", crate::utils::nanoid::unique_id(32))
}

#[cfg(test)]
#[path = "../tests/cli/mod.rs"]
mod tests;
