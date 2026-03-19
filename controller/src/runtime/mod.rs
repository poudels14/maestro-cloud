use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use crate::config::RuntimeType;
use crate::logs::LogEntry;
use crate::supervisor::JobCommand;

pub mod docker;
pub mod nerdctl;

pub struct RunSpec {
    pub container_name: String,
    pub hostname: String,
    pub dns_domain: Option<String>,
    pub network: String,
    pub extra_flags: Vec<String>,
    pub image_and_args: Vec<String>,
}

pub struct BuildSpec {
    pub context_dir: PathBuf,
    pub tag: String,
    pub dockerfile: Option<String>,
    pub build_args: HashMap<String, String>,
}

#[async_trait]
pub trait RuntimeProvider: Send + Sync {
    fn cli_name(&self) -> &str;

    fn requires_explicit_dns(&self) -> bool;

    async fn ensure_network(&self, name: &str, subnet: Option<&str>) -> Result<()>;

    async fn remove_container(&self, name: &str) -> Result<()>;

    fn run_command(&self, spec: &RunSpec) -> JobCommand;

    async fn inspect_container_ip(&self, name: &str) -> Option<String>;

    async fn inspect_network_cidr(&self, name: &str) -> Option<String>;

    async fn build_image(
        &self,
        spec: &BuildSpec,
        log_sender: Option<&flume::Sender<LogEntry>>,
        log_source: Option<&str>,
    ) -> Result<()>;

    async fn exec_in_container(&self, container: &str, cmd: &[&str]) -> Result<String>;

    async fn remove_image(&self, image_id: &str) -> Result<()>;
}

pub fn create_provider(runtime_type: RuntimeType) -> Arc<dyn RuntimeProvider> {
    match runtime_type {
        RuntimeType::Docker => Arc::new(docker::DockerRuntimeProvider),
        RuntimeType::Nerdctl => Arc::new(nerdctl::NerdctlRuntimeProvider),
    }
}

#[cfg(test)]
#[path = "../tests/runtime/parity.rs"]
mod tests;
