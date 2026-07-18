use std::collections::HashMap;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::{Result, anyhow, bail};
use async_trait::async_trait;

use crate::config::{BuilderType, RuntimeType};
use crate::logs::LogEntry;
use crate::supervisor::JobCommand;
use crate::utils::crypto::SecretString;

mod containerd_exec;
pub mod docker;
pub mod nerdctl;

pub const MANAGED_IMAGE_LABEL: (&str, &str) = ("maestro.managed", "true");

pub(crate) fn is_immutable_image_reference(image: &str) -> bool {
    image
        .split_once("@sha256:")
        .is_some_and(|(repository, digest)| {
            !repository.is_empty()
                && digest.len() == 64
                && digest.bytes().all(|byte| byte.is_ascii_hexdigit())
        })
}

pub(crate) fn immutable_image_reference(image: &str, inspect_output: &str) -> Result<String> {
    if is_immutable_image_reference(image) {
        return Ok(image.to_string());
    }

    let inspected: serde_json::Value = serde_json::from_str(inspect_output)
        .map_err(|error| anyhow!("failed to parse image inspection output: {error}"))?;
    let object = inspected
        .as_array()
        .and_then(|items| items.first())
        .or_else(|| inspected.as_object().map(|_| &inspected))
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| anyhow!("image inspection returned no image metadata"))?;
    let repo_digests = object
        .get("RepoDigests")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(serde_json::Value::as_str)
        .filter(|reference| is_immutable_image_reference(reference))
        .collect::<Vec<_>>();
    let repository = image_repository(image);
    repo_digests
        .iter()
        .find(|reference| digest_repository(reference) == repository)
        .or_else(|| {
            if repository.contains('/') {
                None
            } else {
                let mut matches = repo_digests.iter().filter(|reference| {
                    digest_repository(reference).ends_with(&format!("/{repository}"))
                });
                let matched = matches.next()?;
                matches.next().is_none().then_some(matched)
            }
        })
        .copied()
        .map(str::to_string)
        .ok_or_else(|| {
            anyhow!("registry image `{image}` has no matching immutable repository digest")
        })
}

fn image_repository(image: &str) -> &str {
    if let Some((repository, _)) = image.split_once('@') {
        return repository;
    }
    let slash = image.rfind('/');
    match image.rfind(':') {
        Some(colon) if slash.is_none_or(|slash| colon > slash) => &image[..colon],
        _ => image,
    }
}

fn digest_repository(reference: &str) -> &str {
    reference
        .split_once('@')
        .map(|(repository, _)| repository)
        .unwrap_or(reference)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagedContainer {
    pub name: String,
    pub labels: HashMap<String, String>,
}

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
    pub labels: HashMap<String, String>,
    pub build_args: HashMap<String, SecretString>,
    pub secrets: HashMap<String, SecretString>,
    pub command_env: HashMap<String, SecretString>,
    pub builder: BuilderType,
    pub depot_project: Option<String>,
    pub push_to_registry: bool,
}

pub struct InteractiveExecRequest {
    pub container: String,
    pub command: Vec<String>,
    pub tty: bool,
    pub initial_size: Option<crate::exec::TerminalSize>,
    pub session_root: PathBuf,
}

pub type ExecStdin = Pin<Box<dyn tokio::io::AsyncWrite + Send>>;
pub type ExecOutput = Pin<Box<dyn tokio::io::AsyncRead + Send>>;

#[async_trait]
pub trait ExecControl: Send + Sync {
    async fn resize(&self, cols: u16, rows: u16) -> Result<()>;
    async fn wait(&self) -> Result<i32>;
    async fn kill(&self) -> Result<()>;
}

pub struct ExecSession {
    pub stdin: ExecStdin,
    pub output: ExecOutput,
    control: Arc<dyn ExecControl>,
}

impl ExecSession {
    pub fn new(
        stdin: impl tokio::io::AsyncWrite + Send + 'static,
        output: impl tokio::io::AsyncRead + Send + 'static,
        control: Arc<dyn ExecControl>,
    ) -> Self {
        Self {
            stdin: Box::pin(stdin),
            output: Box::pin(output),
            control,
        }
    }

    pub fn control(&self) -> Arc<dyn ExecControl> {
        self.control.clone()
    }

    #[allow(dead_code)]
    pub async fn kill(&self) -> Result<()> {
        self.control.kill().await
    }
}

#[async_trait]
pub trait RuntimeProvider: Send + Sync {
    fn cli_name(&self) -> &str;

    fn requires_explicit_dns(&self) -> bool;

    fn supports_dynamic_network_attachment(&self) -> bool {
        false
    }

    async fn ensure_network(&self, name: &str, subnet: Option<&str>) -> Result<()>;

    async fn remove_network(&self, name: &str) -> Result<()>;

    async fn remove_container(&self, name: &str) -> Result<()>;

    async fn set_container_network_access(
        &self,
        name: &str,
        _network: &str,
        enabled: bool,
        _static_ip: Option<&str>,
    ) -> Result<()> {
        bail!(
            "runtime `{}` cannot {} container `{name}`",
            self.cli_name(),
            if enabled { "enable" } else { "disable" }
        )
    }

    fn run_command(&self, spec: &RunSpec) -> JobCommand;

    async fn inspect_container_ip(&self, name: &str) -> Option<String>;

    async fn inspect_network_cidr(&self, name: &str) -> Option<String>;

    async fn list_managed_containers(&self, _node_id: &str) -> Result<Vec<ManagedContainer>> {
        Ok(Vec::new())
    }

    async fn remove_conflicting_containers(
        &self,
        _network: &str,
        _names: &[String],
        _ips: &[String],
    ) -> Result<()> {
        Ok(())
    }

    async fn prune_images(&self) -> Result<()> {
        Ok(())
    }

    async fn prune_containers(&self) -> Result<()> {
        Ok(())
    }

    async fn build_image(
        &self,
        spec: &BuildSpec,
        log_sender: Option<&flume::Sender<LogEntry>>,
        log_source: Option<&str>,
    ) -> Result<()>;

    async fn pull_image(
        &self,
        image: &str,
        log_sender: Option<&flume::Sender<LogEntry>>,
        log_source: Option<&str>,
    ) -> Result<()>;

    async fn image_exists(&self, _image: &str) -> Result<bool> {
        Ok(false)
    }

    async fn resolve_immutable_image_reference(&self, image: &str) -> Result<String> {
        bail!(
            "runtime `{}` cannot resolve immutable reference for image `{image}`",
            self.cli_name()
        )
    }

    async fn tag_image(&self, source: &str, target: &str) -> Result<()>;

    async fn push_image(&self, tag: &str) -> Result<()>;

    async fn exec_in_container(&self, container: &str, cmd: &[&str]) -> Result<String>;

    async fn interactive_exec(&self, _request: InteractiveExecRequest) -> Result<ExecSession> {
        bail!(
            "interactive exec is not supported for the {} runtime",
            self.cli_name()
        )
    }

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
