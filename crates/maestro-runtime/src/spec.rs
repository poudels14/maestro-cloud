use std::collections::BTreeMap;
use std::future::Future;
use std::net::IpAddr;
use std::path::PathBuf;
use std::time::Duration;

use maestro_core::{ByteSize, Millicores};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::mpsc;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContainerSpec {
    pub id: String,
    pub image: String,
    pub entrypoint: Option<Vec<String>>,
    pub args: Option<Vec<String>>,
    pub env: BTreeMap<String, String>,
    pub labels: BTreeMap<String, String>,
    pub limits: ContainerLimits,
    pub cgroup_parent: String,
    pub mounts: Vec<BindMount>,
    pub network: NetworkMode,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecRequest {
    pub container_id: String,
    pub argv: Vec<String>,
    pub tty: bool,
    pub term: Option<String>,
    pub size: Option<(u16, u16)>,
}

pub struct ExecSession {
    pub container_id: String,
    pub exec_id: String,
    pub stdin: Option<tokio::net::unix::pipe::Sender>,
    pub output: mpsc::Receiver<Vec<u8>>,
    pub exit: tokio::sync::oneshot::Receiver<Result<u32, RuntimeError>>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum NetworkMode {
    #[default]
    Bridge,
    Host,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ContainerLimits {
    pub cpu: Millicores,
    pub memory: ByteSize,
    pub pids: u32,
    pub oom_score_adj: i32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BindMount {
    pub source: PathBuf,
    pub destination: String,
    pub read_only: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ImageConfig {
    #[serde(default)]
    pub env: Option<Vec<String>>,
    #[serde(default)]
    pub entrypoint: Option<Vec<String>>,
    #[serde(default)]
    pub cmd: Option<Vec<String>>,
    #[serde(default)]
    pub working_dir: Option<String>,
    #[serde(default)]
    pub user: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContainerFiles {
    pub hosts: PathBuf,
    pub hostname: PathBuf,
    pub resolv_conf: PathBuf,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StartedContainer {
    pub id: String,
    pub ip: Option<IpAddr>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContainerStatus {
    pub id: String,
    pub labels: BTreeMap<String, String>,
    pub state: ContainerState,
    pub ip: Option<IpAddr>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ContainerState {
    Created,
    Running,
    Stopped { exit_code: u32 },
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RuntimeEvent {
    Exited { id: String, exit_code: u32 },
    OomKilled { id: String },
}

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("containerd request failed: {0}")]
    Rpc(#[from] containerd_client::tonic::Status),
    #[error("failed to connect to containerd: {0}")]
    Connect(String),
    #[error("image {image} is not available: {reason}")]
    Image { image: String, reason: String },
    #[error("container {0} does not exist")]
    NotFound(String),
    #[error("container network setup failed: {0}")]
    Network(String),
    #[error("cannot resolve image user {0}")]
    UnsupportedUser(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

pub trait Runtime: Send + Sync + 'static {
    fn pull(&self, image: &str) -> impl Future<Output = Result<(), RuntimeError>> + Send;

    fn start(
        &self,
        spec: &ContainerSpec,
    ) -> impl Future<Output = Result<StartedContainer, RuntimeError>> + Send;

    fn stop(
        &self,
        id: &str,
        grace: Duration,
    ) -> impl Future<Output = Result<(), RuntimeError>> + Send;

    fn remove(&self, id: &str) -> impl Future<Output = Result<(), RuntimeError>> + Send;

    fn list(&self) -> impl Future<Output = Result<Vec<ContainerStatus>, RuntimeError>> + Send;

    fn events(
        &self,
    ) -> impl Future<Output = Result<mpsc::Receiver<RuntimeEvent>, RuntimeError>> + Send;
}
