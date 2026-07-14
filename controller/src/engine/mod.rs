use std::path::PathBuf;

use crate::deployment::provider::DeployOutput;
use crate::deployment::types::{GitCommitInfo, ServiceDeployment};
use crate::logs::{LogConfig, LogEntry};

pub mod core;
#[cfg(test)]
pub mod in_memory;
pub mod provider;
pub mod replica_supervisor;

pub use core::Engine;

#[cfg(test)]
#[path = "../tests/lifecycle.rs"]
mod lifecycle_tests;

#[derive(Debug, Clone)]
pub enum Artifact {
    Image {
        tag: String,
    },
    #[allow(dead_code)]
    None,
}

impl Artifact {
    pub fn image_tag(&self) -> Option<&str> {
        match self {
            Artifact::Image { tag } => Some(tag.as_str()),
            Artifact::None => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PreparedDeployment {
    pub deployment: ServiceDeployment,
    pub git_commit: Option<GitCommitInfo>,
    pub build_dir: Option<PathBuf>,
}

#[derive(Clone)]
pub struct LogSink {
    pub sender: Option<flume::Sender<LogEntry>>,
    #[allow(dead_code)]
    pub source: String,
}

impl LogSink {
    pub fn new(sender: Option<flume::Sender<LogEntry>>, source: String) -> Self {
        Self { sender, source }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ReplicaHandle {
    pub task_id: String,
    pub service_id: String,
    pub deployment_id: String,
    pub replica_index: u32,
}

pub struct ReplicaSpec<'a> {
    pub task_id: Option<String>,
    pub deployment: &'a ServiceDeployment,
    pub replica_index: u32,
    pub deploy_output: DeployOutput,
    pub max_restarts: Option<u32>,
    pub restart_delay_ms: u64,
    pub shutdown_grace_period_ms: u64,
    pub container_hostname: String,
    pub runtime_cli: String,
    pub log_config: Option<LogConfig>,
}
