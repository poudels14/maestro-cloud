use std::path::PathBuf;

use anyhow::Result;
use async_trait::async_trait;

use crate::deployment::types::{GitCommitInfo, ServiceDeployment};
use crate::logs::LogEntry;

pub mod container;
#[cfg(test)]
pub mod in_memory;

#[derive(Debug, Clone)]
pub enum Artifact {
    Image { tag: String },
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

#[async_trait]
pub trait DeploymentEngine: Send + Sync {
    async fn prepare(
        &self,
        deployment: &ServiceDeployment,
        logs: &LogSink,
    ) -> Result<PreparedDeployment>;

    async fn build(&self, prep: &PreparedDeployment, logs: &LogSink) -> Result<Artifact>;

    async fn cleanup(
        &self,
        deployment: &ServiceDeployment,
        artifact: Option<&Artifact>,
    ) -> Result<()>;
}
