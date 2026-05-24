//! In-memory [`DeploymentProvider`] for tests — no docker, no subprocesses.
//! Pair this with [`super::replica_supervisor::fake::InMemoryReplicaSupervisor`]
//! to drive [`super::engine::Engine`] entirely in memory.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::{Arc, Mutex};

use anyhow::{Result, anyhow};
use async_trait::async_trait;

use super::provider::DeploymentProvider;
use crate::deployment::provider::{BuildOutput, DeployOutput};
use crate::deployment::types::{GitCommitInfo, ServiceDeployment};
use crate::logs::LogEntry;
use crate::supervisor::JobCommand;

#[derive(Debug, Clone, Default)]
pub struct InMemoryProvider {
    state: Arc<Mutex<ProviderState>>,
}

#[derive(Debug, Default)]
struct ProviderState {
    prepared: HashSet<String>,
    built: HashMap<String, String>, // deployment_id → image tag
    cleaned_up: Vec<String>,
    prepare_overrides: HashMap<String, PrepareOutcome>,
    build_overrides: HashMap<String, BuildOutcome>,
    hanging_builds: HashSet<String>,
    next_image_id: u32,
}

#[derive(Debug, Clone)]
enum PrepareOutcome {
    Ok { git_commit: Option<GitCommitInfo> },
    Err(String),
}

#[derive(Debug, Clone)]
enum BuildOutcome {
    Ok(String),
    Err(String),
    Hang,
}

impl InMemoryProvider {
    pub fn new() -> Self {
        Self::default()
    }

    #[allow(dead_code)]
    pub fn set_prepare_ok(&self, deployment_id: &str, git_commit: Option<GitCommitInfo>) {
        self.state
            .lock()
            .expect("provider state")
            .prepare_overrides
            .insert(deployment_id.to_string(), PrepareOutcome::Ok { git_commit });
    }

    pub fn set_prepare_err(&self, deployment_id: &str, message: impl Into<String>) {
        self.state
            .lock()
            .expect("provider state")
            .prepare_overrides
            .insert(
                deployment_id.to_string(),
                PrepareOutcome::Err(message.into()),
            );
    }

    pub fn set_build_ok(&self, deployment_id: &str, tag: impl Into<String>) {
        self.state
            .lock()
            .expect("provider state")
            .build_overrides
            .insert(deployment_id.to_string(), BuildOutcome::Ok(tag.into()));
    }

    pub fn set_build_err(&self, deployment_id: &str, message: impl Into<String>) {
        self.state
            .lock()
            .expect("provider state")
            .build_overrides
            .insert(deployment_id.to_string(), BuildOutcome::Err(message.into()));
    }

    pub fn set_build_hanging(&self, deployment_id: &str) {
        self.state
            .lock()
            .expect("provider state")
            .hanging_builds
            .insert(deployment_id.to_string());
    }

    pub fn prepared_ids(&self) -> Vec<String> {
        self.state
            .lock()
            .expect("provider state")
            .prepared
            .iter()
            .cloned()
            .collect()
    }

    pub fn built_image_tag(&self, deployment_id: &str) -> Option<String> {
        self.state
            .lock()
            .expect("provider state")
            .built
            .get(deployment_id)
            .cloned()
    }

    pub fn cleaned_up_ids(&self) -> Vec<String> {
        self.state
            .lock()
            .expect("provider state")
            .cleaned_up
            .clone()
    }
}

#[async_trait]
impl DeploymentProvider for InMemoryProvider {
    async fn setup(
        &self,
        deployment: &ServiceDeployment,
        _build_dir: &Path,
        _log_sender: Option<&flume::Sender<LogEntry>>,
    ) -> Result<Option<GitCommitInfo>> {
        let outcome = {
            self.state
                .lock()
                .expect("provider state")
                .prepare_overrides
                .get(&deployment.id)
                .cloned()
                .unwrap_or(PrepareOutcome::Ok { git_commit: None })
        };
        match outcome {
            PrepareOutcome::Err(message) => Err(anyhow!(message)),
            PrepareOutcome::Ok { git_commit } => {
                self.state
                    .lock()
                    .expect("provider state")
                    .prepared
                    .insert(deployment.id.clone());
                Ok(git_commit)
            }
        }
    }

    async fn build(
        &self,
        deployment: &ServiceDeployment,
        _build_dir: &Path,
        _image_tag: &str,
        _log_sender: Option<flume::Sender<LogEntry>>,
    ) -> Result<BuildOutput> {
        let deployment_id = deployment.id.clone();
        let outcome = {
            let mut state = self.state.lock().expect("provider state");
            if state.hanging_builds.contains(&deployment_id) {
                BuildOutcome::Hang
            } else if let Some(o) = state.build_overrides.get(&deployment_id).cloned() {
                o
            } else {
                state.next_image_id += 1;
                BuildOutcome::Ok(format!("inmem-image:{:04}", state.next_image_id))
            }
        };
        match outcome {
            BuildOutcome::Err(message) => Err(anyhow!(message)),
            BuildOutcome::Hang => loop {
                tokio::time::sleep(std::time::Duration::from_secs(60)).await;
            },
            BuildOutcome::Ok(tag) => {
                self.state
                    .lock()
                    .expect("provider state")
                    .built
                    .insert(deployment_id, tag.clone());
                Ok(BuildOutput { image_tag: tag })
            }
        }
    }

    fn deploy(&self, deployment: &ServiceDeployment, _replica_index: u32) -> Option<DeployOutput> {
        Some(DeployOutput {
            command: JobCommand::Shell(format!("inmem-noop:{}", deployment.id)),
            secrets_mount: None,
        })
    }

    async fn cleanup(&self, deployment: &ServiceDeployment) -> Result<()> {
        self.state
            .lock()
            .expect("provider state")
            .cleaned_up
            .push(deployment.id.clone());
        Ok(())
    }
}
