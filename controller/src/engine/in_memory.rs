use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use anyhow::{Result, anyhow};
use async_trait::async_trait;

use super::{Artifact, DeploymentEngine, LogSink, PreparedDeployment};
use crate::deployment::types::{GitCommitInfo, ServiceDeployment};

#[derive(Debug, Clone, Default)]
pub struct InMemoryEngine {
    state: Arc<Mutex<EngineState>>,
}

#[derive(Debug, Default)]
struct EngineState {
    prepared: HashMap<String, PreparedDeployment>,
    built: HashMap<String, Artifact>,
    cleaned_up: Vec<String>,
    prepare_overrides: HashMap<String, PrepareOutcome>,
    build_overrides: HashMap<String, BuildOutcome>,
    next_image_id: u32,
}

#[derive(Debug, Clone)]
enum PrepareOutcome {
    Ok { git_commit: Option<GitCommitInfo> },
    Err(String),
}

#[derive(Debug, Clone)]
enum BuildOutcome {
    Ok(Artifact),
    Err(String),
}

impl InMemoryEngine {
    pub fn new() -> Self {
        Self::default()
    }

    #[allow(dead_code)]
    pub fn set_prepare_ok(&self, deployment_id: &str, git_commit: Option<GitCommitInfo>) {
        let mut state = self.state.lock().expect("engine state");
        state
            .prepare_overrides
            .insert(deployment_id.to_string(), PrepareOutcome::Ok { git_commit });
    }

    pub fn set_prepare_err(&self, deployment_id: &str, message: impl Into<String>) {
        let mut state = self.state.lock().expect("engine state");
        state.prepare_overrides.insert(
            deployment_id.to_string(),
            PrepareOutcome::Err(message.into()),
        );
    }

    #[allow(dead_code)]
    pub fn set_build_ok(&self, deployment_id: &str, artifact: Artifact) {
        let mut state = self.state.lock().expect("engine state");
        state
            .build_overrides
            .insert(deployment_id.to_string(), BuildOutcome::Ok(artifact));
    }

    pub fn set_build_err(&self, deployment_id: &str, message: impl Into<String>) {
        let mut state = self.state.lock().expect("engine state");
        state
            .build_overrides
            .insert(deployment_id.to_string(), BuildOutcome::Err(message.into()));
    }

    pub fn prepared_ids(&self) -> Vec<String> {
        self.state
            .lock()
            .expect("engine state")
            .prepared
            .keys()
            .cloned()
            .collect()
    }

    pub fn built_artifact(&self, deployment_id: &str) -> Option<Artifact> {
        self.state
            .lock()
            .expect("engine state")
            .built
            .get(deployment_id)
            .cloned()
    }

    pub fn cleaned_up_ids(&self) -> Vec<String> {
        self.state.lock().expect("engine state").cleaned_up.clone()
    }
}

#[async_trait]
impl DeploymentEngine for InMemoryEngine {
    async fn prepare(
        &self,
        deployment: &ServiceDeployment,
        _logs: &LogSink,
    ) -> Result<PreparedDeployment> {
        let outcome = {
            let state = self.state.lock().expect("engine state");
            state
                .prepare_overrides
                .get(&deployment.id)
                .cloned()
                .unwrap_or(PrepareOutcome::Ok { git_commit: None })
        };
        match outcome {
            PrepareOutcome::Err(message) => Err(anyhow!(message)),
            PrepareOutcome::Ok { git_commit } => {
                let prep = PreparedDeployment {
                    deployment: deployment.clone(),
                    git_commit,
                    build_dir: None,
                };
                self.state
                    .lock()
                    .expect("engine state")
                    .prepared
                    .insert(deployment.id.clone(), prep.clone());
                Ok(prep)
            }
        }
    }

    async fn build(&self, prep: &PreparedDeployment, _logs: &LogSink) -> Result<Artifact> {
        let deployment_id = prep.deployment.id.clone();
        let outcome = {
            let mut state = self.state.lock().expect("engine state");
            if let Some(outcome) = state.build_overrides.get(&deployment_id).cloned() {
                outcome
            } else {
                state.next_image_id += 1;
                let tag = format!("inmem-image:{:04}", state.next_image_id);
                BuildOutcome::Ok(Artifact::Image { tag })
            }
        };
        match outcome {
            BuildOutcome::Err(message) => Err(anyhow!(message)),
            BuildOutcome::Ok(artifact) => {
                self.state
                    .lock()
                    .expect("engine state")
                    .built
                    .insert(deployment_id, artifact.clone());
                Ok(artifact)
            }
        }
    }

    async fn cleanup(
        &self,
        deployment: &ServiceDeployment,
        _artifact: Option<&Artifact>,
    ) -> Result<()> {
        let mut state = self.state.lock().expect("engine state");
        state.cleaned_up.push(deployment.id.clone());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::deployment::types::{
        DeploymentStatus, ServiceConfig, ServiceDeployConfig, ServiceProvider,
    };

    fn deployment(id: &str) -> ServiceDeployment {
        ServiceDeployment {
            id: id.to_string(),
            created_at: 1,
            deployed_at: None,
            drained_at: None,
            status: DeploymentStatus::Queued,
            config: ServiceConfig {
                id: "svc".to_string(),
                name: "svc".to_string(),
                version: "v1".to_string(),
                provider: ServiceProvider::Docker,
                build: None,
                image: Some("placeholder".to_string()),
                deploy: ServiceDeployConfig {
                    flags: vec![],
                    expose_ports: vec![],
                    command: None,
                    healthcheck_path: None,
                    healthcheck_interval: 60,
                    replicas: 1,
                    max_restarts: None,
                    env: Default::default(),
                    secrets: None,
                    volumes: vec![],
                },
                ingress: None,
            },
            git_commit: None,
            build: None,
            upload_archive: None,
        }
    }

    fn logs() -> LogSink {
        LogSink::new(None, String::new())
    }

    #[tokio::test]
    async fn default_prepare_succeeds_and_records_deployment() {
        let engine = InMemoryEngine::new();
        let dep = deployment("DEP1");
        let prep = engine.prepare(&dep, &logs()).await.expect("prepare");
        assert_eq!(prep.deployment.id, "DEP1");
        assert!(prep.git_commit.is_none());
        assert_eq!(engine.prepared_ids(), vec!["DEP1".to_string()]);
    }

    #[tokio::test]
    async fn prepare_err_override_is_returned() {
        let engine = InMemoryEngine::new();
        engine.set_prepare_err("DEP2", "git fetch exploded");
        let err = engine
            .prepare(&deployment("DEP2"), &logs())
            .await
            .expect_err("should fail");
        assert!(err.to_string().contains("git fetch exploded"));
        assert!(engine.prepared_ids().is_empty());
    }

    #[tokio::test]
    async fn build_returns_synthetic_image_tag_by_default() {
        let engine = InMemoryEngine::new();
        let prep = engine
            .prepare(&deployment("DEP3"), &logs())
            .await
            .expect("prepare");
        let artifact = engine.build(&prep, &logs()).await.expect("build");
        match &artifact {
            Artifact::Image { tag } => assert!(tag.starts_with("inmem-image:")),
            other => panic!("expected image, got {other:?}"),
        }
        assert!(engine.built_artifact("DEP3").is_some());
    }

    #[tokio::test]
    async fn build_err_override_propagates() {
        let engine = InMemoryEngine::new();
        engine.set_build_err("DEP4", "dockerfile syntax error");
        let prep = engine
            .prepare(&deployment("DEP4"), &logs())
            .await
            .expect("prepare");
        let err = engine.build(&prep, &logs()).await.expect_err("should fail");
        assert!(err.to_string().contains("dockerfile syntax error"));
        assert!(engine.built_artifact("DEP4").is_none());
    }

    #[tokio::test]
    async fn cleanup_records_deployment_ids() {
        let engine = InMemoryEngine::new();
        engine
            .cleanup(&deployment("DEP5"), None)
            .await
            .expect("cleanup");
        engine
            .cleanup(&deployment("DEP6"), None)
            .await
            .expect("cleanup");
        assert_eq!(
            engine.cleaned_up_ids(),
            vec!["DEP5".to_string(), "DEP6".to_string()],
        );
    }
}
