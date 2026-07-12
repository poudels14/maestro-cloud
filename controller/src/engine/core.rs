//! Generic deployment engine: composes a [`DeploymentProvider`] (what to
//! build / how to deploy) with a [`ReplicaSupervisor`] (how to run replicas).
//!
//! No container-specific code lives here — both axes are pluggable.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;

use super::provider::DeploymentProvider;
use super::replica_supervisor::ReplicaSupervisor;
use super::{Artifact, LogSink, PreparedDeployment, ReplicaHandle, ReplicaSpec};
use crate::deployment::provider::DeployOutput;
use crate::deployment::types::ServiceDeployment;
use crate::supervisor::{
    ContainerRef, ShutdownRequest, SupervisedJobConfig, controller::FinishedJob,
};

pub struct Engine {
    provider: Arc<dyn DeploymentProvider>,
    supervisor: Arc<dyn ReplicaSupervisor>,
    data_dir: PathBuf,
}

impl Engine {
    pub fn new(
        provider: Arc<dyn DeploymentProvider>,
        supervisor: Arc<dyn ReplicaSupervisor>,
        data_dir: PathBuf,
    ) -> Self {
        Self {
            provider,
            supervisor,
            data_dir,
        }
    }

    pub fn build_dir_for(&self, deployment: &ServiceDeployment) -> PathBuf {
        let short_id: String = deployment.id.chars().take(6).collect();
        self.data_dir
            .join("tmp")
            .join(&deployment.config.id)
            .join(short_id)
    }

    fn image_tag_for(&self, deployment: &ServiceDeployment) -> String {
        let short_id: String = deployment.id.chars().take(6).collect();
        format!("{}:{}", deployment.config.id, short_id)
    }

    pub async fn prepare(
        &self,
        deployment: &ServiceDeployment,
        logs: &LogSink,
    ) -> Result<PreparedDeployment> {
        let build_dir = self.build_dir_for(deployment);
        let git_commit = self
            .provider
            .setup(deployment, &build_dir, logs.sender.as_ref())
            .await?;
        Ok(PreparedDeployment {
            deployment: deployment.clone(),
            git_commit,
            build_dir: Some(build_dir),
        })
    }

    pub async fn build(&self, prep: &PreparedDeployment, logs: &LogSink) -> Result<Artifact> {
        let build_dir = prep
            .build_dir
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("engine requires a build directory"))?;
        let image_tag = self.image_tag_for(&prep.deployment);
        let output = self
            .provider
            .build(&prep.deployment, build_dir, &image_tag, logs.sender.clone())
            .await?;
        Ok(Artifact::Image {
            tag: output.image_tag,
        })
    }

    pub fn deploy_command(
        &self,
        deployment: &ServiceDeployment,
        replica_index: u32,
    ) -> Option<DeployOutput> {
        self.provider.deploy(deployment, replica_index)
    }

    pub async fn start_replica(&self, spec: ReplicaSpec<'_>) -> Result<Option<ReplicaHandle>> {
        let job_id = format!("{}-replica-{}", spec.deployment.id, spec.replica_index);
        let job = SupervisedJobConfig {
            id: job_id.clone(),
            name: format!(
                "{}/{}/replica{}",
                spec.deployment.config.id, spec.deployment.id, spec.replica_index,
            ),
            command: spec.deploy_output.command,
            restart_delay_ms: spec.restart_delay_ms,
            max_restart_delay_ms: None,
            max_restarts: spec.max_restarts,
            shutdown_grace_period_ms: spec.shutdown_grace_period_ms,
            container: Some(ContainerRef {
                name: spec.container_hostname,
                runtime_cli: spec.runtime_cli,
            }),
            secrets_mount: spec.deploy_output.secrets_mount,
            log_config: spec.log_config,
        };
        Ok(self
            .supervisor
            .start_job(job)
            .await
            .map(|task_id| ReplicaHandle {
                task_id,
                service_id: spec.deployment.config.id.clone(),
                deployment_id: spec.deployment.id.clone(),
                replica_index: spec.replica_index,
            }))
    }

    pub async fn stop_replica(&self, handle: &ReplicaHandle, request: ShutdownRequest) -> bool {
        self.supervisor.shutdown_job(&handle.task_id, request).await
    }

    pub async fn reap_finished_replicas(&self) -> Vec<FinishedJob> {
        self.supervisor.reap_finished_jobs().await
    }

    pub async fn has_running_replicas(&self) -> bool {
        self.supervisor.has_jobs().await
    }

    pub async fn shutdown_all_replicas(&self, request: ShutdownRequest) -> Vec<FinishedJob> {
        self.supervisor.shutdown_all(request).await
    }

    pub async fn cleanup(
        &self,
        deployment: &ServiceDeployment,
        _artifact: Option<&Artifact>,
    ) -> Result<()> {
        let build_dir = self.build_dir_for(deployment);
        if build_dir.exists() {
            let _ = std::fs::remove_dir_all(&build_dir);
        }
        self.provider.cleanup(deployment).await
    }
}
