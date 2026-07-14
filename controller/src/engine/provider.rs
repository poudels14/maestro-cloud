//! [`DeploymentProvider`]: how a particular deployment style produces its
//! source code (`setup`), its runnable artifact (`build`), the command to run
//! a replica (`deploy`), and how to clean up after itself.
//!
//! Production wires [`ContainerDeploymentProvider`] (Docker/OCI images).
//! Tests can wire an in-memory fake.

use std::path::Path;

use anyhow::Result;
use async_trait::async_trait;

use crate::deployment::provider::{
    BuildOutput, ContainerDeploymentProvider, DeployOutput, ReplicaRuntimeIdentity,
};
use crate::deployment::types::{GitCommitInfo, ServiceDeployment};
use crate::logs::LogEntry;

#[async_trait]
pub trait DeploymentProvider: Send + Sync {
    async fn setup(
        &self,
        deployment: &ServiceDeployment,
        build_dir: &Path,
        log_sender: Option<&flume::Sender<LogEntry>>,
    ) -> Result<Option<GitCommitInfo>>;

    async fn build(
        &self,
        deployment: &ServiceDeployment,
        build_dir: &Path,
        image_tag: &str,
        log_sender: Option<flume::Sender<LogEntry>>,
    ) -> Result<BuildOutput>;

    fn deploy(&self, deployment: &ServiceDeployment, replica_index: u32) -> Option<DeployOutput>;

    fn deploy_with_identity(
        &self,
        deployment: &ServiceDeployment,
        replica_index: u32,
        identity: &ReplicaRuntimeIdentity,
    ) -> Option<DeployOutput> {
        let _ = identity;
        self.deploy(deployment, replica_index)
    }

    /// Provider-specific cleanup after a deployment is terminal — e.g., remove
    /// upload archive, prune images. The engine still removes its own
    /// build_dir; this is for resources the provider owns.
    async fn cleanup(&self, deployment: &ServiceDeployment) -> Result<()>;
}

#[async_trait]
impl DeploymentProvider for ContainerDeploymentProvider {
    async fn setup(
        &self,
        deployment: &ServiceDeployment,
        build_dir: &Path,
        log_sender: Option<&flume::Sender<LogEntry>>,
    ) -> Result<Option<GitCommitInfo>> {
        ContainerDeploymentProvider::setup(self, deployment, build_dir, log_sender).await
    }

    async fn build(
        &self,
        deployment: &ServiceDeployment,
        build_dir: &Path,
        image_tag: &str,
        log_sender: Option<flume::Sender<LogEntry>>,
    ) -> Result<BuildOutput> {
        ContainerDeploymentProvider::build(self, deployment, build_dir, image_tag, log_sender).await
    }

    fn deploy(&self, deployment: &ServiceDeployment, replica_index: u32) -> Option<DeployOutput> {
        ContainerDeploymentProvider::deploy(self, deployment, replica_index)
    }

    fn deploy_with_identity(
        &self,
        deployment: &ServiceDeployment,
        replica_index: u32,
        identity: &ReplicaRuntimeIdentity,
    ) -> Option<DeployOutput> {
        ContainerDeploymentProvider::deploy_with_identity(
            self,
            deployment,
            replica_index,
            Some(identity),
        )
    }

    async fn cleanup(&self, deployment: &ServiceDeployment) -> Result<()> {
        if let Some(archive_filename) = deployment.upload_archive.as_deref() {
            let archive_path = self.uploads_dir.join(archive_filename);
            if archive_path.exists() {
                let _ = std::fs::remove_file(&archive_path);
            }
        }
        Ok(())
    }
}
