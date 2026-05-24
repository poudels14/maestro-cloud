use std::path::PathBuf;

use anyhow::Result;
use async_trait::async_trait;

use super::{Artifact, DeploymentEngine, LogSink, PreparedDeployment};
use crate::deployment::provider::{ContainerDeploymentProvider, ServiceCommandPlanner};
use crate::deployment::types::ServiceDeployment;

pub struct ContainerEngine {
    provider: ContainerDeploymentProvider,
    data_dir: PathBuf,
}

impl ContainerEngine {
    pub fn new(provider: ContainerDeploymentProvider, data_dir: PathBuf) -> Self {
        Self { provider, data_dir }
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
}

#[async_trait]
impl DeploymentEngine for ContainerEngine {
    async fn prepare(
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

    async fn build(&self, prep: &PreparedDeployment, logs: &LogSink) -> Result<Artifact> {
        let build_dir = prep
            .build_dir
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("container engine requires a build directory"))?;
        let image_tag = self.image_tag_for(&prep.deployment);
        let output = self
            .provider
            .build(&prep.deployment, build_dir, &image_tag, logs.sender.clone())
            .await?;
        Ok(Artifact::Image {
            tag: output.image_tag,
        })
    }

    async fn cleanup(
        &self,
        deployment: &ServiceDeployment,
        _artifact: Option<&Artifact>,
    ) -> Result<()> {
        let build_dir = self.build_dir_for(deployment);
        if build_dir.exists() {
            let _ = std::fs::remove_dir_all(&build_dir);
        }
        if let Some(archive_filename) = deployment.upload_archive.as_deref() {
            let archive_path = self.provider.uploads_dir.join(archive_filename);
            if archive_path.exists() {
                let _ = std::fs::remove_file(&archive_path);
            }
        }
        Ok(())
    }
}
