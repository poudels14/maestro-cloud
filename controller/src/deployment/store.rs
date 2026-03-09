use anyhow::{Result, bail};
use async_trait::async_trait;

use crate::deployment::types::{ServiceDeployment, ServiceInfo};

#[derive(Debug, Clone)]
pub struct QueuedDeployment {
    pub service_id: String,
    pub key: String,
    pub mod_revision: u64,
    pub deployment: ServiceDeployment,
}

#[derive(Debug, Clone)]
pub struct ForceQueueOutcome {
    pub deployment_index: usize,
    pub deployment: ServiceDeployment,
}

#[derive(Debug, Clone)]
pub enum UpsertServiceOutcome {
    Queued {
        deployment_index: usize,
        deployment: ServiceDeployment,
    },
    Unchanged {
        service_id: String,
        version: String,
    },
}

#[derive(Debug, Clone)]
pub enum CancelDeploymentOutcome {
    Canceled(ServiceDeployment),
    NotCancelable(ServiceDeployment),
    NotFound,
}

#[async_trait]
pub trait ClusterStore: Send + Sync {
    async fn list_service_ids(&self) -> Result<Vec<String>>;
    async fn list_queued_deployments(&self) -> Result<Vec<QueuedDeployment>>;
    async fn claim_deployment_building(&self, queued_deployment: &QueuedDeployment)
    -> Result<bool>;
    async fn mark_deployment_ready(
        &self,
        service_id: &str,
        deployment_key: &str,
        deployment_id: &str,
    ) -> Result<()>;
    async fn mark_deployment_crashed(&self, service_id: &str, deployment_id: &str) -> Result<()>;
    async fn mark_deployment_terminated(&self, service_id: &str, deployment_id: &str)
    -> Result<()>;

    async fn list_service_infos(&self) -> Result<Vec<ServiceInfo>> {
        bail!("list_service_infos not implemented")
    }

    async fn list_service_deployments(&self, _service_id: &str) -> Result<Vec<ServiceDeployment>> {
        bail!("list_service_deployments not implemented")
    }

    async fn read_service_info(&self, _service_id: &str) -> Result<Option<ServiceInfo>> {
        bail!("read_service_info not implemented")
    }

    async fn read_service_deployment(
        &self,
        _service_id: &str,
        _deployment_id: &str,
    ) -> Result<Option<ServiceDeployment>> {
        bail!("read_service_deployment not implemented")
    }

    async fn queue_deployment(&self, _deployment: ServiceDeployment) -> Result<ForceQueueOutcome> {
        bail!("queue_deployment not implemented")
    }

    async fn cancel_service_deployment(
        &self,
        _service_id: &str,
        _deployment_id: &str,
    ) -> Result<CancelDeploymentOutcome> {
        bail!("cancel_service_deployment not implemented")
    }

    async fn stop_service_deployment(
        &self,
        _service_id: &str,
        _deployment_id: &str,
    ) -> Result<Option<ServiceDeployment>> {
        bail!("stop_service_deployment not implemented")
    }
}
