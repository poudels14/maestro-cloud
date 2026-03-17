use anyhow::{Result, bail};
use async_trait::async_trait;

use crate::deployment::types::{
    CancelDeploymentOutcome, Deployment, DeploymentStatus, DeploymentWithReplicas,
    ForceQueueOutcome, QueuedDeployment, ReplicaState, ServiceConfig, ServiceDeployment,
    ServiceInfo,
};

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
    Scaled {
        service_id: String,
        version: String,
        replicas: u32,
    },
}

#[async_trait]
pub trait ClusterStore: Send + Sync {
    async fn list_service_ids(&self) -> Result<Vec<String>>;
    async fn list_queued_deployments(&self) -> Result<Vec<QueuedDeployment>>;
    async fn claim_deployment_building(&self, queued_deployment: &QueuedDeployment)
    -> Result<bool>;
    async fn update_deployment_status(
        &self,
        deployment: &Deployment,
        status: DeploymentStatus,
    ) -> Result<()>;
    async fn update_replica_status(
        &self,
        _service_id: &str,
        _deployment_id: &str,
        _replica_index: u32,
        _status: DeploymentStatus,
    ) -> Result<()> {
        bail!("update_replica_status not implemented")
    }
    async fn list_replica_states(
        &self,
        _service_id: &str,
        _deployment_id: &str,
    ) -> Result<Vec<ReplicaState>> {
        bail!("list_replica_states not implemented")
    }

    async fn delete_replica_state(
        &self,
        _service_id: &str,
        _deployment_id: &str,
        _replica_index: u32,
    ) -> Result<()> {
        bail!("delete_replica_state not implemented")
    }

    async fn list_service_infos(&self) -> Result<Vec<ServiceInfo>> {
        bail!("list_service_infos not implemented")
    }

    async fn list_service_deployments(&self, _service_id: &str) -> Result<Vec<ServiceDeployment>> {
        bail!("list_service_deployments not implemented")
    }

    async fn list_service_deployments_with_replicas(
        &self,
        _service_id: &str,
    ) -> Result<Vec<DeploymentWithReplicas>> {
        bail!("list_service_deployments_with_replicas not implemented")
    }

    async fn read_service_info(&self, _service_id: &str) -> Result<Option<ServiceInfo>> {
        bail!("read_service_info not implemented")
    }

    async fn get_service_status(&self, _service_id: &str) -> Result<Option<DeploymentStatus>> {
        bail!("get_service_status not implemented")
    }

    async fn read_service_deployment(
        &self,
        _deployment: &Deployment,
    ) -> Result<Option<ServiceDeployment>> {
        bail!("read_service_deployment not implemented")
    }

    async fn read_deployment_secrets(
        &self,
        _service_id: &str,
        _deployment_id: &str,
    ) -> Result<std::collections::HashMap<String, String>> {
        bail!("read_deployment_secrets not implemented")
    }

    async fn queue_deployment(&self, _deployment: ServiceDeployment) -> Result<ForceQueueOutcome> {
        bail!("queue_deployment not implemented")
    }

    async fn cancel_service_deployment(
        &self,
        _deployment: &Deployment,
    ) -> Result<CancelDeploymentOutcome> {
        bail!("cancel_service_deployment not implemented")
    }

    async fn stop_service_deployment(
        &self,
        _deployment: &Deployment,
    ) -> Result<Option<ServiceDeployment>> {
        bail!("stop_service_deployment not implemented")
    }

    async fn delete_service(&self, _service_id: &str) -> Result<()> {
        bail!("delete_service not implemented")
    }

    async fn update_service_config(&self, _service_id: &str, _config: ServiceConfig) -> Result<()> {
        bail!("update_service_config not implemented")
    }
}
