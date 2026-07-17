use anyhow::{Result, bail};
use async_trait::async_trait;

use crate::deployment::types::{
    CancelDeploymentOutcome, Deployment, DeploymentStatus, DeploymentWithReplicas,
    ForceQueueOutcome, IngressRouting, QueuedDeployment, ReplicaState, ServiceConfig,
    ServiceDeployment, ServiceInfo,
};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SystemUpgradeRequest {
    pub system_type: String,
    #[serde(default)]
    pub target_version: Option<String>,
}

impl SystemUpgradeRequest {
    pub fn new(system_type: impl Into<String>, target_version: impl Into<String>) -> Self {
        Self {
            system_type: system_type.into(),
            target_version: Some(target_version.into()),
        }
    }

    pub(crate) fn from_storage(value: &[u8]) -> Result<Self> {
        match serde_json::from_slice(value) {
            Ok(request) => Ok(request),
            Err(json_error) => {
                let system_type = std::str::from_utf8(value)?.trim();
                if system_type.is_empty() || system_type.starts_with(['{', '[']) {
                    return Err(json_error.into());
                }
                Ok(Self {
                    system_type: system_type.to_string(),
                    target_version: None,
                })
            }
        }
    }

    pub(crate) fn to_storage(&self) -> Result<Vec<u8>> {
        Ok(serde_json::to_vec(self)?)
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "outcome", rename_all = "kebab-case")]
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

/// Semantic mutations accepted by the privileged controller daemon. Cluster-mode probes only
/// hold read/health credentials and relay these bounded operations over the protected local
/// control socket; the daemon applies every operation with its live leadership fence.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "mutation", rename_all = "kebab-case")]
pub enum ClusterMutation {
    ClaimRequest {
        request_id: String,
        fingerprint: String,
        now_ms: i64,
    },
    CompleteRequest {
        request_id: String,
        fingerprint: String,
        status_code: u16,
        content_type: Option<String>,
        body: Vec<u8>,
        now_ms: i64,
    },
    QueueDeployment {
        deployment: ServiceDeployment,
    },
    CancelDeployment {
        deployment: Deployment,
    },
    StopDeployment {
        deployment: Deployment,
    },
    DeleteDeployment {
        deployment: Deployment,
    },
    DeleteService {
        service_id: String,
    },
    UpdateServiceConfig {
        service_id: String,
        config: ServiceConfig,
    },
    SetDeployFrozen {
        service_id: String,
        frozen: bool,
    },
    SetReplicasOverride {
        service_id: String,
        override_value: Option<u32>,
    },
    SetBlockedIngressIp {
        address: String,
        blocked: bool,
    },
    WriteSlackWebhooks {
        webhooks: Vec<crate::slack::SlackWebhook>,
    },
}

#[derive(Default)]
pub struct AtomicDeploymentUpdates {
    pub comparisons: Vec<etcd_client::Compare>,
    pub operations: Vec<etcd_client::TxnOp>,
}

pub enum RequestClaim {
    Started,
    InProgress,
    Conflict,
    Complete {
        status_code: u16,
        content_type: Option<String>,
        body: Vec<u8>,
    },
}

impl RequestClaim {
    pub fn into_json(self) -> Result<serde_json::Value> {
        let value = match self {
            Self::Started => serde_json::json!({ "claim": "started" }),
            Self::InProgress => serde_json::json!({ "claim": "in-progress" }),
            Self::Conflict => serde_json::json!({ "claim": "conflict" }),
            Self::Complete {
                status_code,
                content_type,
                body,
            } => serde_json::json!({
                "claim": "complete",
                "statusCode": status_code,
                "contentType": content_type,
                "body": body,
            }),
        };
        Ok(value)
    }

    pub fn from_json(value: serde_json::Value) -> Result<Self> {
        let claim = value
            .get("claim")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("daemon returned an invalid request claim"))?;
        match claim {
            "started" => Ok(Self::Started),
            "in-progress" => Ok(Self::InProgress),
            "conflict" => Ok(Self::Conflict),
            "complete" => Ok(Self::Complete {
                status_code: serde_json::from_value(
                    value.get("statusCode").cloned().unwrap_or_default(),
                )?,
                content_type: serde_json::from_value(
                    value.get("contentType").cloned().unwrap_or_default(),
                )?,
                body: serde_json::from_value(value.get("body").cloned().unwrap_or_default())?,
            }),
            other => bail!("daemon returned unknown request claim `{other}`"),
        }
    }
}

#[async_trait]
pub trait ClusterStore: Send + Sync {
    async fn apply_cluster_mutation(
        &self,
        _token: &crate::cluster::types::LeadershipToken,
        _mutation: ClusterMutation,
    ) -> Result<Option<serde_json::Value>> {
        bail!("fenced cluster mutations are not implemented")
    }
    async fn list_cluster_nodes(&self) -> Result<Vec<crate::cluster::NodeInfo>> {
        Ok(Vec::new())
    }

    async fn read_cluster_leader(&self) -> Result<Option<crate::cluster::LeaderInfo>> {
        Ok(None)
    }

    async fn read_cluster_meta(&self) -> Result<Option<crate::cluster::ClusterMeta>> {
        Ok(None)
    }

    async fn list_cluster_node_records(&self) -> Result<Vec<crate::cluster::NodeRecord>> {
        Ok(Vec::new())
    }

    async fn read_cluster_node_state(&self, _node_id: &str) -> Result<crate::cluster::NodeState> {
        Ok(crate::cluster::NodeState::default())
    }

    async fn list_unschedulable_replicas(
        &self,
    ) -> Result<Vec<crate::cluster::UnschedulableReplica>> {
        Ok(Vec::new())
    }

    async fn list_cluster_traffic(&self) -> Result<Vec<crate::cluster::TrafficGeneration>> {
        Ok(Vec::new())
    }

    async fn list_placement_history(
        &self,
        _service_id: Option<&str>,
        _deployment_id: Option<&str>,
        _replica_index: Option<u32>,
    ) -> Result<Vec<crate::cluster::PlacementHistory>> {
        Ok(Vec::new())
    }

    async fn publish_node_stats(
        &self,
        _node_id: &str,
        _snapshot: &crate::cluster_stats::ControllerStatsSnapshot,
    ) -> Result<()> {
        Ok(())
    }

    async fn list_node_stats(
        &self,
    ) -> Result<std::collections::BTreeMap<String, crate::cluster_stats::ControllerStatsSnapshot>>
    {
        Ok(std::collections::BTreeMap::new())
    }

    async fn publish_node_disks(
        &self,
        _node_id: &str,
        _disks: &[crate::cluster::NodeDiskInfo],
    ) -> Result<()> {
        Ok(())
    }

    async fn list_node_disks(
        &self,
    ) -> Result<std::collections::BTreeMap<String, Vec<crate::cluster::NodeDiskInfo>>> {
        Ok(std::collections::BTreeMap::new())
    }

    async fn claim_cluster_request(
        &self,
        _local_node_id: &str,
        _request_id: &str,
        _fingerprint: &str,
        _now_ms: i64,
    ) -> Result<RequestClaim> {
        Ok(RequestClaim::Started)
    }

    #[allow(clippy::too_many_arguments)]
    async fn complete_cluster_request(
        &self,
        _local_node_id: &str,
        _request_id: &str,
        _fingerprint: &str,
        _status_code: u16,
        _content_type: Option<&str>,
        _body: &[u8],
        _now_ms: i64,
    ) -> Result<()> {
        Ok(())
    }

    async fn sweep_cluster_state(
        &self,
        _token: &crate::cluster::types::LeadershipToken,
        _now_ms: i64,
    ) -> Result<()> {
        Ok(())
    }

    async fn read_cluster_freeze(&self) -> Result<Option<crate::cluster::types::ClusterFreeze>> {
        Ok(None)
    }

    async fn read_cluster_upgrade(&self) -> Result<Option<crate::cluster::UpgradeRun>> {
        Ok(None)
    }

    async fn create_cluster_upgrade(
        &self,
        _token: &crate::cluster::types::LeadershipToken,
        _run: &crate::cluster::UpgradeRun,
        _freeze: &crate::cluster::types::ClusterFreeze,
    ) -> Result<bool> {
        bail!("cluster upgrade creation not implemented")
    }

    async fn update_cluster_upgrade(
        &self,
        _token: &crate::cluster::types::LeadershipToken,
        _run: &crate::cluster::UpgradeRun,
        _clear_freeze: bool,
    ) -> Result<bool> {
        bail!("cluster upgrade update not implemented")
    }

    async fn manually_unfreeze_cluster_upgrade(
        &self,
        _token: &crate::cluster::types::LeadershipToken,
        _run_id: &str,
        _now_ms: i64,
    ) -> Result<crate::cluster::UpgradeRun> {
        bail!("cluster upgrade unfreeze not implemented")
    }

    async fn list_service_ids(&self) -> Result<Vec<String>>;
    async fn list_queued_deployments(&self) -> Result<Vec<QueuedDeployment>>;
    async fn claim_deployment_building(&self, queued_deployment: &QueuedDeployment)
    -> Result<bool>;
    async fn claim_deployment_building_fenced(
        &self,
        _token: &crate::cluster::types::LeadershipToken,
        _queued_deployment: &QueuedDeployment,
    ) -> Result<bool> {
        bail!("fenced build claim not implemented")
    }
    async fn update_deployment_status(
        &self,
        deployment: &Deployment,
        status: DeploymentStatus,
    ) -> Result<()>;
    async fn update_deployment_status_fenced(
        &self,
        _token: &crate::cluster::types::LeadershipToken,
        _deployment: &Deployment,
        _status: DeploymentStatus,
    ) -> Result<()> {
        bail!("fenced deployment status update not implemented")
    }
    async fn prepare_rollout_status_cutover(
        &self,
        _incoming: &Deployment,
        _draining: &[Deployment],
        _now_ms: u64,
    ) -> Result<AtomicDeploymentUpdates> {
        bail!("atomic rollout status preparation not implemented")
    }
    async fn update_replica_status(
        &self,
        _service_id: &str,
        _deployment_id: &str,
        _replica_index: u32,
        _status: DeploymentStatus,
    ) -> Result<()> {
        bail!("update_replica_status not implemented")
    }
    async fn upsert_replica_state(
        &self,
        _service_id: &str,
        _deployment_id: &str,
        _state: ReplicaState,
    ) -> Result<()> {
        bail!("upsert_replica_state not implemented")
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

    async fn read_deployment_env(
        &self,
        _service_id: &str,
        _deployment_id: &str,
    ) -> Result<std::collections::HashMap<String, crate::utils::crypto::SecretString>> {
        bail!("read_deployment_env not implemented")
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

    async fn update_service_config_fenced(
        &self,
        _token: &crate::cluster::types::LeadershipToken,
        _service_id: &str,
        _config: ServiceConfig,
    ) -> Result<()> {
        bail!("fenced update_service_config not implemented")
    }

    async fn set_blocked_ingress_ip(&self, _address: &str, _blocked: bool) -> Result<Vec<String>> {
        bail!("set_blocked_ingress_ip not implemented")
    }

    async fn read_ingress_blocklist(&self) -> Result<Vec<String>> {
        bail!("read_ingress_blocklist not implemented")
    }

    async fn save_build_data(
        &self,
        _service_id: &str,
        _deployment: &ServiceDeployment,
    ) -> Result<()> {
        bail!("save_build_data not implemented")
    }

    async fn save_build_data_fenced(
        &self,
        _token: &crate::cluster::types::LeadershipToken,
        _service_id: &str,
        _deployment: &ServiceDeployment,
    ) -> Result<()> {
        bail!("fenced save_build_data not implemented")
    }

    async fn save_deploy_data(
        &self,
        _service_id: &str,
        _deployment: &ServiceDeployment,
    ) -> Result<()> {
        bail!("save_deploy_data not implemented")
    }

    async fn save_deploy_data_fenced(
        &self,
        _token: &crate::cluster::types::LeadershipToken,
        _service_id: &str,
        _deployment: &ServiceDeployment,
    ) -> Result<()> {
        bail!("fenced save_deploy_data not implemented")
    }

    async fn update_deployment_build_info(
        &self,
        _deployment: &Deployment,
        _updated: &ServiceDeployment,
    ) -> Result<()> {
        bail!("update_deployment_build_info not implemented")
    }
    async fn update_deployment_build_info_fenced(
        &self,
        _token: &crate::cluster::types::LeadershipToken,
        _deployment: &Deployment,
        _updated: &ServiceDeployment,
    ) -> Result<()> {
        bail!("fenced deployment build update not implemented")
    }

    async fn delete_deployment(
        &self,
        _deployment: &Deployment,
    ) -> Result<Option<ServiceDeployment>> {
        bail!("delete_deployment not implemented")
    }

    async fn set_deploy_frozen(&self, _service_id: &str, _frozen: bool) -> Result<()> {
        bail!("set_deploy_frozen not implemented")
    }

    async fn set_replicas_override(
        &self,
        _service_id: &str,
        _override_value: Option<u32>,
    ) -> Result<()> {
        bail!("set_replicas_override not implemented")
    }

    async fn list_slack_webhooks(&self) -> Result<Vec<crate::slack::SlackWebhook>> {
        Ok(Vec::new())
    }

    async fn write_slack_webhooks(&self, _webhooks: &[crate::slack::SlackWebhook]) -> Result<()> {
        bail!("write_slack_webhooks not implemented")
    }

    async fn read_system_upgrade_request(
        &self,
        _node_id: Option<&str>,
    ) -> Result<Option<SystemUpgradeRequest>> {
        bail!("read_system_upgrade_request not implemented")
    }

    async fn put_system_upgrade_request(
        &self,
        _node_id: Option<&str>,
        _request: &SystemUpgradeRequest,
    ) -> Result<()> {
        bail!("put_system_upgrade_request not implemented")
    }

    async fn delete_system_upgrade_request(&self, _node_id: Option<&str>) -> Result<()> {
        bail!("delete_system_upgrade_request not implemented")
    }

    async fn read_system_restart_request(&self, _node_id: Option<&str>) -> Result<bool> {
        bail!("read_system_restart_request not implemented")
    }

    async fn put_system_restart_request(&self, _node_id: Option<&str>) -> Result<()> {
        bail!("put_system_restart_request not implemented")
    }

    async fn delete_system_restart_request(&self, _node_id: Option<&str>) -> Result<()> {
        bail!("delete_system_restart_request not implemented")
    }

    async fn list_ingress_routes(&self) -> Result<Vec<IngressRouting>> {
        bail!("list_ingress_routes not implemented")
    }
}
