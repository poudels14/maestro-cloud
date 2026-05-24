//! Production adapters wiring the cluster module's traits to existing
//! controller primitives (ClusterStore, etcd, DnsManager). Each adapter has a
//! single concrete impl; the matching trait already has an in-memory test impl
//! for unit tests.

use std::sync::Arc;

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use etcd_client::Client as EtcdClient;
use tokio::sync::Mutex;

use super::assignment_store::ASSIGNMENTS_PREFIX;
use super::engine_executor::{DeploymentInputs, DeploymentLookup};
use super::leader_loop::{PlanObserver, ServiceCatalog, TraefikConfigSink};
use super::port_allocator::{Port, PortAllocator};
use super::scheduling::{Assignment, ReplicaSlot};
use super::traefik_aggregator::TraefikDynamicConfig;
use super::types::NodeInfo;
use crate::deployment::dns::DnsManager;
use crate::deployment::store::ClusterStore;
use crate::deployment::types::{Deployment, DeploymentStatus, ServiceConfig};
use crate::engine::Engine;
use crate::logs::{LogConfig, LogEntry, LogOrigin};

const PORT_POOL_PREFIX: &str = "cluster/ports/";
const TRAEFIK_DYNAMIC_KEY: &str = "traefik/dynamic/cluster.json";
const PORT_POOL_RANGE: std::ops::RangeInclusive<Port> = 23_000..=23_999;

pub struct StoreServiceCatalog {
    store: Arc<dyn ClusterStore>,
}

impl StoreServiceCatalog {
    pub fn new(store: Arc<dyn ClusterStore>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl ServiceCatalog for StoreServiceCatalog {
    async fn list(&self) -> Result<Vec<ServiceConfig>> {
        let infos = self.store.list_service_infos().await?;
        Ok(infos
            .into_iter()
            .filter(|info| !info.deploy_frozen)
            .map(|info| info.config)
            .collect())
    }

    async fn current_deployment_id(&self, service_id: &str) -> Result<Option<String>> {
        let deployments = self.store.list_service_deployments(service_id).await?;
        let ready = deployments
            .iter()
            .rev()
            .find(|deployment| {
                matches!(
                    deployment.status,
                    DeploymentStatus::Ready | DeploymentStatus::PendingReady
                )
            })
            .map(|deployment| deployment.id.clone());
        Ok(ready)
    }

    async fn unhealthy_slots(&self) -> Result<Vec<ReplicaSlot>> {
        let infos = self.store.list_service_infos().await?;
        let mut slots = Vec::new();
        for info in infos {
            let deployments = self
                .store
                .list_service_deployments_with_replicas(&info.config.id)
                .await
                .unwrap_or_default();
            let active = deployments.into_iter().rev().find(|deployment| {
                matches!(
                    deployment.deployment.status,
                    DeploymentStatus::Ready | DeploymentStatus::PendingReady
                )
            });
            if let Some(active) = active {
                for replica in active.replicas {
                    let crashed = matches!(replica.status, DeploymentStatus::Crashed);
                    let unhealthy = replica.healthcheck_failures >= UNHEALTHY_THRESHOLD || crashed;
                    if unhealthy {
                        slots.push(ReplicaSlot {
                            service_id: info.config.id.clone(),
                            replica_index: replica.replica_index,
                        });
                    }
                }
            }
        }
        Ok(slots)
    }
}

const UNHEALTHY_THRESHOLD: u32 = 5;

pub struct EtcdPortAllocator {
    client: Arc<Mutex<EtcdClient>>,
}

impl EtcdPortAllocator {
    pub fn new(client: Arc<Mutex<EtcdClient>>) -> Self {
        Self { client }
    }

    fn key_for(service_id: &str) -> String {
        format!("{PORT_POOL_PREFIX}{service_id}")
    }

    async fn used_ports(&self) -> Result<std::collections::HashSet<Port>> {
        let mut client = self.client.lock().await;
        let response = client
            .get(
                PORT_POOL_PREFIX,
                Some(etcd_client::GetOptions::new().with_prefix()),
            )
            .await
            .map_err(|err| anyhow!("failed to read port pool: {err}"))?;
        let ports = response
            .kvs()
            .iter()
            .filter_map(|kv| std::str::from_utf8(kv.value()).ok())
            .filter_map(|value| value.parse::<Port>().ok())
            .collect();
        Ok(ports)
    }
}

#[async_trait]
impl PortAllocator for EtcdPortAllocator {
    async fn allocate(&self, service_id: &str) -> Result<Port> {
        let key = Self::key_for(service_id);
        {
            let mut client = self.client.lock().await;
            let existing = client
                .get(key.as_str(), None)
                .await
                .map_err(|err| anyhow!("failed to read service port: {err}"))?;
            if let Some(kv) = existing.kvs().first() {
                if let Ok(value) = std::str::from_utf8(kv.value()) {
                    if let Ok(port) = value.parse::<Port>() {
                        return Ok(port);
                    }
                }
            }
        }
        let used = self.used_ports().await?;
        for candidate in PORT_POOL_RANGE.clone() {
            if !used.contains(&candidate) {
                let mut client = self.client.lock().await;
                let put = etcd_client::TxnOp::put(
                    key.as_str().as_bytes().to_vec(),
                    candidate.to_string().into_bytes(),
                    None,
                );
                let txn = etcd_client::Txn::new()
                    .when([etcd_client::Compare::version(
                        key.as_str(),
                        etcd_client::CompareOp::Equal,
                        0,
                    )])
                    .and_then([put]);
                let resp = client
                    .txn(txn)
                    .await
                    .map_err(|err| anyhow!("failed to assign port: {err}"))?;
                if resp.succeeded() {
                    return Ok(candidate);
                }
                let existing = client
                    .get(key.as_str(), None)
                    .await
                    .map_err(|err| anyhow!("failed to re-read service port: {err}"))?;
                if let Some(kv) = existing.kvs().first() {
                    if let Ok(value) = std::str::from_utf8(kv.value()) {
                        if let Ok(port) = value.parse::<Port>() {
                            return Ok(port);
                        }
                    }
                }
            }
        }
        Err(anyhow!(
            "port pool {}..={} exhausted",
            PORT_POOL_RANGE.start(),
            PORT_POOL_RANGE.end()
        ))
    }

    async fn get(&self, service_id: &str) -> Result<Option<Port>> {
        let mut client = self.client.lock().await;
        let response = client
            .get(Self::key_for(service_id), None)
            .await
            .map_err(|err| anyhow!("failed to read port: {err}"))?;
        match response.kvs().first() {
            Some(kv) => {
                let value = std::str::from_utf8(kv.value())
                    .map_err(|err| anyhow!("port value is not utf-8: {err}"))?;
                let port = value
                    .parse::<Port>()
                    .map_err(|err| anyhow!("port value is not a number: {err}"))?;
                Ok(Some(port))
            }
            None => Ok(None),
        }
    }

    async fn release(&self, service_id: &str) -> Result<()> {
        let mut client = self.client.lock().await;
        client
            .delete(Self::key_for(service_id), None)
            .await
            .map_err(|err| anyhow!("failed to release port: {err}"))?;
        Ok(())
    }
}

pub struct EtcdTraefikSink {
    client: Arc<Mutex<EtcdClient>>,
    key: String,
}

impl EtcdTraefikSink {
    pub fn new(client: Arc<Mutex<EtcdClient>>) -> Self {
        Self {
            client,
            key: TRAEFIK_DYNAMIC_KEY.to_string(),
        }
    }
}

#[async_trait]
impl TraefikConfigSink for EtcdTraefikSink {
    async fn write(&self, config: &TraefikDynamicConfig) -> Result<()> {
        let body = serde_json::to_vec(config)
            .map_err(|err| anyhow!("failed to serialize traefik config: {err}"))?;
        let mut client = self.client.lock().await;
        client
            .put(self.key.clone(), body, None)
            .await
            .map_err(|err| anyhow!("failed to write traefik config: {err}"))?;
        Ok(())
    }
}

pub struct ClusterDnsWriter {
    dns_manager: Arc<DnsManager>,
    dns_domain: String,
}

impl ClusterDnsWriter {
    pub fn new(dns_manager: Arc<DnsManager>, dns_domain: String) -> Self {
        Self {
            dns_manager,
            dns_domain,
        }
    }

    /// Update per-service A-records given the current cluster-wide assignments
    /// and node addresses.
    pub fn apply(&self, assignments: &[Assignment], nodes: &[NodeInfo]) {
        use std::collections::BTreeMap;
        let node_address: BTreeMap<String, String> = nodes
            .iter()
            .map(|node| {
                (
                    node.node_id.clone(),
                    node.tailscale_ip
                        .clone()
                        .unwrap_or_else(|| node.hostname.clone()),
                )
            })
            .collect();
        let mut per_service: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for assignment in assignments {
            if let Some(address) = node_address.get(&assignment.node_id) {
                per_service
                    .entry(assignment.service_id.clone())
                    .or_default()
                    .push(address.clone());
            }
        }
        for (service_id, addresses) in &per_service {
            let mut sorted = addresses.clone();
            sorted.sort();
            sorted.dedup();
            self.dns_manager
                .set_records(service_id, &self.dns_domain, &sorted);
        }
        let _ = self.dns_manager.flush();
    }
}

pub fn assignments_prefix() -> &'static str {
    ASSIGNMENTS_PREFIX
}

#[async_trait]
impl PlanObserver for ClusterDnsWriter {
    async fn observe(&self, assignments: &[Assignment], nodes: &[NodeInfo]) {
        self.apply(assignments, nodes);
    }
}

/// Production [`DeploymentLookup`] for [`EngineReplicaExecutor`]. Resolves an
/// [`Assignment`] to the [`DeploymentInputs`] needed to launch a replica by
/// reading the matching [`ServiceDeployment`] from [`ClusterStore`] and
/// constructing a [`DeployOutput`] via the runtime/provider.
#[derive(derive_builder::Builder)]
#[builder(pattern = "owned", setter(into))]
pub struct StoreDeploymentLookup {
    store: Arc<dyn ClusterStore>,
    engine: Arc<Engine>,
    runtime_cli: String,
    cluster_name: String,
    #[builder(default)]
    log_sender: Option<flume::Sender<LogEntry>>,
    #[builder(default)]
    config_tags: Vec<String>,
    #[builder(default)]
    max_restarts_default: Option<u32>,
    #[builder(default = "5_000")]
    restart_delay_ms: u64,
    #[builder(default = "60_000")]
    shutdown_grace_period_ms: u64,
}

#[async_trait]
impl DeploymentLookup for StoreDeploymentLookup {
    async fn resolve(&self, assignment: &Assignment) -> anyhow::Result<Option<DeploymentInputs>> {
        let deployment_ref = Deployment {
            id: assignment.deployment_id.clone(),
            service_id: assignment.service_id.clone(),
            replica_index: assignment.replica_index,
        };
        let deployment = match self.store.read_service_deployment(&deployment_ref).await? {
            Some(deployment) => deployment,
            None => return Ok(None),
        };
        let deploy_output = match self
            .engine
            .deploy_command(&deployment, assignment.replica_index)
        {
            Some(output) => output,
            None => return Ok(None),
        };
        let container_hostname = deployment.hostname_for_replica(assignment.replica_index);
        let max_restarts = deployment
            .config
            .deploy
            .max_restarts
            .or(self.max_restarts_default);
        let log_config = self.log_sender.clone().map(|sender| {
            let mut tags = self.config_tags.clone();
            tags.push(format!("service:{}", deployment.config.id));
            tags.push(format!("hostname:{container_hostname}"));
            tags.push(format!("deployment_id:{}", deployment.id));
            tags.push(format!("replica:{}", assignment.replica_index));
            tags.push(format!("cluster:{}", self.cluster_name));
            LogConfig {
                sender,
                tags,
                origin: LogOrigin::Service,
            }
        });
        Ok(Some(DeploymentInputs {
            deployment,
            deploy_output,
            max_restarts,
            restart_delay_ms: self.restart_delay_ms,
            shutdown_grace_period_ms: self.shutdown_grace_period_ms,
            container_hostname,
            runtime_cli: self.runtime_cli.clone(),
            log_config,
        }))
    }
}
