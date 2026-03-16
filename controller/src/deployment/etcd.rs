use std::{collections::HashSet, sync::Arc, time::Duration};

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use backon::{ConstantBuilder, Retryable};
use etcd_client::{
    Client as EtcdClient, Compare, CompareOp, GetOptions, SortOrder, SortTarget, Txn, TxnOp,
};

use crate::deployment::keys::{
    SERVICES_PREFIX, SERVICES_ROOT, replica_state_key, replica_states_prefix,
    service_deployment_history_key, service_deployment_history_prefix,
    service_history_next_index_key, service_id_from_history_key, service_id_from_info_key,
    service_info_key, service_prefix,
};
use crate::deployment::store::ClusterStore;
use crate::deployment::types::{
    CancelDeploymentOutcome, Deployment, DeploymentStatus, DeploymentWithReplicas,
    ForceQueueOutcome, IngressConfig, QueuedDeployment, ReplicaState, ServiceConfig,
    ServiceDeployment, ServiceInfo,
};
use crate::utils::time::current_time_millis;

const MAX_STATUS_TXN_RETRIES: usize = 8;
const MAX_TXN_RETRIES: usize = 16;

#[derive(Clone)]
pub struct EtcdStateStore {
    client: Arc<tokio::sync::Mutex<EtcdClient>>,
}

struct DeploymentSnapshot {
    key: String,
    mod_revision: u64,
    deployment: ServiceDeployment,
}

#[derive(Debug, Clone)]
struct CounterSnapshot {
    next_index: u64,
    mod_revision: Option<u64>,
}

#[derive(Debug, Clone)]
struct InfoSnapshot {
    key: String,
    mod_revision: u64,
    info: ServiceInfo,
}

impl EtcdStateStore {
    pub async fn new(endpoint: &str) -> Result<Self> {
        let backoff = ConstantBuilder::default()
            .with_delay(Duration::from_secs(1))
            .with_max_times(15);

        let client: EtcdClient = (|| async {
            let mut client = EtcdClient::connect([endpoint], None).await?;
            client.status().await?;
            Ok::<EtcdClient, anyhow::Error>(client)
        })
        .retry(backoff)
        .await
        .map_err(|err: anyhow::Error| anyhow!("failed to connect to etcd: {err}"))?;

        Ok(Self {
            client: Arc::new(tokio::sync::Mutex::new(client)),
        })
    }

    async fn get(
        &self,
        key: Vec<u8>,
        options: Option<GetOptions>,
    ) -> Result<etcd_client::GetResponse> {
        let mut client = self.client.lock().await;
        client
            .get(key, options)
            .await
            .map_err(|err| anyhow!("failed etcd get request: {err}"))
    }

    async fn txn(&self, compare: Vec<Compare>, success: Vec<TxnOp>) -> Result<bool> {
        let txn = Txn::new().when(compare).and_then(success);
        let mut client = self.client.lock().await;
        let response = client
            .txn(txn)
            .await
            .map_err(|err| anyhow!("failed etcd txn request: {err}"))?;
        Ok(response.succeeded())
    }

    async fn read_counter(&self, key: &str) -> Result<CounterSnapshot> {
        let response = self.get(key.as_bytes().to_vec(), None).await?;
        let Some(kv) = response.kvs().first() else {
            return Ok(CounterSnapshot {
                next_index: 0,
                mod_revision: None,
            });
        };

        let text = String::from_utf8(kv.value().to_vec())
            .map_err(|err| anyhow!("counter value for key `{key}` is not utf8: {err}"))?;
        let next_index = text
            .trim()
            .parse::<u64>()
            .map_err(|err| anyhow!("counter value for key `{key}` is not u64: {err}"))?;
        let mod_revision = decode_mod_revision(kv.mod_revision(), key)?;

        Ok(CounterSnapshot {
            next_index,
            mod_revision: Some(mod_revision),
        })
    }

    async fn read_service_info_snapshot(&self, service_id: &str) -> Result<Option<InfoSnapshot>> {
        let key = service_info_key(service_id);
        let response = self.get(key.as_bytes().to_vec(), None).await?;
        let Some(kv) = response.kvs().first() else {
            return Ok(None);
        };

        let info = serde_json::from_slice::<ServiceInfo>(kv.value())
            .map_err(|err| anyhow!("invalid service info JSON at key `{key}`: {err}"))?;
        let mod_revision = decode_mod_revision(kv.mod_revision(), &key)?;

        Ok(Some(InfoSnapshot {
            key,
            mod_revision,
            info,
        }))
    }

    async fn configure_ingress(
        &self,
        service_id: &str,
        container_names: &[String],
        ingress: &IngressConfig,
    ) -> Result<()> {
        let router_prefix = format!("traefik/http/routers/{service_id}");
        let service_prefix = format!("traefik/http/services/{service_id}");
        let servers_prefix = format!("{service_prefix}/loadBalancer/servers/");

        let rule = format!("Host(`{}`)", ingress.host);
        let port = ingress.port.unwrap_or(80);

        let mut client = self.client.lock().await;

        if let Some(range_end) = prefix_range_end(servers_prefix.as_bytes()) {
            client
                .delete(
                    servers_prefix.as_bytes(),
                    Some(etcd_client::DeleteOptions::new().with_range(range_end)),
                )
                .await
                .map_err(|err| anyhow!("failed to delete stale ingress servers: {err}"))?;
        }

        let mut put_ops = vec![
            request_put(&format!("{router_prefix}/rule"), &rule),
            request_put(&format!("{router_prefix}/service"), service_id),
            request_put(&format!("{router_prefix}/entryPoints/0"), "web"),
        ];
        for (i, container_name) in container_names.iter().enumerate() {
            let url = format!("http://{container_name}:{port}");
            put_ops.push(request_put(&format!("{servers_prefix}{i}/url"), &url));
        }

        let txn = Txn::new().and_then(put_ops);
        client
            .txn(txn)
            .await
            .map_err(|err| anyhow!("failed to configure traefik ingress: {err}"))?;

        let urls: Vec<String> = container_names
            .iter()
            .map(|c| format!("http://{c}:{port}"))
            .collect();
        eprintln!(
            "[probe] configured ingress for `{service_id}`: {} -> [{}]",
            ingress.host,
            urls.join(", ")
        );
        Ok(())
    }

    async fn find_deployment_snapshot(
        &self,
        deployment: &Deployment,
    ) -> Result<Option<DeploymentSnapshot>> {
        let prefix_key = service_deployment_history_prefix(&deployment.service_id);
        let prefix = prefix_key.as_bytes();
        let range_end = prefix_range_end(prefix)
            .ok_or_else(|| anyhow!("failed to compute range end for deployment lookup"))?;
        let options = GetOptions::new().with_range(range_end);
        let response = self.get(prefix.to_vec(), Some(options)).await?;

        for kv in response.kvs() {
            let d = serde_json::from_slice::<ServiceDeployment>(kv.value())
                .map_err(|err| anyhow!("invalid deployment JSON under `{prefix_key}`: {err}"))?;
            if d.id != deployment.id {
                continue;
            }

            let key = String::from_utf8(kv.key().to_vec())
                .map_err(|err| anyhow!("deployment key for `{prefix_key}` is not utf8: {err}"))?;
            let mod_revision = decode_mod_revision(kv.mod_revision(), &key)?;

            return Ok(Some(DeploymentSnapshot {
                key,
                mod_revision,
                deployment: d,
            }));
        }

        Ok(None)
    }

    async fn find_active_deployment(&self, service_id: &str) -> Option<DeploymentSnapshot> {
        let deployments = self.list_service_deployments(service_id).await.ok()?;
        let active = deployments.into_iter().find(|d| {
            matches!(
                d.status,
                DeploymentStatus::Ready
                    | DeploymentStatus::Building
                    | DeploymentStatus::PendingReady
            )
        })?;
        let deployment_ref = Deployment {
            service_id: service_id.to_string(),
            id: active.id.clone(),
            replica_index: 0,
        };
        self.find_deployment_snapshot(&deployment_ref).await.ok()?
    }

    async fn sync_ingress_for_service(&self, service_id: &str, deployment: &ServiceDeployment) {
        let Some(ingress) = &deployment.config.ingress else {
            eprintln!("[maestro]: sync_ingress skipped for `{service_id}`: no ingress config");
            return;
        };
        let replicas = match self.read_replica_states(service_id, &deployment.id).await {
            Ok(r) => r,
            Err(err) => {
                eprintln!(
                    "[maestro]: sync_ingress failed to read replica states for `{service_id}`: {err}"
                );
                return;
            }
        };
        let containers: Vec<String> = replicas
            .iter()
            .filter(|r| r.status == DeploymentStatus::Ready)
            .map(|r| deployment.hostname_for_replica(r.replica_index))
            .collect();
        if containers.is_empty() {
            return;
        }
        if let Err(err) = self
            .configure_ingress(service_id, &containers, ingress)
            .await
        {
            eprintln!("[maestro]: failed to configure ingress for `{service_id}`: {err}");
        }
    }

    async fn read_replica_states(
        &self,
        service_id: &str,
        deployment_id: &str,
    ) -> Result<Vec<ReplicaState>> {
        let prefix_key = replica_states_prefix(service_id, deployment_id);
        let prefix = prefix_key.as_bytes();
        let range_end = prefix_range_end(prefix)
            .ok_or_else(|| anyhow!("failed to compute range end for replica states prefix"))?;
        let options = GetOptions::new().with_range(range_end);
        let response = self.get(prefix.to_vec(), Some(options)).await?;

        let mut states = Vec::new();
        for kv in response.kvs() {
            let state = serde_json::from_slice::<ReplicaState>(kv.value())
                .map_err(|err| anyhow!("invalid replica state JSON: {err}"))?;
            states.push(state);
        }
        states.sort_by_key(|s| s.replica_index);
        Ok(states)
    }
}

#[async_trait]
impl ClusterStore for EtcdStateStore {
    async fn list_service_ids(&self) -> anyhow::Result<Vec<String>> {
        let prefix_key = format!("{SERVICES_ROOT}/");
        let prefix = prefix_key.as_bytes();
        let range_end = prefix_range_end(prefix)
            .ok_or_else(|| anyhow!("failed to compute range end for services prefix"))?;
        let options = GetOptions::new()
            .with_range(range_end)
            .with_sort(SortTarget::Key, SortOrder::Ascend);
        let response = self.get(prefix.to_vec(), Some(options)).await?;

        let mut ids = HashSet::new();
        for kv in response.kvs() {
            let key = String::from_utf8(kv.key().to_vec())
                .map_err(|err| anyhow!("service key for `{prefix_key}` is not utf8: {err}"))?;
            if !key.ends_with("/info") {
                continue;
            }
            let Some(service_id) = service_id_from_info_key(&key) else {
                continue;
            };
            ids.insert(service_id);
        }

        let mut service_ids = ids.into_iter().collect::<Vec<_>>();
        service_ids.sort();
        Ok(service_ids)
    }

    async fn list_queued_deployments(&self) -> anyhow::Result<Vec<QueuedDeployment>> {
        let prefix_key = format!("{SERVICES_ROOT}/");
        let prefix = prefix_key.as_bytes();
        let range_end = prefix_range_end(prefix)
            .ok_or_else(|| anyhow!("failed to compute range end for services prefix"))?;
        let options = GetOptions::new().with_range(range_end);
        let response = self.get(prefix.to_vec(), Some(options)).await?;

        let mut queued = Vec::new();
        for kv in response.kvs() {
            let key = String::from_utf8(kv.key().to_vec())
                .map_err(|err| anyhow!("deployment key for `{prefix_key}` is not utf8: {err}"))?;
            if !key.contains("/deployments/history/") {
                continue;
            }

            let Some(service_id) = service_id_from_history_key(&key) else {
                continue;
            };

            let deployment = serde_json::from_slice::<ServiceDeployment>(kv.value())
                .map_err(|err| anyhow!("invalid deployment JSON at key `{key}`: {err}"))?;
            if deployment.status != DeploymentStatus::Queued {
                continue;
            }

            let mod_revision = decode_mod_revision(kv.mod_revision(), &key)?;

            queued.push(QueuedDeployment {
                service_id,
                key,
                mod_revision,
                deployment,
            });
        }

        queued.sort_by(|a, b| {
            a.deployment
                .created_at
                .cmp(&b.deployment.created_at)
                .then_with(|| a.key.cmp(&b.key))
        });

        Ok(queued)
    }

    async fn claim_deployment_building(
        &self,
        queued_deployment: &QueuedDeployment,
    ) -> anyhow::Result<bool> {
        let mut building = queued_deployment.deployment.clone();
        building.status = DeploymentStatus::Building;

        let deployment_json = serde_json::to_string(&building)
            .map_err(|err| anyhow!("failed to serialize building deployment: {err}"))?;
        let info_key = service_info_key(&queued_deployment.service_id);
        let existing_info = self
            .read_service_info_snapshot(&queued_deployment.service_id)
            .await?;

        let updated_info = ServiceInfo {
            config: queued_deployment.deployment.config.clone(),
        };
        let info_json = serde_json::to_string(&updated_info)
            .map_err(|err| anyhow!("failed to serialize service info: {err}"))?;

        self.txn(
            vec![
                compare_mod_revision_or_absent(
                    &queued_deployment.key,
                    Some(queued_deployment.mod_revision),
                ),
                compare_mod_revision_or_absent(
                    &info_key,
                    existing_info.as_ref().map(|s| s.mod_revision),
                ),
            ],
            vec![
                request_put(&queued_deployment.key, &deployment_json),
                request_put(&info_key, &info_json),
            ],
        )
        .await
    }

    async fn update_deployment_status(
        &self,
        deployment: &Deployment,
        status: DeploymentStatus,
    ) -> anyhow::Result<()> {
        for _attempt in 0..MAX_STATUS_TXN_RETRIES {
            let Some(snapshot) = self.find_deployment_snapshot(deployment).await? else {
                return Err(anyhow!(
                    "deployment `{}` for service `{}` not found",
                    deployment.id,
                    deployment.service_id,
                ));
            };

            if !snapshot.deployment.status.can_transition_to(&status) {
                return Ok(());
            }

            let mut updated = snapshot.deployment.clone();
            updated.status = status.clone();
            if status == DeploymentStatus::Ready && updated.deployed_at.is_none() {
                updated.deployed_at = Some(current_time_millis()?);
            }

            let deployment_json = serde_json::to_string(&updated)
                .map_err(|err| anyhow!("failed to serialize deployment: {err}"))?;

            let committed = self
                .txn(
                    vec![compare_mod_revision_or_absent(
                        &snapshot.key,
                        Some(snapshot.mod_revision),
                    )],
                    vec![request_put(&snapshot.key, &deployment_json)],
                )
                .await?;

            if committed {
                return Ok(());
            }
        }

        Err(anyhow!(
            "failed to update deployment `{}` to {status:?} due to concurrent updates",
            deployment.id,
        ))
    }

    async fn update_replica_status(
        &self,
        service_id: &str,
        deployment_id: &str,
        replica_index: u32,
        status: DeploymentStatus,
    ) -> anyhow::Result<()> {
        let key = replica_state_key(service_id, deployment_id, replica_index);
        let state = ReplicaState {
            replica_index,
            status,
        };
        let json = serde_json::to_string(&state)
            .map_err(|err| anyhow!("failed to serialize replica state: {err}"))?;

        let mut client = self.client.lock().await;
        client
            .put(key.as_bytes().to_vec(), json.as_bytes().to_vec(), None)
            .await
            .map_err(|err| anyhow!("failed to write replica state: {err}"))?;

        drop(client);

        let deployment_ref = Deployment {
            service_id: service_id.to_string(),
            id: deployment_id.to_string(),
            replica_index,
        };
        if let Ok(Some(deployment)) = self
            .find_deployment_snapshot(&deployment_ref)
            .await
            .map(|s| s.map(|s| s.deployment))
        {
            self.sync_ingress_for_service(service_id, &deployment).await;
        }

        Ok(())
    }

    async fn delete_replica_state(
        &self,
        service_id: &str,
        deployment_id: &str,
        replica_index: u32,
    ) -> anyhow::Result<()> {
        let key = replica_state_key(service_id, deployment_id, replica_index);
        let mut client = self.client.lock().await;
        client
            .delete(key.as_bytes(), None)
            .await
            .map_err(|err| anyhow!("failed to delete replica state: {err}"))?;

        drop(client);

        let deployment_ref = Deployment {
            service_id: service_id.to_string(),
            id: deployment_id.to_string(),
            replica_index,
        };
        if let Ok(Some(deployment)) = self
            .find_deployment_snapshot(&deployment_ref)
            .await
            .map(|s| s.map(|s| s.deployment))
        {
            self.sync_ingress_for_service(service_id, &deployment).await;
        }

        Ok(())
    }

    async fn list_replica_states(
        &self,
        service_id: &str,
        deployment_id: &str,
    ) -> anyhow::Result<Vec<ReplicaState>> {
        self.read_replica_states(service_id, deployment_id).await
    }

    async fn list_service_deployments_with_replicas(
        &self,
        service_id: &str,
    ) -> anyhow::Result<Vec<DeploymentWithReplicas>> {
        let deployments = self.list_service_deployments(service_id).await?;
        let mut result = Vec::with_capacity(deployments.len());
        for deployment in deployments {
            let replicas: Vec<ReplicaState> = self
                .read_replica_states(service_id, &deployment.id)
                .await
                .unwrap_or_default()
                .into_iter()
                .filter(|r| {
                    !matches!(
                        r.status,
                        DeploymentStatus::Terminated | DeploymentStatus::Removed
                    )
                })
                .collect();
            result.push(DeploymentWithReplicas {
                deployment,
                replicas,
            });
        }
        Ok(result)
    }

    async fn list_service_infos(&self) -> anyhow::Result<Vec<ServiceInfo>> {
        let prefix = SERVICES_PREFIX.as_bytes();
        let range_end = prefix_range_end(prefix)
            .ok_or_else(|| anyhow!("failed to compute range end for services prefix"))?;
        let options = GetOptions::new()
            .with_range(range_end)
            .with_sort(SortTarget::Key, SortOrder::Ascend);
        let response = self.get(prefix.to_vec(), Some(options)).await?;

        let mut infos = Vec::new();
        for kv in response.kvs() {
            let key = String::from_utf8(kv.key().to_vec()).map_err(|err| {
                anyhow!("service key is not utf8 for prefix `{SERVICES_PREFIX}`: {err}")
            })?;
            if !key.ends_with("/info") {
                continue;
            }

            let info = serde_json::from_slice::<ServiceInfo>(kv.value())
                .map_err(|err| anyhow!("failed to parse service info at key `{key}`: {err}"))?;
            infos.push(info);
        }
        infos.sort_by(|a, b| a.config.id.cmp(&b.config.id));
        Ok(infos)
    }

    async fn list_service_deployments(
        &self,
        service_id: &str,
    ) -> anyhow::Result<Vec<ServiceDeployment>> {
        let prefix_key = service_deployment_history_prefix(service_id);
        let prefix = prefix_key.as_bytes();
        let range_end = prefix_range_end(prefix)
            .ok_or_else(|| anyhow!("failed to compute range end for deployments prefix"))?;
        let options = GetOptions::new()
            .with_range(range_end)
            .with_sort(SortTarget::Key, SortOrder::Descend);
        let response = self.get(prefix.to_vec(), Some(options)).await?;

        let mut deployments = Vec::with_capacity(response.kvs().len());
        for kv in response.kvs() {
            let deployment =
                serde_json::from_slice::<ServiceDeployment>(kv.value()).map_err(|err| {
                    anyhow!(
                        "failed to parse deployment JSON from etcd key prefix `{}`: {err}",
                        prefix_key
                    )
                })?;
            deployments.push(deployment);
        }

        Ok(deployments)
    }

    async fn read_service_info(&self, service_id: &str) -> anyhow::Result<Option<ServiceInfo>> {
        Ok(self
            .read_service_info_snapshot(service_id)
            .await?
            .map(|snapshot| snapshot.info))
    }

    async fn get_service_status(
        &self,
        service_id: &str,
    ) -> anyhow::Result<Option<DeploymentStatus>> {
        let deployments = self.list_service_deployments(service_id).await?;
        Ok(deployments.first().map(|d| d.status.clone()))
    }

    async fn read_service_deployment(
        &self,
        deployment: &Deployment,
    ) -> anyhow::Result<Option<ServiceDeployment>> {
        Ok(self
            .find_deployment_snapshot(deployment)
            .await?
            .map(|snapshot| snapshot.deployment))
    }

    async fn queue_deployment(
        &self,
        deployment: ServiceDeployment,
    ) -> anyhow::Result<ForceQueueOutcome> {
        if deployment.status != DeploymentStatus::Queued {
            return Err(anyhow!(
                "queue_deployment requires deployment status QUEUED"
            ));
        }

        let service_id = &deployment.config.id;
        let service_counter_key = service_history_next_index_key(service_id);
        let info_key = service_info_key(service_id);

        for _attempt in 0..MAX_TXN_RETRIES {
            let service_counter = self.read_counter(&service_counter_key).await?;
            let existing_info = self.read_service_info_snapshot(service_id).await?;

            let deployment_index_u64 = service_counter.next_index;
            let deployment_index = usize::try_from(deployment_index_u64)
                .map_err(|_| anyhow!("deployment index overflowed usize"))?;

            let deployment_json = serde_json::to_string(&deployment)
                .map_err(|err| anyhow!("failed to serialize deployment: {err}"))?;

            let info = ServiceInfo {
                config: deployment.config.clone(),
            };
            let info_json = serde_json::to_string(&info)
                .map_err(|err| anyhow!("failed to serialize service info: {err}"))?;

            let service_history_key = service_deployment_history_key(service_id, deployment_index);

            let compare = vec![
                compare_counter(&service_counter_key, &service_counter),
                compare_mod_revision_or_absent(
                    &info_key,
                    existing_info.as_ref().map(|s| s.mod_revision),
                ),
            ];
            let success = vec![
                request_put(
                    &service_counter_key,
                    &(deployment_index_u64 + 1).to_string(),
                ),
                request_put(&service_history_key, &deployment_json),
                request_put(&info_key, &info_json),
            ];

            let committed = self.txn(compare, success).await?;
            if committed {
                return Ok(ForceQueueOutcome {
                    deployment_index,
                    deployment: deployment.clone(),
                });
            }
        }

        Err(anyhow!(
            "failed to queue deployment due to concurrent updates; retry"
        ))
    }

    async fn cancel_service_deployment(
        &self,
        deployment: &Deployment,
    ) -> anyhow::Result<CancelDeploymentOutcome> {
        for _attempt in 0..MAX_TXN_RETRIES {
            let Some(snapshot) = self.find_deployment_snapshot(deployment).await? else {
                return Ok(CancelDeploymentOutcome::NotFound);
            };

            match snapshot.deployment.status {
                DeploymentStatus::Queued | DeploymentStatus::Building => {}
                DeploymentStatus::Canceled => {
                    return Ok(CancelDeploymentOutcome::Canceled(snapshot.deployment));
                }
                _ => {
                    return Ok(CancelDeploymentOutcome::NotCancelable(snapshot.deployment));
                }
            }

            let mut updated = snapshot.deployment.clone();
            updated.status = DeploymentStatus::Canceled;
            // replica states are in separate keys; no need to update them here
            let updated_json = serde_json::to_string(&updated)
                .map_err(|err| anyhow!("failed to serialize canceled deployment: {err}"))?;

            let compare = vec![compare_mod_revision_or_absent(
                &snapshot.key,
                Some(snapshot.mod_revision),
            )];
            let success = vec![request_put(&snapshot.key, &updated_json)];

            let committed = self.txn(compare, success).await?;
            if committed {
                return Ok(CancelDeploymentOutcome::Canceled(updated));
            }
        }

        Err(anyhow!(
            "failed to cancel deployment due to concurrent updates; retry"
        ))
    }

    async fn stop_service_deployment(
        &self,
        deployment: &Deployment,
    ) -> anyhow::Result<Option<ServiceDeployment>> {
        for _attempt in 0..MAX_TXN_RETRIES {
            let Some(snapshot) = self.find_deployment_snapshot(deployment).await? else {
                return Ok(None);
            };

            match snapshot.deployment.status {
                DeploymentStatus::Ready
                | DeploymentStatus::PendingReady
                | DeploymentStatus::Building => {}
                DeploymentStatus::Removed => {
                    return Ok(Some(snapshot.deployment));
                }
                _ => {
                    return Ok(Some(snapshot.deployment));
                }
            }

            let mut updated = snapshot.deployment.clone();
            updated.status = DeploymentStatus::Removed;
            // replica states are in separate keys; no need to update them here
            let updated_json = serde_json::to_string(&updated)
                .map_err(|err| anyhow!("failed to serialize remove-requested deployment: {err}"))?;

            let compare = vec![compare_mod_revision_or_absent(
                &snapshot.key,
                Some(snapshot.mod_revision),
            )];
            let success = vec![request_put(&snapshot.key, &updated_json)];

            let committed = self.txn(compare, success).await?;
            if committed {
                return Ok(Some(updated));
            }
        }

        Err(anyhow!(
            "failed to request stop due to concurrent updates; retry"
        ))
    }

    async fn update_service_config(
        &self,
        service_id: &str,
        config: ServiceConfig,
    ) -> anyhow::Result<()> {
        for _attempt in 0..MAX_STATUS_TXN_RETRIES {
            let Some(info_snapshot) = self.read_service_info_snapshot(service_id).await? else {
                return Err(anyhow!("service `{service_id}` not found"));
            };

            let mut updated_info = info_snapshot.info.clone();
            updated_info.config = config.clone();
            let info_json = serde_json::to_string(&updated_info)
                .map_err(|err| anyhow!("failed to serialize service info: {err}"))?;

            let active_deployment = self.find_active_deployment(service_id).await;

            let mut compare = vec![compare_mod_revision_or_absent(
                &info_snapshot.key,
                Some(info_snapshot.mod_revision),
            )];
            let mut success = vec![request_put(&info_snapshot.key, &info_json)];

            if let Some(dep_snapshot) = &active_deployment {
                let mut updated_dep = dep_snapshot.deployment.clone();
                updated_dep.config = config.clone();
                let dep_json = serde_json::to_string(&updated_dep)
                    .map_err(|err| anyhow!("failed to serialize deployment: {err}"))?;
                compare.push(compare_mod_revision_or_absent(
                    &dep_snapshot.key,
                    Some(dep_snapshot.mod_revision),
                ));
                success.push(request_put(&dep_snapshot.key, &dep_json));
            }

            let committed = self.txn(compare, success).await?;

            if committed {
                return Ok(());
            }
        }

        Err(anyhow!(
            "failed to update service config for `{service_id}` due to concurrent updates"
        ))
    }

    async fn delete_service(&self, service_id: &str) -> anyhow::Result<()> {
        let mut client = self.client.lock().await;

        let prefix = service_prefix(service_id);
        let range_end = prefix_range_end(prefix.as_bytes())
            .ok_or_else(|| anyhow!("failed to compute range end for service prefix"))?;
        client
            .delete(
                prefix.as_bytes(),
                Some(etcd_client::DeleteOptions::new().with_range(range_end)),
            )
            .await
            .map_err(|err| anyhow!("failed to delete service keys: {err}"))?;

        Ok(())
    }
}

fn compare_counter(key: &str, snapshot: &CounterSnapshot) -> Compare {
    compare_mod_revision_or_absent(key, snapshot.mod_revision)
}

fn compare_mod_revision_or_absent(key: &str, mod_revision: Option<u64>) -> Compare {
    match mod_revision {
        Some(rev) => Compare::mod_revision(
            key.as_bytes().to_vec(),
            CompareOp::Equal,
            i64::try_from(rev).expect("mod_revision from etcd must fit i64"),
        ),
        None => Compare::version(key.as_bytes().to_vec(), CompareOp::Equal, 0),
    }
}

fn request_put(key: &str, value: &str) -> TxnOp {
    TxnOp::put(key.as_bytes().to_vec(), value.as_bytes().to_vec(), None)
}

fn decode_mod_revision(mod_revision: i64, key: &str) -> Result<u64> {
    u64::try_from(mod_revision)
        .map_err(|err| anyhow!("invalid mod_revision `{mod_revision}` for key `{key}`: {err}"))
}

fn prefix_range_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    for idx in (0..end.len()).rev() {
        if end[idx] < 0xff {
            end[idx] += 1;
            end.truncate(idx + 1);
            return Some(end);
        }
    }
    None
}

#[cfg(test)]
#[path = "../tests/deployment/etcd.rs"]
mod tests;
