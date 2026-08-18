use std::{collections::HashSet, sync::Arc, time::Duration};

use anyhow::{Result, anyhow, bail};
use async_trait::async_trait;
use backon::{ConstantBuilder, Retryable};
use etcd_client::{Client as EtcdClient, Compare, CompareOp, GetOptions, PutOptions, Txn, TxnOp};

use crate::deployment::ingress_blocklist;
use crate::deployment::keys::{
    CLUSTER_FREEZE_KEY, CLUSTER_UPGRADE_KEY, SERVICES_ROOT, deployment_build_env_key,
    deployment_build_secrets_key, deployment_deploy_env_key, deployment_deploy_secrets_key,
    deployment_prefix, replica_state_key, replica_states_prefix, service_deployment_history_key,
    service_deployment_history_prefix, service_history_next_index_key, service_id_from_info_key,
    service_info_key, service_prefix, system_restart_request_key, system_upgrade_request_key,
};
use crate::deployment::store::{ClusterStore, SystemUpgradeRequest};
use crate::deployment::types::{
    CancelDeploymentOutcome, Deployment, DeploymentStatus, DeploymentWithReplicas,
    ForceQueueOutcome, IngressConfig, IngressRouting, QueuedDeployment, ReplicaState,
    ServiceConfig, ServiceDeployment, ServiceInfo,
};
use crate::utils::time::current_time_millis;

const MAX_STATUS_TXN_RETRIES: usize = 8;
const MAX_TXN_RETRIES: usize = 16;

fn is_missing_election_leader(error: &etcd_client::Error) -> bool {
    matches!(
        error,
        etcd_client::Error::GRpcStatus(status) if is_missing_election_leader_message(status.message())
    )
}

fn is_missing_election_leader_message(message: &str) -> bool {
    message == "election: no leader"
}

#[derive(Clone)]
pub struct EtcdStateStore {
    client: Arc<tokio::sync::Mutex<EtcdClient>>,
    encryption_key: crate::utils::crypto::EncryptionKey,
    mutation_relay: Option<MutationRelay>,
}

#[derive(Clone)]
struct MutationRelay {
    socket_path: String,
    token: String,
}

tokio::task_local! {
    static CLUSTER_WRITE_FENCE: crate::cluster::types::LeadershipToken;
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

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct ClusterRequestReceipt {
    fingerprint: String,
    state: String,
    status_code: Option<u16>,
    content_type: Option<String>,
    body: Vec<u8>,
    updated_at_ms: i64,
}

impl EtcdStateStore {
    pub async fn new_with_endpoints(
        endpoints: &[String],
        encryption_key: crate::utils::crypto::EncryptionKey,
        tls: Option<etcd_client::TlsOptions>,
    ) -> Result<Self> {
        if endpoints.is_empty() {
            return Err(anyhow!("at least one etcd endpoint is required"));
        }
        let backoff = ConstantBuilder::default()
            .with_delay(Duration::from_secs(1))
            .with_max_times(15);

        let connect_options =
            tls.map(|tls_opts| etcd_client::ConnectOptions::new().with_tls(tls_opts));

        let endpoints = endpoints.to_vec();
        let client: EtcdClient = (|| {
            let opts = connect_options.clone();
            let endpoints = endpoints.clone();
            async move {
                let mut client = EtcdClient::connect(endpoints, opts).await?;
                client
                    .get(
                        "/maetro/",
                        Some(GetOptions::new().with_prefix().with_limit(1)),
                    )
                    .await?;
                Ok::<EtcdClient, anyhow::Error>(client)
            }
        })
        .retry(backoff)
        .await
        .map_err(|err: anyhow::Error| anyhow!("failed to connect to etcd: {err}"))?;

        Ok(Self {
            client: Arc::new(tokio::sync::Mutex::new(client)),
            encryption_key,
            mutation_relay: None,
        })
    }

    pub fn with_mutation_relay(mut self, socket_path: String, token: String) -> Self {
        self.mutation_relay = Some(MutationRelay { socket_path, token });
        self
    }

    async fn relay_mutation(
        &self,
        mutation: crate::deployment::store::ClusterMutation,
    ) -> Result<Option<serde_json::Value>> {
        let relay = self
            .mutation_relay
            .as_ref()
            .ok_or_else(|| anyhow!("cluster mutation relay is not configured"))?;
        crate::cluster::control::send_command_with_response(
            &relay.socket_path,
            &relay.token,
            crate::cluster::control::ControlCommand::StoreMutation { mutation },
        )
        .await
    }

    fn current_write_fence() -> Option<crate::cluster::types::LeadershipToken> {
        CLUSTER_WRITE_FENCE.try_with(Clone::clone).ok()
    }

    fn strip_deployment_with_metadata(
        &self,
        deployment: &ServiceDeployment,
        prev_keys: &std::collections::HashMap<String, crate::deployment::types::SecretKeyMeta>,
    ) -> ServiceDeployment {
        let mut d = deployment.clone();
        if let Some(secrets) = &d.config.deploy.secrets {
            d.config.deploy.secrets = Some(secrets.to_metadata(prev_keys));
        }
        d.config.deploy.env.items.clear();
        if let Some(build) = &mut d.config.build {
            build.env.items.clear();
        }
        d
    }

    async fn prev_secret_keys(
        &self,
        service_id: &str,
    ) -> std::collections::HashMap<String, crate::deployment::types::SecretKeyMeta> {
        let deployments = match self.list_service_deployments(service_id).await {
            Ok(d) => d,
            Err(_) => return Default::default(),
        };
        deployments
            .first()
            .and_then(|d| d.config.deploy.secrets.as_ref())
            .map(|s| s.keys.clone())
            .unwrap_or_default()
    }

    async fn write_encrypted(&self, key: &str, data: &impl serde::Serialize) -> Result<()> {
        if !self
            .txn(Vec::new(), vec![self.encrypted_put(key, data)?])
            .await?
        {
            bail!("leadership changed while writing encrypted cluster state");
        }
        Ok(())
    }

    fn encrypted_put(&self, key: &str, data: &impl serde::Serialize) -> Result<TxnOp> {
        let json = serde_json::to_string(data)?;
        let value = crate::utils::crypto::encrypt_string(&self.encryption_key, &json)
            .map_err(anyhow::Error::msg)?;
        Ok(request_put(key, &value))
    }

    async fn read_encrypted<T: serde::de::DeserializeOwned + Default>(&self, key: &str) -> T {
        let response = match self.get(key.as_bytes().to_vec(), None).await {
            Ok(r) => r,
            Err(_) => return Default::default(),
        };
        let Some(kv) = response.kvs().first() else {
            return Default::default();
        };
        let value = String::from_utf8_lossy(kv.value()).to_string();
        let json =
            crate::utils::crypto::decrypt_string(&self.encryption_key, &value).unwrap_or(value);
        serde_json::from_str(&json).unwrap_or_default()
    }

    fn deployment_data_operations(
        &self,
        service_id: &str,
        deployment: &ServiceDeployment,
    ) -> Result<Vec<TxnOp>> {
        let mut operations = Vec::new();
        let deployment_id = &deployment.id;
        if !deployment.config.deploy.env.items.is_empty() {
            let key = deployment_deploy_env_key(service_id, deployment_id);
            operations.push(self.encrypted_put(&key, &deployment.config.deploy.env.items)?);
        }
        if let Some(secrets) = &deployment.config.deploy.secrets
            && !secrets.items.is_empty()
            && secrets.source.is_none()
        {
            let key = deployment_deploy_secrets_key(service_id, deployment_id);
            operations.push(self.encrypted_put(&key, &secrets.items)?);
        }
        if let Some(build) = &deployment.config.build {
            if !build.env.items.is_empty() {
                let key = deployment_build_env_key(service_id, deployment_id);
                operations.push(self.encrypted_put(&key, &build.env.items)?);
            }
            if !build.secrets.items.is_empty() {
                let key = deployment_build_secrets_key(service_id, deployment_id);
                operations.push(self.encrypted_put(&key, &build.secrets.items)?);
            }
        }
        Ok(operations)
    }

    async fn restore_deployment_data(&self, service_id: &str, deployment: &mut ServiceDeployment) {
        let deployment_id = &deployment.id;
        if deployment.config.deploy.env.items.is_empty() {
            let key = deployment_deploy_env_key(service_id, deployment_id);
            deployment.config.deploy.env.items = self.read_encrypted(&key).await;
        }
        if let Some(build) = &mut deployment.config.build {
            if build.env.items.is_empty() {
                let key = deployment_build_env_key(service_id, deployment_id);
                build.env.items = self.read_encrypted(&key).await;
            }
            if build.secrets.items.is_empty() {
                let key = deployment_build_secrets_key(service_id, deployment_id);
                build.secrets.items = self.read_encrypted(&key).await;
            }
        }
    }

    async fn restore_deployment_env(&self, service_id: &str, deployment: &mut ServiceDeployment) {
        let deployment_id = &deployment.id;
        if deployment.config.deploy.env.items.is_empty() {
            let key = deployment_deploy_env_key(service_id, deployment_id);
            deployment.config.deploy.env.items = self.read_encrypted(&key).await;
        }
        if let Some(build) = &mut deployment.config.build {
            if build.env.items.is_empty() {
                let key = deployment_build_env_key(service_id, deployment_id);
                build.env.items = self.read_encrypted(&key).await;
            }
            if build.secrets.items.is_empty() {
                let key = deployment_build_secrets_key(service_id, deployment_id);
                build.secrets.items = self.read_encrypted(&key).await;
            }
        }
    }

    async fn get(
        &self,
        key: Vec<u8>,
        options: Option<GetOptions>,
    ) -> Result<etcd_client::GetResponse> {
        let client = self.client.lock().await;
        let mut kv_client = client
            .kv_client()
            .max_decoding_message_size(crate::utils::etcd::MAX_DECODING_MESSAGE_SIZE);
        kv_client
            .get(key, options)
            .await
            .map_err(|err| anyhow!("failed etcd get request: {err}"))
    }

    async fn get_prefix_entries(
        &self,
        prefix: impl Into<Vec<u8>>,
        keys_only: bool,
        max_results: Option<usize>,
    ) -> Result<Vec<etcd_client::KeyValue>> {
        let client = self.client.lock().await;
        crate::utils::etcd::get_prefix(&client, prefix, keys_only, max_results)
            .await
            .map_err(|err| anyhow!("failed paginated etcd prefix request: {err}"))
    }

    async fn get_range_entries(
        &self,
        start_key: Vec<u8>,
        range_end: Vec<u8>,
        keys_only: bool,
    ) -> Result<Vec<etcd_client::KeyValue>> {
        let client = self.client.lock().await;
        crate::utils::etcd::get_range(&client, start_key, range_end, keys_only, None)
            .await
            .map_err(|err| anyhow!("failed paginated etcd range request: {err}"))
    }

    async fn txn(&self, compare: Vec<Compare>, success: Vec<TxnOp>) -> Result<bool> {
        let mut compare = compare;
        if let Some(token) = Self::current_write_fence() {
            compare.push(cluster_leadership_compare(&token));
        }
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

        let rule = ingress
            .hosts()
            .iter()
            .map(|h| host_rule(h))
            .collect::<Vec<_>>()
            .join(" || ");
        let port = ingress.port.unwrap_or(80);

        let mut put_ops = vec![
            request_put(&format!("{router_prefix}/rule"), &rule),
            request_put(&format!("{router_prefix}/service"), service_id),
            request_put(&format!("{router_prefix}/entryPoints/0"), "web"),
        ];
        let new_keys: Vec<String> = container_names
            .iter()
            .enumerate()
            .map(|(i, container_name)| {
                let key = format!("{servers_prefix}{i}/url");
                let url = format!("http://{container_name}:{port}");
                put_ops.push(request_put(&key, &url));
                key
            })
            .collect();

        let mut client = self.client.lock().await;

        let txn = Txn::new().and_then(put_ops);
        client
            .txn(txn)
            .await
            .map_err(|err| anyhow!("failed to configure traefik ingress: {err}"))?;

        if let Some(range_end) = prefix_range_end(servers_prefix.as_bytes()) {
            if let Ok(entries) = crate::utils::etcd::get_range(
                &client,
                servers_prefix.as_bytes().to_vec(),
                range_end,
                true,
                None,
            )
            .await
            {
                for kv in &entries {
                    let key = String::from_utf8_lossy(kv.key()).to_string();
                    if !new_keys.contains(&key) {
                        let _ = client.delete(kv.key(), None).await;
                    }
                }
            }
        }
        drop(client);
        let blocked_ips = ingress_blocklist::read(&self.client).await?;
        ingress_blocklist::reconcile_traefik(&self.client, &blocked_ips, None).await?;

        let urls: Vec<String> = container_names
            .iter()
            .map(|c| format!("http://{c}:{port}"))
            .collect();
        eprintln!(
            "configured ingress for `{service_id}`: [{}] -> [{}]",
            ingress.hosts().join(", "),
            urls.join(", ")
        );
        Ok(())
    }

    async fn remove_ingress(&self, service_id: &str) -> Result<()> {
        let router_prefix = format!("traefik/http/routers/{service_id}/");
        let service_prefix = format!("traefik/http/services/{service_id}/");
        let mut operations = Vec::new();
        for prefix in [&router_prefix, &service_prefix] {
            if let Some(range_end) = prefix_range_end(prefix.as_bytes()) {
                operations.push(TxnOp::delete(
                    prefix.as_bytes(),
                    Some(etcd_client::DeleteOptions::new().with_range(range_end)),
                ));
            }
        }
        if !operations.is_empty() && !self.txn(Vec::new(), operations).await? {
            bail!("leadership changed while removing ingress for `{service_id}`");
        }

        eprintln!("removed ingress for `{service_id}`");
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
        let entries = self
            .get_range_entries(prefix.to_vec(), range_end, false)
            .await?;

        for kv in &entries {
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

    async fn sync_ingress_for_service(&self, service_id: &str) {
        if self.read_cluster_meta().await.ok().flatten().is_some() {
            return;
        }
        let deployments = match self.list_service_deployments(service_id).await {
            Ok(d) => d,
            Err(_) => return,
        };

        let ingress = deployments
            .iter()
            .find(|d| {
                matches!(
                    d.status,
                    DeploymentStatus::Ready
                        | DeploymentStatus::PendingReady
                        | DeploymentStatus::Building
                )
            })
            .and_then(|d| d.config.ingress.clone());

        let Some(ingress) = ingress else {
            if let Err(err) = self.remove_ingress(service_id).await {
                eprintln!("[maestro]: failed to remove ingress for `{service_id}`: {err}");
            }
            return;
        };

        let mut containers = Vec::new();
        for deployment in &deployments {
            if !matches!(
                deployment.status,
                DeploymentStatus::Ready
                    | DeploymentStatus::PendingReady
                    | DeploymentStatus::Building
            ) {
                continue;
            }
            let replicas = self
                .read_replica_states(service_id, &deployment.id)
                .await
                .unwrap_or_default();
            for replica in &replicas {
                if replica.status == DeploymentStatus::Ready {
                    containers.push(deployment.hostname_for_replica(replica.replica_index));
                }
            }
        }

        if let Err(err) = self
            .configure_ingress(service_id, &containers, &ingress)
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
        let entries = self
            .get_range_entries(prefix.to_vec(), range_end, false)
            .await?;

        let mut states = Vec::new();
        for kv in &entries {
            let state = serde_json::from_slice::<ReplicaState>(kv.value())
                .map_err(|err| anyhow!("invalid replica state JSON: {err}"))?;
            states.push(state);
        }
        let cluster_entries = self
            .get_prefix_entries("/maetro/cluster/replica-states/", false, None)
            .await?;
        for kv in &cluster_entries {
            let state = serde_json::from_slice::<ReplicaState>(kv.value())
                .map_err(|err| anyhow!("invalid scheduled replica state JSON: {err}"))?;
            if state.service_id.as_deref() == Some(service_id)
                && state.deployment_id.as_deref() == Some(deployment_id)
            {
                let (Some(node_id), Some(assignment_id)) =
                    (state.node_id.as_deref(), state.assignment_id.as_deref())
                else {
                    continue;
                };
                let manifest = self
                    .get(
                        format!("/maetro/cluster/assignments/{node_id}").into_bytes(),
                        None,
                    )
                    .await?;
                let desired = manifest.kvs().first().is_some_and(|entry| {
                    serde_json::from_slice::<crate::cluster::AssignmentManifest>(entry.value())
                        .ok()
                        .is_some_and(|manifest| {
                            manifest.assignments.iter().any(|assignment| {
                                assignment.assignment_id == assignment_id
                                    && assignment.service_id == service_id
                                    && assignment.deployment_id == deployment_id
                                    && assignment.replica_index == state.replica_index
                            })
                        })
                });
                if !desired {
                    continue;
                }
                states.push(state);
            }
        }
        states.sort_by_key(|s| s.replica_index);
        Ok(states)
    }
}

#[async_trait]
impl ClusterStore for EtcdStateStore {
    async fn apply_cluster_mutation(
        &self,
        token: &crate::cluster::types::LeadershipToken,
        mutation: crate::deployment::store::ClusterMutation,
    ) -> Result<Option<serde_json::Value>> {
        use crate::deployment::store::ClusterMutation;

        if self.mutation_relay.is_some() {
            bail!("a relaying store cannot execute privileged cluster mutations");
        }
        CLUSTER_WRITE_FENCE
            .scope(token.clone(), async {
                let value = match mutation {
                    ClusterMutation::ClaimRequest {
                        request_id,
                        fingerprint,
                        now_ms,
                    } => Some(
                        self.claim_cluster_request(
                            &token.info.node_id,
                            &request_id,
                            &fingerprint,
                            now_ms,
                        )
                        .await?
                        .into_json()?,
                    ),
                    ClusterMutation::CompleteRequest {
                        request_id,
                        fingerprint,
                        status_code,
                        content_type,
                        body,
                        now_ms,
                    } => {
                        self.complete_cluster_request(
                            &token.info.node_id,
                            &request_id,
                            &fingerprint,
                            status_code,
                            content_type.as_deref(),
                            &body,
                            now_ms,
                        )
                        .await?;
                        None
                    }
                    ClusterMutation::QueueDeployment { deployment } => Some(serde_json::to_value(
                        self.queue_deployment(deployment).await?,
                    )?),
                    ClusterMutation::CancelDeployment { deployment } => Some(serde_json::to_value(
                        self.cancel_service_deployment(&deployment).await?,
                    )?),
                    ClusterMutation::StopDeployment { deployment } => Some(serde_json::to_value(
                        self.stop_service_deployment(&deployment).await?,
                    )?),
                    ClusterMutation::DeleteDeployment { deployment } => Some(serde_json::to_value(
                        self.delete_deployment(&deployment).await?,
                    )?),
                    ClusterMutation::DeleteService { service_id } => {
                        self.delete_service(&service_id).await?;
                        None
                    }
                    ClusterMutation::UpdateServiceConfig { service_id, config } => {
                        self.update_service_config(&service_id, config).await?;
                        None
                    }
                    ClusterMutation::SetDeployFrozen { service_id, frozen } => {
                        self.set_deploy_frozen(&service_id, frozen).await?;
                        None
                    }
                    ClusterMutation::SetReplicasOverride {
                        service_id,
                        override_value,
                    } => {
                        self.set_replicas_override(&service_id, override_value)
                            .await?;
                        None
                    }
                    ClusterMutation::SetBlockedIngressIp { address, blocked } => {
                        Some(serde_json::to_value(
                            self.set_blocked_ingress_ip(&address, blocked).await?,
                        )?)
                    }
                    ClusterMutation::WriteSlackWebhooks { webhooks } => {
                        self.write_slack_webhooks(&webhooks).await?;
                        None
                    }
                };
                Ok(value)
            })
            .await
    }

    async fn list_cluster_nodes(&self) -> Result<Vec<crate::cluster::NodeInfo>> {
        let entries = self
            .get_prefix_entries("/maetro/cluster/nodes/", false, None)
            .await?;
        entries
            .iter()
            .map(|entry| serde_json::from_slice(entry.value()).map_err(Into::into))
            .collect()
    }

    async fn read_cluster_leader(&self) -> Result<Option<crate::cluster::LeaderInfo>> {
        let response = match self
            .client
            .lock()
            .await
            .leader("/maetro/cluster/leader")
            .await
        {
            Ok(response) => response,
            Err(error) if is_missing_election_leader(&error) => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        Ok(response.kv().and_then(|entry| {
            std::str::from_utf8(entry.value())
                .ok()
                .map(|node_id| crate::cluster::LeaderInfo {
                    node_id: node_id.to_string(),
                })
        }))
    }

    async fn read_cluster_meta(&self) -> Result<Option<crate::cluster::ClusterMeta>> {
        let response = self
            .get(b"/maetro/system/cluster-meta".to_vec(), None)
            .await?;
        response
            .kvs()
            .first()
            .map(|entry| serde_json::from_slice(entry.value()).map_err(Into::into))
            .transpose()
    }

    async fn list_cluster_node_records(&self) -> Result<Vec<crate::cluster::NodeRecord>> {
        let entries = self
            .get_prefix_entries("/maetro/cluster/node-records/", false, None)
            .await?;
        entries
            .iter()
            .map(|entry| serde_json::from_slice(entry.value()).map_err(Into::into))
            .collect()
    }

    async fn read_cluster_node_state(&self, node_id: &str) -> Result<crate::cluster::NodeState> {
        let response = self
            .get(
                format!("/maetro/cluster/node-state/{node_id}").into_bytes(),
                None,
            )
            .await?;
        response
            .kvs()
            .first()
            .map(|entry| serde_json::from_slice(entry.value()).map_err(Into::into))
            .transpose()
            .map(|state| state.unwrap_or_default())
    }

    async fn read_cluster_freeze(&self) -> Result<Option<crate::cluster::types::ClusterFreeze>> {
        let response = self
            .get(CLUSTER_FREEZE_KEY.as_bytes().to_vec(), None)
            .await?;
        response
            .kvs()
            .first()
            .map(|entry| serde_json::from_slice(entry.value()).map_err(Into::into))
            .transpose()
    }

    async fn read_cluster_upgrade(&self) -> Result<Option<crate::cluster::UpgradeRun>> {
        let response = self
            .get(CLUSTER_UPGRADE_KEY.as_bytes().to_vec(), None)
            .await?;
        response
            .kvs()
            .first()
            .map(|entry| serde_json::from_slice(entry.value()).map_err(Into::into))
            .transpose()
    }

    async fn create_cluster_upgrade(
        &self,
        token: &crate::cluster::types::LeadershipToken,
        run: &crate::cluster::UpgradeRun,
        freeze: &crate::cluster::types::ClusterFreeze,
    ) -> Result<bool> {
        let mut client = self.client.lock().await;
        let existing = client.get(CLUSTER_UPGRADE_KEY, None).await?;
        let mut comparisons = vec![
            cluster_leadership_compare(token),
            Compare::version(CLUSTER_FREEZE_KEY, CompareOp::Equal, 0),
        ];
        if let Some(entry) = existing.kvs().first() {
            let previous: crate::cluster::UpgradeRun = serde_json::from_slice(entry.value())?;
            if !previous.phase.is_terminal() {
                bail!(
                    "cluster {} `{}` is already active",
                    previous.operation_name(),
                    previous.run_id
                );
            }
            comparisons.push(Compare::value(
                CLUSTER_UPGRADE_KEY,
                CompareOp::Equal,
                entry.value(),
            ));
        } else {
            comparisons.push(Compare::version(CLUSTER_UPGRADE_KEY, CompareOp::Equal, 0));
        }
        let transaction = Txn::new().when(comparisons).and_then([
            TxnOp::put(CLUSTER_UPGRADE_KEY, serde_json::to_vec(run)?, None),
            TxnOp::put(CLUSTER_FREEZE_KEY, serde_json::to_vec(freeze)?, None),
        ]);
        Ok(client.txn(transaction).await?.succeeded())
    }

    async fn update_cluster_upgrade(
        &self,
        token: &crate::cluster::types::LeadershipToken,
        run: &crate::cluster::UpgradeRun,
        clear_freeze: bool,
    ) -> Result<bool> {
        let mut client = self.client.lock().await;
        let response = client.get(CLUSTER_UPGRADE_KEY, None).await?;
        let Some(entry) = response.kvs().first() else {
            bail!("cluster maintenance run disappeared");
        };
        let existing: crate::cluster::UpgradeRun = serde_json::from_slice(entry.value())?;
        if existing.run_id != run.run_id {
            bail!("cluster maintenance run changed");
        }
        let mut operations = vec![TxnOp::put(
            CLUSTER_UPGRADE_KEY,
            serde_json::to_vec(run)?,
            None,
        )];
        if clear_freeze {
            operations.push(TxnOp::delete(CLUSTER_FREEZE_KEY, None));
        }
        let transaction = Txn::new()
            .when([
                cluster_leadership_compare(token),
                Compare::value(CLUSTER_UPGRADE_KEY, CompareOp::Equal, entry.value()),
            ])
            .and_then(operations);
        Ok(client.txn(transaction).await?.succeeded())
    }

    async fn manually_unfreeze_cluster_upgrade(
        &self,
        token: &crate::cluster::types::LeadershipToken,
        run_id: &str,
        now_ms: i64,
    ) -> Result<crate::cluster::UpgradeRun> {
        let mut client = self.client.lock().await;
        let response = client.get(CLUSTER_UPGRADE_KEY, None).await?;
        let entry = response
            .kvs()
            .first()
            .ok_or_else(|| anyhow!("no cluster maintenance run exists"))?;
        let mut run: crate::cluster::UpgradeRun = serde_json::from_slice(entry.value())?;
        if run.run_id != run_id {
            bail!("maintenance run id does not match the active run");
        }
        if run.phase.is_terminal() {
            let transaction = Txn::new()
                .when([
                    cluster_leadership_compare(token),
                    Compare::value(CLUSTER_UPGRADE_KEY, CompareOp::Equal, entry.value()),
                ])
                .and_then([TxnOp::delete(CLUSTER_FREEZE_KEY, None)]);
            if !client.txn(transaction).await?.succeeded() {
                bail!("leadership changed while clearing a terminal maintenance freeze");
            }
            return Ok(run);
        }
        if now_ms.saturating_sub(run.updated_at_ms) < 30_000 {
            bail!("maintenance orchestrator is active; manual unfreeze is unsafe");
        }
        run.phase = crate::cluster::UpgradePhase::Failed;
        let failure = "manually unfrozen by an operator".to_string();
        run.failure = Some(failure.clone());
        run.updated_at_ms = now_ms;
        if let Some(node) = run.current_node_mut() {
            node.status = crate::cluster::types::UpgradeNodeStatus::Failed;
            node.error = Some(failure);
            node.completed_at_ms = Some(now_ms);
        }
        run.history.push(crate::cluster::UpgradeEvent {
            at_ms: now_ms,
            phase: run.phase,
            node_id: run.current_node().map(|node| node.node_id.clone()),
            message: "cluster manually unfrozen; maintenance run aborted".to_string(),
        });
        let transaction = Txn::new()
            .when([
                cluster_leadership_compare(token),
                Compare::value(CLUSTER_UPGRADE_KEY, CompareOp::Equal, entry.value()),
            ])
            .and_then([
                TxnOp::put(CLUSTER_UPGRADE_KEY, serde_json::to_vec(&run)?, None),
                TxnOp::delete(CLUSTER_FREEZE_KEY, None),
            ]);
        if !client.txn(transaction).await?.succeeded() {
            bail!("leadership changed while manually unfreezing the cluster");
        }
        Ok(run)
    }

    async fn list_unschedulable_replicas(
        &self,
    ) -> Result<Vec<crate::cluster::UnschedulableReplica>> {
        let response = self
            .get(b"/maetro/cluster/unschedulable".to_vec(), None)
            .await?;
        response
            .kvs()
            .first()
            .map(|entry| serde_json::from_slice(entry.value()).map_err(Into::into))
            .transpose()
            .map(|entries| entries.unwrap_or_default())
    }

    async fn list_cluster_traffic(&self) -> Result<Vec<crate::cluster::TrafficGeneration>> {
        let entries = self
            .get_prefix_entries("/maetro/cluster/traffic/", false, None)
            .await?;
        entries
            .iter()
            .map(|entry| serde_json::from_slice(entry.value()).map_err(Into::into))
            .collect()
    }

    async fn list_placement_history(
        &self,
        service_id: Option<&str>,
        deployment_id: Option<&str>,
        replica_index: Option<u32>,
    ) -> Result<Vec<crate::cluster::PlacementHistory>> {
        let entries = self
            .get_prefix_entries("/maetro/cluster/placements/", false, Some(10_000))
            .await?;
        let mut placements = entries
            .iter()
            .filter_map(|entry| {
                serde_json::from_slice::<crate::cluster::PlacementHistory>(entry.value()).ok()
            })
            .filter(|placement| {
                service_id.is_none_or(|value| placement.service_id == value)
                    && deployment_id.is_none_or(|value| placement.deployment_id == value)
                    && replica_index.is_none_or(|value| placement.replica_index == value)
            })
            .collect::<Vec<_>>();
        placements.sort_by(|left, right| {
            right
                .started_at_ms
                .cmp(&left.started_at_ms)
                .then_with(|| left.assignment_id.cmp(&right.assignment_id))
        });
        Ok(placements)
    }

    async fn publish_node_stats(
        &self,
        node_id: &str,
        snapshot: &crate::cluster_stats::ControllerStatsSnapshot,
    ) -> Result<()> {
        let mut client = self.client.lock().await;
        let lease = client.lease_grant(30, None).await?.id();
        client
            .put(
                format!("/maetro/cluster/stats/{node_id}"),
                serde_json::to_vec(snapshot)?,
                Some(PutOptions::new().with_lease(lease)),
            )
            .await?;
        Ok(())
    }

    async fn list_node_stats(
        &self,
    ) -> Result<std::collections::BTreeMap<String, crate::cluster_stats::ControllerStatsSnapshot>>
    {
        let entries = self
            .get_prefix_entries("/maetro/cluster/stats/", false, None)
            .await?;
        entries
            .iter()
            .map(|entry| {
                let node_id = std::str::from_utf8(entry.key())?
                    .trim_start_matches("/maetro/cluster/stats/")
                    .to_string();
                Ok((node_id, serde_json::from_slice(entry.value())?))
            })
            .collect()
    }

    async fn publish_node_disks(
        &self,
        node_id: &str,
        disks: &[crate::cluster::NodeDiskInfo],
    ) -> Result<()> {
        let mut client = self.client.lock().await;
        let lease = client.lease_grant(90, None).await?.id();
        client
            .put(
                format!("/maetro/cluster/disks/{node_id}"),
                serde_json::to_vec(disks)?,
                Some(PutOptions::new().with_lease(lease)),
            )
            .await?;
        Ok(())
    }

    async fn list_node_disks(
        &self,
    ) -> Result<std::collections::BTreeMap<String, Vec<crate::cluster::NodeDiskInfo>>> {
        let entries = self
            .get_prefix_entries("/maetro/cluster/disks/", false, None)
            .await?;
        entries
            .iter()
            .map(|entry| {
                let node_id = std::str::from_utf8(entry.key())?
                    .trim_start_matches("/maetro/cluster/disks/")
                    .to_string();
                Ok((node_id, serde_json::from_slice(entry.value())?))
            })
            .collect()
    }

    async fn claim_cluster_request(
        &self,
        local_node_id: &str,
        request_id: &str,
        fingerprint: &str,
        now_ms: i64,
    ) -> Result<crate::deployment::store::RequestClaim> {
        if self.mutation_relay.is_some() {
            let value = self
                .relay_mutation(crate::deployment::store::ClusterMutation::ClaimRequest {
                    request_id: request_id.to_string(),
                    fingerprint: fingerprint.to_string(),
                    now_ms,
                })
                .await?
                .ok_or_else(|| anyhow!("daemon returned no request claim"))?;
            return crate::deployment::store::RequestClaim::from_json(value);
        }
        validate_request_id(request_id)?;
        let key = format!("/maetro/cluster/requests/{request_id}");
        let mut client = self.client.lock().await;
        let leader = client.leader("/maetro/cluster/leader").await?;
        let leader = leader
            .kv()
            .filter(|entry| entry.value() == local_node_id.as_bytes())
            .ok_or_else(|| anyhow!("local node is no longer cluster leader"))?;
        let response = client.get(key.clone(), None).await?;
        if let Some(entry) = response.kvs().first() {
            return request_claim_from_receipt(entry.value(), fingerprint);
        }
        let receipt = ClusterRequestReceipt {
            fingerprint: fingerprint.to_string(),
            state: "in-progress".to_string(),
            status_code: None,
            content_type: None,
            body: Vec::new(),
            updated_at_ms: now_ms,
        };
        let transaction = Txn::new()
            .when([
                Compare::create_revision(leader.key(), CompareOp::Equal, leader.create_revision()),
                Compare::version(key.clone(), CompareOp::Equal, 0),
            ])
            .and_then([TxnOp::put(key.clone(), serde_json::to_vec(&receipt)?, None)]);
        if client.txn(transaction).await?.succeeded() {
            return Ok(crate::deployment::store::RequestClaim::Started);
        }
        let response = client.get(key, None).await?;
        response.kvs().first().map_or_else(
            || Err(anyhow!("leadership changed while claiming request")),
            |entry| request_claim_from_receipt(entry.value(), fingerprint),
        )
    }

    async fn complete_cluster_request(
        &self,
        local_node_id: &str,
        request_id: &str,
        fingerprint: &str,
        status_code: u16,
        content_type: Option<&str>,
        body: &[u8],
        now_ms: i64,
    ) -> Result<()> {
        if self.mutation_relay.is_some() {
            self.relay_mutation(crate::deployment::store::ClusterMutation::CompleteRequest {
                request_id: request_id.to_string(),
                fingerprint: fingerprint.to_string(),
                status_code,
                content_type: content_type.map(str::to_string),
                body: body.to_vec(),
                now_ms,
            })
            .await?;
            return Ok(());
        }
        validate_request_id(request_id)?;
        if body.len() > 1024 * 1024 {
            bail!("cluster request response exceeds the 1 MiB receipt limit");
        }
        let key = format!("/maetro/cluster/requests/{request_id}");
        let mut client = self.client.lock().await;
        let leader = client.leader("/maetro/cluster/leader").await?;
        let leader = leader
            .kv()
            .filter(|entry| entry.value() == local_node_id.as_bytes())
            .ok_or_else(|| anyhow!("local node is no longer cluster leader"))?;
        let response = client.get(key.clone(), None).await?;
        let current = response
            .kvs()
            .first()
            .ok_or_else(|| anyhow!("cluster request receipt disappeared"))?;
        let current_value = current.value().to_vec();
        let current_receipt: ClusterRequestReceipt = serde_json::from_slice(&current_value)?;
        if current_receipt.fingerprint != fingerprint {
            bail!("cluster request fingerprint changed");
        }
        let receipt = ClusterRequestReceipt {
            fingerprint: fingerprint.to_string(),
            state: "complete".to_string(),
            status_code: Some(status_code),
            content_type: content_type.map(str::to_string),
            body: body.to_vec(),
            updated_at_ms: now_ms,
        };
        let transaction = Txn::new()
            .when([
                Compare::create_revision(leader.key(), CompareOp::Equal, leader.create_revision()),
                Compare::value(key.clone(), CompareOp::Equal, current_value),
            ])
            .and_then([TxnOp::put(key, serde_json::to_vec(&receipt)?, None)]);
        if !client.txn(transaction).await?.succeeded() {
            bail!("leadership or request receipt changed before completion");
        }
        Ok(())
    }

    async fn sweep_cluster_state(
        &self,
        token: &crate::cluster::types::LeadershipToken,
        now_ms: i64,
    ) -> Result<()> {
        let mut client = self.client.lock().await;
        let records =
            crate::utils::etcd::get_prefix(&client, "/maetro/cluster/node-records/", false, None)
                .await?;
        let parsed_records = records
            .iter()
            .map(|entry| {
                serde_json::from_slice::<crate::cluster::NodeRecord>(entry.value())
                    .map(|record| (record.last_info.node_id.clone(), record))
                    .map_err(Into::into)
            })
            .collect::<Result<std::collections::BTreeMap<_, _>>>()?;
        for (node_id, record) in &parsed_records {
            if record
                .lost_at_ms
                .is_some_and(|lost_at| now_ms.saturating_sub(lost_at) >= 7 * 24 * 60 * 60 * 1000)
            {
                let transaction = Txn::new()
                    .when([cluster_leadership_compare(token)])
                    .and_then([TxnOp::delete(
                        format!("/maetro/cluster/node-state/{node_id}"),
                        None,
                    )]);
                if !client.txn(transaction).await?.succeeded() {
                    bail!("leadership changed during node-state garbage collection");
                }
            }
        }

        let states =
            crate::utils::etcd::get_prefix(&client, "/maetro/cluster/replica-states/", false, None)
                .await?;
        for entry in &states {
            let key = entry.key().to_vec();
            let previous = entry.value().to_vec();
            let mut state: ReplicaState = serde_json::from_slice(&previous)?;
            let dead_past_grace = state
                .node_id
                .as_ref()
                .and_then(|node_id| parsed_records.get(node_id))
                .and_then(|record| record.lost_at_ms)
                .is_some_and(|lost_at| now_ms.saturating_sub(lost_at) >= 30_000);
            if !dead_past_grace || state.status == DeploymentStatus::Crashed {
                continue;
            }
            state.status = DeploymentStatus::Crashed;
            state.error = Some("node lost".to_string());
            let transaction = Txn::new()
                .when([
                    cluster_leadership_compare(token),
                    Compare::value(key.clone(), CompareOp::Equal, previous),
                ])
                .and_then([TxnOp::put(key, serde_json::to_vec(&state)?, None)]);
            if !client.txn(transaction).await?.succeeded() {
                bail!("leadership or replica state changed during lost-node sweep");
            }
        }

        let placements =
            crate::utils::etcd::get_prefix(&client, "/maetro/cluster/placements/", false, None)
                .await?;
        for entry in &placements {
            let placement: crate::cluster::PlacementHistory =
                serde_json::from_slice(entry.value())?;
            let index_key = format!(
                "/maetro/cluster/placement-index/{}/{}/{}/{}",
                placement.service_id,
                placement.deployment_id,
                placement.replica_index,
                placement.assignment_id
            );
            let index = client.get(index_key.clone(), None).await?;
            if index
                .kvs()
                .first()
                .is_some_and(|index| index.value() == entry.key())
            {
                continue;
            }
            let transaction = Txn::new()
                .when([cluster_leadership_compare(token)])
                .and_then([TxnOp::put(index_key, entry.key(), None)]);
            if !client.txn(transaction).await?.succeeded() {
                bail!("leadership changed while rebuilding placement index");
            }
        }

        let members = client.member_list().await?;
        let mut desired_voters = std::collections::BTreeMap::new();
        for member in members
            .members()
            .iter()
            .filter(|member| !member.is_learner())
        {
            desired_voters.insert(
                format!("/maetro/cluster/voters/{:016x}", member.id()),
                serde_json::to_vec(&serde_json::json!({
                    "memberId": member.id(),
                    "name": member.name(),
                    "peerUrls": member.peer_urls(),
                    "clientUrls": member.client_urls(),
                }))?,
            );
        }
        let existing_voters =
            crate::utils::etcd::get_prefix(&client, "/maetro/cluster/voters/", false, None).await?;
        let existing_voter_values = existing_voters
            .iter()
            .map(|entry| (entry.key().to_vec(), entry.value().to_vec()))
            .collect::<std::collections::BTreeMap<_, _>>();
        for (key, value) in &desired_voters {
            if existing_voter_values
                .get(key.as_bytes())
                .is_some_and(|existing| existing == value)
            {
                continue;
            }
            let transaction = Txn::new()
                .when([cluster_leadership_compare(token)])
                .and_then([TxnOp::put(key.as_str(), value.clone(), None)]);
            if !client.txn(transaction).await?.succeeded() {
                bail!("leadership changed while reconciling voter records");
            }
        }
        for stale in existing_voter_values.keys() {
            if desired_voters.contains_key(std::str::from_utf8(stale)?) {
                continue;
            }
            let transaction = Txn::new()
                .when([cluster_leadership_compare(token)])
                .and_then([TxnOp::delete(stale.clone(), None)]);
            if !client.txn(transaction).await?.succeeded() {
                bail!("leadership changed while removing a stale voter record");
            }
        }

        let receipts =
            crate::utils::etcd::get_prefix(&client, "/maetro/cluster/requests/", false, None)
                .await?;
        for entry in &receipts {
            let receipt: ClusterRequestReceipt = serde_json::from_slice(entry.value())?;
            if now_ms.saturating_sub(receipt.updated_at_ms) < 24 * 60 * 60 * 1000 {
                continue;
            }
            let transaction = Txn::new()
                .when([
                    cluster_leadership_compare(token),
                    Compare::value(entry.key(), CompareOp::Equal, entry.value()),
                ])
                .and_then([TxnOp::delete(entry.key(), None)]);
            if !client.txn(transaction).await?.succeeded() {
                bail!("leadership or request receipt changed during garbage collection");
            }
        }
        Ok(())
    }

    async fn list_service_ids(&self) -> anyhow::Result<Vec<String>> {
        let prefix_key = format!("{SERVICES_ROOT}/");
        let entries = self
            .get_prefix_entries(prefix_key.as_bytes(), true, None)
            .await?;

        let mut ids = HashSet::new();
        for kv in &entries {
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
        let mut queued = Vec::new();
        for service_id in self.list_service_ids().await? {
            let prefix_key = service_deployment_history_prefix(&service_id);
            let entries = self
                .get_prefix_entries(prefix_key.as_bytes(), false, None)
                .await?;
            for kv in &entries {
                let key = String::from_utf8(kv.key().to_vec()).map_err(|err| {
                    anyhow!("deployment key for `{prefix_key}` is not utf8: {err}")
                })?;
                let deployment = serde_json::from_slice::<ServiceDeployment>(kv.value())
                    .map_err(|err| anyhow!("invalid deployment JSON at key `{key}`: {err}"))?;
                if deployment.status != DeploymentStatus::Queued {
                    continue;
                }

                let mod_revision = decode_mod_revision(kv.mod_revision(), &key)?;
                let mut deployment = deployment;
                if let Some(secrets) = &mut deployment.config.deploy.secrets
                    && secrets.items.is_empty()
                    && !secrets.keys.is_empty()
                {
                    let key = deployment_deploy_secrets_key(&service_id, &deployment.id);
                    secrets.items = self.read_encrypted(&key).await;
                }
                self.restore_deployment_data(&service_id, &mut deployment)
                    .await;
                queued.push(QueuedDeployment {
                    service_id: service_id.clone(),
                    key,
                    mod_revision,
                    deployment,
                });
            }
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
        let prev_keys = self.prev_secret_keys(&queued_deployment.service_id).await;
        let building = self.strip_deployment_with_metadata(&building, &prev_keys);

        let deployment_json = serde_json::to_string(&building)
            .map_err(|err| anyhow!("failed to serialize building deployment: {err}"))?;
        let info_key = service_info_key(&queued_deployment.service_id);
        let existing_info = self
            .read_service_info_snapshot(&queued_deployment.service_id)
            .await?;

        let updated_info = ServiceInfo {
            deploy_frozen: existing_info
                .as_ref()
                .map(|s| s.info.deploy_frozen)
                .unwrap_or(false),
            replicas_override: existing_info
                .as_ref()
                .and_then(|s| s.info.replicas_override),
            config: queued_deployment
                .deployment
                .config
                .strip_secrets(&Default::default()),
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

    async fn claim_deployment_building_fenced(
        &self,
        token: &crate::cluster::types::LeadershipToken,
        queued_deployment: &QueuedDeployment,
    ) -> anyhow::Result<bool> {
        let mut building = queued_deployment.deployment.clone();
        building.status = DeploymentStatus::Building;
        let prev_keys = self.prev_secret_keys(&queued_deployment.service_id).await;
        let building = self.strip_deployment_with_metadata(&building, &prev_keys);
        let deployment_json = serde_json::to_vec(&building)?;
        let info_key = service_info_key(&queued_deployment.service_id);
        let existing_info = self
            .read_service_info_snapshot(&queued_deployment.service_id)
            .await?;
        let updated_info = ServiceInfo {
            deploy_frozen: existing_info
                .as_ref()
                .map(|snapshot| snapshot.info.deploy_frozen)
                .unwrap_or(false),
            replicas_override: existing_info
                .as_ref()
                .and_then(|snapshot| snapshot.info.replicas_override),
            config: queued_deployment
                .deployment
                .config
                .strip_secrets(&Default::default()),
        };
        let transaction = Txn::new()
            .when([
                Compare::create_revision(
                    token.election_key.clone(),
                    CompareOp::Equal,
                    token.create_revision,
                ),
                compare_mod_revision_or_absent(
                    &queued_deployment.key,
                    Some(queued_deployment.mod_revision),
                ),
                compare_mod_revision_or_absent(
                    &info_key,
                    existing_info.as_ref().map(|snapshot| snapshot.mod_revision),
                ),
            ])
            .and_then([
                TxnOp::put(queued_deployment.key.clone(), deployment_json, None),
                TxnOp::put(info_key, serde_json::to_vec(&updated_info)?, None),
            ]);
        Ok(self.client.lock().await.txn(transaction).await?.succeeded())
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
            if status == DeploymentStatus::Draining && updated.drained_at.is_none() {
                updated.drained_at = Some(current_time_millis()?);
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

    async fn update_deployment_status_fenced(
        &self,
        token: &crate::cluster::types::LeadershipToken,
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
            if status == DeploymentStatus::Draining && updated.drained_at.is_none() {
                updated.drained_at = Some(current_time_millis()?);
            }
            let transaction = Txn::new()
                .when([
                    Compare::create_revision(
                        token.election_key.clone(),
                        CompareOp::Equal,
                        token.create_revision,
                    ),
                    Compare::mod_revision(
                        snapshot.key.clone(),
                        CompareOp::Equal,
                        i64::try_from(snapshot.mod_revision)
                            .map_err(|_| anyhow!("deployment revision does not fit i64"))?,
                    ),
                ])
                .and_then([TxnOp::put(
                    snapshot.key,
                    serde_json::to_vec(&updated)?,
                    None,
                )]);
            if self.client.lock().await.txn(transaction).await?.succeeded() {
                return Ok(());
            }
            let election = self
                .client
                .lock()
                .await
                .get(token.election_key.clone(), None)
                .await?;
            if !election
                .kvs()
                .first()
                .is_some_and(|entry| entry.create_revision() == token.create_revision)
            {
                return Err(anyhow!("leadership fence rejected stale status writer"));
            }
        }
        Err(anyhow!(
            "failed to update deployment `{}` to {status:?} due to concurrent updates",
            deployment.id,
        ))
    }

    async fn prepare_rollout_status_cutover(
        &self,
        incoming: &Deployment,
        draining: &[Deployment],
        now_ms: u64,
    ) -> anyhow::Result<crate::deployment::store::AtomicDeploymentUpdates> {
        let mut comparisons = Vec::with_capacity(1 + draining.len());
        let mut operations = Vec::with_capacity(1 + draining.len());
        let Some(incoming_snapshot) = self.find_deployment_snapshot(incoming).await? else {
            return Err(anyhow!(
                "incoming deployment `{}` no longer exists",
                incoming.id
            ));
        };
        if !incoming_snapshot
            .deployment
            .status
            .can_transition_to(&DeploymentStatus::Ready)
        {
            return Err(anyhow!(
                "incoming deployment `{}` cannot transition to Ready",
                incoming.id
            ));
        }
        let mut incoming_value = incoming_snapshot.deployment;
        incoming_value.status = DeploymentStatus::Ready;
        incoming_value.deployed_at.get_or_insert(now_ms);
        comparisons.push(Compare::mod_revision(
            incoming_snapshot.key.clone(),
            CompareOp::Equal,
            i64::try_from(incoming_snapshot.mod_revision)
                .map_err(|_| anyhow!("deployment revision does not fit i64"))?,
        ));
        operations.push(TxnOp::put(
            incoming_snapshot.key,
            serde_json::to_vec(&incoming_value)?,
            None,
        ));
        for deployment in draining {
            let Some(snapshot) = self.find_deployment_snapshot(deployment).await? else {
                return Err(anyhow!(
                    "old deployment `{}` no longer exists",
                    deployment.id
                ));
            };
            if !snapshot
                .deployment
                .status
                .can_transition_to(&DeploymentStatus::Draining)
            {
                continue;
            }
            let mut value = snapshot.deployment;
            value.status = DeploymentStatus::Draining;
            value.drained_at.get_or_insert(now_ms);
            comparisons.push(Compare::mod_revision(
                snapshot.key.clone(),
                CompareOp::Equal,
                i64::try_from(snapshot.mod_revision)
                    .map_err(|_| anyhow!("deployment revision does not fit i64"))?,
            ));
            operations.push(TxnOp::put(snapshot.key, serde_json::to_vec(&value)?, None));
        }
        Ok(crate::deployment::store::AtomicDeploymentUpdates {
            comparisons,
            operations,
        })
    }

    async fn save_build_data(
        &self,
        service_id: &str,
        deployment: &ServiceDeployment,
    ) -> anyhow::Result<()> {
        let deployment_id = &deployment.id;
        if let Some(build) = &deployment.config.build {
            if !build.env.items.is_empty() {
                let key = deployment_build_env_key(service_id, deployment_id);
                self.write_encrypted(&key, &build.env.items).await?;
            }
            if !build.secrets.items.is_empty() {
                let key = deployment_build_secrets_key(service_id, deployment_id);
                self.write_encrypted(&key, &build.secrets.items).await?;
            }
        }
        Ok(())
    }

    async fn save_build_data_fenced(
        &self,
        token: &crate::cluster::types::LeadershipToken,
        service_id: &str,
        deployment: &ServiceDeployment,
    ) -> anyhow::Result<()> {
        CLUSTER_WRITE_FENCE
            .scope(token.clone(), self.save_build_data(service_id, deployment))
            .await
    }

    async fn save_deploy_data(
        &self,
        service_id: &str,
        deployment: &ServiceDeployment,
    ) -> anyhow::Result<()> {
        let deployment_id = &deployment.id;
        if !deployment.config.deploy.env.items.is_empty() {
            let key = deployment_deploy_env_key(service_id, deployment_id);
            self.write_encrypted(&key, &deployment.config.deploy.env.items)
                .await?;
        }

        let dep = Deployment {
            id: deployment_id.clone(),
            service_id: service_id.to_string(),
            replica_index: 0,
        };
        let prev_keys = self.prev_secret_keys(service_id).await;
        let stripped = self.strip_deployment_with_metadata(deployment, &prev_keys);

        for _attempt in 0..MAX_STATUS_TXN_RETRIES {
            let Some(snapshot) = self.find_deployment_snapshot(&dep).await? else {
                return Err(anyhow!(
                    "deployment `{}` for service `{service_id}` not found",
                    deployment_id,
                ));
            };
            let mut stored = snapshot.deployment.clone();
            stored.config.deploy.env = stripped.config.deploy.env.clone();
            stored.config.deploy.secrets = stripped.config.deploy.secrets.clone();

            let deployment_json = serde_json::to_string(&stored)
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
            "failed to save deploy data for `{}` after retries",
            deployment_id,
        ))
    }

    async fn save_deploy_data_fenced(
        &self,
        token: &crate::cluster::types::LeadershipToken,
        service_id: &str,
        deployment: &ServiceDeployment,
    ) -> anyhow::Result<()> {
        CLUSTER_WRITE_FENCE
            .scope(token.clone(), self.save_deploy_data(service_id, deployment))
            .await
    }

    async fn update_deployment_build_info(
        &self,
        deployment: &Deployment,
        updated: &ServiceDeployment,
    ) -> anyhow::Result<()> {
        let Some(snapshot) = self.find_deployment_snapshot(deployment).await? else {
            return Err(anyhow!(
                "deployment `{}` for service `{}` not found",
                deployment.id,
                deployment.service_id,
            ));
        };

        let mut stored = snapshot.deployment.clone();
        stored.build = updated.build.clone();
        stored.git_commit = updated.git_commit.clone();

        let deployment_json = serde_json::to_string(&stored)
            .map_err(|err| anyhow!("failed to serialize deployment: {err}"))?;

        self.txn(
            vec![compare_mod_revision_or_absent(
                &snapshot.key,
                Some(snapshot.mod_revision),
            )],
            vec![request_put(&snapshot.key, &deployment_json)],
        )
        .await?;

        Ok(())
    }

    async fn update_deployment_build_info_fenced(
        &self,
        token: &crate::cluster::types::LeadershipToken,
        deployment: &Deployment,
        updated: &ServiceDeployment,
    ) -> anyhow::Result<()> {
        let Some(snapshot) = self.find_deployment_snapshot(deployment).await? else {
            return Err(anyhow!(
                "deployment `{}` for service `{}` not found",
                deployment.id,
                deployment.service_id,
            ));
        };
        let mut stored = snapshot.deployment.clone();
        stored.build = updated.build.clone();
        stored.git_commit = updated.git_commit.clone();
        let transaction = Txn::new()
            .when([
                Compare::create_revision(
                    token.election_key.clone(),
                    CompareOp::Equal,
                    token.create_revision,
                ),
                Compare::mod_revision(
                    snapshot.key.clone(),
                    CompareOp::Equal,
                    i64::try_from(snapshot.mod_revision)
                        .map_err(|_| anyhow!("deployment revision does not fit i64"))?,
                ),
            ])
            .and_then([TxnOp::put(snapshot.key, serde_json::to_vec(&stored)?, None)]);
        if !self.client.lock().await.txn(transaction).await?.succeeded() {
            return Err(anyhow!("leadership fence rejected stale build result"));
        }
        Ok(())
    }

    async fn delete_deployment(
        &self,
        deployment: &Deployment,
    ) -> anyhow::Result<Option<ServiceDeployment>> {
        if self.mutation_relay.is_some() {
            let value = self
                .relay_mutation(
                    crate::deployment::store::ClusterMutation::DeleteDeployment {
                        deployment: deployment.clone(),
                    },
                )
                .await?
                .ok_or_else(|| anyhow!("daemon returned no deployment deletion result"))?;
            return Ok(serde_json::from_value(value)?);
        }
        let Some(snapshot) = self.find_deployment_snapshot(deployment).await? else {
            return Ok(None);
        };

        let mut operations = vec![TxnOp::delete(snapshot.key.as_bytes(), None)];
        let replicas_prefix = replica_states_prefix(&deployment.service_id, &deployment.id);
        if let Some(range_end) = prefix_range_end(replicas_prefix.as_bytes()) {
            operations.push(TxnOp::delete(
                replicas_prefix.as_bytes(),
                Some(etcd_client::DeleteOptions::new().with_range(range_end)),
            ));
        }

        let dep_prefix = deployment_prefix(&deployment.service_id, &deployment.id);
        if let Some(range_end) = prefix_range_end(dep_prefix.as_bytes()) {
            operations.push(TxnOp::delete(
                dep_prefix.as_bytes(),
                Some(etcd_client::DeleteOptions::new().with_range(range_end)),
            ));
        }
        if !self
            .txn(
                vec![compare_mod_revision_or_absent(
                    &snapshot.key,
                    Some(snapshot.mod_revision),
                )],
                operations,
            )
            .await?
        {
            bail!("leadership or deployment state changed while deleting deployment");
        }

        Ok(Some(snapshot.deployment))
    }

    async fn update_replica_status(
        &self,
        service_id: &str,
        deployment_id: &str,
        replica_index: u32,
        status: DeploymentStatus,
    ) -> anyhow::Result<()> {
        let key = replica_state_key(service_id, deployment_id, replica_index);
        let existing = self
            .read_replica_states(service_id, deployment_id)
            .await
            .unwrap_or_default()
            .into_iter()
            .find(|state| state.replica_index == replica_index);
        let state = ReplicaState {
            service_id: existing.as_ref().and_then(|state| state.service_id.clone()),
            deployment_id: existing
                .as_ref()
                .and_then(|state| state.deployment_id.clone()),
            replica_index,
            status,
            healthcheck_failures: existing
                .as_ref()
                .map(|state| state.healthcheck_failures)
                .unwrap_or(0),
            restart_attempts: existing
                .as_ref()
                .map(|state| state.restart_attempts)
                .unwrap_or(0),
            node_id: existing.as_ref().and_then(|state| state.node_id.clone()),
            assignment_id: existing
                .as_ref()
                .and_then(|state| state.assignment_id.clone()),
            endpoint: existing.as_ref().and_then(|state| state.endpoint.clone()),
            error: existing.as_ref().and_then(|state| state.error.clone()),
        };
        let json = serde_json::to_string(&state)
            .map_err(|err| anyhow!("failed to serialize replica state: {err}"))?;

        let mut client = self.client.lock().await;
        client
            .put(key.as_bytes().to_vec(), json.as_bytes().to_vec(), None)
            .await
            .map_err(|err| anyhow!("failed to write replica state: {err}"))?;

        drop(client);

        self.sync_ingress_for_service(service_id).await;

        Ok(())
    }

    async fn upsert_replica_state(
        &self,
        service_id: &str,
        deployment_id: &str,
        state: ReplicaState,
    ) -> anyhow::Result<()> {
        if let (Some(node_id), Some(assignment_id)) =
            (state.node_id.as_deref(), state.assignment_id.as_deref())
        {
            let manifest_key = format!("/maetro/cluster/assignments/{node_id}");
            let state_key = format!("/maetro/cluster/replica-states/{node_id}/{assignment_id}");
            let mut client = self.client.lock().await;
            let response = client.get(manifest_key.clone(), None).await?;
            let Some(entry) = response.kvs().first() else {
                return Err(anyhow!("scheduled assignment manifest is absent"));
            };
            let manifest: crate::cluster::AssignmentManifest =
                serde_json::from_slice(entry.value())?;
            if !manifest.assignments.iter().any(|assignment| {
                assignment.assignment_id == assignment_id
                    && assignment.service_id == service_id
                    && assignment.deployment_id == deployment_id
                    && assignment.replica_index == state.replica_index
            }) {
                return Err(anyhow!("scheduled assignment is no longer desired"));
            }
            let transaction = Txn::new()
                .when([Compare::mod_revision(
                    manifest_key,
                    CompareOp::Equal,
                    entry.mod_revision(),
                )])
                .and_then([TxnOp::put(state_key, serde_json::to_vec(&state)?, None)]);
            if !client.txn(transaction).await?.succeeded() {
                return Err(anyhow!("scheduled assignment changed during state update"));
            }
            return Ok(());
        }
        let key = replica_state_key(service_id, deployment_id, state.replica_index);
        let json = serde_json::to_string(&state)
            .map_err(|err| anyhow!("failed to serialize replica state: {err}"))?;

        let mut client = self.client.lock().await;
        client
            .put(key.as_bytes().to_vec(), json.as_bytes().to_vec(), None)
            .await
            .map_err(|err| anyhow!("failed to write replica state: {err}"))?;

        drop(client);

        self.sync_ingress_for_service(service_id).await;

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

        self.sync_ingress_for_service(service_id).await;

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
        for mut deployment in deployments {
            self.restore_deployment_env(service_id, &mut deployment)
                .await;
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
        let mut infos = Vec::new();
        for service_id in self.list_service_ids().await? {
            let key = service_info_key(&service_id);
            let response = self.get(key.as_bytes().to_vec(), None).await?;
            let Some(kv) = response.kvs().first() else {
                continue;
            };

            match serde_json::from_slice::<ServiceInfo>(kv.value()) {
                Ok(info) => infos.push(info),
                Err(err) => {
                    eprintln!("[maestro]: failed to parse service info at key `{key}`: {err}");
                }
            }
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
        let entries = self
            .get_range_entries(prefix.to_vec(), range_end, false)
            .await?;

        let mut deployments = Vec::with_capacity(entries.len());
        for kv in entries.iter().rev() {
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
        let Some(snapshot) = self.read_service_info_snapshot(service_id).await? else {
            return Ok(None);
        };
        let mut info = snapshot.info;
        let deployments = self.list_service_deployments(service_id).await?;
        if let Some(latest) = deployments.first() {
            if info.config.deploy.env.items.is_empty() {
                let key = deployment_deploy_env_key(service_id, &latest.id);
                info.config.deploy.env.items = self.read_encrypted(&key).await;
            }
            if let Some(latest_secrets) = &latest.config.deploy.secrets
                && let Some(info_secrets) = &mut info.config.deploy.secrets
            {
                if info_secrets.keys.is_empty() && !latest_secrets.keys.is_empty() {
                    info_secrets.keys = latest_secrets.keys.clone();
                }
                if info_secrets.source.is_none() && latest_secrets.source.is_some() {
                    info_secrets.source = latest_secrets.source.clone();
                }
            }
            if let Some(build) = &mut info.config.build {
                if build.secrets.items.is_empty() {
                    let key = deployment_build_secrets_key(service_id, &latest.id);
                    build.secrets.items = self.read_encrypted(&key).await;
                }
                if build.env.items.is_empty() {
                    let key = deployment_build_env_key(service_id, &latest.id);
                    build.env.items = self.read_encrypted(&key).await;
                }
            }
        }
        Ok(Some(info))
    }

    async fn get_service_status(
        &self,
        service_id: &str,
    ) -> anyhow::Result<Option<DeploymentStatus>> {
        let deployments = self.list_service_deployments(service_id).await?;
        let Some(latest) = deployments.first() else {
            return Ok(None);
        };
        if matches!(
            latest.status,
            DeploymentStatus::Queued
                | DeploymentStatus::Draining
                | DeploymentStatus::Removed
                | DeploymentStatus::Canceled
                | DeploymentStatus::Terminated
        ) {
            return Ok(Some(latest.status.clone()));
        }

        let mut has_ready_replica = false;
        let mut has_any_replica = false;
        let mut all_replicas_crashed = true;

        for deployment in &deployments {
            if matches!(
                deployment.status,
                DeploymentStatus::Draining
                    | DeploymentStatus::Removed
                    | DeploymentStatus::Canceled
                    | DeploymentStatus::Terminated
            ) {
                continue;
            }
            let replicas = self
                .read_replica_states(service_id, &deployment.id)
                .await
                .unwrap_or_default();
            if replicas.is_empty() {
                continue;
            }
            has_any_replica = true;
            if replicas.iter().any(|r| r.status == DeploymentStatus::Ready) {
                has_ready_replica = true;
                break;
            }
            if !replicas
                .iter()
                .all(|r| r.status == DeploymentStatus::Crashed)
            {
                all_replicas_crashed = false;
            }
        }

        if has_ready_replica {
            Ok(Some(DeploymentStatus::Ready))
        } else if has_any_replica && all_replicas_crashed {
            Ok(Some(DeploymentStatus::Crashed))
        } else if has_any_replica {
            Ok(Some(DeploymentStatus::PendingReady))
        } else {
            Ok(Some(latest.status.clone()))
        }
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

    async fn read_deployment_secrets(
        &self,
        service_id: &str,
        deployment_id: &str,
    ) -> anyhow::Result<std::collections::HashMap<String, String>> {
        let key = deployment_deploy_secrets_key(service_id, deployment_id);
        Ok(self.read_encrypted(&key).await)
    }

    async fn read_deployment_env(
        &self,
        service_id: &str,
        deployment_id: &str,
    ) -> anyhow::Result<std::collections::HashMap<String, crate::utils::crypto::SecretString>> {
        let key = deployment_deploy_env_key(service_id, deployment_id);
        Ok(self.read_encrypted(&key).await)
    }

    async fn queue_deployment(
        &self,
        deployment: ServiceDeployment,
    ) -> anyhow::Result<ForceQueueOutcome> {
        if self.mutation_relay.is_some() {
            let value = self
                .relay_mutation(crate::deployment::store::ClusterMutation::QueueDeployment {
                    deployment,
                })
                .await?
                .ok_or_else(|| anyhow!("daemon returned no queue result"))?;
            return Ok(serde_json::from_value(value)?);
        }
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

            let prev_keys = self.prev_secret_keys(&deployment.config.id).await;
            let stripped_deployment = self.strip_deployment_with_metadata(&deployment, &prev_keys);
            let deployment_json = serde_json::to_string(&stripped_deployment)
                .map_err(|err| anyhow!("failed to serialize deployment: {err}"))?;

            let info = ServiceInfo {
                deploy_frozen: existing_info
                    .as_ref()
                    .map(|s| s.info.deploy_frozen)
                    .unwrap_or(false),
                replicas_override: existing_info
                    .as_ref()
                    .and_then(|s| s.info.replicas_override),
                config: deployment.config.strip_secrets(&Default::default()),
            };
            let info_json = serde_json::to_string(&info)
                .map_err(|err| anyhow!("failed to serialize service info: {err}"))?;

            let service_history_key = service_deployment_history_key(service_id, deployment_index);

            let compare = vec![
                Compare::version(CLUSTER_FREEZE_KEY, CompareOp::Equal, 0),
                compare_counter(&service_counter_key, &service_counter),
                compare_mod_revision_or_absent(
                    &info_key,
                    existing_info.as_ref().map(|s| s.mod_revision),
                ),
            ];
            let mut success = vec![
                request_put(
                    &service_counter_key,
                    &(deployment_index_u64 + 1).to_string(),
                ),
                request_put(&service_history_key, &deployment_json),
                request_put(&info_key, &info_json),
            ];
            success.extend(self.deployment_data_operations(service_id, &deployment)?);

            let committed = self.txn(compare, success).await?;
            if committed {
                return Ok(ForceQueueOutcome {
                    deployment_index,
                    deployment: deployment.clone(),
                });
            }
            if !self
                .get(CLUSTER_FREEZE_KEY.as_bytes().to_vec(), None)
                .await?
                .kvs()
                .is_empty()
            {
                bail!("cluster deploys are frozen for a rolling upgrade");
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
        if self.mutation_relay.is_some() {
            let value = self
                .relay_mutation(
                    crate::deployment::store::ClusterMutation::CancelDeployment {
                        deployment: deployment.clone(),
                    },
                )
                .await?
                .ok_or_else(|| anyhow!("daemon returned no cancellation result"))?;
            return Ok(serde_json::from_value(value)?);
        }
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
        if self.mutation_relay.is_some() {
            let value = self
                .relay_mutation(crate::deployment::store::ClusterMutation::StopDeployment {
                    deployment: deployment.clone(),
                })
                .await?
                .ok_or_else(|| anyhow!("daemon returned no stop result"))?;
            return Ok(serde_json::from_value(value)?);
        }
        for _attempt in 0..MAX_TXN_RETRIES {
            let Some(snapshot) = self.find_deployment_snapshot(deployment).await? else {
                return Ok(None);
            };

            match snapshot.deployment.status {
                DeploymentStatus::Ready
                | DeploymentStatus::PendingReady
                | DeploymentStatus::Building => {}
                DeploymentStatus::Draining | DeploymentStatus::Removed => {
                    return Ok(Some(snapshot.deployment));
                }
                _ => {
                    return Ok(Some(snapshot.deployment));
                }
            }

            let mut updated = snapshot.deployment.clone();
            updated.status = DeploymentStatus::Draining;
            updated.drained_at = Some(current_time_millis()?);
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
        if self.mutation_relay.is_some() {
            self.relay_mutation(
                crate::deployment::store::ClusterMutation::UpdateServiceConfig {
                    service_id: service_id.to_string(),
                    config,
                },
            )
            .await?;
            return Ok(());
        }
        for _attempt in 0..MAX_STATUS_TXN_RETRIES {
            let Some(info_snapshot) = self.read_service_info_snapshot(service_id).await? else {
                return Err(anyhow!("service `{service_id}` not found"));
            };

            let mut updated_info = info_snapshot.info.clone();
            updated_info.config = config.strip_secrets(&Default::default());
            let info_json = serde_json::to_string(&updated_info)
                .map_err(|err| anyhow!("failed to serialize service info: {err}"))?;

            let active_deployment = self.find_active_deployment(service_id).await;

            let mut compare = vec![
                Compare::version(CLUSTER_FREEZE_KEY, CompareOp::Equal, 0),
                compare_mod_revision_or_absent(
                    &info_snapshot.key,
                    Some(info_snapshot.mod_revision),
                ),
            ];
            let mut success = vec![request_put(&info_snapshot.key, &info_json)];

            if let Some(dep_snapshot) = &active_deployment {
                let mut updated_dep = dep_snapshot.deployment.clone();
                updated_dep.config = config.clone();
                let updated_dep = updated_dep;
                let dep_json = serde_json::to_string(&updated_dep)
                    .map_err(|err| anyhow!("failed to serialize deployment: {err}"))?;
                compare.push(compare_mod_revision_or_absent(
                    &dep_snapshot.key,
                    Some(dep_snapshot.mod_revision),
                ));
                success.push(request_put(&dep_snapshot.key, &dep_json));
                if let Some(build) = &config.build {
                    if !build.secrets.items.is_empty() {
                        let key =
                            deployment_build_secrets_key(service_id, &dep_snapshot.deployment.id);
                        success.push(self.encrypted_put(&key, &build.secrets.items)?);
                    }
                    if !build.env.items.is_empty() {
                        let key = deployment_build_env_key(service_id, &dep_snapshot.deployment.id);
                        success.push(self.encrypted_put(&key, &build.env.items)?);
                    }
                }
            }

            let committed = self.txn(compare, success).await?;

            if committed {
                self.sync_ingress_for_service(service_id).await;
                return Ok(());
            }
            if !self
                .get(CLUSTER_FREEZE_KEY.as_bytes().to_vec(), None)
                .await?
                .kvs()
                .is_empty()
            {
                bail!("cluster deploys are frozen for a rolling upgrade");
            }
        }

        Err(anyhow!(
            "failed to update service config for `{service_id}` due to concurrent updates"
        ))
    }

    async fn update_service_config_fenced(
        &self,
        token: &crate::cluster::types::LeadershipToken,
        service_id: &str,
        config: ServiceConfig,
    ) -> anyhow::Result<()> {
        CLUSTER_WRITE_FENCE
            .scope(
                token.clone(),
                self.update_service_config(service_id, config),
            )
            .await
    }

    async fn set_blocked_ingress_ip(
        &self,
        address: &str,
        blocked: bool,
    ) -> anyhow::Result<Vec<String>> {
        if self.mutation_relay.is_some() {
            let value = self
                .relay_mutation(
                    crate::deployment::store::ClusterMutation::SetBlockedIngressIp {
                        address: address.to_string(),
                        blocked,
                    },
                )
                .await?
                .ok_or_else(|| anyhow!("daemon returned no ingress blocklist"))?;
            return Ok(serde_json::from_value(value)?);
        }
        let fence = Self::current_write_fence();
        let blocked_ips =
            ingress_blocklist::set(&self.client, address, blocked, fence.as_ref()).await?;
        if self.read_cluster_meta().await?.is_none() {
            ingress_blocklist::reconcile_traefik(&self.client, &blocked_ips, fence.as_ref())
                .await?;
        }
        Ok(blocked_ips)
    }

    async fn read_ingress_blocklist(&self) -> anyhow::Result<Vec<String>> {
        ingress_blocklist::read(&self.client).await
    }

    async fn set_deploy_frozen(&self, service_id: &str, frozen: bool) -> anyhow::Result<()> {
        if self.mutation_relay.is_some() {
            self.relay_mutation(crate::deployment::store::ClusterMutation::SetDeployFrozen {
                service_id: service_id.to_string(),
                frozen,
            })
            .await?;
            return Ok(());
        }
        let key = service_info_key(service_id);
        let response = self.get(key.as_bytes().to_vec(), None).await?;
        let kv = response
            .kvs()
            .first()
            .ok_or_else(|| anyhow!("service `{service_id}` not found"))?;
        let mut info: ServiceInfo = serde_json::from_slice(kv.value())
            .map_err(|err| anyhow!("invalid service info JSON: {err}"))?;
        info.deploy_frozen = frozen;
        let info_json = serde_json::to_string(&info)
            .map_err(|err| anyhow!("failed to serialize service info: {err}"))?;
        if !self
            .txn(
                vec![compare_mod_revision_or_absent(
                    &key,
                    Some(kv.mod_revision() as u64),
                )],
                vec![request_put(&key, &info_json)],
            )
            .await?
        {
            bail!("leadership or service state changed while updating deploy freeze");
        }
        Ok(())
    }

    async fn set_replicas_override(
        &self,
        service_id: &str,
        override_value: Option<u32>,
    ) -> anyhow::Result<()> {
        if self.mutation_relay.is_some() {
            self.relay_mutation(
                crate::deployment::store::ClusterMutation::SetReplicasOverride {
                    service_id: service_id.to_string(),
                    override_value,
                },
            )
            .await?;
            return Ok(());
        }
        let key = service_info_key(service_id);
        let response = self.get(key.as_bytes().to_vec(), None).await?;
        let kv = response
            .kvs()
            .first()
            .ok_or_else(|| anyhow!("service `{service_id}` not found"))?;
        let mut info: ServiceInfo = serde_json::from_slice(kv.value())
            .map_err(|err| anyhow!("invalid service info JSON: {err}"))?;
        info.replicas_override = override_value;
        let info_json = serde_json::to_string(&info)
            .map_err(|err| anyhow!("failed to serialize service info: {err}"))?;
        if !self
            .txn(
                vec![compare_mod_revision_or_absent(
                    &key,
                    Some(kv.mod_revision() as u64),
                )],
                vec![request_put(&key, &info_json)],
            )
            .await?
        {
            bail!("leadership or service state changed while updating replicas");
        }
        Ok(())
    }

    async fn list_slack_webhooks(&self) -> anyhow::Result<Vec<crate::slack::SlackWebhook>> {
        Ok(self
            .read_encrypted::<Vec<crate::slack::SlackWebhook>>(
                crate::deployment::keys::SLACK_WEBHOOKS_KEY,
            )
            .await)
    }

    async fn write_slack_webhooks(
        &self,
        webhooks: &[crate::slack::SlackWebhook],
    ) -> anyhow::Result<()> {
        if self.mutation_relay.is_some() {
            self.relay_mutation(
                crate::deployment::store::ClusterMutation::WriteSlackWebhooks {
                    webhooks: webhooks.to_vec(),
                },
            )
            .await?;
            return Ok(());
        }
        self.write_encrypted(crate::deployment::keys::SLACK_WEBHOOKS_KEY, &webhooks)
            .await?;
        Ok(())
    }

    async fn delete_service(&self, service_id: &str) -> anyhow::Result<()> {
        if self.mutation_relay.is_some() {
            self.relay_mutation(crate::deployment::store::ClusterMutation::DeleteService {
                service_id: service_id.to_string(),
            })
            .await?;
            return Ok(());
        }
        let prefix = service_prefix(service_id);
        let range_end = prefix_range_end(prefix.as_bytes())
            .ok_or_else(|| anyhow!("failed to compute range end for service prefix"))?;
        if !self
            .txn(
                Vec::new(),
                vec![TxnOp::delete(
                    prefix.as_bytes(),
                    Some(etcd_client::DeleteOptions::new().with_range(range_end)),
                )],
            )
            .await?
        {
            bail!("leadership changed while deleting service `{service_id}`");
        }
        let _ = self.remove_ingress(service_id).await;

        Ok(())
    }

    async fn read_system_upgrade_request(
        &self,
        node_id: Option<&str>,
    ) -> anyhow::Result<Option<SystemUpgradeRequest>> {
        let response = self
            .get(system_upgrade_request_key(node_id).into_bytes(), None)
            .await?;
        if let Some(kv) = response.kvs().first() {
            SystemUpgradeRequest::from_storage(kv.value())
                .map(Some)
                .map_err(|err| anyhow!("invalid upgrade request value: {err}"))
        } else {
            Ok(None)
        }
    }

    async fn put_system_upgrade_request(
        &self,
        node_id: Option<&str>,
        request: &SystemUpgradeRequest,
    ) -> anyhow::Result<()> {
        let value = request
            .to_storage()
            .map_err(|err| anyhow!("failed to encode upgrade request: {err}"))?;
        let mut client = self.client.lock().await;
        client
            .put(system_upgrade_request_key(node_id), value, None)
            .await
            .map_err(|err| anyhow!("failed to write upgrade request: {err}"))?;
        Ok(())
    }

    async fn delete_system_upgrade_request(&self, node_id: Option<&str>) -> anyhow::Result<()> {
        let mut client = self.client.lock().await;
        client
            .delete(system_upgrade_request_key(node_id), None)
            .await
            .map_err(|err| anyhow!("failed to delete upgrade request: {err}"))?;
        Ok(())
    }

    async fn read_system_restart_request(&self, node_id: Option<&str>) -> anyhow::Result<bool> {
        let response = self
            .get(system_restart_request_key(node_id).into_bytes(), None)
            .await?;
        Ok(!response.kvs().is_empty())
    }

    async fn put_system_restart_request(&self, node_id: Option<&str>) -> anyhow::Result<()> {
        let mut client = self.client.lock().await;
        client
            .put(system_restart_request_key(node_id), b"1".to_vec(), None)
            .await
            .map_err(|err| anyhow!("failed to write restart request: {err}"))?;
        Ok(())
    }

    async fn delete_system_restart_request(&self, node_id: Option<&str>) -> anyhow::Result<()> {
        let mut client = self.client.lock().await;
        client
            .delete(system_restart_request_key(node_id), None)
            .await
            .map_err(|err| anyhow!("failed to delete restart request: {err}"))?;
        Ok(())
    }

    async fn list_ingress_routes(&self) -> anyhow::Result<Vec<IngressRouting>> {
        use std::collections::HashMap;

        let routers_prefix = "traefik/http/routers/";
        let services_prefix = "traefik/http/services/";

        let traffic_entries = self
            .get_prefix_entries("/maetro/cluster/traffic/", false, None)
            .await?;
        let cluster_services = traffic_entries
            .iter()
            .filter_map(|entry| {
                serde_json::from_slice::<crate::cluster::types::TrafficGeneration>(entry.value())
                    .ok()
                    .map(|traffic| traffic.service_id)
            })
            .collect::<HashSet<_>>();
        let mut routers: HashMap<String, (String, Vec<String>, String)> = HashMap::new();

        if let Some(range_end) = prefix_range_end(routers_prefix.as_bytes()) {
            let entries = self
                .get_range_entries(routers_prefix.as_bytes().to_vec(), range_end, false)
                .await?;
            for kv in &entries {
                let key = String::from_utf8_lossy(kv.key()).to_string();
                let value = String::from_utf8_lossy(kv.value()).to_string();
                let rest = &key[routers_prefix.len()..];
                let service_id = rest.split('/').next().unwrap_or_default().to_string();
                if service_id.is_empty() || ingress_blocklist::is_internal_router_label(&service_id)
                {
                    continue;
                }
                if !cluster_services.is_empty() && !cluster_services.contains(&service_id) {
                    continue;
                }
                let entry = routers
                    .entry(service_id)
                    .or_insert_with(|| (String::new(), Vec::new(), String::new()));
                if key.ends_with("/rule") {
                    entry.0 = value;
                } else if key.contains("/entryPoints/") {
                    entry.1.push(value);
                } else if key.ends_with("/service") {
                    entry.2 = value;
                }
            }
        }

        let mut server_map: HashMap<String, Vec<String>> = HashMap::new();

        if let Some(range_end) = prefix_range_end(services_prefix.as_bytes()) {
            let entries = self
                .get_range_entries(services_prefix.as_bytes().to_vec(), range_end, false)
                .await?;
            for kv in &entries {
                let key = String::from_utf8_lossy(kv.key()).to_string();
                let value = String::from_utf8_lossy(kv.value()).to_string();
                let rest = &key[services_prefix.len()..];
                let service_id = rest.split('/').next().unwrap_or_default().to_string();
                if !service_id.is_empty() && key.ends_with("/url") {
                    server_map.entry(service_id).or_default().push(value);
                }
            }
        }

        let mut routes: Vec<IngressRouting> = routers
            .into_iter()
            .filter(|(_, (rule, _, _))| !rule.is_empty())
            .map(|(service_id, (rule, entry_points, service_label))| {
                let servers = server_map.remove(&service_label).unwrap_or_default();
                IngressRouting {
                    service_id,
                    rule,
                    entry_points,
                    servers,
                }
            })
            .collect();
        routes.sort_by(|a, b| a.service_id.cmp(&b.service_id));

        Ok(routes)
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

fn cluster_leadership_compare(token: &crate::cluster::types::LeadershipToken) -> Compare {
    Compare::create_revision(
        token.election_key.clone(),
        CompareOp::Equal,
        token.create_revision,
    )
}

fn validate_request_id(request_id: &str) -> Result<()> {
    if request_id.is_empty()
        || request_id.len() > 128
        || !request_id
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || "-_.".contains(character))
    {
        bail!("invalid Idempotency-Key");
    }
    Ok(())
}

fn request_claim_from_receipt(
    value: &[u8],
    fingerprint: &str,
) -> Result<crate::deployment::store::RequestClaim> {
    let receipt: ClusterRequestReceipt = serde_json::from_slice(value)?;
    if receipt.fingerprint != fingerprint {
        return Ok(crate::deployment::store::RequestClaim::Conflict);
    }
    if receipt.state == "complete" {
        Ok(crate::deployment::store::RequestClaim::Complete {
            status_code: receipt.status_code.unwrap_or(500),
            content_type: receipt.content_type,
            body: receipt.body,
        })
    } else {
        Ok(crate::deployment::store::RequestClaim::InProgress)
    }
}

fn host_rule(host: &str) -> String {
    if host
        .chars()
        .any(|c| !c.is_ascii_alphanumeric() && c != '.' && c != '-')
    {
        format!("HostRegexp(`{host}`)")
    } else {
        format!("Host(`{host}`)")
    }
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
