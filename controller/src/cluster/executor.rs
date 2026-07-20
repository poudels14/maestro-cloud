use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, anyhow, bail};

use crate::{
    cluster::{
        Assignment, AssignmentManifest, ImageAssignment, NodeGatewayEndpoint, PlacementHistory,
        ReplicaEndpoint, assignment_store::AssignmentStore, registry::NodeRegistry,
        retain_peer_image,
    },
    deployment::{
        provider::{ContainerDeploymentProvider, ReplicaRuntimeIdentity, replica_container_name},
        store::ClusterStore,
        types::{ControllerConfig, Deployment, DeploymentStatus, ReplicaState, ServiceDeployment},
    },
    engine::{Engine, ReplicaHandle, ReplicaSpec},
    logs::{LogConfig, LogEntry, LogOrigin, Logger},
    runtime::RuntimeProvider,
    supervisor::ShutdownRequest,
};

const RESTART_DELAY_MS: u64 = 5_000;
const SHUTDOWN_GRACE_MS: u64 = 60_000;
const MAX_RESTARTS: Option<u32> = Some(10);
const IMAGE_PRUNE_DELAY_MS: i64 = 60 * 60 * 1_000;

#[derive(Debug, Clone)]
pub struct RunningReplica {
    pub assignment: Assignment,
    pub endpoint: Option<ReplicaEndpoint>,
    pub container_hostname: String,
    pub handle: Option<ReplicaHandle>,
}

pub struct EngineReplicaExecutor {
    node_id: String,
    cluster_name: String,
    cluster_host_ip: String,
    cluster_api_port: u16,
    cluster_gateway_port: u16,
    log_tags: Vec<String>,
    runtime_suffix: Option<String>,
    runtime: Arc<dyn RuntimeProvider>,
    store: Arc<dyn ClusterStore>,
    assignments: Arc<dyn AssignmentStore>,
    registry: Arc<dyn NodeRegistry>,
    image_http: reqwest::Client,
    image_peer_secret: String,
    engine: Arc<Engine>,
    logger: Logger,
    log_sender: Option<flume::Sender<LogEntry>>,
    egress_firewall: Option<crate::firewall::FirewallManager>,
    running: BTreeMap<String, RunningReplica>,
    image_prune_candidates: BTreeMap<String, i64>,
}

impl EngineReplicaExecutor {
    pub fn new(
        config: &ControllerConfig,
        runtime: Arc<dyn RuntimeProvider>,
        store: Arc<dyn ClusterStore>,
        assignments: Arc<dyn AssignmentStore>,
        registry: Arc<dyn NodeRegistry>,
        log_sender: Option<flume::Sender<LogEntry>>,
    ) -> Result<Self> {
        let cluster = config
            .cluster
            .as_ref()
            .ok_or_else(|| anyhow!("assignment executor requires cluster configuration"))?;
        let ca = std::fs::read(config.certs_dir().join("ca.pem"))?;
        let mut identity = std::fs::read(config.certs_dir().join("client.pem"))?;
        identity.extend_from_slice(b"\n");
        identity.extend_from_slice(&std::fs::read(config.certs_dir().join("client-key.pem"))?);
        let image_http = reqwest::Client::builder()
            .add_root_certificate(reqwest::Certificate::from_pem(&ca)?)
            .identity(reqwest::Identity::from_pem(&identity)?)
            .https_only(true)
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(6 * 60 * 60))
            .build()?;
        let image_peer_secret = config
            .jwt_secret_key
            .clone()
            .ok_or_else(|| anyhow!("cluster image distribution requires jwt-secret-key"))?;
        let dns_domain = Some(format!("{}.maestro.internal", config.cluster_name));
        let dns_server = config
            .subnet
            .as_deref()
            .map(crate::cluster::network::Ipv4Cidr::parse)
            .transpose()?
            .map(|subnet| subnet.gateway_address().to_string());
        let provider = Arc::new(ContainerDeploymentProvider {
            runtime: runtime.clone(),
            build_command_env: config.build_command_env.clone(),
            network: config.network.clone(),
            dns_domain,
            dns_server,
            secrets_dir: std::fs::canonicalize(&config.data_dir)
                .unwrap_or_else(|_| config.data_dir.clone())
                .join("secrets"),
            uploads_dir: config.probe_dir().join("data/uploads"),
        });
        let supervisor = Arc::new(crate::engine::replica_supervisor::JobReplicaSupervisor::new());
        Ok(Self {
            node_id: cluster.node_id.clone(),
            cluster_name: config.cluster_name.clone(),
            cluster_host_ip: cluster.host_ip.to_string(),
            cluster_api_port: cluster.api_port,
            cluster_gateway_port: cluster.gateway_port,
            log_tags: config.tags.clone(),
            runtime_suffix: cluster.resource_suffix(),
            runtime,
            store,
            assignments,
            registry,
            image_http,
            image_peer_secret,
            engine: Arc::new(Engine::new(provider, supervisor, config.data_dir.clone())),
            logger: Logger::new(log_sender.clone()),
            log_sender,
            egress_firewall: None,
            running: BTreeMap::new(),
            image_prune_candidates: BTreeMap::new(),
        })
    }

    pub fn set_egress_firewall(&mut self, firewall: crate::firewall::FirewallManager) {
        self.egress_firewall = Some(firewall);
    }

    pub fn actual(&self) -> &BTreeMap<String, RunningReplica> {
        &self.running
    }

    pub async fn reconcile_images(&self, desired: &AssignmentManifest) -> Result<()> {
        let mut failures = Vec::new();
        for image in &desired.images {
            if let Err(error) = self.ensure_peer_image(image).await {
                failures.push(format!("{}: {error}", image.image));
            }
        }
        if !failures.is_empty() {
            bail!("{}", failures.join("; "));
        }
        Ok(())
    }

    pub async fn prune_images(&mut self, desired: &AssignmentManifest) -> Result<()> {
        let mut retained = desired
            .images
            .iter()
            .map(|image| image.image.clone())
            .collect::<BTreeSet<_>>();
        let holders = self.registry.list_local_image_holders().await?;
        if holders.is_empty() {
            self.image_prune_candidates.clear();
            return Ok(());
        }
        retained.extend(self.retained_source_images().await?);
        let now_ms = i64::try_from(crate::utils::time::current_time_millis()?).unwrap_or(i64::MAX);
        let local_images = holders
            .iter()
            .map(|holder| holder.image.clone())
            .collect::<BTreeSet<_>>();
        self.image_prune_candidates
            .retain(|image, _| local_images.contains(image) && !retained.contains(image));
        for holder in holders {
            if retained.contains(&holder.image) {
                continue;
            }
            let unassigned_at_ms = *self
                .image_prune_candidates
                .entry(holder.image.clone())
                .or_insert(now_ms);
            if !peer_image_is_prunable(unassigned_at_ms, now_ms) {
                continue;
            }
            self.registry.remove_image_holder(&holder.image).await?;
            self.runtime.remove_image(&holder.image).await?;
            self.image_prune_candidates.remove(&holder.image);
            self.logger.emit(
                "info",
                &format!("removed unassigned peer image `{}`", holder.image),
            );
        }
        Ok(())
    }

    async fn retained_source_images(&self) -> Result<BTreeSet<String>> {
        let mut retained = BTreeSet::new();
        for service in self.store.list_service_infos().await? {
            let deployments = self
                .store
                .list_service_deployments(&service.config.id)
                .await?;
            retained.extend(retained_source_images_for_node(&deployments, &self.node_id));
        }
        Ok(retained)
    }

    async fn ensure_peer_image(&self, desired: &ImageAssignment) -> Result<()> {
        if self
            .runtime
            .image_exists(&desired.image)
            .await
            .unwrap_or(false)
        {
            self.registry.publish_image_holder(&desired.image).await?;
            return Ok(());
        }
        let nodes = self
            .registry
            .list_nodes()
            .await?
            .into_iter()
            .map(|node| (node.node_id.clone(), node))
            .collect::<BTreeMap<_, _>>();
        let mut candidates = self
            .registry
            .list_image_holders(&desired.image)
            .await?
            .into_iter()
            .map(|holder| holder.node_id)
            .collect::<Vec<_>>();
        if !candidates.contains(&desired.source_node_id) {
            candidates.push(desired.source_node_id.clone());
        }
        let mut failures = Vec::new();
        for node_id in candidates {
            if node_id == self.node_id {
                continue;
            }
            let Some(node) = nodes.get(&node_id) else {
                failures.push(format!("{node_id}: node is not live"));
                continue;
            };
            let mut url = reqwest::Url::parse(&format!(
                "https://{}:{}/api/cluster/images/export",
                node.cluster_host_ip, node.cluster_api_port
            ))?;
            url.query_pairs_mut().append_pair("image", &desired.image);
            let response = match self
                .image_http
                .get(url)
                .bearer_auth(image_peer_token(&self.image_peer_secret)?)
                .send()
                .await
            {
                Ok(response) => response,
                Err(error) => {
                    failures.push(format!("{node_id}: {error}"));
                    continue;
                }
            };
            if !response.status().is_success() {
                let status = response.status();
                let detail = response.text().await.unwrap_or_default();
                failures.push(format!("{node_id}: {status} {detail}"));
                continue;
            }
            use futures_util::TryStreamExt;
            let stream = response.bytes_stream().map_err(std::io::Error::other);
            let reader = tokio_util::io::StreamReader::new(stream);
            match self
                .runtime
                .import_image(&desired.image, Box::pin(reader))
                .await
            {
                Ok(()) => {
                    self.registry.publish_image_holder(&desired.image).await?;
                    self.logger.emit(
                        "info",
                        &format!(
                            "peer image `{}` imported from node `{node_id}`",
                            desired.image
                        ),
                    );
                    return Ok(());
                }
                Err(error) => {
                    let _ = self.runtime.remove_image(&desired.image).await;
                    failures.push(format!("{node_id}: {error}"));
                }
            }
        }
        bail!(
            "image `{}` is unavailable from its cluster peers{}",
            desired.image,
            if failures.is_empty() {
                String::new()
            } else {
                format!(": {}", failures.join("; "))
            }
        )
    }

    pub async fn reconcile_egress(&self, desired: &AssignmentManifest) -> Result<()> {
        let Some(firewall) = &self.egress_firewall else {
            return Ok(());
        };
        let mut configs = BTreeMap::new();
        let mut allows = Vec::new();
        for assignment in &desired.assignments {
            let key = (
                assignment.service_id.clone(),
                assignment.deployment_id.clone(),
            );
            if !configs.contains_key(&key) {
                let deployment_ref = Deployment {
                    service_id: assignment.service_id.clone(),
                    id: assignment.deployment_id.clone(),
                    replica_index: assignment.replica_index,
                };
                let deployment = self
                    .store
                    .read_service_deployment(&deployment_ref)
                    .await?
                    .ok_or_else(|| {
                        anyhow!(
                            "assigned deployment `{}` for service `{}` no longer exists",
                            assignment.deployment_id,
                            assignment.service_id
                        )
                    })?;
                configs.insert(key.clone(), deployment.config.deploy.egress);
            }
            let egress = configs.get(&key).expect("egress config was cached");
            if egress.allow.is_empty() {
                continue;
            }
            let source = assignment
                .container_ip
                .ok_or_else(|| anyhow!("assignment has no reserved workload address"))?;
            allows.extend(crate::firewall::service_allows_for_source(
                &assignment.service_id,
                source,
                egress,
            ));
        }
        firewall.replace_service_allows(allows).await
    }

    pub async fn discover(&mut self, desired: &AssignmentManifest) -> Result<()> {
        let desired_by_id: BTreeMap<&str, &Assignment> = desired
            .assignments
            .iter()
            .map(|assignment| (assignment.assignment_id.as_str(), assignment))
            .collect();
        for container in self.runtime.list_managed_containers(&self.node_id).await? {
            let Some(assignment_id) = container.labels.get("maestro.assignment-id") else {
                continue;
            };
            let Some(assignment) = desired_by_id.get(assignment_id.as_str()).copied() else {
                self.logger.emit(
                    "warn",
                    &format!(
                        "removing stale local assignment container `{}` after manifest resync",
                        container.name
                    ),
                );
                self.runtime.remove_container(&container.name).await?;
                continue;
            };
            if !labels_match_assignment(&container.labels, assignment) {
                bail!(
                    "container `{}` has corrupt immutable assignment labels",
                    container.name
                );
            }
            let endpoint = self
                .endpoint_for(assignment, &container.name)
                .await
                .unwrap_or(None);
            self.running.insert(
                assignment.assignment_id.clone(),
                RunningReplica {
                    assignment: assignment.clone(),
                    endpoint,
                    container_hostname: container.name,
                    handle: None,
                },
            );
        }
        Ok(())
    }

    pub async fn start(&mut self, assignment: &Assignment) -> Result<()> {
        if assignment.node_id != self.node_id {
            bail!("refusing an assignment owned by another node");
        }
        if self.running.contains_key(&assignment.assignment_id) {
            return Ok(());
        }
        let previous_state = self
            .assignments
            .list_replica_states()
            .await?
            .into_iter()
            .find(|state| {
                state.node_id.as_ref() == Some(&self.node_id)
                    && state.assignment_id.as_deref() == Some(assignment.assignment_id.as_str())
            });
        if previous_state.as_ref().is_some_and(|state| {
            state.status == DeploymentStatus::Crashed
                && state.restart_attempts >= crate::health::MAX_REPLICA_RESTART_ATTEMPTS
        }) {
            return Ok(());
        }
        let restart_attempts = previous_state
            .as_ref()
            .map(|state| state.restart_attempts)
            .unwrap_or(0);
        let deployment_ref = Deployment {
            service_id: assignment.service_id.clone(),
            id: assignment.deployment_id.clone(),
            replica_index: assignment.replica_index,
        };
        let mut deployment = self
            .store
            .read_service_deployment(&deployment_ref)
            .await?
            .ok_or_else(|| anyhow!("assigned deployment no longer exists"))?;
        deployment.config.deploy.env.items = self
            .store
            .read_deployment_env(&assignment.service_id, &assignment.deployment_id)
            .await?;
        if let Some(secrets) = &mut deployment.config.deploy.secrets {
            secrets.items = self
                .store
                .read_deployment_secrets(&assignment.service_id, &assignment.deployment_id)
                .await?;
        }
        prepare_volumes(&deployment)?;

        let image = deployment
            .build
            .as_ref()
            .map(|build| build.docker_image_id.as_str())
            .or(deployment.config.image.as_deref())
            .ok_or_else(|| anyhow!("assigned deployment has no runnable image"))?;
        let source = format!(
            "{}/{}/replica{}",
            assignment.service_id, assignment.deployment_id, assignment.replica_index
        );
        if !self.runtime.image_exists(image).await.unwrap_or(false) {
            if deployment
                .config
                .build
                .as_ref()
                .is_some_and(|build| build.registry.is_none())
            {
                bail!("registry-free build image `{image}` was not replicated to this node");
            }
            self.runtime
                .pull_image(image, self.log_sender.as_ref(), Some(&source))
                .await
                .with_context(|| format!("failed to pull assigned image `{image}`"))?;
        }

        let identity = ReplicaRuntimeIdentity {
            node_id: self.node_id.clone(),
            service_id: assignment.service_id.clone(),
            deployment_id: assignment.deployment_id.clone(),
            replica_index: assignment.replica_index,
            assignment_id: assignment.assignment_id.clone(),
            container_ip: assignment
                .container_ip
                .ok_or_else(|| anyhow!("assignment has no reserved workload address"))?,
            runtime_suffix: self.runtime_suffix.clone(),
        };
        let deploy_output = self
            .engine
            .deploy_command_for_assignment(&deployment, assignment.replica_index, &identity)
            .ok_or_else(|| anyhow!("assigned deployment has no deploy command"))?;
        let container_hostname = replica_container_name(
            &deployment.config.id,
            &deployment.id,
            assignment.replica_index,
            self.runtime_suffix.as_deref(),
        );
        let log_config = self.log_sender.clone().map(|sender| LogConfig {
            sender,
            tags: replica_log_tags(
                &self.log_tags,
                &self.cluster_name,
                &self.node_id,
                assignment,
                &container_hostname,
                deployment.config.deploy.healthcheck_path.as_deref(),
            ),
            origin: LogOrigin::Service,
        });
        let handle = self
            .engine
            .start_replica(ReplicaSpec {
                task_id: Some(format!("assignment-{}", assignment.assignment_id)),
                deployment: &deployment,
                replica_index: assignment.replica_index,
                deploy_output,
                max_restarts: deployment.config.deploy.max_restarts.or(MAX_RESTARTS),
                restart_delay_ms: RESTART_DELAY_MS,
                shutdown_grace_period_ms: SHUTDOWN_GRACE_MS,
                container_hostname: container_hostname.clone(),
                runtime_cli: self.runtime.cli_name().to_string(),
                log_config,
            })
            .await?
            .ok_or_else(|| anyhow!("assignment job already exists"))?;
        let endpoint = self.endpoint_for(assignment, &container_hostname).await?;
        let status = initial_replica_status(deployment.config.deploy.healthcheck_path.as_deref());
        let state = ReplicaState {
            service_id: Some(assignment.service_id.clone()),
            deployment_id: Some(assignment.deployment_id.clone()),
            replica_index: assignment.replica_index,
            status,
            healthcheck_failures: 0,
            restart_attempts,
            node_id: Some(self.node_id.clone()),
            assignment_id: Some(assignment.assignment_id.clone()),
            endpoint: endpoint.clone(),
            error: None,
        };
        if !self
            .assignments
            .upsert_replica_state_if_assignment(
                &self.node_id,
                &assignment.assignment_id,
                state,
                Some(&PlacementHistory {
                    assignment_id: assignment.assignment_id.clone(),
                    service_id: assignment.service_id.clone(),
                    deployment_id: assignment.deployment_id.clone(),
                    replica_index: assignment.replica_index,
                    node_id: self.node_id.clone(),
                    cluster_host_ip: self.cluster_host_ip.clone(),
                    cluster_api_port: self.cluster_api_port,
                    container_hostname: container_hostname.clone(),
                    started_at_ms: i64::try_from(
                        crate::utils::time::current_time_millis().unwrap_or_default(),
                    )
                    .unwrap_or(i64::MAX),
                    ended_at_ms: None,
                }),
            )
            .await?
        {
            let _ = self
                .engine
                .stop_replica(&handle, ShutdownRequest::Graceful)
                .await;
            bail!("assignment changed while its container was starting");
        }
        self.running.insert(
            assignment.assignment_id.clone(),
            RunningReplica {
                assignment: assignment.clone(),
                endpoint,
                container_hostname,
                handle: Some(handle),
            },
        );
        Ok(())
    }

    pub async fn stop(&mut self, assignment_id: &str) -> Result<()> {
        let Some(replica) = self.running.remove(assignment_id) else {
            let _ = self
                .assignments
                .delete_replica_state_if_assignment(&self.node_id, assignment_id)
                .await?;
            return Ok(());
        };
        if let Some(handle) = &replica.handle {
            let _ = self
                .engine
                .stop_replica(handle, ShutdownRequest::Graceful)
                .await;
        } else {
            self.runtime
                .remove_container(&replica.container_hostname)
                .await?;
        }
        let _ = self
            .assignments
            .delete_replica_state_if_assignment(&self.node_id, assignment_id)
            .await?;
        Ok(())
    }

    pub async fn reap_finished(&mut self) -> Result<()> {
        for finished in self.engine.reap_finished_replicas().await {
            let Some((assignment_id, replica)) = self
                .running
                .iter()
                .find(|(_, replica)| {
                    replica
                        .handle
                        .as_ref()
                        .is_some_and(|handle| handle.task_id == finished.id)
                })
                .map(|(id, replica)| (id.clone(), replica.clone()))
            else {
                continue;
            };
            self.running.remove(&assignment_id);
            let prior_attempts = self
                .assignments
                .list_replica_states()
                .await?
                .into_iter()
                .find(|state| state.assignment_id.as_deref() == Some(assignment_id.as_str()))
                .map(|state| state.restart_attempts)
                .unwrap_or(0);
            let state = ReplicaState {
                service_id: Some(replica.assignment.service_id.clone()),
                deployment_id: Some(replica.assignment.deployment_id.clone()),
                replica_index: replica.assignment.replica_index,
                status: DeploymentStatus::Crashed,
                healthcheck_failures: 0,
                restart_attempts: prior_attempts
                    .saturating_add(crate::health::MAX_REPLICA_RESTART_ATTEMPTS),
                node_id: Some(self.node_id.clone()),
                assignment_id: Some(assignment_id.clone()),
                endpoint: replica.endpoint,
                error: Some("replica supervisor exhausted its restart budget".to_string()),
            };
            let _ = self
                .assignments
                .upsert_replica_state_if_assignment(&self.node_id, &assignment_id, state, None)
                .await?;
        }
        Ok(())
    }

    pub async fn record_start_failure(
        &self,
        assignment: &Assignment,
        error: &anyhow::Error,
    ) -> Result<()> {
        let states = self.assignments.list_replica_states().await?;
        let attempts = next_start_failure_attempt(&states, assignment);
        let _ = self
            .assignments
            .upsert_replica_state_if_assignment(
                &self.node_id,
                &assignment.assignment_id,
                ReplicaState {
                    service_id: Some(assignment.service_id.clone()),
                    deployment_id: Some(assignment.deployment_id.clone()),
                    replica_index: assignment.replica_index,
                    status: DeploymentStatus::Crashed,
                    healthcheck_failures: 0,
                    restart_attempts: attempts,
                    node_id: Some(self.node_id.clone()),
                    assignment_id: Some(assignment.assignment_id.clone()),
                    endpoint: None,
                    error: Some(format!("{error:#}")),
                },
                None,
            )
            .await?;
        Ok(())
    }

    async fn endpoint_for(
        &self,
        assignment: &Assignment,
        hostname: &str,
    ) -> Result<Option<ReplicaEndpoint>> {
        let deployment_ref = Deployment {
            service_id: assignment.service_id.clone(),
            id: assignment.deployment_id.clone(),
            replica_index: assignment.replica_index,
        };
        let Some(deployment) = self.store.read_service_deployment(&deployment_ref).await? else {
            return Ok(None);
        };
        let Some(ingress) = deployment.config.ingress else {
            return Ok(None);
        };
        let container_ip = self
            .runtime
            .inspect_container_ip(hostname)
            .await
            .ok_or_else(|| anyhow!("container `{hostname}` has no network address"))?;
        if let Some(expected) = assignment.container_ip
            && container_ip != expected.to_string()
        {
            bail!(
                "container `{hostname}` received address `{container_ip}`, expected `{expected}`"
            );
        }
        Ok(Some(ReplicaEndpoint {
            container_ip,
            container_hostname: hostname.to_string(),
            ingress_container_port: ingress.port.unwrap_or(80),
            gateway: NodeGatewayEndpoint {
                host_ip: self.cluster_host_ip.parse()?,
                port: self.cluster_gateway_port,
            },
        }))
    }
}

fn peer_image_is_prunable(unassigned_at_ms: i64, now_ms: i64) -> bool {
    now_ms.saturating_sub(unassigned_at_ms) >= IMAGE_PRUNE_DELAY_MS
}

fn retained_source_images_for_node(
    deployments: &[ServiceDeployment],
    node_id: &str,
) -> BTreeSet<String> {
    let latest_peer_image_deployment = deployments
        .iter()
        .filter(|deployment| {
            deployment.build.is_some()
                && deployment
                    .config
                    .build
                    .as_ref()
                    .is_some_and(|build| build.registry.is_none())
        })
        .max_by_key(|deployment| deployment.created_at)
        .map(|deployment| deployment.id.as_str());
    deployments
        .iter()
        .filter_map(|deployment| {
            let build_config = deployment.config.build.as_ref()?;
            let build = deployment.build.as_ref()?;
            (build_config.registry.is_none()
                && build.source_node_id.as_deref() == Some(node_id)
                && retain_peer_image(
                    &deployment.status,
                    &deployment.id,
                    latest_peer_image_deployment,
                ))
            .then(|| build.docker_image_id.clone())
        })
        .collect()
}

fn image_peer_token(secret: &str) -> Result<String> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs();
    Ok(jsonwebtoken::encode(
        &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::HS256),
        &serde_json::json!({
            "sub": "maestro-image-peer",
            "scope": "image-export",
            "iat": now,
            "exp": now.saturating_add(24 * 60 * 60),
        }),
        &jsonwebtoken::EncodingKey::from_secret(secret.as_bytes()),
    )?)
}

fn initial_replica_status(healthcheck_path: Option<&str>) -> DeploymentStatus {
    if healthcheck_path.is_some_and(|path| !path.trim().is_empty()) {
        DeploymentStatus::PendingReady
    } else {
        DeploymentStatus::Ready
    }
}

fn next_start_failure_attempt(states: &[ReplicaState], assignment: &Assignment) -> u32 {
    states
        .iter()
        .filter(|state| {
            state.service_id.as_deref() == Some(assignment.service_id.as_str())
                && state.deployment_id.as_deref() == Some(assignment.deployment_id.as_str())
                && state.replica_index == assignment.replica_index
        })
        .map(|state| state.restart_attempts)
        .max()
        .unwrap_or(0)
        .saturating_add(1)
}

fn replica_log_tags(
    configured_tags: &[String],
    cluster_name: &str,
    node_id: &str,
    assignment: &Assignment,
    container_hostname: &str,
    healthcheck_path: Option<&str>,
) -> Vec<String> {
    let mut tags = configured_tags.to_vec();
    tags.extend([
        format!("service:{}", assignment.service_id),
        format!("hostname:{container_hostname}"),
        format!("deployment_id:{}", assignment.deployment_id),
        format!("replica:{}", assignment.replica_index),
        format!("cluster:{cluster_name}"),
        format!("node:{node_id}"),
        format!("assignment_id:{}", assignment.assignment_id),
    ]);
    if let Some(path) = healthcheck_path
        .map(str::trim)
        .filter(|path| !path.is_empty())
    {
        tags.push(crate::logs::healthcheck_path_tag(path));
    }
    tags
}

fn labels_match_assignment(
    labels: &std::collections::HashMap<String, String>,
    assignment: &Assignment,
) -> bool {
    labels.get("maestro.node-id") == Some(&assignment.node_id)
        && labels.get("maestro.service-id") == Some(&assignment.service_id)
        && labels.get("maestro.deployment-id") == Some(&assignment.deployment_id)
        && labels.get("maestro.replica-index").map(String::as_str)
            == Some(assignment.replica_index.to_string().as_str())
        && labels.get("maestro.assignment-id") == Some(&assignment.assignment_id)
}

fn prepare_volumes(deployment: &ServiceDeployment) -> std::io::Result<()> {
    for volume in &deployment.config.deploy.volumes {
        let path = std::path::Path::new(&volume.host_path);
        if !path.exists() {
            std::fs::create_dir_all(path)?;
        }
        if let Some(owner) = volume.owner.as_ref() {
            std::os::unix::fs::chown(path, Some(owner.uid), Some(owner.resolved_gid()))?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replicas_without_a_healthcheck_are_ready_after_start() {
        assert_eq!(initial_replica_status(None), DeploymentStatus::Ready);
        assert_eq!(initial_replica_status(Some("  ")), DeploymentStatus::Ready);
        assert_eq!(
            initial_replica_status(Some("/health")),
            DeploymentStatus::PendingReady
        );
    }

    #[test]
    fn cluster_replica_logs_include_healthcheck_and_configured_tags() {
        let assignment = Assignment {
            assignment_id: "assignment-1".to_string(),
            placement_epoch: 1,
            service_id: "app".to_string(),
            deployment_id: "deployment-1".to_string(),
            replica_index: 2,
            node_id: "node00000001".to_string(),
            container_ip: None,
            replaces_assignment_id: None,
            created_at_ms: 1,
        };
        let tags = replica_log_tags(
            &["environment:staging".to_string()],
            "cluster-a",
            "node00000001",
            &assignment,
            "app-qXMETj",
            Some(" /api/_status/db "),
        );

        for expected in [
            "environment:staging",
            "service:app",
            "hostname:app-qXMETj",
            "cluster:cluster-a",
            "node:node00000001",
            "maestro.internal.healthcheck-path:/api/_status/db",
        ] {
            assert!(
                tags.iter().any(|tag| tag == expected),
                "missing `{expected}`"
            );
        }
    }

    #[test]
    fn start_failure_attempts_continue_across_reassigned_nodes() {
        let assignment = Assignment {
            assignment_id: "assignment-3".to_string(),
            placement_epoch: 3,
            service_id: "app".to_string(),
            deployment_id: "deployment-1".to_string(),
            replica_index: 0,
            node_id: "node-c".to_string(),
            container_ip: None,
            replaces_assignment_id: Some("assignment-2".to_string()),
            created_at_ms: 3,
        };
        let states = [
            ReplicaState {
                service_id: Some("app".to_string()),
                deployment_id: Some("deployment-1".to_string()),
                replica_index: 0,
                status: DeploymentStatus::Crashed,
                healthcheck_failures: 0,
                restart_attempts: 1,
                node_id: Some("node-a".to_string()),
                assignment_id: Some("assignment-1".to_string()),
                endpoint: None,
                error: Some("pull failed".to_string()),
            },
            ReplicaState {
                service_id: Some("app".to_string()),
                deployment_id: Some("deployment-1".to_string()),
                replica_index: 0,
                status: DeploymentStatus::Crashed,
                healthcheck_failures: 0,
                restart_attempts: 2,
                node_id: Some("node-b".to_string()),
                assignment_id: Some("assignment-2".to_string()),
                endpoint: None,
                error: Some("pull failed".to_string()),
            },
        ];

        assert_eq!(next_start_failure_attempt(&states, &assignment), 3);
    }

    #[test]
    fn pruning_waits_from_when_the_image_becomes_unassigned() {
        let now_ms = 100_000;

        assert!(!peer_image_is_prunable(now_ms, now_ms));
        assert!(!peer_image_is_prunable(
            now_ms - IMAGE_PRUNE_DELAY_MS + 1,
            now_ms
        ));
        assert!(peer_image_is_prunable(
            now_ms - IMAGE_PRUNE_DELAY_MS,
            now_ms
        ));
    }

    #[test]
    fn source_keeps_active_and_latest_registry_free_images() {
        fn deployment(
            id: &str,
            created_at: u64,
            status: &str,
            image: &str,
            source_node_id: &str,
        ) -> ServiceDeployment {
            serde_json::from_value(serde_json::json!({
                "id": id,
                "createdAt": created_at,
                "status": status,
                "config": {
                    "id": "web",
                    "name": "Web",
                    "version": id,
                    "build": { "dockerfile": "Dockerfile" },
                    "deploy": { "command": null }
                },
                "gitCommit": null,
                "build": {
                    "dockerImageId": image,
                    "sourceNodeId": source_node_id
                }
            }))
            .unwrap()
        }

        let retained = retained_source_images_for_node(
            &[
                deployment("active", 1, "READY", "web:active", "node-a"),
                deployment("old", 2, "CRASHED", "web:old", "node-a"),
                deployment("latest", 3, "CRASHED", "web:latest", "node-a"),
                deployment("remote", 0, "READY", "web:remote", "node-b"),
            ],
            "node-a",
        );

        assert_eq!(
            retained,
            ["web:active", "web:latest"]
                .into_iter()
                .map(str::to_string)
                .collect()
        );
    }
}
