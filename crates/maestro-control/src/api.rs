use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, patch, post};
use axum::{Json, Router};
use maestro_core::{
    Assignment, ByteSize, Deployment, DeploymentId, DeploymentStatus, MAX_REPLICAS_OVERRIDE,
    Millicores, NodeCapacity, NodeId, NodeRecord, NodeState, ReplicaId, ReplicaStatus, RouteTable,
    ServiceControls, ServiceId, ServiceSpec, ServicesFile, SpecError, UnschedulableReplica,
};
use maestro_store::{ConsensusError, Mutation, SecretKey, mask_spec};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::deployer::{ObservedReplicas, unix_millis};
use crate::error::ControlError;
use crate::routing::cluster_routes;
use crate::writer::StoreWriter;

const CANCELLABLE_STATUSES: [DeploymentStatus; 3] = [
    DeploymentStatus::Queued,
    DeploymentStatus::Building,
    DeploymentStatus::Deploying,
];

const STOPPABLE_STATUSES: [DeploymentStatus; 2] =
    [DeploymentStatus::Deploying, DeploymentStatus::Ready];

const DEPLOYMENT_ID_ALPHABET: [char; 36] = [
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i',
    'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
];

pub struct ApiState<W: StoreWriter, O: ObservedReplicas> {
    pub writer: Arc<W>,
    pub observed: Arc<O>,
    pub key: Arc<SecretKey>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ServiceSummary {
    id: ServiceId,
    spec: ServiceSpec,
    latest_deployment: Option<Deployment>,
    deploy_frozen: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    replicas_override: Option<u32>,
}

#[derive(Default, Serialize)]
#[serde(rename_all = "camelCase")]
struct RolloutPlan {
    created: Vec<ServiceId>,
    updated: Vec<ServiceId>,
    unchanged: Vec<ServiceId>,
    unmanaged: Vec<ServiceId>,
    deployments: Vec<DeploymentId>,
    applied: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ServiceDetail {
    id: ServiceId,
    spec: ServiceSpec,
    deploy_frozen: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    replicas_override: Option<u32>,
    deployments: Vec<Deployment>,
    replicas: Vec<ReplicaStatus>,
    assignments: Vec<Assignment>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct NodeSummary {
    #[serde(flatten)]
    node: NodeRecord,
    available: bool,
    replicas: usize,
    allocated: NodeCapacity,
}

#[derive(Deserialize)]
struct ReplicasRequest {
    replicas: u32,
}

#[derive(Deserialize)]
struct FreezeRequest {
    frozen: bool,
}

#[derive(Deserialize)]
struct RolloutQuery {
    #[serde(default)]
    apply: bool,
    #[serde(default)]
    force: bool,
}

#[derive(Deserialize)]
struct ForceQuery {
    #[serde(default)]
    force: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct FreezeResponse {
    service_id: ServiceId,
    deploy_frozen: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ReplicasResponse {
    service_id: ServiceId,
    replicas: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    replicas_override: Option<u32>,
}

#[derive(Deserialize)]
struct BlockIpRequest {
    ip: String,
    blocked: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct BlockedIps {
    blocked_ips: Vec<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct NodeDrainState {
    node_id: NodeId,
    unschedulable: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct StoppedDeployment {
    service_id: ServiceId,
    deployment_id: DeploymentId,
    status: DeploymentStatus,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct QueuedDeployment {
    deployment_id: DeploymentId,
}

pub fn router<W: StoreWriter, O: ObservedReplicas>(state: Arc<ApiState<W, O>>) -> Router {
    Router::new()
        .route("/api/health", get(health))
        .route("/api/services", get(list_services::<W, O>))
        .route(
            "/api/services/{service_id}",
            get(get_service::<W, O>)
                .put(put_service::<W, O>)
                .delete(delete_service::<W, O>),
        )
        .route(
            "/api/services/{service_id}/deployments",
            get(list_deployments::<W, O>).post(queue_deployment::<W, O>),
        )
        .route(
            "/api/services/{service_id}/deployments/{deployment_id}",
            axum::routing::delete(delete_deployment::<W, O>),
        )
        .route(
            "/api/services/{service_id}/deployments/{deployment_id}/cancel",
            patch(cancel_deployment::<W, O>),
        )
        .route(
            "/api/services/{service_id}/deployments/{deployment_id}/remove",
            patch(stop_deployment::<W, O>),
        )
        .route(
            "/api/services/{service_id}/redeploy",
            post(redeploy_service::<W, O>),
        )
        .route(
            "/api/services/{service_id}/restart",
            post(restart_service::<W, O>),
        )
        .route(
            "/api/services/{service_id}/freeze",
            patch(freeze_service::<W, O>),
        )
        .route(
            "/api/services/{service_id}/replicas",
            patch(set_replicas::<W, O>).delete(clear_replicas::<W, O>),
        )
        .route("/api/cluster/unfreeze", post(unfreeze_cluster::<W, O>))
        .route(
            "/api/ingress/blocked-ips",
            get(list_blocked_ips::<W, O>).patch(set_blocked_ip::<W, O>),
        )
        .route(
            "/api/cluster/unschedulable",
            get(list_unschedulable::<W, O>),
        )
        .route(
            "/api/cluster/nodes/{node_id}/drain",
            post(drain_node::<W, O>),
        )
        .route(
            "/api/cluster/nodes/{node_id}/restore",
            post(restore_node::<W, O>),
        )
        .route("/api/rollout", post(rollout::<W, O>))
        .route("/api/replicas", get(list_replicas::<W, O>))
        .route("/api/nodes", get(list_nodes::<W, O>))
        .route("/api/routes", get(list_routes::<W, O>))
        .with_state(state)
}

async fn health() -> Json<serde_json::Value> {
    Json(json!({ "status": "ok" }))
}

async fn list_services<W: StoreWriter, O: ObservedReplicas>(
    State(state): State<Arc<ApiState<W, O>>>,
) -> Result<Json<Vec<ServiceSummary>>, ControlError> {
    let store = state.writer.state();
    store
        .services()?
        .into_iter()
        .map(|(id, spec)| {
            let latest_deployment = store
                .deployments(&id)?
                .into_iter()
                .max_by_key(|deployment| deployment.created_at_ms)
                .map(masked_deployment);
            let controls = store.service_controls(&id)?;
            Ok(ServiceSummary {
                spec: mask_spec(&spec),
                latest_deployment,
                deploy_frozen: controls.frozen,
                replicas_override: controls.replicas_override,
                id,
            })
        })
        .collect::<Result<Vec<_>, ControlError>>()
        .map(Json)
}

async fn put_service<W: StoreWriter, O: ObservedReplicas>(
    State(state): State<Arc<ApiState<W, O>>>,
    Path(service_id): Path<String>,
    Json(spec): Json<ServiceSpec>,
) -> Result<Json<serde_json::Value>, ControlError> {
    let id = ServiceId::new(service_id);
    spec.validate(&id)?;
    reject_cluster_freeze(state.writer.state())?;
    let sealed = state.key.seal_spec(&spec);
    let version = state
        .writer
        .write(vec![Mutation::PutService { id, spec: sealed }])
        .await?;
    Ok(Json(json!({ "version": version })))
}

async fn delete_service<W: StoreWriter, O: ObservedReplicas>(
    State(state): State<Arc<ApiState<W, O>>>,
    Path(service_id): Path<String>,
) -> Result<StatusCode, ControlError> {
    let id = ServiceId::new(service_id);
    let store = state.writer.state();
    match store.service(&id)? {
        None => Err(ControlError::NotFound(format!("service {id}"))),
        Some(_) => {
            let now = unix_millis();
            let stops = store
                .deployments(&id)?
                .into_iter()
                .filter(|deployment| !deployment.status.is_terminal())
                .map(|deployment| Mutation::SetDeploymentStatus {
                    service_id: id.clone(),
                    deployment_id: deployment.id,
                    status: DeploymentStatus::Cancelled,
                    image: None,
                    failure: None,
                    at_ms: now,
                });
            let releases = store
                .nodes()?
                .iter()
                .map(|node| {
                    let kept = store
                        .assignments(node.id)?
                        .into_iter()
                        .filter(|assignment| assignment.service_id != id)
                        .collect();
                    Ok(Mutation::SetNodeAssignments {
                        node_id: node.id,
                        assignments: kept,
                    })
                })
                .collect::<Result<Vec<_>, ControlError>>()?;
            let deletion = Mutation::DeleteService { id: id.clone() };
            let mutations = stops.chain(releases).chain([deletion]).collect();
            state.writer.write(mutations).await?;
            Ok(StatusCode::NO_CONTENT)
        }
    }
}

async fn list_deployments<W: StoreWriter, O: ObservedReplicas>(
    State(state): State<Arc<ApiState<W, O>>>,
    Path(service_id): Path<String>,
) -> Result<Json<Vec<Deployment>>, ControlError> {
    let mut deployments = state
        .writer
        .state()
        .deployments(&ServiceId::new(service_id))?;
    deployments.sort_by_key(|deployment| std::cmp::Reverse(deployment.created_at_ms));
    Ok(Json(
        deployments.into_iter().map(masked_deployment).collect(),
    ))
}

async fn queue_deployment<W: StoreWriter, O: ObservedReplicas>(
    State(state): State<Arc<ApiState<W, O>>>,
    Path(service_id): Path<String>,
    Query(query): Query<ForceQuery>,
) -> Result<(StatusCode, Json<QueuedDeployment>), ControlError> {
    let id = ServiceId::new(service_id);
    let spec = state
        .writer
        .state()
        .service(&id)?
        .ok_or_else(|| ControlError::NotFound(format!("service {id}")))?;
    reject_frozen(state.writer.state(), &id, query.force)?;
    let deployment = new_deployment(&id, spec);
    let deployment_id = deployment.id.clone();
    state
        .writer
        .write(vec![Mutation::CreateDeployment { deployment }])
        .await?;
    Ok((
        StatusCode::ACCEPTED,
        Json(QueuedDeployment { deployment_id }),
    ))
}

async fn rollout<W: StoreWriter, O: ObservedReplicas>(
    State(state): State<Arc<ApiState<W, O>>>,
    Query(query): Query<RolloutQuery>,
    Json(file): Json<ServicesFile>,
) -> Result<Json<RolloutPlan>, ControlError> {
    file.services
        .iter()
        .try_for_each(|(id, spec)| spec.validate(id))?;
    let existing = state
        .writer
        .state()
        .services()?
        .into_iter()
        .map(|(id, spec)| Ok((id, state.key.open_spec(&spec)?)))
        .collect::<Result<Vec<_>, ControlError>>()?;
    let mut plan = RolloutPlan {
        unmanaged: existing
            .iter()
            .map(|(id, _)| id.clone())
            .filter(|id| !file.services.contains_key(id))
            .collect(),
        ..RolloutPlan::default()
    };
    let changed = file
        .services
        .iter()
        .filter(
            |(id, spec)| match existing.iter().find(|(existing_id, _)| existing_id == *id) {
                None => {
                    plan.created.push((*id).clone());
                    true
                }
                Some((_, current)) if current != *spec => {
                    plan.updated.push((*id).clone());
                    true
                }
                Some(_) => {
                    plan.unchanged.push((*id).clone());
                    false
                }
            },
        )
        .collect::<Vec<_>>();
    if query.apply && !changed.is_empty() {
        changed
            .iter()
            .try_for_each(|(id, _)| reject_frozen(state.writer.state(), id, query.force))?;
        let mutations = changed
            .into_iter()
            .flat_map(|(id, spec)| {
                let sealed = state.key.seal_spec(spec);
                let deployment = new_deployment(id, sealed.clone());
                plan.deployments.push(deployment.id.clone());
                [
                    Mutation::PutService {
                        id: id.clone(),
                        spec: sealed,
                    },
                    Mutation::CreateDeployment { deployment },
                ]
            })
            .collect();
        state.writer.write(mutations).await?;
    }
    plan.applied = query.apply;
    Ok(Json(plan))
}

async fn list_replicas<W: StoreWriter, O: ObservedReplicas>(
    State(state): State<Arc<ApiState<W, O>>>,
) -> Json<Vec<ReplicaStatus>> {
    Json(state.observed.replicas())
}

async fn get_service<W: StoreWriter, O: ObservedReplicas>(
    State(state): State<Arc<ApiState<W, O>>>,
    Path(service_id): Path<String>,
) -> Result<Json<ServiceDetail>, ControlError> {
    let id = ServiceId::new(service_id);
    let store = state.writer.state();
    let spec = store
        .service(&id)?
        .ok_or_else(|| ControlError::NotFound(format!("service {id}")))?;
    let mut deployments = store
        .deployments(&id)?
        .into_iter()
        .map(masked_deployment)
        .collect::<Vec<_>>();
    deployments.sort_by_key(|deployment| std::cmp::Reverse(deployment.created_at_ms));
    let replicas = state
        .observed
        .replicas()
        .into_iter()
        .filter(|replica| replica.service_id == id)
        .collect();
    let assignments = all_assignments(store)?
        .into_iter()
        .filter(|assignment| assignment.service_id == id)
        .collect();
    let controls = store.service_controls(&id)?;
    Ok(Json(ServiceDetail {
        deploy_frozen: controls.frozen,
        replicas_override: controls.replicas_override,
        id,
        spec: mask_spec(&spec),
        deployments,
        replicas,
        assignments,
    }))
}

async fn cancel_deployment<W: StoreWriter, O: ObservedReplicas>(
    State(state): State<Arc<ApiState<W, O>>>,
    Path((service_id, deployment_id)): Path<(String, String)>,
) -> Result<StatusCode, ControlError> {
    let service_id = ServiceId::new(service_id);
    let deployment_id = DeploymentId::new(deployment_id);
    let deployment = state
        .writer
        .state()
        .deployment(&service_id, &deployment_id)?
        .ok_or_else(|| ControlError::NotFound(format!("deployment {deployment_id}")))?;
    match CANCELLABLE_STATUSES.contains(&deployment.status) {
        false => Err(ControlError::Conflict(format!(
            "deployment {deployment_id} is {:?} and can no longer be cancelled",
            deployment.status
        ))),
        true => {
            state
                .writer
                .write(vec![Mutation::SetDeploymentStatus {
                    service_id,
                    deployment_id,
                    status: DeploymentStatus::Cancelled,
                    image: None,
                    failure: None,
                    at_ms: unix_millis(),
                }])
                .await?;
            Ok(StatusCode::NO_CONTENT)
        }
    }
}

async fn stop_deployment<W: StoreWriter, O: ObservedReplicas>(
    State(state): State<Arc<ApiState<W, O>>>,
    Path((service_id, deployment_id)): Path<(String, String)>,
) -> Result<Json<StoppedDeployment>, ControlError> {
    let service_id = ServiceId::new(service_id);
    let deployment_id = DeploymentId::new(deployment_id);
    let deployment = state
        .writer
        .state()
        .deployment(&service_id, &deployment_id)?
        .ok_or_else(|| ControlError::NotFound(format!("deployment {deployment_id}")))?;
    match STOPPABLE_STATUSES.contains(&deployment.status) {
        false => Err(ControlError::Conflict(format!(
            "deployment {deployment_id} is {:?} and cannot be stopped",
            deployment.status
        ))),
        true => {
            state
                .writer
                .write(vec![Mutation::SetDeploymentStatus {
                    service_id: service_id.clone(),
                    deployment_id: deployment_id.clone(),
                    status: DeploymentStatus::Draining,
                    image: None,
                    failure: None,
                    at_ms: unix_millis(),
                }])
                .await?;
            Ok(Json(StoppedDeployment {
                service_id,
                deployment_id,
                status: DeploymentStatus::Draining,
            }))
        }
    }
}

async fn delete_deployment<W: StoreWriter, O: ObservedReplicas>(
    State(state): State<Arc<ApiState<W, O>>>,
    Path((service_id, deployment_id)): Path<(String, String)>,
) -> Result<StatusCode, ControlError> {
    let service_id = ServiceId::new(service_id);
    let deployment_id = DeploymentId::new(deployment_id);
    let deployment = state
        .writer
        .state()
        .deployment(&service_id, &deployment_id)?
        .ok_or_else(|| ControlError::NotFound(format!("deployment {deployment_id}")))?;
    match deployment.status.is_terminal() {
        false => Err(ControlError::Conflict(format!(
            "deployment {deployment_id} is {:?}; stop or cancel it before deleting it",
            deployment.status
        ))),
        true => {
            state
                .writer
                .write(vec![Mutation::DeleteDeployment {
                    service_id,
                    deployment_id,
                }])
                .await?;
            Ok(StatusCode::NO_CONTENT)
        }
    }
}

async fn redeploy_service<W: StoreWriter, O: ObservedReplicas>(
    state: State<Arc<ApiState<W, O>>>,
    service_id: Path<String>,
    query: Query<ForceQuery>,
) -> Result<(StatusCode, Json<QueuedDeployment>), ControlError> {
    queue_deployment(state, service_id, query).await
}

async fn restart_service<W: StoreWriter, O: ObservedReplicas>(
    State(state): State<Arc<ApiState<W, O>>>,
    Path(service_id): Path<String>,
    Query(query): Query<ForceQuery>,
) -> Result<(StatusCode, Json<QueuedDeployment>), ControlError> {
    let id = ServiceId::new(service_id);
    let store = state.writer.state();
    let spec = store
        .service(&id)?
        .ok_or_else(|| ControlError::NotFound(format!("service {id}")))?;
    reject_frozen(store, &id, query.force)?;
    let source = restart_source(store.deployments(&id)?).ok_or_else(|| {
        ControlError::Conflict(format!(
            "service {id} has no deployed image to restart from"
        ))
    })?;
    let deployment = Deployment {
        image: source.image,
        git_commit: source.git_commit,
        built_on: source.built_on,
        ..new_deployment(&id, spec)
    };
    let deployment_id = deployment.id.clone();
    state
        .writer
        .write(vec![Mutation::CreateDeployment { deployment }])
        .await?;
    Ok((
        StatusCode::ACCEPTED,
        Json(QueuedDeployment { deployment_id }),
    ))
}

async fn freeze_service<W: StoreWriter, O: ObservedReplicas>(
    State(state): State<Arc<ApiState<W, O>>>,
    Path(service_id): Path<String>,
    Json(request): Json<FreezeRequest>,
) -> Result<Json<FreezeResponse>, ControlError> {
    let id = ServiceId::new(service_id);
    let store = state.writer.state();
    store
        .service(&id)?
        .ok_or_else(|| ControlError::NotFound(format!("service {id}")))?;
    let controls = ServiceControls {
        frozen: request.frozen,
        ..store.service_controls(&id)?
    };
    state
        .writer
        .write(vec![Mutation::SetServiceControls {
            id: id.clone(),
            controls,
        }])
        .await?;
    Ok(Json(FreezeResponse {
        service_id: id,
        deploy_frozen: request.frozen,
    }))
}

async fn set_replicas<W: StoreWriter, O: ObservedReplicas>(
    State(state): State<Arc<ApiState<W, O>>>,
    Path(service_id): Path<String>,
    Json(request): Json<ReplicasRequest>,
) -> Result<Json<ReplicasResponse>, ControlError> {
    let id = ServiceId::new(service_id);
    let store = state.writer.state();
    let spec = store
        .service(&id)?
        .ok_or_else(|| ControlError::NotFound(format!("service {id}")))?;
    let configured = spec.deploy.replicas;
    let writable_volume = spec.deploy.volumes.iter().any(|volume| !volume.read_only);
    let problem = match request.replicas {
        0 => Some("replicas must be at least 1".to_string()),
        replicas if replicas > MAX_REPLICAS_OVERRIDE => {
            Some(format!("replicas cannot exceed {MAX_REPLICAS_OVERRIDE}"))
        }
        replicas if replicas < configured => Some(format!(
            "replicas cannot be less than the configured value ({configured})"
        )),
        replicas if replicas > 1 && writable_volume => {
            Some("services with writable volumes run a single replica".to_string())
        }
        _ => None,
    };
    match problem {
        Some(problem) => Err(ControlError::Invalid(problem)),
        None => {
            let replicas_override = (request.replicas != configured).then_some(request.replicas);
            write_replicas_override(&state, &id, replicas_override).await?;
            Ok(Json(ReplicasResponse {
                service_id: id,
                replicas: request.replicas,
                replicas_override,
            }))
        }
    }
}

async fn clear_replicas<W: StoreWriter, O: ObservedReplicas>(
    State(state): State<Arc<ApiState<W, O>>>,
    Path(service_id): Path<String>,
) -> Result<Json<ReplicasResponse>, ControlError> {
    let id = ServiceId::new(service_id);
    let spec = state
        .writer
        .state()
        .service(&id)?
        .ok_or_else(|| ControlError::NotFound(format!("service {id}")))?;
    write_replicas_override(&state, &id, None).await?;
    Ok(Json(ReplicasResponse {
        service_id: id,
        replicas: spec.deploy.replicas,
        replicas_override: None,
    }))
}

async fn list_blocked_ips<W: StoreWriter, O: ObservedReplicas>(
    State(state): State<Arc<ApiState<W, O>>>,
) -> Result<Json<BlockedIps>, ControlError> {
    Ok(Json(BlockedIps {
        blocked_ips: state.writer.state().blocked_ips()?,
    }))
}

async fn set_blocked_ip<W: StoreWriter, O: ObservedReplicas>(
    State(state): State<Arc<ApiState<W, O>>>,
    Json(request): Json<BlockIpRequest>,
) -> Result<Json<BlockedIps>, ControlError> {
    let canonical = request
        .ip
        .parse::<std::net::IpAddr>()
        .map_err(|_| ControlError::Invalid(format!("invalid IP address `{}`", request.ip)))?
        .to_string();
    match canonical == request.ip {
        false => Err(ControlError::Invalid(format!(
            "IP address `{}` must use canonical form `{canonical}`",
            request.ip
        ))),
        true => {
            state
                .writer
                .write(vec![Mutation::SetBlockedIp {
                    ip: canonical,
                    blocked: request.blocked,
                }])
                .await?;
            Ok(Json(BlockedIps {
                blocked_ips: state.writer.state().blocked_ips()?,
            }))
        }
    }
}

async fn list_unschedulable<W: StoreWriter, O: ObservedReplicas>(
    State(state): State<Arc<ApiState<W, O>>>,
) -> Result<Json<Vec<UnschedulableReplica>>, ControlError> {
    Ok(Json(state.writer.state().unschedulable()?))
}

async fn drain_node<W: StoreWriter, O: ObservedReplicas>(
    state: State<Arc<ApiState<W, O>>>,
    node_id: Path<String>,
) -> Result<Json<NodeDrainState>, ControlError> {
    set_node_state(state, node_id, NodeState::Draining).await
}

async fn restore_node<W: StoreWriter, O: ObservedReplicas>(
    state: State<Arc<ApiState<W, O>>>,
    node_id: Path<String>,
) -> Result<Json<NodeDrainState>, ControlError> {
    set_node_state(state, node_id, NodeState::Active).await
}

async fn unfreeze_cluster<W: StoreWriter, O: ObservedReplicas>(
    State(state): State<Arc<ApiState<W, O>>>,
) -> Result<StatusCode, ControlError> {
    state
        .writer
        .write(vec![Mutation::SetClusterFreeze { freeze: None }])
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn list_nodes<W: StoreWriter, O: ObservedReplicas>(
    State(state): State<Arc<ApiState<W, O>>>,
) -> Result<Json<Vec<NodeSummary>>, ControlError> {
    let store = state.writer.state();
    let assignments = all_assignments(store)?;
    let nodes = store
        .nodes()?
        .into_iter()
        .map(|node| {
            let on_node = assignments
                .iter()
                .filter(|assignment| assignment.node_id == node.id)
                .collect::<Vec<_>>();
            let allocated = NodeCapacity {
                cpu: Millicores::new(
                    on_node
                        .iter()
                        .map(|assignment| assignment.resources.request.cpu.get())
                        .sum(),
                ),
                memory: ByteSize::new(
                    on_node
                        .iter()
                        .map(|assignment| assignment.resources.request.memory.bytes())
                        .sum(),
                ),
            };
            NodeSummary {
                available: state.observed.node_available(node.id),
                replicas: on_node.len(),
                allocated,
                node,
            }
        })
        .collect();
    Ok(Json(nodes))
}

async fn list_routes<W: StoreWriter, O: ObservedReplicas>(
    State(state): State<Arc<ApiState<W, O>>>,
) -> Result<Json<RouteTable>, ControlError> {
    let observed = |replica_id: &ReplicaId| state.observed.replica(replica_id);
    Ok(Json(cluster_routes(
        state.writer.state(),
        &observed,
        &RouteTable::default(),
    )?))
}

impl IntoResponse for ControlError {
    fn into_response(self) -> Response {
        let status = match &self {
            ControlError::Spec(SpecError::Invalid { .. } | SpecError::Parse(_)) => {
                StatusCode::BAD_REQUEST
            }
            ControlError::NotFound(_) => StatusCode::NOT_FOUND,
            ControlError::Invalid(_) => StatusCode::BAD_REQUEST,
            ControlError::Conflict(_) => StatusCode::CONFLICT,
            ControlError::Secret(_) => StatusCode::INTERNAL_SERVER_ERROR,
            ControlError::Consensus(ConsensusError::Rejected(_)) => StatusCode::CONFLICT,
            ControlError::Store(_) | ControlError::Consensus(_) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
        };
        (status, Json(json!({ "error": self.to_string() }))).into_response()
    }
}

async fn set_node_state<W: StoreWriter, O: ObservedReplicas>(
    State(state): State<Arc<ApiState<W, O>>>,
    Path(node_id): Path<String>,
    next: NodeState,
) -> Result<Json<NodeDrainState>, ControlError> {
    let node = node_id
        .parse()
        .ok()
        .map(NodeId)
        .map(|id| state.writer.state().node(id))
        .transpose()?
        .flatten()
        .ok_or_else(|| ControlError::NotFound(format!("node {node_id}")))?;
    match node.state {
        NodeState::Removed => Err(ControlError::Conflict(format!(
            "node {node_id} has been removed"
        ))),
        _ => {
            let node_id = node.id;
            state
                .writer
                .write(vec![Mutation::PutNode {
                    node: NodeRecord {
                        state: next,
                        ..node
                    },
                }])
                .await?;
            Ok(Json(NodeDrainState {
                node_id,
                unschedulable: next == NodeState::Draining,
            }))
        }
    }
}

async fn write_replicas_override<W: StoreWriter, O: ObservedReplicas>(
    state: &ApiState<W, O>,
    id: &ServiceId,
    replicas_override: Option<u32>,
) -> Result<(), ControlError> {
    let controls = ServiceControls {
        replicas_override,
        ..state.writer.state().service_controls(id)?
    };
    state
        .writer
        .write(vec![Mutation::SetServiceControls {
            id: id.clone(),
            controls,
        }])
        .await?;
    Ok(())
}

fn reject_frozen(
    store: &maestro_store::StateMachine,
    id: &ServiceId,
    force: bool,
) -> Result<(), ControlError> {
    reject_cluster_freeze(store)?;
    match store.service_controls(id)?.frozen && !force {
        true => Err(ControlError::Conflict(format!(
            "deploy is frozen for service {id}; use ?force=true to override"
        ))),
        false => Ok(()),
    }
}

fn reject_cluster_freeze(store: &maestro_store::StateMachine) -> Result<(), ControlError> {
    match store.cluster_freeze()? {
        Some(freeze) => Err(ControlError::Conflict(format!(
            "cluster deploys are frozen: {}",
            freeze.reason
        ))),
        None => Ok(()),
    }
}

fn restart_source(mut deployments: Vec<Deployment>) -> Option<Deployment> {
    deployments.sort_by_key(|deployment| std::cmp::Reverse(deployment.created_at_ms));
    let ready = deployments
        .iter()
        .position(|deployment| deployment.status == DeploymentStatus::Ready);
    let with_image = deployments
        .iter()
        .position(|deployment| deployment.image.is_some());
    ready
        .or(with_image)
        .map(|index| deployments.swap_remove(index))
        .filter(|deployment| deployment.image.is_some())
}

fn all_assignments(store: &maestro_store::StateMachine) -> Result<Vec<Assignment>, ControlError> {
    Ok(store
        .nodes()?
        .iter()
        .map(|node| store.assignments(node.id))
        .collect::<Result<Vec<_>, _>>()?
        .concat())
}

fn masked_deployment(deployment: Deployment) -> Deployment {
    Deployment {
        spec: mask_spec(&deployment.spec),
        ..deployment
    }
}

pub(crate) fn new_deployment(service_id: &ServiceId, spec: ServiceSpec) -> Deployment {
    let now = unix_millis();
    let suffix = nanoid::format(nanoid::rngs::default, &DEPLOYMENT_ID_ALPHABET, 4);
    Deployment {
        id: DeploymentId::new(format!("d{}{suffix}", to_base36(now))),
        service_id: service_id.clone(),
        spec,
        status: DeploymentStatus::Queued,
        image: None,
        failure: None,
        created_at_ms: now,
        updated_at_ms: now,
        git_commit: None,
        built_on: None,
    }
}

fn to_base36(mut value: u64) -> String {
    let mut digits = Vec::new();
    loop {
        digits.push(DEPLOYMENT_ID_ALPHABET[(value % 36) as usize]);
        value /= 36;
        if value == 0 {
            break;
        }
    }
    digits.iter().rev().collect()
}
