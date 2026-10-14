use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use maestro_core::{
    Assignment, ByteSize, Deployment, DeploymentId, DeploymentStatus, Millicores, NodeCapacity,
    NodeRecord, ReplicaId, ReplicaStatus, RouteTable, ServiceId, ServiceSpec, ServicesFile,
    SpecError,
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
struct ScaleRequest {
    replicas: u32,
}

#[derive(Deserialize)]
struct RolloutQuery {
    #[serde(default)]
    apply: bool,
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
            "/api/services/{service_id}/deployments/{deployment_id}/cancel",
            post(cancel_deployment::<W, O>),
        )
        .route(
            "/api/services/{service_id}/scale",
            post(scale_service::<W, O>),
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
            Ok(ServiceSummary {
                id,
                spec: mask_spec(&spec),
                latest_deployment,
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
) -> Result<(StatusCode, Json<QueuedDeployment>), ControlError> {
    let id = ServiceId::new(service_id);
    let spec = state
        .writer
        .state()
        .service(&id)?
        .ok_or_else(|| ControlError::NotFound(format!("service {id}")))?;
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
    Ok(Json(ServiceDetail {
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

async fn scale_service<W: StoreWriter, O: ObservedReplicas>(
    State(state): State<Arc<ApiState<W, O>>>,
    Path(service_id): Path<String>,
    Json(request): Json<ScaleRequest>,
) -> Result<(StatusCode, Json<QueuedDeployment>), ControlError> {
    let id = ServiceId::new(service_id);
    let mut spec = state
        .writer
        .state()
        .service(&id)?
        .ok_or_else(|| ControlError::NotFound(format!("service {id}")))?;
    spec.deploy.replicas = request.replicas;
    let deployment = new_deployment(&id, spec.clone());
    let deployment_id = deployment.id.clone();
    state
        .writer
        .write(vec![
            Mutation::PutService { id, spec },
            Mutation::CreateDeployment { deployment },
        ])
        .await?;
    Ok((
        StatusCode::ACCEPTED,
        Json(QueuedDeployment { deployment_id }),
    ))
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

fn new_deployment(service_id: &ServiceId, spec: ServiceSpec) -> Deployment {
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
