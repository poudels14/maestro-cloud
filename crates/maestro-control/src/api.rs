use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use maestro_core::{
    Deployment, DeploymentId, DeploymentStatus, NodeRecord, ReplicaStatus, ServiceId, ServiceSpec,
    ServicesFile, SpecError,
};
use maestro_store::{ConsensusError, Mutation, SecretKey, mask_spec};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::deployer::{ObservedReplicas, unix_millis};
use crate::error::ControlError;
use crate::writer::StoreWriter;

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
            put(put_service::<W, O>).delete(delete_service::<W, O>),
        )
        .route(
            "/api/services/{service_id}/deployments",
            get(list_deployments::<W, O>).post(queue_deployment::<W, O>),
        )
        .route("/api/rollout", post(rollout::<W, O>))
        .route("/api/replicas", get(list_replicas::<W, O>))
        .route("/api/nodes", get(list_nodes::<W, O>))
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

async fn list_nodes<W: StoreWriter, O: ObservedReplicas>(
    State(state): State<Arc<ApiState<W, O>>>,
) -> Result<Json<Vec<NodeRecord>>, ControlError> {
    Ok(Json(state.writer.state().nodes()?))
}

impl IntoResponse for ControlError {
    fn into_response(self) -> Response {
        let status = match &self {
            ControlError::Spec(SpecError::Invalid { .. } | SpecError::Parse(_)) => {
                StatusCode::BAD_REQUEST
            }
            ControlError::NotFound(_) => StatusCode::NOT_FOUND,
            ControlError::Secret(_) => StatusCode::INTERNAL_SERVER_ERROR,
            ControlError::Consensus(ConsensusError::Rejected(_)) => StatusCode::CONFLICT,
            ControlError::Store(_) | ControlError::Consensus(_) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
        };
        (status, Json(json!({ "error": self.to_string() }))).into_response()
    }
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
