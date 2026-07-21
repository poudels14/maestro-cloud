use axum::extract::rejection::JsonRejection;
use axum::extract::{DefaultBodyLimit, Extension, Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use axum::{Json, Router};
use kernel_api::{
    BuiltinKind, CommandRequest, Deployment, DeploymentCommandResponse, DeploymentGoal,
    DeploymentId, DeploymentPhase, ResourceKind, Service, ServiceId,
};
use kernel_store::{Compare, ExpectedVersion, Keyspace, Mutation, Transaction};

use super::service_commands::next_generation;
use crate::mutation::{MAXIMUM_REQUEST_BYTES, MutationRequest};
use crate::{ApiError, AppState, OperatorIdentity, mutation, resource};

pub(super) fn router() -> Router<AppState> {
    Router::new()
        .route(
            "/api/services/{service_id}/deployments/{deployment_id}/restart",
            post(restart),
        )
        .route(
            "/api/services/{service_id}/deployments/{deployment_id}/cancel",
            post(cancel),
        )
        .route(
            "/api/services/{service_id}/deployments/{deployment_id}/remove",
            post(remove),
        )
        .layer(DefaultBodyLimit::max(MAXIMUM_REQUEST_BYTES))
}

async fn restart(
    state: State<AppState>,
    path: Path<(String, String)>,
    operator: Extension<OperatorIdentity>,
    headers: HeaderMap,
    payload: Result<Json<CommandRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<DeploymentCommandResponse>), ApiError> {
    command(
        state,
        path,
        operator,
        headers,
        payload,
        DeploymentCommandKind {
            operation: "POST /api/services/{serviceId}/deployments/{deploymentId}/restart",
            action: DeploymentMutation::Restart,
        },
    )
    .await
}

async fn cancel(
    state: State<AppState>,
    path: Path<(String, String)>,
    operator: Extension<OperatorIdentity>,
    headers: HeaderMap,
    payload: Result<Json<CommandRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<DeploymentCommandResponse>), ApiError> {
    command(
        state,
        path,
        operator,
        headers,
        payload,
        DeploymentCommandKind {
            operation: "POST /api/services/{serviceId}/deployments/{deploymentId}/cancel",
            action: DeploymentMutation::Goal(DeploymentGoal::Cancel),
        },
    )
    .await
}

async fn remove(
    state: State<AppState>,
    path: Path<(String, String)>,
    operator: Extension<OperatorIdentity>,
    headers: HeaderMap,
    payload: Result<Json<CommandRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<DeploymentCommandResponse>), ApiError> {
    command(
        state,
        path,
        operator,
        headers,
        payload,
        DeploymentCommandKind {
            operation: "POST /api/services/{serviceId}/deployments/{deploymentId}/remove",
            action: DeploymentMutation::Goal(DeploymentGoal::Remove),
        },
    )
    .await
}

async fn command(
    State(state): State<AppState>,
    Path((service_id, deployment_id)): Path<(String, String)>,
    Extension(operator): Extension<OperatorIdentity>,
    headers: HeaderMap,
    payload: Result<Json<CommandRequest>, JsonRejection>,
    command: DeploymentCommandKind,
) -> Result<(StatusCode, Json<DeploymentCommandResponse>), ApiError> {
    let service_id =
        ServiceId::new(service_id).map_err(|error| ApiError::bad_request(error.to_string()))?;
    let deployment_id = DeploymentId::new(deployment_id)
        .map_err(|error| ApiError::bad_request(error.to_string()))?;
    let payload = payload
        .map_err(|rejection| mutation::json_rejection(rejection, "deployment command"))?
        .0;
    let request = MutationRequest::new(
        &state,
        &headers,
        &operator,
        command.operation,
        &[service_id.as_str(), deployment_id.as_str()],
        &payload,
    )?;
    if let Some(response) = request.replay(&state).await? {
        return Ok((StatusCode::ACCEPTED, Json(response)));
    }
    let _: Service = resource::get(&state, BuiltinKind::Service, service_id.clone()).await?;
    let keys = Keyspace::new(&state.cluster_id);
    let kind = ResourceKind::new(BuiltinKind::Deployment.as_str())
        .map_err(|error| ApiError::internal(error.to_string()))?;
    let key = keys.resource(&kind, &deployment_id.clone().into());
    let stored = state
        .store
        .get(&key)
        .await
        .map_err(|error| ApiError::internal(format!("failed to read Deployment: {error}")))?
        .ok_or_else(|| {
            ApiError::not_found(format!("Deployment `{deployment_id}` does not exist"))
        })?;
    let mut deployment: Deployment =
        resource::decode(&stored, &keys, &kind, BuiltinKind::Deployment)?;
    if deployment.spec.service_id != service_id {
        return Err(ApiError::not_found(format!(
            "Deployment `{deployment_id}` does not exist for Service `{service_id}`"
        )));
    }
    if deployment.meta.revision != payload.expected_revision {
        return Err(ApiError::conflict(
            "revisionConflict",
            "Deployment is not at the expected revision",
        ));
    }
    let write = mutate_deployment(&mut deployment, command.action)?;
    let response = command_response(&deployment);
    let mutations = if write {
        vec![Mutation::Put {
            key: key.clone(),
            value: serde_json::to_vec(&deployment).map_err(|error| {
                ApiError::internal(format!("failed to encode Deployment resource: {error}"))
            })?,
            session: None,
        }]
    } else {
        Vec::new()
    };
    let response = request
        .commit(
            &state,
            response,
            Transaction {
                compares: vec![Compare {
                    key,
                    expected: ExpectedVersion::Exact(stored.version),
                }],
                mutations,
            },
            "revisionConflict",
            "Deployment changed after its expected revision was read",
        )
        .await?;
    Ok((StatusCode::ACCEPTED, Json(response)))
}

fn mutate_deployment(
    deployment: &mut Deployment,
    action: DeploymentMutation,
) -> Result<bool, ApiError> {
    if deployment.meta.deletion_timestamp.is_some() {
        return Err(ApiError::conflict(
            "deletionInProgress",
            "Deployment deletion is already in progress",
        ));
    }
    match action {
        DeploymentMutation::Restart => {
            if deployment.spec.goal != DeploymentGoal::Run
                || !matches!(
                    deployment.status.phase,
                    DeploymentPhase::Building
                        | DeploymentPhase::PendingReady
                        | DeploymentPhase::Ready
                )
            {
                return Err(ApiError::conflict(
                    "invalidLifecycle",
                    "Only a running deployment can be restarted",
                ));
            }
            deployment.meta.generation = next_generation(deployment.meta.generation, "Deployment")?;
            deployment.spec.restart_generation =
                next_generation(deployment.spec.restart_generation, "Deployment restart")?;
            Ok(true)
        }
        DeploymentMutation::Goal(DeploymentGoal::Cancel) => {
            if !matches!(
                deployment.status.phase,
                DeploymentPhase::Queued | DeploymentPhase::Building
            ) {
                return Err(ApiError::conflict(
                    "invalidLifecycle",
                    "Only a queued or building deployment can be canceled",
                ));
            }
            set_goal(deployment, DeploymentGoal::Cancel)
        }
        DeploymentMutation::Goal(DeploymentGoal::Remove) => {
            if deployment.status.phase == DeploymentPhase::Removed {
                return Ok(false);
            }
            set_goal(deployment, DeploymentGoal::Remove)
        }
        DeploymentMutation::Goal(DeploymentGoal::Run) => Ok(false),
    }
}

fn set_goal(deployment: &mut Deployment, goal: DeploymentGoal) -> Result<bool, ApiError> {
    if deployment.spec.goal == goal {
        return Ok(false);
    }
    if deployment.spec.goal == DeploymentGoal::Remove {
        return Err(ApiError::conflict(
            "invalidLifecycle",
            "A deployment being removed cannot accept another lifecycle goal",
        ));
    }
    deployment.meta.generation = next_generation(deployment.meta.generation, "Deployment")?;
    deployment.spec.goal = goal;
    Ok(true)
}

fn command_response(deployment: &Deployment) -> DeploymentCommandResponse {
    DeploymentCommandResponse {
        deployment_id: deployment.meta.id.clone(),
        generation: deployment.meta.generation,
        restart_generation: deployment.spec.restart_generation,
        goal: deployment.spec.goal,
    }
}

enum DeploymentMutation {
    Restart,
    Goal(DeploymentGoal),
}

struct DeploymentCommandKind {
    operation: &'static str,
    action: DeploymentMutation,
}
