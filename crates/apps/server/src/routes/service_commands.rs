use axum::extract::rejection::JsonRejection;
use axum::extract::{DefaultBodyLimit, Extension, Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{delete, post, put};
use axum::{Json, Router};
use kernel_api::{
    AnnotationKey, ArtifactTemplate, BUILD_RESOLVED_GENERATION_ANNOTATION,
    BUILD_RESOLVED_REVISION_ANNOTATION, BUILD_WATCH_REVISION_ANNOTATION, BuildSource, BuiltinKind,
    CommandRequest, Generation, Ownership, Preview, PreviewId, PreviewPhase, PullRequestState,
    ResourceKind, ResourceRevision, RolloutState, Service, ServiceCommandResponse, ServiceId,
    ServiceReplicaOverrideRequest, Timestamp,
};
use kernel_store::{Compare, ExpectedVersion, Keyspace, Mutation, StoredValue, Transaction};
use serde::Serialize;

use crate::mutation::{MAXIMUM_REQUEST_BYTES, MutationRequest};
use crate::system_resources::ensure_user_resource_id;
use crate::{ApiError, AppState, OperatorIdentity, mutation, resource};

pub(super) fn router() -> Router<AppState> {
    Router::new()
        .route("/api/services/{service_id}/redeploy", post(redeploy))
        .route("/api/services/{service_id}/freeze", post(freeze))
        .route("/api/services/{service_id}/unfreeze", post(unfreeze))
        .route("/api/services/{service_id}/replicas", put(set_replicas))
        .route("/api/services/{service_id}", delete(delete_service))
        .layer(DefaultBodyLimit::max(MAXIMUM_REQUEST_BYTES))
}

async fn redeploy(
    State(state): State<AppState>,
    Path(service_id): Path<String>,
    Extension(operator): Extension<OperatorIdentity>,
    headers: HeaderMap,
    payload: Result<Json<CommandRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<ServiceCommandResponse>), ApiError> {
    command(
        state,
        service_id,
        operator,
        headers,
        parse(payload)?,
        "POST /api/services/{serviceId}/redeploy",
        ServiceMutation::Redeploy,
    )
    .await
}

async fn freeze(
    State(state): State<AppState>,
    Path(service_id): Path<String>,
    Extension(operator): Extension<OperatorIdentity>,
    headers: HeaderMap,
    payload: Result<Json<CommandRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<ServiceCommandResponse>), ApiError> {
    command(
        state,
        service_id,
        operator,
        headers,
        parse(payload)?,
        "POST /api/services/{serviceId}/freeze",
        ServiceMutation::Rollout(RolloutState::Frozen),
    )
    .await
}

async fn unfreeze(
    State(state): State<AppState>,
    Path(service_id): Path<String>,
    Extension(operator): Extension<OperatorIdentity>,
    headers: HeaderMap,
    payload: Result<Json<CommandRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<ServiceCommandResponse>), ApiError> {
    command(
        state,
        service_id,
        operator,
        headers,
        parse(payload)?,
        "POST /api/services/{serviceId}/unfreeze",
        ServiceMutation::Rollout(RolloutState::Active),
    )
    .await
}

async fn set_replicas(
    State(state): State<AppState>,
    Path(service_id): Path<String>,
    Extension(operator): Extension<OperatorIdentity>,
    headers: HeaderMap,
    payload: Result<Json<ServiceReplicaOverrideRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<ServiceCommandResponse>), ApiError> {
    let payload = payload
        .map_err(|rejection| mutation::json_rejection(rejection, "service command"))?
        .0;
    let expected_revision = payload.expected_revision;
    let replicas = payload.replicas;
    command_with_payload(
        ServiceCommandInput {
            state,
            service_id,
            operator,
            headers,
            operation: "PUT /api/services/{serviceId}/replicas",
            action: ServiceMutation::Replicas(replicas),
        },
        payload,
        expected_revision,
    )
    .await
}

async fn delete_service(
    State(state): State<AppState>,
    Path(service_id): Path<String>,
    Extension(operator): Extension<OperatorIdentity>,
    headers: HeaderMap,
    payload: Result<Json<CommandRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<ServiceCommandResponse>), ApiError> {
    let now = state.timestamp_clock.now();
    command(
        state,
        service_id,
        operator,
        headers,
        parse(payload)?,
        "DELETE /api/services/{serviceId}",
        ServiceMutation::Delete(now),
    )
    .await
}

async fn command(
    state: AppState,
    service_id: String,
    operator: OperatorIdentity,
    headers: HeaderMap,
    payload: CommandRequest,
    operation: &'static str,
    action: ServiceMutation,
) -> Result<(StatusCode, Json<ServiceCommandResponse>), ApiError> {
    let expected_revision = payload.expected_revision;
    command_with_payload(
        ServiceCommandInput {
            state,
            service_id,
            operator,
            headers,
            operation,
            action,
        },
        payload,
        expected_revision,
    )
    .await
}

async fn command_with_payload<Payload: Serialize>(
    input: ServiceCommandInput,
    payload: Payload,
    expected_revision: ResourceRevision,
) -> Result<(StatusCode, Json<ServiceCommandResponse>), ApiError> {
    let ServiceCommandInput {
        state,
        service_id,
        operator,
        headers,
        operation,
        action,
    } = input;
    let service_id =
        ServiceId::new(service_id).map_err(|error| ApiError::bad_request(error.to_string()))?;
    ensure_user_resource_id("Service", service_id.as_str())?;
    let request = MutationRequest::new(
        &state,
        &headers,
        &operator,
        operation,
        &[service_id.as_str()],
        &payload,
    )?;
    if let Some(response) = request.replay(&state).await? {
        return Ok((StatusCode::ACCEPTED, Json(response)));
    }
    let keys = Keyspace::new(&state.cluster_id);
    let kind = ResourceKind::new(BuiltinKind::Service.as_str())
        .map_err(|error| ApiError::internal(error.to_string()))?;
    let key = keys.resource(&kind, &service_id.clone().into());
    let stored = state
        .store
        .get(&key)
        .await
        .map_err(|error| ApiError::internal(format!("failed to read Service: {error}")))?
        .ok_or_else(|| ApiError::not_found(format!("Service `{service_id}` does not exist")))?;
    let mut service: Service = resource::decode(&stored, &keys, &kind, BuiltinKind::Service)?;
    if service.meta.revision != expected_revision {
        return Err(ApiError::conflict(
            "revisionConflict",
            "Service is not at the expected revision",
        ));
    }
    let redeploy = matches!(&action, ServiceMutation::Redeploy);
    let write = mutate_service(&mut service, action)?;
    let related = if redeploy {
        refresh_git_source(&state, &keys, &mut service).await?
    } else {
        None
    };
    let response = command_response(&service);
    let mut compares = vec![Compare {
        key: key.clone(),
        expected: ExpectedVersion::Exact(stored.version),
    }];
    let mut mutations = if write {
        vec![Mutation::Put {
            key: key.clone(),
            value: serde_json::to_vec(&service).map_err(|error| {
                ApiError::internal(format!("failed to encode Service resource: {error}"))
            })?,
            session: None,
        }]
    } else {
        Vec::new()
    };
    if let Some(related) = related {
        compares.push(Compare {
            key: related.stored.key.clone(),
            expected: ExpectedVersion::Exact(related.stored.version),
        });
        if let Some(preview) = related.preview {
            mutations.push(Mutation::Put {
                key: related.stored.key,
                value: serde_json::to_vec(&preview).map_err(|error| {
                    ApiError::internal(format!("failed to encode Preview resource: {error}"))
                })?,
                session: None,
            });
        }
    }
    let response = request
        .commit(
            &state,
            response,
            Transaction {
                compares,
                mutations,
            },
            "revisionConflict",
            "Service changed after its expected revision was read",
        )
        .await?;
    Ok((StatusCode::ACCEPTED, Json(response)))
}

async fn refresh_git_source(
    state: &AppState,
    keys: &Keyspace,
    service: &mut Service,
) -> Result<Option<RelatedPreview>, ApiError> {
    let (mut source, github_token, watched) = match &service.spec.artifact {
        ArtifactTemplate::Build { template } => match &template.source {
            source @ BuildSource::Git { .. } => (
                source.clone(),
                template.secrets.get("GH_TOKEN").cloned(),
                template.watch,
            ),
            BuildSource::Tarball { .. } => return Ok(None),
        },
        ArtifactTemplate::Image { .. } => return Ok(None),
    };
    let related = load_owned_preview(state, keys, service).await?;
    if let Some(related) = &related {
        if related.preview.meta.deletion_timestamp.is_some()
            || related.preview.status.pull_request_state != PullRequestState::Open
        {
            return Err(ApiError::conflict(
                "previewClosed",
                "the pull request is closed and cannot be redeployed",
            ));
        }
        let head_reference = related.preview.spec.head_reference.trim();
        if head_reference.is_empty() {
            return Err(ApiError::conflict(
                "previewSourceUnavailable",
                "the pull request does not have a source branch",
            ));
        }
        let BuildSource::Git { revision, .. } = &mut source else {
            unreachable!("Git source was selected above");
        };
        *revision = head_reference.to_string();
    }
    let resolver = state.build_revisions.as_ref().ok_or_else(|| {
        ApiError::service_unavailable("Git revision resolution is not configured on this node")
    })?;
    let revision = resolver
        .resolve_revision(&source, github_token.as_ref())
        .await
        .map_err(build_source_error)?
        .filter(|revision| !revision.trim().is_empty())
        .ok_or_else(|| ApiError::bad_gateway("Git source did not resolve to a commit"))?;
    let revision_annotation = if watched {
        BUILD_WATCH_REVISION_ANNOTATION
    } else {
        BUILD_RESOLVED_REVISION_ANNOTATION
    };
    service.meta.annotations.insert(
        AnnotationKey(revision_annotation.to_string()),
        revision.clone(),
    );
    service.meta.annotations.insert(
        AnnotationKey(BUILD_RESOLVED_GENERATION_ANNOTATION.to_string()),
        service.meta.generation.0.to_string(),
    );

    let Some(mut related) = related else {
        return Ok(None);
    };
    if related.preview.spec.head_revision != revision {
        related.preview.spec.head_revision.clone_from(&revision);
        related.preview.meta.generation =
            next_generation(related.preview.meta.generation, "Preview")?;
        related.preview.status.phase = PreviewPhase::Pending;
        related.preview.status.teardown_at = None;
        related.changed = true;
    }
    let ArtifactTemplate::Build { template } = &mut service.spec.artifact else {
        unreachable!("build artifact was selected above");
    };
    let BuildSource::Git {
        revision: captured, ..
    } = &mut template.source
    else {
        unreachable!("Git source was selected above");
    };
    if *captured != revision {
        captured.clone_from(&revision);
        service.status.active_deployment_id = None;
    }
    Ok(Some(related.into_write()))
}

async fn load_owned_preview(
    state: &AppState,
    keys: &Keyspace,
    service: &Service,
) -> Result<Option<LoadedPreview>, ApiError> {
    let preview_owner = service.meta.owner_refs.iter().find(|owner| {
        owner.ownership == Ownership::Controller && owner.resource.kind.as_str() == "Preview"
    });
    let Some(owner) = preview_owner else {
        return Ok(None);
    };
    let preview_id = PreviewId::new(owner.resource.id.to_string())
        .map_err(|error| ApiError::internal(format!("invalid Preview owner: {error}")))?;
    let kind = ResourceKind::new(BuiltinKind::Preview.as_str())
        .map_err(|error| ApiError::internal(error.to_string()))?;
    let key = keys.resource(&kind, &preview_id.clone().into());
    let stored = state
        .store
        .get(&key)
        .await
        .map_err(|error| ApiError::internal(format!("failed to read Preview: {error}")))?
        .ok_or_else(|| ApiError::conflict("previewMissing", "the owning Preview does not exist"))?;
    let preview: Preview = resource::decode(&stored, keys, &kind, BuiltinKind::Preview)?;
    if preview.spec.service_id != service.meta.id {
        return Err(ApiError::internal(format!(
            "Preview `{preview_id}` does not own Service `{}`",
            service.meta.id
        )));
    }
    Ok(Some(LoadedPreview {
        stored,
        preview,
        changed: false,
    }))
}

fn build_source_error(error: build::BuildSourceError) -> ApiError {
    match error {
        build::BuildSourceError::Unavailable { message } => ApiError::service_unavailable(format!(
            "failed to resolve the latest Git revision: {message}"
        )),
        build::BuildSourceError::Rejected { message } => ApiError::bad_gateway(format!(
            "failed to resolve the latest Git revision: {message}"
        )),
    }
}

struct LoadedPreview {
    stored: StoredValue,
    preview: Preview,
    changed: bool,
}

impl LoadedPreview {
    fn into_write(self) -> RelatedPreview {
        RelatedPreview {
            stored: self.stored,
            preview: self.changed.then_some(self.preview),
        }
    }
}

struct RelatedPreview {
    stored: StoredValue,
    preview: Option<Preview>,
}

fn mutate_service(service: &mut Service, action: ServiceMutation) -> Result<bool, ApiError> {
    if service.meta.deletion_timestamp.is_some() {
        return match action {
            ServiceMutation::Delete(_) => Ok(false),
            _ => Err(ApiError::conflict(
                "deletionInProgress",
                "Service deletion is already in progress",
            )),
        };
    }
    match action {
        ServiceMutation::Redeploy => {
            service.meta.generation = next_generation(service.meta.generation, "Service")?;
            Ok(true)
        }
        ServiceMutation::Rollout(rollout) if service.status.rollout != rollout => {
            service.status.rollout = rollout;
            Ok(true)
        }
        ServiceMutation::Replicas(replicas) if service.status.replica_override != replicas => {
            service.status.replica_override = replicas;
            Ok(true)
        }
        ServiceMutation::Delete(timestamp) => {
            service.meta.deletion_timestamp = Some(timestamp);
            Ok(true)
        }
        ServiceMutation::Rollout(_) | ServiceMutation::Replicas(_) => Ok(false),
    }
}

pub(super) fn next_generation(current: Generation, kind: &str) -> Result<Generation, ApiError> {
    current.0.checked_add(1).map(Generation).ok_or_else(|| {
        ApiError::conflict(
            "generationExhausted",
            format!("{kind} generation is exhausted"),
        )
    })
}

fn parse(payload: Result<Json<CommandRequest>, JsonRejection>) -> Result<CommandRequest, ApiError> {
    Ok(payload
        .map_err(|rejection| mutation::json_rejection(rejection, "service command"))?
        .0)
}

fn command_response(service: &Service) -> ServiceCommandResponse {
    ServiceCommandResponse {
        service_id: service.meta.id.clone(),
        generation: service.meta.generation,
        rollout: service.status.rollout,
        replica_override: service.status.replica_override,
        deletion_timestamp: service.meta.deletion_timestamp,
    }
}

enum ServiceMutation {
    Redeploy,
    Rollout(RolloutState),
    Replicas(Option<u32>),
    Delete(Timestamp),
}

struct ServiceCommandInput {
    state: AppState,
    service_id: String,
    operator: OperatorIdentity,
    headers: HeaderMap,
    operation: &'static str,
    action: ServiceMutation,
}
