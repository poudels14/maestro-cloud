use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode, header};
use build::{BuildRevisionResolver, BuildSourceError};
use kernel_api::{
    AnnotationKey, ArtifactTemplate, BUILD_RESOLVED_GENERATION_ANNOTATION,
    BUILD_RESOLVED_REVISION_ANNOTATION, BuildSource, BuildTemplate, ClusterId, DeploymentId,
    Generation, Object, OwnerReference, Ownership, Preview, PreviewId, PreviewPhase, PreviewSpec,
    PreviewStatus, PullRequestState, ResourceId, ResourceKind, ResourceName, SecretValue, Service,
    ServiceId, Timestamp,
};
use kernel_store::{InMemoryStore, Keyspace, Store, TokioClock};
use serde_json::{Value, json};
use tower::ServiceExt;

use crate::{ApiServer, ServerSettings};

use super::{decode, request, seeded_store};

const OLD_REVISION: &str = "1111111111111111111111111111111111111111";
const NEW_REVISION: &str = "2222222222222222222222222222222222222222";

#[tokio::test]
async fn redeploy_resolves_and_pins_the_latest_service_commit()
-> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemoryStore::new(Arc::new(TokioClock::new())));
    let cluster_id = ClusterId::new("service-redeploy-source")?;
    let mut service = super::service()?;
    service.spec.artifact = git_artifact("main", false);
    super::put(
        &store,
        &cluster_id,
        "Service",
        service.meta.id.as_str(),
        &service,
    )
    .await?;
    let resolver = Arc::new(RecordingRevisionResolver::new(Ok(Some(
        NEW_REVISION.to_string(),
    ))));
    let server = ApiServer::new(
        store.clone(),
        cluster_id.clone(),
        ServerSettings::new("127.0.0.1:3000".parse()?, None),
    )?
    .with_build_revision_resolver(resolver.clone());
    let current: Value = decode(request(&server, "/api/services/api", None).await?).await?;

    let response = mutate(
        &server,
        Method::POST,
        "/api/services/api/redeploy",
        "latest-service-commit",
        &json!({"expectedRevision": revision(&current)?}),
    )
    .await?;

    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let updated: Service = stored_resource(&store, &cluster_id, "Service", "api").await?;
    assert_eq!(updated.meta.generation, Generation(2));
    assert_eq!(git_revision(&updated)?, "main");
    assert_eq!(resolved_annotation(&updated), Some(NEW_REVISION));
    assert_eq!(resolved_generation(&updated), Some("2"));
    assert_eq!(
        resolver.calls(),
        vec![(
            BuildSource::Git {
                repository: "https://github.com/example/repository.git".to_string(),
                revision: "main".to_string(),
            },
            Some("github-token".to_string()),
        )]
    );
    Ok(())
}

#[tokio::test]
async fn preview_redeploy_refreshes_the_branch_head_and_preview_atomically()
-> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemoryStore::new(Arc::new(TokioClock::new())));
    let cluster_id = ClusterId::new("preview-redeploy-source")?;
    let preview_id = PreviewId::new("api-pr-42")?;
    let mut service = super::service()?;
    service.meta.id = ServiceId::new("api-pr-42")?;
    service.meta.owner_refs = vec![OwnerReference {
        resource: ResourceId::new(
            ResourceKind::new("Preview")?,
            ResourceName::from(preview_id.clone()),
        ),
        ownership: Ownership::Controller,
    }];
    service.spec.artifact = git_artifact(OLD_REVISION, false);
    service.status.active_deployment_id = Some(DeploymentId::new("deployment-old")?);
    let preview = Object {
        meta: super::metadata(preview_id.clone()),
        spec: PreviewSpec {
            base_service_id: ServiceId::new("api")?,
            repository: "example/repository".to_string(),
            pull_request_number: 42,
            title: "Fresh changes".to_string(),
            head_reference: "feature/fresh-changes".to_string(),
            author: "octocat".to_string(),
            head_revision: OLD_REVISION.to_string(),
            service_id: service.meta.id.clone(),
            close_grace_period_secs: 300,
            expires_at: Timestamp(10_000),
        },
        status: PreviewStatus {
            pull_request_state: PullRequestState::Open,
            phase: PreviewPhase::Active,
            teardown_at: Some(Timestamp(9_000)),
            conditions: Vec::new(),
        },
    };
    super::put(
        &store,
        &cluster_id,
        "Service",
        service.meta.id.as_str(),
        &service,
    )
    .await?;
    super::put(
        &store,
        &cluster_id,
        "Preview",
        preview.meta.id.as_str(),
        &preview,
    )
    .await?;
    let resolver = Arc::new(RecordingRevisionResolver::new(Ok(Some(
        NEW_REVISION.to_string(),
    ))));
    let server = ApiServer::new(
        store.clone(),
        cluster_id.clone(),
        ServerSettings::new("127.0.0.1:3000".parse()?, None),
    )?
    .with_build_revision_resolver(resolver.clone());
    let current: Value = decode(request(&server, "/api/services/api-pr-42", None).await?).await?;

    let response = mutate(
        &server,
        Method::POST,
        "/api/services/api-pr-42/redeploy",
        "latest-preview-commit",
        &json!({"expectedRevision": revision(&current)?}),
    )
    .await?;

    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let updated_service: Service =
        stored_resource(&store, &cluster_id, "Service", "api-pr-42").await?;
    let updated_preview: Preview =
        stored_resource(&store, &cluster_id, "Preview", "api-pr-42").await?;
    assert_eq!(updated_service.meta.generation, Generation(2));
    assert_eq!(git_revision(&updated_service)?, NEW_REVISION);
    assert_eq!(resolved_annotation(&updated_service), Some(NEW_REVISION));
    assert_eq!(resolved_generation(&updated_service), Some("2"));
    assert_eq!(updated_service.status.active_deployment_id, None);
    assert_eq!(updated_preview.meta.generation, Generation(2));
    assert_eq!(updated_preview.spec.head_revision, NEW_REVISION);
    assert_eq!(updated_preview.status.phase, PreviewPhase::Pending);
    assert_eq!(updated_preview.status.teardown_at, None);
    assert_eq!(
        resolver.calls(),
        vec![(
            BuildSource::Git {
                repository: "https://github.com/example/repository.git".to_string(),
                revision: "feature/fresh-changes".to_string(),
            },
            Some("github-token".to_string()),
        )]
    );
    Ok(())
}

#[tokio::test]
async fn redeploy_does_not_advance_when_latest_commit_resolution_fails()
-> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemoryStore::new(Arc::new(TokioClock::new())));
    let cluster_id = ClusterId::new("failed-service-redeploy-source")?;
    let mut service = super::service()?;
    service.spec.artifact = git_artifact("main", true);
    super::put(&store, &cluster_id, "Service", "api", &service).await?;
    let server = ApiServer::new(
        store.clone(),
        cluster_id.clone(),
        ServerSettings::new("127.0.0.1:3000".parse()?, None),
    )?
    .with_build_revision_resolver(Arc::new(RecordingRevisionResolver::new(Err(
        BuildSourceError::rejected("authentication failed"),
    ))));
    let current: Value = decode(request(&server, "/api/services/api", None).await?).await?;

    let response = mutate(
        &server,
        Method::POST,
        "/api/services/api/redeploy",
        "failed-latest-service-commit",
        &json!({"expectedRevision": revision(&current)?}),
    )
    .await?;

    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    let unchanged: Service = stored_resource(&store, &cluster_id, "Service", "api").await?;
    assert_eq!(unchanged.meta.generation, Generation(1));
    assert_eq!(resolved_annotation(&unchanged), None);
    Ok(())
}

#[tokio::test]
async fn service_commands_replay_and_preserve_lifecycle_boundaries()
-> Result<(), Box<dyn std::error::Error>> {
    let (store, cluster_id) = seeded_store().await?;
    let server = ApiServer::new(
        store,
        cluster_id,
        ServerSettings::new("127.0.0.1:3000".parse()?, None),
    )?;
    let initial = get_service(&server).await?;
    let initial_revision = revision(&initial)?.clone();

    let redeploy = mutate(
        &server,
        Method::POST,
        "/api/services/api/redeploy",
        "service-redeploy",
        &json!({"expectedRevision": initial_revision}),
    )
    .await?;
    assert_eq!(redeploy.status(), StatusCode::ACCEPTED);
    assert_eq!(field(&decode(redeploy).await?, "generation")?, &json!(2));
    let replay = mutate(
        &server,
        Method::POST,
        "/api/services/api/redeploy",
        "service-redeploy",
        &json!({"expectedRevision": initial_revision}),
    )
    .await?;
    assert_eq!(replay.status(), StatusCode::ACCEPTED);
    assert_eq!(field(&decode(replay).await?, "generation")?, &json!(2));
    assert_eq!(
        mutate(
            &server,
            Method::POST,
            "/api/services/api/freeze",
            "service-redeploy",
            &json!({"expectedRevision": initial_revision}),
        )
        .await?
        .status(),
        StatusCode::CONFLICT
    );
    assert_eq!(
        mutate(
            &server,
            Method::POST,
            "/api/services/api/freeze",
            "service-stale-revision",
            &json!({"expectedRevision": initial_revision}),
        )
        .await?
        .status(),
        StatusCode::CONFLICT
    );

    let current = get_service(&server).await?;
    let current_revision = revision(&current)?.clone();
    let frozen = mutate(
        &server,
        Method::POST,
        "/api/services/api/freeze",
        "service-freeze",
        &json!({"expectedRevision": current_revision}),
    )
    .await?;
    assert_eq!(frozen.status(), StatusCode::ACCEPTED);
    let frozen = decode(frozen).await?;
    assert_eq!(field(&frozen, "generation")?, &json!(2));
    assert_eq!(field(&frozen, "rollout")?, &json!("frozen"));

    let current = get_service(&server).await?;
    let current_revision = revision(&current)?.clone();
    let scaled = mutate(
        &server,
        Method::PUT,
        "/api/services/api/replicas",
        "service-scale",
        &json!({"expectedRevision": current_revision, "replicas": 3}),
    )
    .await?;
    assert_eq!(scaled.status(), StatusCode::ACCEPTED);
    assert_eq!(field(&decode(scaled).await?, "replicaOverride")?, &json!(3));

    let current = get_service(&server).await?;
    let current_revision = revision(&current)?.clone();
    let cleared = mutate(
        &server,
        Method::PUT,
        "/api/services/api/replicas",
        "service-scale-clear",
        &json!({"expectedRevision": current_revision, "replicas": null}),
    )
    .await?;
    assert_eq!(cleared.status(), StatusCode::ACCEPTED);
    assert!(
        decode::<Value>(cleared)
            .await?
            .get("replicaOverride")
            .is_none()
    );

    let current = get_service(&server).await?;
    let current_revision = revision(&current)?.clone();
    let deleted = mutate(
        &server,
        Method::DELETE,
        "/api/services/api",
        "service-delete",
        &json!({"expectedRevision": current_revision}),
    )
    .await?;
    assert_eq!(deleted.status(), StatusCode::ACCEPTED);
    assert!(field(&decode(deleted).await?, "deletionTimestamp")?.is_number());

    let deleting = get_service(&server).await?;
    assert_eq!(
        mutate(
            &server,
            Method::POST,
            "/api/services/api/unfreeze",
            "service-unfreeze-after-delete",
            &json!({"expectedRevision": revision(&deleting)?}),
        )
        .await?
        .status(),
        StatusCode::CONFLICT
    );
    Ok(())
}

#[tokio::test]
async fn service_commands_require_current_revisions_and_idempotency_keys()
-> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemoryStore::new(Arc::new(TokioClock::new())));
    let server = ApiServer::new(
        store,
        ClusterId::new("service-command-validation")?,
        ServerSettings::new("127.0.0.1:3000".parse()?, None),
    )?;
    assert_eq!(
        mutate(
            &server,
            Method::POST,
            "/api/services/missing/redeploy",
            "missing-service",
            &json!({"expectedRevision": 1}),
        )
        .await?
        .status(),
        StatusCode::NOT_FOUND
    );
    let (store, cluster_id) = seeded_store().await?;
    let server = ApiServer::new(
        store,
        cluster_id,
        ServerSettings::new("127.0.0.1:3000".parse()?, None),
    )?;
    let current = get_service(&server).await?;
    assert_eq!(
        mutate(
            &server,
            Method::PUT,
            "/api/services/api/replicas",
            "missing-replicas",
            &json!({"expectedRevision": revision(&current)?}),
        )
        .await?
        .status(),
        StatusCode::BAD_REQUEST
    );
    let request = Request::builder()
        .method(Method::POST)
        .uri("/api/services/missing/redeploy")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(r#"{"expectedRevision":1}"#))?;
    assert_eq!(
        server.router().oneshot(request).await?.status(),
        StatusCode::BAD_REQUEST
    );
    Ok(())
}

async fn get_service(server: &ApiServer) -> Result<Value, Box<dyn std::error::Error>> {
    decode(request(server, "/api/services/api", None).await?).await
}

async fn mutate(
    server: &ApiServer,
    method: Method,
    uri: &str,
    idempotency_key: &str,
    payload: &Value,
) -> Result<axum::response::Response, Box<dyn std::error::Error>> {
    Ok(server
        .router()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header(header::CONTENT_TYPE, "application/json")
                .header("Idempotency-Key", idempotency_key)
                .body(Body::from(serde_json::to_vec(payload)?))?,
        )
        .await?)
}

fn revision(service: &Value) -> Result<&Value, std::io::Error> {
    field(field(service, "meta")?, "revision")
}

fn field<'a>(value: &'a Value, name: &str) -> Result<&'a Value, std::io::Error> {
    value
        .get(name)
        .ok_or_else(|| std::io::Error::other(format!("field `{name}` is missing")))
}

fn git_artifact(revision: &str, watch: bool) -> ArtifactTemplate {
    ArtifactTemplate::Build {
        template: BuildTemplate {
            source: BuildSource::Git {
                repository: "https://github.com/example/repository.git".to_string(),
                revision: revision.to_string(),
            },
            dockerfile: "Dockerfile".to_string(),
            watch,
            registry: None,
            registry_repository: None,
            depot: None,
            environment: BTreeMap::new(),
            environment_source: None,
            secrets: BTreeMap::from([("GH_TOKEN".to_string(), SecretValue::new("github-token"))]),
            secrets_source: None,
        },
    }
}

fn git_revision(service: &Service) -> Result<&str, std::io::Error> {
    let ArtifactTemplate::Build { template } = &service.spec.artifact else {
        return Err(std::io::Error::other(
            "service does not use a build artifact",
        ));
    };
    let BuildSource::Git { revision, .. } = &template.source else {
        return Err(std::io::Error::other("service does not use a Git source"));
    };
    Ok(revision)
}

fn resolved_annotation(service: &Service) -> Option<&str> {
    service
        .meta
        .annotations
        .get(&AnnotationKey(
            BUILD_RESOLVED_REVISION_ANNOTATION.to_string(),
        ))
        .map(String::as_str)
}

fn resolved_generation(service: &Service) -> Option<&str> {
    service
        .meta
        .annotations
        .get(&AnnotationKey(
            BUILD_RESOLVED_GENERATION_ANNOTATION.to_string(),
        ))
        .map(String::as_str)
}

async fn stored_resource<Resource: serde::de::DeserializeOwned>(
    store: &InMemoryStore,
    cluster_id: &ClusterId,
    kind: &str,
    id: &str,
) -> Result<Resource, Box<dyn std::error::Error>> {
    let key =
        Keyspace::new(cluster_id).resource(&ResourceKind::new(kind)?, &ResourceName::new(id)?);
    let stored = store.get(&key).await?.ok_or("resource is missing")?;
    Ok(serde_json::from_slice(&stored.value)?)
}

struct RecordingRevisionResolver {
    result: Result<Option<String>, BuildSourceError>,
    calls: Mutex<Vec<(BuildSource, Option<String>)>>,
}

impl RecordingRevisionResolver {
    fn new(result: Result<Option<String>, BuildSourceError>) -> Self {
        Self {
            result,
            calls: Mutex::new(Vec::new()),
        }
    }

    fn calls(&self) -> Vec<(BuildSource, Option<String>)> {
        self.calls.lock().expect("resolver calls lock").clone()
    }
}

#[async_trait]
impl BuildRevisionResolver for RecordingRevisionResolver {
    async fn resolve_revision(
        &self,
        source: &BuildSource,
        github_token: Option<&SecretValue>,
    ) -> Result<Option<String>, BuildSourceError> {
        self.calls.lock().expect("resolver calls lock").push((
            source.clone(),
            github_token.map(|token| token.expose().to_string()),
        ));
        self.result.clone()
    }
}
