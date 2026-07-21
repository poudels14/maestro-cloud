use std::collections::{BTreeMap, BTreeSet};
use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use http_body_util::BodyExt;
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use kernel_api::{
    ArtifactTemplate, ClusterId, ExecPolicy, Generation, Node, NodeApiAccess, NodeId,
    NodeInstanceId, NodeRole, NodeSpec, NodeStatus, Object, ObjectMeta, PlacementConstraint,
    ResourceKind, ResourceName, ResourceRevision, RolloutState, SecretMountSpec, SecretValue,
    Service, ServiceId, ServiceSpec, ServiceStatus, Timestamp,
};
use kernel_store::{
    CasOutcome, ExpectedVersion, InMemoryStore, Keyspace, PutRequest, Store, TokioClock,
};
use serde_json::{Value, json};
use tower::ServiceExt;

use crate::{ApiServer, ServerSettings, TlsIdentity, openapi_document};

#[test]
fn settings_fail_closed_for_exposed_or_weakly_authenticated_listeners()
-> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemoryStore::new(Arc::new(TokioClock::new())));
    let cluster_id = ClusterId::new("server-test")?;
    assert!(
        ApiServer::new(
            store.clone(),
            cluster_id.clone(),
            ServerSettings::new("0.0.0.0:3000".parse()?, None),
        )
        .is_err()
    );
    assert!(
        ApiServer::new(
            store.clone(),
            cluster_id.clone(),
            ServerSettings::new(
                "0.0.0.0:3000".parse()?,
                Some(SecretValue::new("s".repeat(32))),
            ),
        )
        .is_err()
    );
    assert!(
        ApiServer::new(
            store,
            cluster_id,
            ServerSettings::new(
                "127.0.0.1:3000".parse()?,
                Some(SecretValue::new("too-short")),
            ),
        )
        .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn resource_routes_report_store_revisions_and_mask_service_secrets()
-> Result<(), Box<dyn std::error::Error>> {
    let (store, cluster_id) = seeded_store().await?;
    let server = ApiServer::new(
        store,
        cluster_id,
        ServerSettings::new("127.0.0.1:3000".parse()?, None),
    )?;

    let nodes = request(&server, "/api/cluster/nodes", None).await?;
    assert_eq!(nodes.status(), StatusCode::OK);
    let nodes: Vec<Node> = decode(nodes).await?;
    assert_eq!(nodes.len(), 1);
    assert!(nodes.first().is_some_and(|node| node.meta.revision.0 > 0));

    let service = request(&server, "/api/services/api", None).await?;
    assert_eq!(service.status(), StatusCode::OK);
    let body = String::from_utf8(service.into_body().collect().await?.to_bytes().to_vec())?;
    assert!(!body.contains("database-password"));
    assert!(body.contains("••••word"));
    Ok(())
}

#[tokio::test]
async fn protected_routes_require_a_valid_operator_scope() -> Result<(), Box<dyn std::error::Error>>
{
    let (store, cluster_id) = seeded_store().await?;
    let secret = "s".repeat(32);
    let server = ApiServer::new(
        store,
        cluster_id,
        ServerSettings::new("127.0.0.1:3000".parse()?, Some(SecretValue::new(&secret))),
    )?;

    assert_eq!(
        request(&server, "/api/cluster/nodes", None).await?.status(),
        StatusCode::UNAUTHORIZED
    );
    let viewer = token(&secret, "viewer")?;
    assert_eq!(
        request(&server, "/api/cluster/nodes", Some(&viewer))
            .await?
            .status(),
        StatusCode::FORBIDDEN
    );
    let operator = token(&secret, "metrics:read operator")?;
    assert_eq!(
        request(&server, "/api/cluster/nodes", Some(&operator))
            .await?
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        request(&server, "/healthz", None).await?.status(),
        StatusCode::OK
    );
    Ok(())
}

#[tokio::test]
async fn bind_rejects_malformed_tls_identity_before_claiming_the_port()
-> Result<(), Box<dyn std::error::Error>> {
    let (store, cluster_id) = seeded_store().await?;
    let server = ApiServer::new(
        store,
        cluster_id,
        ServerSettings::new(
            "127.0.0.1:0".parse()?,
            Some(SecretValue::new("s".repeat(32))),
        )
        .with_tls_identity(TlsIdentity::new(
            "not-a-certificate",
            SecretValue::new("not-a-private-key"),
        )),
    )?;
    assert!(server.bind().await.is_err());
    Ok(())
}

#[tokio::test]
async fn bound_https_server_serves_and_shuts_down_cleanly() -> Result<(), Box<dyn std::error::Error>>
{
    let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])?;
    let certificate_pem = certified.cert.pem();
    let (store, cluster_id) = seeded_store().await?;
    let server = ApiServer::new(
        store,
        cluster_id,
        ServerSettings::new(
            "127.0.0.1:0".parse()?,
            Some(SecretValue::new("s".repeat(32))),
        )
        .with_tls_identity(TlsIdentity::new(
            certificate_pem.clone(),
            SecretValue::new(certified.signing_key.serialize_pem()),
        )),
    )?
    .bind()
    .await?;
    let port = server.local_address().port();
    let (shutdown, shutdown_receiver) = tokio::sync::watch::channel(false);
    let task = tokio::spawn(server.serve(shutdown_receiver));
    let client = reqwest::Client::builder()
        .add_root_certificate(reqwest::Certificate::from_pem(certificate_pem.as_bytes())?)
        .https_only(true)
        .build()?;

    let response = client
        .get(format!("https://localhost:{port}/healthz"))
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    shutdown.send(true)?;
    task.await??;
    Ok(())
}

#[test]
fn server_openapi_contains_domain_paths_and_bearer_policy() {
    let document = openapi_document();
    assert!(document.pointer("/paths/~1api~1services/get").is_some());
    assert!(
        document
            .pointer("/paths/~1api~1cluster~1nodes~1{nodeId}/get")
            .is_some()
    );
    assert_eq!(
        document.pointer("/components/securitySchemes/bearerAuth/scheme"),
        Some(&Value::String("bearer".to_string()))
    );
}

async fn seeded_store() -> Result<(Arc<InMemoryStore>, ClusterId), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemoryStore::new(Arc::new(TokioClock::new())));
    let cluster_id = ClusterId::new("server-test")?;
    put(&store, &cluster_id, "Node", "node-1", &node()?).await?;
    put(&store, &cluster_id, "Service", "api", &service()?).await?;
    Ok((store, cluster_id))
}

async fn put(
    store: &InMemoryStore,
    cluster_id: &ClusterId,
    kind: &str,
    id: &str,
    value: &impl serde::Serialize,
) -> Result<(), Box<dyn std::error::Error>> {
    let outcome = store
        .put_cas(PutRequest {
            key: Keyspace::new(cluster_id)
                .resource(&ResourceKind::new(kind)?, &ResourceName::new(id)?),
            value: serde_json::to_vec(value)?,
            expected: ExpectedVersion::Missing,
            session: None,
        })
        .await?;
    if matches!(outcome, CasOutcome::Applied(_)) {
        Ok(())
    } else {
        Err("resource seed conflicted".into())
    }
}

fn node() -> Result<Node, kernel_api::InvalidIdentifier> {
    Ok(Object {
        meta: metadata(NodeId::new("node-1")?),
        spec: NodeSpec {
            hostname: "node-1.internal".to_string(),
            host_address: IpAddr::V4(Ipv4Addr::new(10, 20, 0, 1)),
            role: NodeRole::Master,
            scheduling_labels: BTreeMap::new(),
        },
        status: NodeStatus {
            instance_id: NodeInstanceId::new("instance-1")?,
            version: "0.1.0".to_string(),
            last_seen: Timestamp(1_000),
            conditions: Vec::new(),
        },
    })
}

fn service() -> Result<Service, kernel_api::InvalidIdentifier> {
    Ok(Object {
        meta: metadata(ServiceId::new("api")?),
        spec: ServiceSpec {
            name: "API".to_string(),
            version: "1.0.0".to_string(),
            artifact: ArtifactTemplate::Image {
                reference: "registry.test/api:latest".to_string(),
            },
            preview: None,
            command: None,
            replicas: 1,
            exposed_ports: vec![8080],
            health_check: None,
            max_restarts: Some(3),
            environment: BTreeMap::new(),
            user: None,
            node_api: NodeApiAccess::Disabled,
            secrets: Some(SecretMountSpec {
                mount_path: "/run/secrets/api.env".to_string(),
                items: BTreeMap::from([(
                    "DATABASE_PASSWORD".to_string(),
                    SecretValue::new("database-password"),
                )]),
            }),
            volumes: Vec::new(),
            placement: PlacementConstraint::default(),
            exec: ExecPolicy::Allowed,
        },
        status: ServiceStatus {
            active_deployment_id: None,
            replica_override: None,
            rollout: RolloutState::Active,
            conditions: Vec::new(),
        },
    })
}

fn metadata<Id>(id: Id) -> ObjectMeta<Id> {
    ObjectMeta {
        id,
        labels: BTreeMap::new(),
        annotations: BTreeMap::new(),
        revision: ResourceRevision::default(),
        generation: Generation(1),
        owner_refs: Vec::new(),
        finalizers: BTreeSet::new(),
        deletion_timestamp: None,
    }
}

async fn request(
    server: &ApiServer,
    uri: &str,
    token: Option<&str>,
) -> Result<axum::response::Response, Box<dyn std::error::Error>> {
    let mut request = Request::builder().uri(uri);
    if let Some(token) = token {
        request = request.header(header::AUTHORIZATION, format!("Bearer {token}"));
    }
    Ok(server
        .router()
        .oneshot(request.body(Body::empty())?)
        .await?)
}

async fn decode<ValueType>(
    response: axum::response::Response,
) -> Result<ValueType, Box<dyn std::error::Error>>
where
    ValueType: serde::de::DeserializeOwned,
{
    Ok(serde_json::from_slice(
        &response.into_body().collect().await?.to_bytes(),
    )?)
}

fn token(secret: &str, scope: &str) -> Result<String, jsonwebtoken::errors::Error> {
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs();
    jsonwebtoken::encode(
        &Header::new(Algorithm::HS256),
        &json!({
            "sub": "test-operator",
            "scope": scope,
            "iat": now,
            "exp": now.saturating_add(300)
        }),
        &EncodingKey::from_secret(secret.as_bytes()),
    )
}
