use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode, header};
use kernel_api::{
    AssignmentId, BuiltinKind, ClusterId, DeploymentId, IngressBlocklist, IngressBlocklistId,
    NodeId, ResourceKind, SecretValue, ServiceId, TRAEFIK_SERVICE_ID, Timestamp, WorkloadId,
};
use kernel_store::{Keyspace, Store};
use logs::{
    InMemoryLogStore, IngestLogEntry, IngressTrafficBreakdown, IngressTrafficQuery,
    IngressTrafficScope, LogBody, LogOrigin, LogProducer, LogRecordId, LogStore, LogStream,
    NodeTrafficQueryStore, OriginCursor, ServiceTrafficQuery, TrafficMetricPoint,
    TrafficQueryError, TrafficQueryStore,
};
use runtime::WorkloadMetadata;
use tower::ServiceExt;

use super::{decode, request, seeded_store, token};
use crate::{ApiServer, HttpNodeLogQueryStore, ServerSettings, TlsIdentity};

#[tokio::test]
async fn unblocking_a_missing_address_does_not_create_the_blocklist()
-> Result<(), Box<dyn std::error::Error>> {
    let (kernel_store, cluster_id) = seeded_store().await?;
    let server = ApiServer::new(
        kernel_store.clone(),
        cluster_id.clone(),
        ServerSettings::new("127.0.0.1:3000".parse()?, None),
    )?;

    let response: serde_json::Value =
        decode(patch_blocklist(&server, "203.0.113.9", false).await?).await?;
    assert_eq!(response, serde_json::json!({"blockedIps": []}));

    let keys = Keyspace::new(&cluster_id);
    let key = keys.resource(
        &ResourceKind::new(BuiltinKind::IngressBlocklist.as_str())?,
        &IngressBlocklistId::new("global")?.into(),
    );
    assert!(kernel_store.get(&key).await?.is_none());
    Ok(())
}

#[tokio::test]
async fn ingress_blocklist_is_canonical_idempotent_and_cluster_scoped()
-> Result<(), Box<dyn std::error::Error>> {
    let (kernel_store, cluster_id) = seeded_store().await?;
    let server = ApiServer::new(
        kernel_store.clone(),
        cluster_id.clone(),
        ServerSettings::new("127.0.0.1:3000".parse()?, None),
    )?;

    let empty: serde_json::Value =
        decode(request(&server, "/api/ingress/blocked-ips", None).await?).await?;
    assert_eq!(empty, serde_json::json!({"blockedIps": []}));

    let (ipv4, ipv6) = tokio::join!(
        patch_blocklist(&server, "203.0.113.9", true),
        patch_blocklist(&server, " 2001:0db8::9 ", true),
    );
    assert_eq!(ipv4?.status(), StatusCode::OK);
    assert_eq!(ipv6?.status(), StatusCode::OK);
    let updated: serde_json::Value =
        decode(request(&server, "/api/ingress/blocked-ips", None).await?).await?;
    assert_eq!(
        updated,
        serde_json::json!({"blockedIps": ["203.0.113.9", "2001:db8::9"]})
    );
    let response = patch_blocklist(&server, "2001:db8::9", true).await?;
    assert_eq!(response.status(), StatusCode::OK);

    let keys = Keyspace::new(&cluster_id);
    let key = keys.resource(
        &ResourceKind::new(BuiltinKind::IngressBlocklist.as_str())?,
        &IngressBlocklistId::new("global")?.into(),
    );
    let stored = kernel_store.get(&key).await?.ok_or("blocklist missing")?;
    let blocklist: IngressBlocklist = serde_json::from_slice(&stored.value)?;
    assert_eq!(blocklist.meta.generation.0, 2);

    assert_eq!(
        patch_blocklist(&server, "not-an-ip", true).await?.status(),
        StatusCode::BAD_REQUEST
    );
    patch_blocklist(&server, "203.0.113.9", false).await?;
    let response = patch_blocklist(&server, "2001:db8::9", false).await?;
    let updated: serde_json::Value = decode(response).await?;
    assert_eq!(updated, serde_json::json!({"blockedIps": []}));
    assert_eq!(
        request(&server, "/_maestro/ingress-denied", None)
            .await?
            .status(),
        StatusCode::FORBIDDEN
    );
    Ok(())
}

#[tokio::test]
async fn traffic_routes_merge_nodes_and_honor_exact_node_selection()
-> Result<(), Box<dyn std::error::Error>> {
    let (kernel_store, cluster_id) = seeded_store().await?;
    let service_id = ServiceId::new("api")?;
    let router = format!(
        "{}route@etcd",
        ingress::traefik_service_router_prefix(&service_id)
    );
    let node_one = NodeId::new("node-1")?;
    let node_two = NodeId::new("node-2")?;
    let first = Arc::new(InMemoryLogStore::new());
    let second = Arc::new(InMemoryLogStore::new());
    first
        .append(&[access_entry(
            1,
            &cluster_id,
            &node_one,
            10_001,
            &router,
            "203.0.113.1",
            "/api",
            200,
            10,
        )?])
        .await?;
    second
        .append(&[
            access_entry(
                2,
                &cluster_id,
                &node_two,
                10_002,
                &router,
                "203.0.113.1",
                "/api",
                200,
                20,
            )?,
            access_entry(
                3,
                &cluster_id,
                &node_two,
                10_003,
                &format!("{}route@etcd", ingress::TRAEFIK_BLOCKED_ROUTER_PREFIX),
                "198.51.100.8",
                "/blocked",
                403,
                0,
            )?,
        ])
        .await?;
    let stores = Arc::new(TestNodeTrafficQueries {
        stores: BTreeMap::from([
            (node_one.clone(), first.clone()),
            (node_two.clone(), second),
        ]),
    });
    let server = ApiServer::new(
        kernel_store,
        cluster_id,
        ServerSettings::new("127.0.0.1:3000".parse()?, None),
    )?
    .with_traffic_query_stores(first, vec![node_one.clone(), node_two.clone()], stores);

    let response = request(
        &server,
        "/api/ingress/traffic?from=10000&to=11000&limit=10",
        None,
    )
    .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let traffic: IngressTrafficBreakdown = decode(response).await?;
    let api_ip = traffic.by_ip.first().ok_or("merged API IP missing")?;
    assert_eq!(traffic.by_ip.len(), 1);
    assert_eq!(api_ip.value, "203.0.113.1");
    assert_eq!(api_ip.requests, 2);

    let response = request(
        &server,
        "/api/ingress/traffic?from=10000&to=11000&limit=10&nodeId=node-1",
        None,
    )
    .await?;
    let selected: IngressTrafficBreakdown = decode(response).await?;
    assert_eq!(selected.by_ip.first().map(|entry| entry.requests), Some(1));

    let response = request(
        &server,
        "/api/ingress/blocked-traffic?from=10000&to=11000&limit=10",
        None,
    )
    .await?;
    let blocked: IngressTrafficBreakdown = decode(response).await?;
    assert_eq!(
        blocked.by_ip.first().map(|entry| entry.value.as_str()),
        Some("198.51.100.8")
    );

    let response = request(
        &server,
        "/api/services/api/traffic?from=10000&to=11000",
        None,
    )
    .await?;
    let points: Vec<TrafficMetricPoint> = decode(response).await?;
    let point = points.first().ok_or("service traffic point missing")?;
    assert_eq!(points.len(), 1);
    assert_eq!(point.requests, 2);
    assert_eq!(point.bytes_in, 30);

    let response = request(
        &server,
        "/api/services/api/traffic/breakdown?from=10000&to=11000",
        None,
    )
    .await?;
    let breakdown: IngressTrafficBreakdown = decode(response).await?;
    assert_eq!(
        breakdown.by_path.first().map(|entry| entry.requests),
        Some(2)
    );

    assert_eq!(
        request(
            &server,
            "/api/ingress/traffic?from=10000&to=11000&nodeId=outside",
            None,
        )
        .await?
        .status(),
        StatusCode::BAD_REQUEST
    );
    Ok(())
}

#[tokio::test]
async fn node_traffic_client_uses_mutual_tls_node_scope_and_local_bypass()
-> Result<(), Box<dyn std::error::Error>> {
    let secret = SecretValue::new("cluster-traffic-test-secret-that-is-long-enough");
    let certified = rcgen::generate_simple_self_signed(vec!["127.0.0.1".to_owned()])?;
    let certificate_pem = certified.cert.pem();
    let identity = TlsIdentity::new(
        certificate_pem.clone(),
        SecretValue::new(certified.signing_key.serialize_pem()),
    );
    let (kernel_store, cluster_id) = seeded_store().await?;
    let remote_node = NodeId::new("node-remote")?;
    let remote_logs = Arc::new(InMemoryLogStore::new());
    remote_logs
        .append(&[access_entry(
            1,
            &cluster_id,
            &remote_node,
            10_001,
            "remote-router@etcd",
            "203.0.113.9",
            "/remote",
            200,
            4,
        )?])
        .await?;
    let remote_queries = Arc::new(TestNodeTrafficQueries {
        stores: BTreeMap::from([(remote_node.clone(), remote_logs.clone())]),
    });
    let remote = ApiServer::new(
        kernel_store,
        cluster_id,
        ServerSettings::new("127.0.0.1:0".parse()?, Some(secret.clone()))
            .with_tls_identity(identity.clone())
            .with_cluster_trust_root(certificate_pem.clone()),
    )?
    .with_traffic_query_stores(remote_logs, vec![remote_node.clone()], remote_queries)
    .bind()
    .await?;
    let remote_address = remote.local_address();
    let (shutdown, shutdown_receiver) = tokio::sync::watch::channel(false);
    let task = tokio::spawn(remote.serve(shutdown_receiver));

    let anonymous = reqwest::Client::builder()
        .https_only(true)
        .add_root_certificate(reqwest::Certificate::from_pem(certificate_pem.as_bytes())?)
        .build()?;
    let response = anonymous
        .get(format!(
            "https://{remote_address}/api/node/traffic/ingress?scope=service&routerPrefix=remote-&from=0&to=20000&limit=10"
        ))
        .bearer_auth(token(secret.expose(), "node")?)
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::FORBIDDEN);

    let local_node = NodeId::new("node-local")?;
    let local_logs = Arc::new(InMemoryLogStore::new());
    let client = HttpNodeLogQueryStore::new(
        local_node.clone(),
        BTreeMap::from([
            (local_node.clone(), "127.0.0.1:1".parse()?),
            (remote_node.clone(), remote_address),
        ]),
        &certificate_pem,
        &identity,
        &secret,
        local_logs.clone(),
        local_logs,
    )?;
    let query = IngressTrafficQuery::new(
        IngressTrafficScope::Service {
            router_prefix: "remote-".to_owned(),
        },
        Timestamp(0),
        Timestamp(20_000),
        10,
    )?;
    let remote_result = client
        .query_node_ingress_traffic(&remote_node, &query)
        .await?;
    assert_eq!(remote_result.by_ip.len(), 1);
    let local_result = client
        .query_node_ingress_traffic(&local_node, &query)
        .await?;
    assert!(local_result.by_ip.is_empty());

    shutdown.send(true)?;
    task.await??;
    Ok(())
}

struct TestNodeTrafficQueries {
    stores: BTreeMap<NodeId, Arc<InMemoryLogStore>>,
}

#[async_trait]
impl NodeTrafficQueryStore for TestNodeTrafficQueries {
    async fn query_node_ingress_traffic(
        &self,
        node_id: &NodeId,
        query: &IngressTrafficQuery,
    ) -> Result<IngressTrafficBreakdown, TrafficQueryError> {
        self.store(node_id)?.query_ingress_traffic(query).await
    }

    async fn query_node_service_traffic(
        &self,
        node_id: &NodeId,
        query: &ServiceTrafficQuery,
    ) -> Result<Vec<TrafficMetricPoint>, TrafficQueryError> {
        self.store(node_id)?.query_service_traffic(query).await
    }
}

impl TestNodeTrafficQueries {
    fn store(&self, node_id: &NodeId) -> Result<&Arc<InMemoryLogStore>, TrafficQueryError> {
        self.stores
            .get(node_id)
            .ok_or_else(|| TrafficQueryError::Unavailable {
                message: format!("unexpected test node `{node_id}`"),
            })
    }
}

#[allow(clippy::too_many_arguments)]
fn access_entry(
    index: u64,
    cluster_id: &ClusterId,
    node_id: &NodeId,
    event_at: i64,
    router: &str,
    ip: &str,
    path: &str,
    status: u16,
    bytes_in: i64,
) -> Result<IngestLogEntry, kernel_api::InvalidIdentifier> {
    let workload_id = WorkloadId::new(format!("traefik-workload-{index}"))?;
    Ok(IngestLogEntry {
        id: LogRecordId {
            node_id: node_id.clone(),
            producer: LogProducer::Workload(workload_id.clone()),
            cursor: OriginCursor::new(index.to_string()),
        },
        observed_at: Timestamp(event_at),
        event_at: Timestamp(event_at),
        severity: "info".to_owned(),
        stream: LogStream::Stdout,
        origin: LogOrigin::Workload {
            metadata: WorkloadMetadata {
                cluster_id: cluster_id.clone(),
                node_id: node_id.clone(),
                service_id: ServiceId::new(TRAEFIK_SERVICE_ID)?,
                deployment_id: DeploymentId::new("traefik-v1")?,
                assignment_id: AssignmentId::new(format!("traefik-assignment-{index}"))?,
                workload_id,
                labels: BTreeMap::new(),
            },
        },
        body: LogBody::Text(String::new()),
        attributes: BTreeMap::from([
            ("maestro.log_type".to_owned(), "ingress_access".to_owned()),
            ("RouterName".to_owned(), router.to_owned()),
            ("maestro.client_ip".to_owned(), ip.to_owned()),
            ("RequestPath".to_owned(), path.to_owned()),
            ("RequestMethod".to_owned(), "GET".to_owned()),
            ("DownstreamStatus".to_owned(), status.to_string()),
            ("RequestContentSize".to_owned(), bytes_in.to_string()),
            ("DownstreamContentSize".to_owned(), "5".to_owned()),
            ("Duration".to_owned(), "500000000".to_owned()),
        ]),
    })
}

async fn patch_blocklist(
    server: &ApiServer,
    ip: &str,
    blocked: bool,
) -> Result<axum::response::Response, Box<dyn std::error::Error>> {
    Ok(server
        .router()
        .oneshot(
            Request::builder()
                .method(Method::PATCH)
                .uri("/api/ingress/blocked-ips")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(serde_json::to_vec(&serde_json::json!({
                    "ip": ip,
                    "blocked": blocked
                }))?))?,
        )
        .await?)
}
