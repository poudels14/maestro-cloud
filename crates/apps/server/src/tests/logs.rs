use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use axum::http::StatusCode;
use kernel_api::{
    AssignmentId, ClusterId, DeploymentId, NodeId, SecretValue, ServiceId, Timestamp, WorkloadId,
};
use logs::{
    ClusterLogPage, InMemoryLogStore, IngestLogEntry, LogBody, LogHistogramBucket,
    LogHistogramGroupBy, LogHistogramQuery, LogOrigin, LogProducer, LogQueryScope, LogQueryStore,
    LogQueryStoreError, LogReadOrder, LogReadQuery, LogRecordId, LogSequence, LogStore, LogStream,
    NodeLogQueryStore, OriginCursor, SequencedLogEntry,
};
use runtime::WorkloadMetadata;

use crate::{ApiServer, HttpNodeLogQueryStore, ServerSettings, TlsIdentity};

use super::deployments::deployment;
use super::{decode, put, request, seeded_store, token};

#[tokio::test]
async fn service_deployment_system_and_histogram_routes_share_typed_queries()
-> Result<(), Box<dyn std::error::Error>> {
    let (store, cluster_id) = seeded_store().await?;
    let deployment = deployment("api-deployment", "api")?;
    put(
        &store,
        &cluster_id,
        "Deployment",
        deployment.meta.id.as_str(),
        &deployment,
    )
    .await?;
    let logs = Arc::new(InMemoryLogStore::new());
    let mut failed = workload_entry(1, "api", "api-deployment", 100, "error", "failed")?;
    failed
        .attributes
        .insert("DownstreamStatus".to_owned(), "503".to_owned());
    logs.append(&[
        failed,
        workload_entry(2, "worker", "worker-deployment", 150, "info", "other")?,
        system_entry(3, 175)?,
    ])
    .await?;
    let server = with_test_logs(
        ApiServer::new(
            store,
            cluster_id,
            ServerSettings::new("127.0.0.1:3000".parse()?, None),
        )?,
        logs.clone(),
    )?;

    let response = request(
        &server,
        "/api/services/api/logs?query=%40http.status_code%3A%5B500%20TO%20599%5D&tail=10",
        None,
    )
    .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let service_logs: ClusterLogPage = decode(response).await?;
    assert_eq!(service_logs.entries.len(), 1);
    assert_eq!(
        service_logs
            .entries
            .first()
            .ok_or("service log missing")?
            .sequence,
        LogSequence(1)
    );

    let response = request(
        &server,
        "/api/services/api/deployments/api-deployment/logs?tail=10",
        None,
    )
    .await?;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(decode::<ClusterLogPage>(response).await?.entries.len(), 1);

    let response = request(&server, "/api/system/logs?component=daemon", None).await?;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(decode::<ClusterLogPage>(response).await?.entries.len(), 1);

    let response = request(
        &server,
        "/api/services/api/logs/histogram?from=0&to=1000&bucketMs=100&groupBy=status",
        None,
    )
    .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let histogram: Vec<LogHistogramBucket> = decode(response).await?;
    assert_eq!(histogram.len(), 1);
    assert_eq!(
        histogram
            .first()
            .and_then(|bucket| bucket.groups.get("5xx")),
        Some(&1)
    );

    logs.append(&[workload_entry(
        4,
        "api",
        "api-deployment",
        250,
        "info",
        "started",
    )?])
    .await?;
    let response = request(
        &server,
        "/api/services/api/logs?tail=10&cursor=%7B%22node-one%22%3A1%7D",
        None,
    )
    .await?;
    let followed: ClusterLogPage = decode(response).await?;
    assert_eq!(followed.entries.len(), 1);
    assert_eq!(
        followed.entries.first().map(|entry| entry.sequence),
        Some(LogSequence(4))
    );
    Ok(())
}

#[tokio::test]
async fn log_routes_reject_ambiguous_cursors_and_missing_store()
-> Result<(), Box<dyn std::error::Error>> {
    let (store, cluster_id) = seeded_store().await?;
    let server = ApiServer::new(
        store,
        cluster_id,
        ServerSettings::new("127.0.0.1:3000".parse()?, None),
    )?;
    assert_eq!(
        request(&server, "/api/logs", None).await?.status(),
        StatusCode::SERVICE_UNAVAILABLE
    );

    let (store, cluster_id) = seeded_store().await?;
    let server = with_test_logs(
        ApiServer::new(
            store,
            cluster_id,
            ServerSettings::new("127.0.0.1:3000".parse()?, None),
        )?,
        Arc::new(InMemoryLogStore::new()),
    )?;
    assert_eq!(
        request(&server, "/api/logs?after=1&before=2", None)
            .await?
            .status(),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        request(&server, "/api/logs?query=unknown%3Avalue", None)
            .await?
            .status(),
        StatusCode::BAD_REQUEST
    );
    Ok(())
}

#[tokio::test]
async fn node_client_queries_a_peer_over_authenticated_mutual_tls()
-> Result<(), Box<dyn std::error::Error>> {
    let secret = SecretValue::new("cluster-log-test-secret-that-is-long-enough");
    let certified = rcgen::generate_simple_self_signed(vec!["127.0.0.1".to_owned()])?;
    let certificate_pem = certified.cert.pem();
    let identity = TlsIdentity::new(
        certificate_pem.clone(),
        SecretValue::new(certified.signing_key.serialize_pem()),
    );
    let remote_logs = Arc::new(InMemoryLogStore::new());
    remote_logs.append(&[system_entry(1, 100)?]).await?;
    let (store, cluster_id) = seeded_store().await?;
    let remote = ApiServer::new(
        store,
        cluster_id,
        ServerSettings::new("127.0.0.1:0".parse()?, Some(secret.clone()))
            .with_tls_identity(identity.clone())
            .with_cluster_trust_root(certificate_pem.clone()),
    )?
    .with_log_query_store(remote_logs)
    .bind()
    .await?;
    let remote_address = remote.local_address();
    let (shutdown, shutdown_receiver) = tokio::sync::watch::channel(false);
    let task = tokio::spawn(remote.serve(shutdown_receiver));

    let anonymous = reqwest::Client::builder()
        .https_only(true)
        .add_root_certificate(reqwest::Certificate::from_pem(certificate_pem.as_bytes())?)
        .build()?;
    let anonymous_response = anonymous
        .get(format!("https://{remote_address}/api/node/logs?scope=all"))
        .bearer_auth(token(secret.expose(), "node")?)
        .send()
        .await?;
    assert_eq!(anonymous_response.status(), StatusCode::FORBIDDEN);

    let local_node = NodeId::new("node-local")?;
    let remote_node = NodeId::new("node-remote")?;
    let local_logs = Arc::new(InMemoryLogStore::new());
    let client = HttpNodeLogQueryStore::new(
        local_node.clone(),
        BTreeMap::from([
            (local_node, "127.0.0.1:1".parse()?),
            (remote_node.clone(), remote_address),
        ]),
        &certificate_pem,
        &identity,
        &secret,
        local_logs.clone(),
        local_logs,
    )?;
    let query = LogReadQuery::new(LogQueryScope::System, LogReadOrder::NewestFirst, 10)?
        .with_search(r#"message:"system""#.parse()?);
    let entries = client.query_node_logs(&remote_node, &query).await?;
    let histogram = client
        .query_node_histogram(
            &remote_node,
            &LogHistogramQuery::new(
                LogQueryScope::System,
                Timestamp(0),
                Timestamp(1_000),
                100,
                LogHistogramGroupBy::Level,
            )?,
        )
        .await?;

    assert_eq!(entries.len(), 1);
    assert_eq!(
        entries.first().map(|entry| entry.sequence),
        Some(LogSequence(1))
    );
    assert_eq!(histogram.first().map(|bucket| bucket.count), Some(1));
    shutdown.send(true)?;
    task.await??;
    Ok(())
}

#[tokio::test]
async fn node_local_routes_require_the_peer_scope_and_reject_unknown_cluster_cursors()
-> Result<(), Box<dyn std::error::Error>> {
    let secret = "node-route-test-secret-that-is-long-enough";
    let (store, cluster_id) = seeded_store().await?;
    let server = with_test_logs(
        ApiServer::new(
            store,
            cluster_id,
            ServerSettings::new("127.0.0.1:3000".parse()?, Some(SecretValue::new(secret))),
        )?,
        Arc::new(InMemoryLogStore::new()),
    )?;
    let operator = token(secret, "operator")?;
    let node = token(secret, "node")?;

    assert_eq!(
        request(&server, "/api/node/logs?scope=all", Some(&operator))
            .await?
            .status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        request(&server, "/api/node/logs?scope=all", Some(&node))
            .await?
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        request(
            &server,
            "/api/logs?cursor=%7B%22outside%22%3A1%7D",
            Some(&operator),
        )
        .await?
        .status(),
        StatusCode::BAD_REQUEST
    );
    Ok(())
}

struct TestNodeLogQueries {
    node_id: NodeId,
    store: Arc<InMemoryLogStore>,
}

#[async_trait]
impl NodeLogQueryStore for TestNodeLogQueries {
    async fn query_node_logs(
        &self,
        node_id: &NodeId,
        query: &LogReadQuery,
    ) -> Result<Vec<SequencedLogEntry>, LogQueryStoreError> {
        self.ensure_node(node_id)?;
        self.store.query_logs(query).await
    }

    async fn query_node_histogram(
        &self,
        node_id: &NodeId,
        query: &LogHistogramQuery,
    ) -> Result<Vec<LogHistogramBucket>, LogQueryStoreError> {
        self.ensure_node(node_id)?;
        self.store.query_log_histogram(query).await
    }
}

impl TestNodeLogQueries {
    fn ensure_node(&self, node_id: &NodeId) -> Result<(), LogQueryStoreError> {
        if node_id == &self.node_id {
            Ok(())
        } else {
            Err(LogQueryStoreError::Unavailable {
                message: format!("unexpected test node `{node_id}`"),
            })
        }
    }
}

fn with_test_logs(
    server: ApiServer,
    store: Arc<InMemoryLogStore>,
) -> Result<ApiServer, kernel_api::InvalidIdentifier> {
    let node_id = NodeId::new("node-one")?;
    Ok(server
        .with_log_query_store(store.clone())
        .with_cluster_log_query_store(
            vec![node_id.clone()],
            Arc::new(TestNodeLogQueries { node_id, store }),
        ))
}

fn workload_entry(
    index: u64,
    service_id: &str,
    deployment_id: &str,
    event_at: i64,
    severity: &str,
    body: &str,
) -> Result<IngestLogEntry, kernel_api::InvalidIdentifier> {
    let node_id = NodeId::new("node-one")?;
    let workload_id = WorkloadId::new(format!("workload-{index}"))?;
    Ok(IngestLogEntry {
        id: LogRecordId {
            node_id: node_id.clone(),
            producer: LogProducer::Workload(workload_id.clone()),
            cursor: OriginCursor::new(index.to_string()),
        },
        observed_at: Timestamp(event_at),
        event_at: Timestamp(event_at),
        severity: severity.to_owned(),
        stream: LogStream::Stdout,
        origin: LogOrigin::Workload {
            metadata: WorkloadMetadata {
                cluster_id: ClusterId::new("server-test")?,
                node_id,
                service_id: ServiceId::new(service_id)?,
                deployment_id: DeploymentId::new(deployment_id)?,
                assignment_id: AssignmentId::new(format!("assignment-{index}"))?,
                workload_id,
                labels: BTreeMap::new(),
            },
        },
        body: LogBody::Text(body.to_owned()),
        attributes: BTreeMap::new(),
    })
}

fn system_entry(
    index: u64,
    event_at: i64,
) -> Result<IngestLogEntry, kernel_api::InvalidIdentifier> {
    let node_id = NodeId::new("node-one")?;
    Ok(IngestLogEntry {
        id: LogRecordId {
            node_id: node_id.clone(),
            producer: LogProducer::System("daemon".to_owned()),
            cursor: OriginCursor::new(index.to_string()),
        },
        observed_at: Timestamp(event_at),
        event_at: Timestamp(event_at),
        severity: "info".to_owned(),
        stream: LogStream::System,
        origin: LogOrigin::System {
            cluster_id: ClusterId::new("server-test")?,
            node_id: Some(node_id),
            component: "daemon".to_owned(),
        },
        body: LogBody::Text("system".to_owned()),
        attributes: BTreeMap::new(),
    })
}
