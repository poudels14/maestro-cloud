use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::http::StatusCode;
use kernel_api::{NodeId, SecretValue};
use logs::{
    BackupStatsProvider, BackupStatsProviderError, BackupStatsSnapshot, ClusterStatsResponse,
    ControllerStatsProvider, ControllerStatsSnapshot, InMemoryLogStore, LiveControllerStats,
    LogSinkId, SinkRuntimeRegistry, StatsMetricPoint, StatsMetricQuery, StatsMetricStore,
    UptimeClock,
};

use super::{decode, request, seeded_store, token};
use crate::{
    ApiServer, HttpNodeStatsQueryStore, NodeStatsQueryError, NodeStatsQueryStore, ServerSettings,
    TlsIdentity,
};

#[tokio::test]
async fn stats_routes_join_live_sink_backup_and_cluster_health()
-> Result<(), Box<dyn std::error::Error>> {
    let secret = "cluster-stats-test-secret-that-is-long-enough";
    let (store, cluster_id) = seeded_store().await?;
    let logs = Arc::new(InMemoryLogStore::new());
    let sink_id = LogSinkId::new("datadog")?;
    let runtime = SinkRuntimeRegistry::default();
    let uptime_clock = Arc::new(FixedUptimeClock(Duration::from_secs(42)));
    runtime.record_failure(&sink_id, "destination rejected payload");
    logs.append_stats_metrics(&[stats_metric("requests", "node-1")])
        .await?;
    let controller = Arc::new(
        LiveControllerStats::new(
            logs.clone(),
            vec![sink_id],
            runtime,
            env!("CARGO_PKG_VERSION"),
        )
        .with_uptime_clock(uptime_clock.clone()),
    ) as Arc<dyn ControllerStatsProvider>;
    let backup = Arc::new(TestBackupStats(BackupStatsSnapshot {
        configured: true,
        last_error_at_ms: Some(20),
        last_success_at_ms: Some(10),
        pending_partitions: 2,
        ..BackupStatsSnapshot::default()
    })) as Arc<dyn BackupStatsProvider>;
    let nodes = vec![NodeId::new("node-1")?, NodeId::new("node-2")?];
    let cluster = Arc::new(TestNodeStats {
        controller: controller.clone(),
        metrics: logs.clone(),
    }) as Arc<dyn NodeStatsQueryStore>;
    let server = ApiServer::new(
        store,
        cluster_id,
        ServerSettings::new("127.0.0.1:3000".parse()?, Some(SecretValue::new(secret))),
    )?
    .with_stats_providers(controller, Some(backup), logs, nodes, cluster)
    .with_uptime_clock(uptime_clock);
    let operator = token(secret, "operator")?;
    let node = token(secret, "node")?;

    let response = request(&server, "/api/cluster/stats", Some(&operator)).await?;
    assert_eq!(response.status(), StatusCode::OK);
    let stats: ClusterStatsResponse = decode(response).await?;
    assert_eq!(stats.probe.uptime_ms, 42_000);
    let controller = stats.controller.ok_or("controller stats missing")?;
    assert_eq!(controller.uptime_ms, 42_000);
    assert_eq!(controller.sinks.len(), 1);
    assert_eq!(
        controller
            .sinks
            .first()
            .ok_or("sink stats missing")?
            .consecutive_failures,
        1
    );
    let warning_codes = stats
        .warnings
        .iter()
        .map(|warning| warning.code.as_str())
        .collect::<Vec<_>>();
    assert!(warning_codes.contains(&"sink-datadog-failing"));
    assert!(warning_codes.contains(&"log-backup-failing"));

    let response = request(&server, "/api/cluster/stats/nodes", Some(&operator)).await?;
    let nodes: BTreeMap<NodeId, ControllerStatsSnapshot> = decode(response).await?;
    assert_eq!(nodes.len(), 2);
    let response = request(
        &server,
        "/api/metrics/stats?name=requests&from=9999&to=10001&limit=2",
        Some(&operator),
    )
    .await?;
    let points: Vec<StatsMetricPoint> = decode(response).await?;
    assert_eq!(points.len(), 2);
    assert!(points.iter().all(|point| point.name == "requests"));
    assert_eq!(
        points
            .iter()
            .filter_map(|point| point.labels.get("node").map(String::as_str))
            .collect::<Vec<_>>(),
        vec!["node-1", "node-2"]
    );
    assert_eq!(
        request(
            &server,
            "/api/metrics/stats?from=10001&to=9999",
            Some(&operator),
        )
        .await?
        .status(),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        request(&server, "/api/node/stats", Some(&operator))
            .await?
            .status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        request(&server, "/api/node/metrics/stats?to=10001", Some(&node),)
            .await?
            .status(),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        request(&server, "/api/node/stats", Some(&node))
            .await?
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        request(
            &server,
            "/api/node/metrics/stats?from=9999&to=10001",
            Some(&operator),
        )
        .await?
        .status(),
        StatusCode::FORBIDDEN
    );
    let response = request(
        &server,
        "/api/node/metrics/stats?from=9999&to=10001",
        Some(&node),
    )
    .await?;
    assert_eq!(decode::<Vec<StatsMetricPoint>>(response).await?.len(), 1);
    Ok(())
}

#[tokio::test]
async fn stats_routes_fail_closed_without_live_composition()
-> Result<(), Box<dyn std::error::Error>> {
    let (store, cluster_id) = seeded_store().await?;
    let server = ApiServer::new(
        store,
        cluster_id,
        ServerSettings::new("127.0.0.1:3000".parse()?, None),
    )?;
    assert_eq!(
        request(&server, "/api/cluster/stats", None).await?.status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        request(&server, "/api/cluster/stats/nodes", None)
            .await?
            .status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        request(&server, "/api/metrics/stats", None).await?.status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    Ok(())
}

#[tokio::test]
async fn node_stats_client_queries_a_peer_over_authenticated_mutual_tls()
-> Result<(), Box<dyn std::error::Error>> {
    let secret = SecretValue::new("node-stats-test-secret-that-is-long-enough");
    let certified = rcgen::generate_simple_self_signed(vec!["127.0.0.1".to_owned()])?;
    let certificate_pem = certified.cert.pem();
    let identity = TlsIdentity::new(
        certificate_pem.clone(),
        SecretValue::new(certified.signing_key.serialize_pem()),
    );
    let remote_node = NodeId::new("node-remote")?;
    let remote_controller = controller_provider("remote-version")?;
    let remote_metrics = Arc::new(InMemoryLogStore::new());
    remote_metrics
        .append_stats_metrics(&[stats_metric("remote.metric", "node-remote")])
        .await?;
    let remote_cluster = Arc::new(TestNodeStats {
        controller: remote_controller.clone(),
        metrics: remote_metrics.clone(),
    }) as Arc<dyn NodeStatsQueryStore>;
    let (store, cluster_id) = seeded_store().await?;
    let remote = ApiServer::new(
        store,
        cluster_id,
        ServerSettings::new("127.0.0.1:0".parse()?, Some(secret.clone()))
            .with_tls_identity(identity.clone())
            .with_cluster_trust_root(certificate_pem.clone()),
    )?
    .with_stats_providers(
        remote_controller,
        None,
        remote_metrics,
        vec![remote_node.clone()],
        remote_cluster,
    )
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
        .get(format!("https://{remote_address}/api/node/stats"))
        .bearer_auth(token(secret.expose(), "node")?)
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::FORBIDDEN);

    let local_node = NodeId::new("node-local")?;
    let local_metrics = Arc::new(InMemoryLogStore::new());
    local_metrics
        .append_stats_metrics(&[stats_metric("local.metric", "node-local")])
        .await?;
    let client = HttpNodeStatsQueryStore::new(
        local_node.clone(),
        BTreeMap::from([
            (local_node.clone(), "127.0.0.1:1".parse()?),
            (remote_node.clone(), remote_address),
        ]),
        &certificate_pem,
        &identity,
        &secret,
        controller_provider("local-version")?,
        local_metrics,
    )?;
    let stats = client.query_node_stats(&remote_node, 10_000).await?;
    assert_eq!(stats.version, "remote-version");
    let points = client
        .query_node_stats_metrics(
            &remote_node,
            &StatsMetricQuery::new(Some("remote.metric".to_owned()), 9_999, 10_001, 8)?,
        )
        .await?;
    assert_eq!(points, vec![stats_metric("remote.metric", "node-remote")]);
    assert_eq!(
        client
            .query_node_stats_metrics(
                &local_node,
                &StatsMetricQuery::new(Some("local.metric".to_owned()), 9_999, 10_001, 8)?,
            )
            .await?,
        vec![stats_metric("local.metric", "node-local")]
    );
    shutdown.send(true)?;
    task.await??;
    Ok(())
}

struct TestBackupStats(BackupStatsSnapshot);

struct FixedUptimeClock(Duration);

impl UptimeClock for FixedUptimeClock {
    fn elapsed(&self) -> Duration {
        self.0
    }
}

impl BackupStatsProvider for TestBackupStats {
    fn backup_stats(&self) -> Result<BackupStatsSnapshot, BackupStatsProviderError> {
        Ok(self.0.clone())
    }
}

struct TestNodeStats {
    controller: Arc<dyn ControllerStatsProvider>,
    metrics: Arc<dyn StatsMetricStore>,
}

#[async_trait]
impl NodeStatsQueryStore for TestNodeStats {
    async fn query_node_stats(
        &self,
        _node_id: &NodeId,
        reported_at_ms: i64,
    ) -> Result<ControllerStatsSnapshot, NodeStatsQueryError> {
        self.controller
            .controller_stats(reported_at_ms)
            .await
            .map_err(|error| NodeStatsQueryError::Unavailable {
                message: error.to_string(),
            })
    }

    async fn query_node_stats_metrics(
        &self,
        node_id: &NodeId,
        query: &StatsMetricQuery,
    ) -> Result<Vec<StatsMetricPoint>, NodeStatsQueryError> {
        let mut points = self
            .metrics
            .query_stats_metrics(query)
            .await
            .map_err(|error| NodeStatsQueryError::Unavailable {
                message: error.to_string(),
            })?;
        for point in &mut points {
            point
                .labels
                .insert("node".to_owned(), node_id.as_str().to_owned());
        }
        Ok(points)
    }
}

fn stats_metric(name: &str, node: &str) -> StatsMetricPoint {
    StatsMetricPoint {
        ts: 10_000,
        name: name.to_owned(),
        value: 1.0,
        labels: BTreeMap::from([("node".to_owned(), node.to_owned())]),
    }
}

fn controller_provider(
    version: &str,
) -> Result<Arc<dyn ControllerStatsProvider>, Box<dyn std::error::Error>> {
    Ok(Arc::new(LiveControllerStats::new(
        Arc::new(InMemoryLogStore::new()),
        vec![LogSinkId::new("datadog")?],
        SinkRuntimeRegistry::default(),
        version,
    )))
}
