use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use axum::http::StatusCode;
use kernel_api::{NodeId, SecretValue};
use logs::{
    BackupStatsProvider, BackupStatsProviderError, BackupStatsSnapshot, ClusterStatsResponse,
    ControllerStatsProvider, ControllerStatsSnapshot, InMemoryLogStore, LiveControllerStats,
    LogSinkId, SinkRuntimeRegistry,
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
    runtime.record_failure(&sink_id, "destination rejected payload");
    let controller = Arc::new(LiveControllerStats::new(
        logs,
        vec![sink_id],
        runtime,
        env!("CARGO_PKG_VERSION"),
    )) as Arc<dyn ControllerStatsProvider>;
    let backup = Arc::new(TestBackupStats(BackupStatsSnapshot {
        configured: true,
        last_error_at_ms: Some(20),
        last_success_at_ms: Some(10),
        pending_partitions: 2,
        ..BackupStatsSnapshot::default()
    })) as Arc<dyn BackupStatsProvider>;
    let nodes = vec![NodeId::new("node-1")?, NodeId::new("node-2")?];
    let cluster = Arc::new(TestNodeStats(controller.clone())) as Arc<dyn NodeStatsQueryStore>;
    let server = ApiServer::new(
        store,
        cluster_id,
        ServerSettings::new("127.0.0.1:3000".parse()?, Some(SecretValue::new(secret))),
    )?
    .with_stats_providers(controller, Some(backup), nodes, cluster);
    let operator = token(secret, "operator")?;
    let node = token(secret, "node")?;

    let response = request(&server, "/api/cluster/stats", Some(&operator)).await?;
    assert_eq!(response.status(), StatusCode::OK);
    let stats: ClusterStatsResponse = decode(response).await?;
    let controller = stats.controller.ok_or("controller stats missing")?;
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
    assert_eq!(
        request(&server, "/api/node/stats", Some(&operator))
            .await?
            .status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        request(&server, "/api/node/stats", Some(&node))
            .await?
            .status(),
        StatusCode::OK
    );
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
    let remote_cluster =
        Arc::new(TestNodeStats(remote_controller.clone())) as Arc<dyn NodeStatsQueryStore>;
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
    let client = HttpNodeStatsQueryStore::new(
        local_node.clone(),
        BTreeMap::from([
            (local_node, "127.0.0.1:1".parse()?),
            (remote_node.clone(), remote_address),
        ]),
        &certificate_pem,
        &identity,
        &secret,
        controller_provider("local-version")?,
    )?;
    let stats = client.query_node_stats(&remote_node, 10_000).await?;
    assert_eq!(stats.version, "remote-version");
    shutdown.send(true)?;
    task.await??;
    Ok(())
}

struct TestBackupStats(BackupStatsSnapshot);

impl BackupStatsProvider for TestBackupStats {
    fn backup_stats(&self) -> Result<BackupStatsSnapshot, BackupStatsProviderError> {
        Ok(self.0.clone())
    }
}

struct TestNodeStats(Arc<dyn ControllerStatsProvider>);

#[async_trait]
impl NodeStatsQueryStore for TestNodeStats {
    async fn query_node_stats(
        &self,
        _node_id: &NodeId,
        reported_at_ms: i64,
    ) -> Result<ControllerStatsSnapshot, NodeStatsQueryError> {
        self.0
            .controller_stats(reported_at_ms)
            .await
            .map_err(|error| NodeStatsQueryError::Unavailable {
                message: error.to_string(),
            })
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
