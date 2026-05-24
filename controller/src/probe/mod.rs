mod healthcheck;
mod traffic;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use base64::Engine;
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::broadcast;
use tokio::time::sleep;

use crate::deployment::etcd::EtcdStateStore;
use crate::health::{DEFAULT_MAX_HEALTHCHECK_FAILURES, DefaultHealthMonitor, ReplicaHealthMonitor};
use crate::server;
use crate::signal::ShutdownEvent;

const POLL_TICK_INTERVAL: Duration = Duration::from_secs(5);
const HEALTH_TIMEOUT: Duration = Duration::from_secs(5);

pub async fn run(etcd_endpoint: &str, port: u16) -> Result<()> {
    eprintln!("starting probe etcd={etcd_endpoint} port={port}");

    let encryption_key = std::env::var("MAESTRO_ENCRYPTION_KEY_FILE")
        .ok()
        .and_then(|path| std::fs::read_to_string(path).ok())
        .unwrap_or_default();
    let derived_key = crate::utils::crypto::derive_key(encryption_key.trim());
    let etcd_tls = match (
        std::env::var("ETCD_CA_FILE"),
        std::env::var("ETCD_CERT_FILE"),
        std::env::var("ETCD_KEY_FILE"),
    ) {
        (Ok(ca), Ok(cert), Ok(key)) => {
            crate::deployment::build_etcd_tls_from_files(&ca, &cert, &key)
        }
        _ => None,
    };
    let store = EtcdStateStore::new(etcd_endpoint, derived_key, etcd_tls.clone()).await?;
    let store: Arc<dyn crate::deployment::store::ClusterStore> = Arc::new(store);

    let cluster_client = {
        let connect_options = etcd_tls.map(|tls| etcd_client::ConnectOptions::new().with_tls(tls));
        let raw = etcd_client::Client::connect([etcd_endpoint], connect_options).await?;
        Arc::new(tokio::sync::Mutex::new(raw))
    };
    let this_node_id = std::env::var("MAESTRO_NODE_ID").ok();
    let cluster_registry: Arc<dyn crate::cluster::NodeRegistry> =
        Arc::new(crate::cluster::EtcdNodeRegistry::new(
            cluster_client.clone(),
            this_node_id
                .clone()
                .unwrap_or_else(|| "unknown".to_string()),
        ));
    let cluster_elector_inner = Arc::new(crate::cluster::EtcdLeaderElector::new(
        cluster_client.clone(),
        this_node_id
            .clone()
            .unwrap_or_else(|| "unknown".to_string()),
    ));
    cluster_elector_inner.spawn_observer().await;
    let cluster_elector: Arc<dyn crate::cluster::LeaderElector> = cluster_elector_inner.clone();
    let cluster_assignment_store: Arc<dyn crate::cluster::assignment_store::AssignmentStore> =
        Arc::new(
            crate::cluster::etcd_assignment_store::EtcdAssignmentStore::new(cluster_client.clone()),
        );

    let (shutdown_tx, _) = broadcast::channel::<ShutdownEvent>(4);

    let log_store = Arc::new(
        crate::logs::LogStore::open(std::path::Path::new("/data/logs.db"))
            .expect("failed to open probe log store"),
    );
    let traffic_log_store = log_store.clone();
    tokio::spawn(async move {
        traffic::run(traffic_log_store).await;
    });
    let dns_domain = std::env::var("MAESTRO_DNS_DOMAIN").ok();
    // Prefer the file-mounted secret; fall back to the env var so existing
    // single-node setups (which still pass --jwt-secret-key as env) keep working.
    let jwt_secret_key = read_secret_file("MAESTRO_JWT_SECRET_KEY_FILE")
        .or_else(|| std::env::var("MAESTRO_JWT_SECRET_KEY").ok());
    let system_type = std::env::var("MAESTRO_SYSTEM_TYPE").ok();
    let cluster_name = std::env::var("MAESTRO_CLUSTER_NAME").unwrap_or_default();
    let cluster_alias = std::env::var("MAESTRO_CLUSTER_ALIAS").unwrap_or_default();
    let masked_config = std::env::var("MAESTRO_CONFIG").ok().map(|encoded| {
        let json = base64::engine::general_purpose::STANDARD
            .decode(encoded.as_bytes())
            .expect("failed to base64-decode MAESTRO_CONFIG");
        let parsed: crate::config::MaskedConfig =
            serde_json::from_slice(&json).expect("failed to parse MAESTRO_CONFIG");
        Arc::new(parsed)
    });
    let slack_webhook_url = read_secret_file("MAESTRO_SLACK_WEBHOOK_URL_FILE")
        .or_else(|| std::env::var("MAESTRO_SLACK_WEBHOOK_URL").ok())
        .filter(|value| !value.is_empty())
        .map(crate::utils::crypto::SecretString::new);
    let slack_notifier = crate::slack::SlackNotifier::new(
        slack_webhook_url,
        Some(store.clone()),
        cluster_name.clone(),
        crate::logs::Logger::noop(),
    );
    let allow_cli_deployment = masked_config
        .as_ref()
        .map(|cfg| cfg.allow_cli_deployment)
        .unwrap_or(false);
    let upload_dir = std::env::var("MAESTRO_UPLOAD_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::path::PathBuf::from("/data/uploads"));
    let server = server::Server::new(server::ServerOptions {
        store: store.clone(),
        log_store: Some(log_store),
        jwt_secret_key,
        system_type,
        cluster_name,
        cluster_alias,
        masked_config,
        slack: slack_notifier,
        allow_cli_deployment,
        upload_dir,
        cluster_registry: Some(cluster_registry),
        cluster_elector: Some(cluster_elector),
        cluster_assignment_store: Some(cluster_assignment_store),
        cluster_metrics: None,
        cluster_etcd_client: Some(cluster_client.clone()),
        this_node_id,
    });
    let bind_addr = format!("0.0.0.0:{port}");
    let server_shutdown_rx = shutdown_tx.subscribe();
    let server_shutdown_tx = shutdown_tx.clone();
    let server_future = async move {
        let result = server.serve(&bind_addr, server_shutdown_rx).await;
        let _ = server_shutdown_tx.send(ShutdownEvent::Graceful);
        result
    };

    let healthcheck_shutdown_tx = shutdown_tx.clone();
    let healthcheck_monitor: Arc<dyn ReplicaHealthMonitor> = Arc::new(DefaultHealthMonitor::new(
        store.clone(),
        DEFAULT_MAX_HEALTHCHECK_FAILURES,
    ));
    let healthcheck_future = async move {
        let http_client = reqwest::Client::builder()
            .timeout(HEALTH_TIMEOUT)
            .build()
            .expect("failed to build http client");
        let mut health_state = healthcheck::HealthState::new();
        let mut last_polled: HashMap<String, Instant> = HashMap::new();

        let mut sigterm =
            signal(SignalKind::terminate()).expect("failed to install SIGTERM handler");
        let mut sigint = signal(SignalKind::interrupt()).expect("failed to install SIGINT handler");

        loop {
            let poll = async {
                sleep(POLL_TICK_INTERVAL).await;
                if let Err(err) = healthcheck::check_deployments(
                    store.as_ref(),
                    healthcheck_monitor.as_ref(),
                    &http_client,
                    &mut health_state,
                    &mut last_polled,
                    dns_domain.as_deref(),
                )
                .await
                {
                    eprintln!("poll error: {err}");
                }
            };

            tokio::select! {
                _ = sigterm.recv() => {
                    eprintln!("received SIGTERM, shutting down");
                    break;
                }
                _ = sigint.recv() => {
                    eprintln!("received SIGINT, shutting down");
                    break;
                }
                _ = poll => {}
            }
        }

        let _ = healthcheck_shutdown_tx.send(ShutdownEvent::Graceful);
        Ok::<(), crate::error::Error>(())
    };

    let _ = tokio::try_join!(server_future, healthcheck_future);
    Ok(())
}

fn read_secret_file(env_var: &str) -> Option<String> {
    let path = std::env::var(env_var)
        .ok()
        .filter(|p| !p.trim().is_empty())?;
    std::fs::read_to_string(&path)
        .ok()
        .map(|value| value.trim_end_matches(['\n', '\r']).to_string())
        .filter(|value| !value.is_empty())
}
