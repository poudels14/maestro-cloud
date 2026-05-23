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
    let store = EtcdStateStore::new(etcd_endpoint, derived_key, etcd_tls).await?;
    let store: Arc<dyn crate::deployment::store::ClusterStore> = Arc::new(store);

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
    let jwt_secret_key = std::env::var("MAESTRO_JWT_SECRET_KEY").ok();
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
    let slack_webhook_url = std::env::var("MAESTRO_SLACK_WEBHOOK_URL")
        .ok()
        .filter(|value| !value.is_empty())
        .map(crate::utils::crypto::SecretString::new);
    let slack_notifier = crate::slack::SlackNotifier::new(
        slack_webhook_url,
        Some(store.clone()),
        cluster_name.clone(),
        crate::logs::Logger::noop(),
    );
    let healthcheck_slack = slack_notifier.clone();
    let server = server::Server::new(
        store.clone(),
        Some(log_store),
        jwt_secret_key,
        system_type,
        cluster_name,
        cluster_alias,
        masked_config,
        slack_notifier,
    );
    let bind_addr = format!("0.0.0.0:{port}");
    let server_shutdown_rx = shutdown_tx.subscribe();
    let server_shutdown_tx = shutdown_tx.clone();
    let server_future = async move {
        let result = server.serve(&bind_addr, server_shutdown_rx).await;
        let _ = server_shutdown_tx.send(ShutdownEvent::Graceful);
        result
    };

    let healthcheck_shutdown_tx = shutdown_tx.clone();
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
                    &http_client,
                    &mut health_state,
                    &mut last_polled,
                    dns_domain.as_deref(),
                    &healthcheck_slack,
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
