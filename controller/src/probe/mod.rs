mod healthcheck;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use backon::{BackoffBuilder, ExponentialBuilder};
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::broadcast;
use tokio::time::sleep;

use crate::deployment::etcd::EtcdStateStore;
use crate::server;
use crate::signal::ShutdownEvent;

const UNHEALTHY_MIN_POLL_INTERVAL: Duration = Duration::from_secs(5);
const HEALTHY_POLL_INTERVAL: Duration = Duration::from_secs(30);
const HEALTH_TIMEOUT: Duration = Duration::from_secs(5);

pub async fn run(etcd_endpoint: &str, port: u16) -> Result<()> {
    eprintln!("starting probe etcd={etcd_endpoint} port={port}");

    let encryption_key = std::env::var("MAESTRO_ENCRYPTION_KEY_FILE")
        .ok()
        .and_then(|path| std::fs::read_to_string(path).ok())
        .unwrap_or_default();
    let derived_key = crate::utils::crypto::derive_key(encryption_key.trim());
    let store = EtcdStateStore::new(etcd_endpoint, derived_key).await?;
    let store: Arc<dyn crate::deployment::store::ClusterStore> = Arc::new(store);

    let (shutdown_tx, _) = broadcast::channel::<ShutdownEvent>(4);

    let log_store = Arc::new(
        crate::logs::LogStore::open(std::path::Path::new("/data/logs.db"))
            .expect("failed to open probe log store"),
    );
    let dns_domain = std::env::var("MAESTRO_DNS_DOMAIN").ok();
    let jwt_secret = std::env::var("MAESTRO_JWT_SECRET").ok();
    let system_type = std::env::var("MAESTRO_SYSTEM_TYPE").ok();
    let cluster_name = std::env::var("MAESTRO_CLUSTER_NAME").unwrap_or_default();
    let cluster_alias = std::env::var("MAESTRO_CLUSTER_ALIAS").unwrap_or_default();
    let server = server::Server::new(
        store.clone(),
        Some(log_store),
        jwt_secret,
        system_type,
        cluster_name,
        cluster_alias,
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

        let mut sigterm =
            signal(SignalKind::terminate()).expect("failed to install SIGTERM handler");
        let mut sigint = signal(SignalKind::interrupt()).expect("failed to install SIGINT handler");
        let mut unhealthy_backoff = ExponentialBuilder::default()
            .with_min_delay(UNHEALTHY_MIN_POLL_INTERVAL)
            .with_max_delay(HEALTHY_POLL_INTERVAL)
            .with_factor(2.0)
            .without_max_times()
            .build();
        let mut poll_interval = UNHEALTHY_MIN_POLL_INTERVAL;

        loop {
            let poll = async {
                sleep(poll_interval).await;
                if let Err(err) = healthcheck::check_deployments(
                    store.as_ref(),
                    &http_client,
                    &mut health_state,
                    dns_domain.as_deref(),
                )
                .await
                {
                    eprintln!("poll error: {err}");
                    poll_interval = unhealthy_backoff.next().unwrap_or(HEALTHY_POLL_INTERVAL);
                    return;
                }

                let has_unhealthy = health_state.values().any(|healthy| !healthy);
                if has_unhealthy {
                    poll_interval = unhealthy_backoff.next().unwrap_or(HEALTHY_POLL_INTERVAL);
                } else {
                    poll_interval = HEALTHY_POLL_INTERVAL;
                    unhealthy_backoff = ExponentialBuilder::default()
                        .with_min_delay(UNHEALTHY_MIN_POLL_INTERVAL)
                        .with_max_delay(HEALTHY_POLL_INTERVAL)
                        .with_factor(2.0)
                        .without_max_times()
                        .build();
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
