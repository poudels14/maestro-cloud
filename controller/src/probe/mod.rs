mod healthcheck;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::broadcast;
use tokio::time::sleep;

use crate::deployment::etcd::EtcdStateStore;
use crate::server;
use crate::signal::ShutdownEvent;

const POLL_INTERVAL: Duration = Duration::from_secs(2);
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
    let jwt_secret = std::env::var("MAESTRO_JWT_SECRET").ok();
    let server = server::Server::new(store.clone(), Some(log_store), jwt_secret);
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

        loop {
            let poll = async {
                sleep(POLL_INTERVAL).await;
                if let Err(err) =
                    healthcheck::check_deployments(store.as_ref(), &http_client, &mut health_state)
                        .await
                {
                    eprintln!("[probe] poll error: {err}");
                }
            };

            tokio::select! {
                _ = sigterm.recv() => {
                    eprintln!("[probe] received SIGTERM, shutting down");
                    break;
                }
                _ = sigint.recv() => {
                    eprintln!("[probe] received SIGINT, shutting down");
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
