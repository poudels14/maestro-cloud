mod backup;
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
    let etcd_endpoints = std::env::var("ETCD_ENDPOINTS")
        .unwrap_or_else(|_| etcd_endpoint.to_string())
        .split(',')
        .map(str::trim)
        .filter(|endpoint| !endpoint.is_empty())
        .map(str::to_string)
        .collect::<Vec<_>>();
    eprintln!(
        "starting probe etcd={} port={port}",
        etcd_endpoints.join(",")
    );

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
    let internal_control_token =
        read_secret_env_or_file("MAESTRO_CONTROL_TOKEN", "MAESTRO_CONTROL_TOKEN_FILE");
    let control_socket = std::env::var("MAESTRO_CONTROL_SOCKET").ok();
    let local_node_id = std::env::var("MAESTRO_NODE_ID").ok();
    let mut store =
        EtcdStateStore::new_with_endpoints(&etcd_endpoints, derived_key, etcd_tls).await?;
    if let Some((socket, token)) = mutation_relay_credentials(
        local_node_id.as_deref(),
        control_socket.as_deref(),
        internal_control_token.as_deref(),
    ) {
        store = store.with_mutation_relay(socket.to_string(), token.to_string());
    }
    let store: Arc<dyn crate::deployment::store::ClusterStore> = Arc::new(store);

    let (shutdown_tx, _) = broadcast::channel::<ShutdownEvent>(4);
    let cluster_name = std::env::var("MAESTRO_CLUSTER_NAME").unwrap_or_default();
    let masked_config = std::env::var("MAESTRO_CONFIG").ok().map(|encoded| {
        let json = base64::engine::general_purpose::STANDARD
            .decode(encoded.as_bytes())
            .expect("failed to base64-decode MAESTRO_CONFIG");
        let parsed: crate::config::MaskedConfig =
            serde_json::from_slice(&json).expect("failed to parse MAESTRO_CONFIG");
        Arc::new(parsed)
    });
    let log_backup = masked_config
        .as_ref()
        .and_then(|config| config.log_backup.clone());
    let controller_stats: crate::cluster_stats::SharedControllerStats =
        Arc::new(std::sync::RwLock::new(None));
    let backup_stats: crate::cluster_stats::SharedBackupStats = Arc::new(std::sync::RwLock::new(
        crate::cluster_stats::BackupStatsSnapshot {
            configured: log_backup.is_some(),
            ..crate::cluster_stats::BackupStatsSnapshot::default()
        },
    ));

    let data_root = std::env::var_os("MAESTRO_DATA_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::path::PathBuf::from("/data"));
    let log_store = Arc::new(
        crate::logs::DuckLogStore::open(&data_root).expect("failed to open probe DuckDB stores"),
    );
    match log_store.load_backup_stats().await {
        Ok(Some(mut saved)) => {
            saved.configured = log_backup.is_some();
            *backup_stats.write().unwrap_or_else(|err| err.into_inner()) = saved;
        }
        Ok(None) => {}
        Err(err) => eprintln!("failed to restore backup stats: {err:#}"),
    }
    let rollover_store = log_store.clone();
    tokio::spawn(async move {
        if let Err(err) = rollover_store.rollover().await {
            eprintln!("initial DuckDB rollover failed: {err}");
        }
        let period = Duration::from_secs(60 * 60);
        let mut interval = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            if let Err(err) = rollover_store.rollover().await {
                eprintln!("DuckDB rollover failed: {err}");
            }
        }
    });
    let cleanup_store = log_store.clone();
    let retention_days = log_backup
        .as_ref()
        .and_then(|config| config.retention_days)
        .map(i64::from)
        .filter(|days| *days > 0);
    tokio::spawn(async move {
        let period = Duration::from_secs(24 * 60 * 60);
        let mut interval = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            if let Err(err) = cleanup_store
                .cleanup_old_metrics(7 * 24 * 60 * 60 * 1000)
                .await
            {
                eprintln!("DuckDB metrics cleanup failed: {err}");
            }
            if let Some(days) = retention_days {
                let cutoff = chrono::Utc::now().date_naive() - chrono::Duration::days(days);
                if let Err(err) = cleanup_store.prune_backed_up_before(cutoff).await {
                    eprintln!("Parquet retention cleanup failed: {err}");
                }
            }
        }
    });
    if let Some(settings) = log_backup.as_ref() {
        let config = backup::BackupConfig::from_config(
            &data_root,
            &cluster_name,
            local_node_id.as_deref(),
            settings,
        )?;
        tokio::spawn(backup::run(log_store.clone(), config, backup_stats.clone()));
    }
    let traffic_log_store = log_store.clone();
    tokio::spawn(async move {
        traffic::run(traffic_log_store).await;
    });
    let dns_domain = std::env::var("MAESTRO_DNS_DOMAIN").ok();
    let jwt_secret_key =
        read_secret_env_or_file("MAESTRO_JWT_SECRET_KEY", "MAESTRO_JWT_SECRET_KEY_FILE");
    let ingestion_token =
        read_secret_env_or_file("MAESTRO_INGESTION_TOKEN", "MAESTRO_INGESTION_TOKEN_FILE");
    let system_type = std::env::var("MAESTRO_SYSTEM_TYPE").ok();
    let cluster_alias = std::env::var("MAESTRO_CLUSTER_ALIAS").unwrap_or_default();
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
    let allow_cli_deployment = masked_config
        .as_ref()
        .map(|cfg| cfg.allow_cli_deployment)
        .unwrap_or(false);
    let upload_dir = std::env::var("MAESTRO_UPLOAD_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::path::PathBuf::from("/data/uploads"));
    let server = server::Server::new(
        store.clone(),
        Some(log_store),
        server::ServerConfig {
            jwt_secret_key,
            ingestion_token,
            internal_control_token,
            control_socket,
            system_type,
            cluster_name,
            cluster_alias,
            masked_config,
            slack: slack_notifier,
            allow_cli_deployment,
            upload_dir,
            controller_stats,
            backup_stats,
            local_node_id: local_node_id.clone(),
        },
    );
    let bind_addr = format!("0.0.0.0:{port}");
    let server_shutdown_rx = shutdown_tx.subscribe();
    let tls_shutdown_rx = shutdown_tx.subscribe();
    let server_shutdown_tx = shutdown_tx.clone();
    let server_future = async move {
        let http_server = server.clone().serve(&bind_addr, server_shutdown_rx);
        let result = match (
            std::env::var("MAESTRO_TLS_PORT").ok(),
            std::env::var("MAESTRO_API_CERT_FILE").ok(),
            std::env::var("MAESTRO_API_KEY_FILE").ok(),
        ) {
            (Some(tls_port), Some(certificate), Some(key)) => {
                let tls_bind_addr = format!("0.0.0.0:{tls_port}");
                let client_ca = std::env::var("ETCD_CA_FILE").ok();
                let tls_server = server.serve_tls(
                    &tls_bind_addr,
                    &certificate,
                    &key,
                    client_ca.as_deref(),
                    tls_shutdown_rx,
                );
                tokio::try_join!(http_server, tls_server).map(|_| ())
            }
            _ => http_server.await,
        };
        let _ = server_shutdown_tx.send(ShutdownEvent::Graceful);
        result
    };

    let healthcheck_shutdown_tx = shutdown_tx.clone();
    let healthcheck_monitor: Arc<dyn ReplicaHealthMonitor> = Arc::new(
        DefaultHealthMonitor::new(store.clone(), DEFAULT_MAX_HEALTHCHECK_FAILURES)
            .for_node(local_node_id.clone()),
    );
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
                    local_node_id.as_deref(),
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

fn read_secret_env_or_file(value_name: &str, file_name: &str) -> Option<String> {
    std::env::var(file_name)
        .ok()
        .and_then(|path| std::fs::read_to_string(path).ok())
        .or_else(|| std::env::var(value_name).ok())
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn mutation_relay_credentials<'a>(
    local_node_id: Option<&str>,
    socket: Option<&'a str>,
    token: Option<&'a str>,
) -> Option<(&'a str, &'a str)> {
    local_node_id?;
    Some((socket?, token?))
}

#[cfg(test)]
mod tests {
    use super::mutation_relay_credentials;

    #[test]
    fn standalone_control_socket_does_not_enable_cluster_mutation_relay() {
        assert_eq!(
            mutation_relay_credentials(None, Some("/control.sock"), Some("token")),
            None
        );
        assert_eq!(
            mutation_relay_credentials(Some("node-a"), Some("/control.sock"), Some("token")),
            Some(("/control.sock", "token"))
        );
    }
}
