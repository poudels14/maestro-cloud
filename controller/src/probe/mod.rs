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

    let use_duckdb = env_bool("MAESTRO_DUCKDB", true);
    let data_root = std::env::var_os("MAESTRO_DATA_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::path::PathBuf::from("/data"));
    let log_store = if use_duckdb {
        let duck = Arc::new(
            crate::logs::DuckLogStore::open(&data_root)
                .expect("failed to open probe DuckDB stores"),
        );
        // This is the retired probe archive. Active controller spools are shipped
        // through /api/logs and must never be scanned by the SQLite migrator.
        for source in [data_root.join("logs.db")] {
            let archive_hash = sqlite_archive_hash(&source);
            if matches!(
                store.has_log_migration_marker(&archive_hash).await,
                Ok(true)
            ) {
                continue;
            }
            match duck.migrate_sqlite(&source).await {
                Ok(count) if count > 0 => {
                    eprintln!("migrated {count} telemetry rows from {}", source.display());
                    if let Err(err) = store.put_log_migration_marker(&archive_hash).await {
                        eprintln!("failed to record migration completion: {err}");
                    }
                }
                Ok(_) => {
                    if source.exists()
                        && let Err(err) = store.put_log_migration_marker(&archive_hash).await
                    {
                        eprintln!("failed to record migration completion: {err}");
                    }
                }
                Err(err) => eprintln!("SQLite migration failed for {}: {err}", source.display()),
            }
        }
        if let Err(err) = duck.rollover().await {
            eprintln!("initial DuckDB rollover failed: {err}");
        }
        let rollover_store = duck.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60 * 60));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                if let Err(err) = rollover_store.rollover().await {
                    eprintln!("DuckDB rollover failed: {err}");
                }
            }
        });
        let cleanup_store = duck.clone();
        let retention_days = log_backup
            .as_ref()
            .and_then(|config| config.retention_days)
            .map(i64::from)
            .filter(|days| *days > 0);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(24 * 60 * 60));
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
            let config = backup::BackupConfig::from_config(&data_root, &cluster_name, settings)?;
            tokio::spawn(backup::run(duck.clone(), config));
        }
        crate::logs::TelemetryStore::duck(duck)
    } else {
        let sqlite = Arc::new(
            crate::logs::LogStore::open(&data_root.join("logs.db"))
                .expect("failed to open probe SQLite log store"),
        );
        let cleanup_store = sqlite.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(24 * 60 * 60));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                if let Err(err) = cleanup_store
                    .cleanup_old_metrics(7 * 24 * 60 * 60 * 1000)
                    .await
                {
                    eprintln!("SQLite metrics cleanup failed: {err}");
                }
            }
        });
        crate::logs::TelemetryStore::sqlite(sqlite)
    };
    let traffic_log_store = log_store.clone();
    tokio::spawn(async move {
        traffic::run(traffic_log_store).await;
    });
    let dns_domain = std::env::var("MAESTRO_DNS_DOMAIN").ok();
    let jwt_secret_key = std::env::var("MAESTRO_JWT_SECRET_KEY").ok();
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
        jwt_secret_key,
        system_type,
        cluster_name,
        cluster_alias,
        masked_config,
        slack_notifier,
        allow_cli_deployment,
        upload_dir,
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

fn env_bool(name: &str, default: bool) -> bool {
    std::env::var(name)
        .map(|value| parse_bool(&value, default))
        .unwrap_or(default)
}

fn parse_bool(value: &str, default: bool) -> bool {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => true,
        "0" | "false" | "no" | "off" => false,
        _ => default,
    }
}

fn sqlite_archive_hash(path: &std::path::Path) -> String {
    use sha2::{Digest, Sha256};
    let canonical = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
    let mut digest = Sha256::new();
    digest.update(canonical.to_string_lossy().as_bytes());
    if let Ok(metadata) = path.metadata() {
        digest.update(metadata.len().to_le_bytes());
        if let Ok(modified) = metadata.modified()
            && let Ok(duration) = modified.duration_since(std::time::UNIX_EPOCH)
        {
            digest.update(duration.as_nanos().to_le_bytes());
        }
    }
    format!("{:x}", digest.finalize())
}

#[cfg(test)]
mod tests {
    use super::parse_bool;

    #[test]
    fn boolean_environment_values_accept_common_spellings() {
        for value in ["1", "true", "TRUE", "yes", "on"] {
            assert!(parse_bool(value, false), "{value}");
        }
        for value in ["0", "false", "FALSE", "no", "off"] {
            assert!(!parse_bool(value, true), "{value}");
        }
        assert!(parse_bool("invalid", true));
        assert!(!parse_bool("invalid", false));
    }
}
