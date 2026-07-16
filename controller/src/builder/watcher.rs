use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use tokio::sync::broadcast;

use super::BuildSource;
use crate::deployment::store::ClusterStore;
use crate::deployment::types::{DeploymentStatus, ServiceDeployment, ServiceInfo};
use crate::logs::Logger;
use crate::signal::ShutdownEvent;

const WATCH_POLL_INTERVAL: Duration = Duration::from_secs(30);
const INITIAL_BACKOFF: Duration = Duration::from_secs(30);
const MAX_BACKOFF: Duration = Duration::from_secs(5 * 60);

pub struct BuildWatcher {
    store: Arc<dyn ClusterStore>,
    #[allow(dead_code)]
    data_dir: PathBuf,
    signal_rx: broadcast::Receiver<ShutdownEvent>,
    backoff: HashMap<String, (Instant, Duration)>,
    logger: Logger,
}

impl BuildWatcher {
    pub fn new(
        store: Arc<dyn ClusterStore>,
        data_dir: PathBuf,
        signal_rx: broadcast::Receiver<ShutdownEvent>,
        logger: Logger,
    ) -> Self {
        Self {
            store,
            data_dir,
            signal_rx,
            backoff: HashMap::new(),
            logger,
        }
    }

    pub async fn run(mut self) {
        self.logger.emit(
            "info",
            &format!(
                "build watcher started (poll interval: {}s)",
                WATCH_POLL_INTERVAL.as_secs()
            ),
        );
        loop {
            tokio::select! {
                signal = self.signal_rx.recv() => {
                    match signal {
                        Ok(ShutdownEvent::Graceful)
                            | Ok(ShutdownEvent::Force)
                            | Ok(ShutdownEvent::Restart)
                        | Err(broadcast::error::RecvError::Closed) => {
                            self.logger.emit("info", "build watcher shutting down");
                            return;
                        }
                        Err(broadcast::error::RecvError::Lagged(_)) => {}
                    }
                }
                _ = tokio::time::sleep(WATCH_POLL_INTERVAL) => {
                    if let Err(err) = self.poll_services().await {
                        self.logger.emit("error", &format!("build watcher poll error: {err}"));
                    }
                }
            }
        }
    }

    async fn poll_services(&mut self) -> Result<()> {
        let service_ids = self.store.list_service_ids().await?;
        for service_id in service_ids {
            let info = self.store.read_service_info(&service_id).await?;
            let Some(info) = info else { continue };
            let Some(build_config) = &info.config.build else {
                continue;
            };
            if !build_config.watch || info.deploy_frozen {
                continue;
            }
            if let Some(preview_source) = &info.config.preview_source {
                if preview_source.closed_at.is_some() {
                    continue;
                }
                let Some(base) = self
                    .store
                    .read_service_info(&preview_source.base_service_id)
                    .await?
                else {
                    continue;
                };
                if base.deploy_frozen
                    || !base
                        .config
                        .preview
                        .as_ref()
                        .is_some_and(|preview| preview.enabled)
                {
                    continue;
                }
            }
            if let Some((retry_at, _)) = self.backoff.get(&service_id)
                && Instant::now() < *retry_at
            {
                continue;
            }
            match self.check_service(&service_id, &info).await {
                Ok(()) => {
                    self.backoff.remove(&service_id);
                }
                Err(err) => {
                    self.logger.emit(
                        "error",
                        &format!("build watcher error for `{service_id}`: {err}"),
                    );
                    let current_backoff = self
                        .backoff
                        .get(&service_id)
                        .map(|(_, dur)| *dur)
                        .unwrap_or(INITIAL_BACKOFF);
                    let next_backoff = (current_backoff * 2).min(MAX_BACKOFF);
                    self.backoff
                        .insert(service_id, (Instant::now() + current_backoff, next_backoff));
                }
            }
        }
        Ok(())
    }

    async fn check_service(&self, service_id: &str, info: &ServiceInfo) -> Result<()> {
        let build_config = info.config.build.as_ref().unwrap();

        let deployments = self.store.list_service_deployments(service_id).await?;
        let latest = deployments.first();
        if let Some(latest) = latest {
            match (&latest.status, &latest.git_commit) {
                (DeploymentStatus::Queued | DeploymentStatus::Building, _) => return Ok(()),
                (DeploymentStatus::Crashed, None) => return Ok(()),
                _ => {}
            }
        }

        let source = build_config.resolved_source(&self.logger).await?;
        let remote_sha = source.remote_head().await?;
        let current_sha = latest
            .and_then(|d| d.git_commit.as_ref())
            .map(|c| c.reference.as_str());

        if current_sha == Some(&remote_sha) {
            return Ok(());
        }

        self.logger.emit(
            "info",
            &format!(
                "build watcher detected new commit for `{service_id}` ({})",
                &remote_sha[..7.min(remote_sha.len())]
            ),
        );
        let deployment = ServiceDeployment::new(info.config.clone())?;
        self.store.queue_deployment(deployment).await?;
        Ok(())
    }
}
