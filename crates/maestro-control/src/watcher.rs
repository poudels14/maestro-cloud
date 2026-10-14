use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use maestro_core::{Deployment, DeploymentStatus, ServiceId, ServiceSpec};
use maestro_store::{Mutation, SecretKey};
use tokio::time::Instant;

use crate::api::new_deployment;
use crate::error::ControlError;
use crate::writer::StoreWriter;

const INITIAL_BACKOFF: Duration = Duration::from_secs(30);
const MAX_BACKOFF: Duration = Duration::from_secs(300);
const GIT_TOKEN_SECRET: &str = "GH_TOKEN";

pub trait RemoteHeads: Send + Sync + 'static {
    fn head(
        &self,
        repo: String,
        branch: String,
        token: Option<String>,
    ) -> impl Future<Output = Result<String, String>> + Send;
}

pub struct BuildWatcher<W: StoreWriter, H: RemoteHeads> {
    writer: Arc<W>,
    heads: Arc<H>,
    key: Arc<SecretKey>,
    backoff: Mutex<HashMap<ServiceId, Backoff>>,
}

#[derive(Clone, Copy)]
struct Backoff {
    retry_at: Instant,
    next: Duration,
}

impl<W: StoreWriter, H: RemoteHeads> BuildWatcher<W, H> {
    pub fn new(writer: Arc<W>, heads: Arc<H>, key: Arc<SecretKey>) -> Self {
        Self {
            writer,
            heads,
            key,
            backoff: Mutex::default(),
        }
    }

    pub async fn poll(&self) -> Result<(), ControlError> {
        let state = self.writer.state();
        let services = match state.cluster_freeze()? {
            Some(_) => Vec::new(),
            None => state.services()?,
        };
        for (service_id, spec) in services {
            let watched = spec
                .build
                .as_ref()
                .is_some_and(|build| build.watch && build.branch.is_some());
            let backing_off = self
                .lock_backoff()
                .get(&service_id)
                .is_some_and(|backoff| backoff.retry_at > Instant::now());
            if watched && !backing_off && !state.service_controls(&service_id)?.frozen {
                self.check(&service_id, spec).await?;
            }
        }
        Ok(())
    }

    async fn check(&self, service_id: &ServiceId, spec: ServiceSpec) -> Result<(), ControlError> {
        let latest = self
            .writer
            .state()
            .deployments(service_id)?
            .into_iter()
            .max_by_key(|deployment| deployment.created_at_ms);
        if latest.as_ref().is_none_or(needs_head_check) {
            let opened = self.key.open_spec(&spec)?;
            let build = opened.build.ok_or_else(|| {
                ControlError::Invalid(format!("service {service_id} has no build configuration"))
            })?;
            let head = self
                .heads
                .head(
                    build.repo.unwrap_or_default(),
                    build.branch.unwrap_or_default(),
                    build.secrets.items.get(GIT_TOKEN_SECRET).cloned(),
                )
                .await;
            match head {
                Ok(head) => {
                    self.lock_backoff().remove(service_id);
                    let deployed = latest
                        .and_then(|deployment| deployment.git_commit)
                        .map(|commit| commit.reference);
                    if deployed.as_deref() != Some(head.as_str()) {
                        tracing::info!(service = %service_id, %head, "new commit; queueing a build");
                        self.writer
                            .write(vec![Mutation::CreateDeployment {
                                deployment: new_deployment(service_id, spec),
                            }])
                            .await?;
                    }
                }
                Err(error) => {
                    let mut backoff = self.lock_backoff();
                    let next = backoff
                        .get(service_id)
                        .map_or(INITIAL_BACKOFF, |backoff| backoff.next);
                    tracing::warn!(service = %service_id, %error, retry_in = ?next, "watch check failed");
                    backoff.insert(
                        service_id.clone(),
                        Backoff {
                            retry_at: Instant::now() + next,
                            next: (next * 2).min(MAX_BACKOFF),
                        },
                    );
                }
            }
        }
        Ok(())
    }

    fn lock_backoff(&self) -> std::sync::MutexGuard<'_, HashMap<ServiceId, Backoff>> {
        self.backoff
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

fn needs_head_check(latest: &Deployment) -> bool {
    let in_flight = matches!(
        latest.status,
        DeploymentStatus::Queued | DeploymentStatus::Building
    );
    let failed_before_checkout =
        latest.status == DeploymentStatus::Failed && latest.git_commit.is_none();
    !in_flight && !failed_before_checkout
}
