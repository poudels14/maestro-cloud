use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use maestro_core::{ReplicaId, ReplicaPhase, ReplicaPlan, ReplicaStatus};
use maestro_runtime::{
    BindMount, ContainerLimits, ContainerSpec, ContainerState, ContainerStatus, NetworkMode,
    Runtime, RuntimeError, RuntimeEvent,
};
use sha2::{Digest, Sha256};
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio::time::Instant;

use crate::error::AgentError;
use crate::secret_files;
use crate::source::PlanSource;
use crate::status::StatusBoard;

const REPLICA_LABEL: &str = "maestro.replica";
const SERVICE_LABEL: &str = "maestro.service";
const DEPLOYMENT_LABEL: &str = "maestro.deployment";
const PLAN_HASH_LABEL: &str = "maestro.plan-hash";

#[derive(Clone, Debug)]
pub struct ReconcilerSettings {
    pub stop_grace: Duration,
    pub restart_base_delay: Duration,
    pub restart_max_delay: Duration,
    pub stable_after: Duration,
    pub default_max_restarts: u32,
    pub resync_interval: Duration,
    pub pids_limit: u32,
    pub cgroup_parent: String,
    pub secrets_dir: PathBuf,
    pub volume_roots: Vec<PathBuf>,
}

pub struct Reconciler<R: Runtime, S: PlanSource> {
    runtime: Arc<R>,
    source: Arc<S>,
    board: StatusBoard,
    settings: ReconcilerSettings,
    restarts: HashMap<ReplicaId, RestartState>,
    replaced: HashSet<ReplicaId>,
    fetcher: Option<Arc<dyn ImageFetcher>>,
    plans: watch::Sender<Vec<ReplicaPlan>>,
    stop_policies: HashMap<ReplicaId, StopPolicy>,
    retirements: JoinSet<(ReplicaId, Result<(), RuntimeError>)>,
    retiring: HashSet<ReplicaId>,
}

pub trait ImageFetcher: Send + Sync + 'static {
    fn fetch<'a>(
        &'a self,
        image: &'a str,
        source: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;
}

/// Kept after a replica leaves the desired plans, because it is stopped only then.
#[derive(Clone, Copy, Debug)]
struct StopPolicy {
    grace: Duration,
    drain_deadline_ms: Option<u64>,
}

#[derive(Clone, Copy, Debug)]
struct RestartState {
    attempts: u32,
    not_before: Instant,
    started_at: Option<Instant>,
}

impl<R: Runtime, S: PlanSource> Reconciler<R, S> {
    pub fn new(
        runtime: Arc<R>,
        source: Arc<S>,
        board: StatusBoard,
        settings: ReconcilerSettings,
    ) -> Self {
        let (plans, _) = watch::channel(Vec::new());
        Self {
            runtime,
            source,
            board,
            settings,
            restarts: HashMap::new(),
            replaced: HashSet::new(),
            fetcher: None,
            plans,
            stop_policies: HashMap::new(),
            retirements: JoinSet::new(),
            retiring: HashSet::new(),
        }
    }

    pub fn with_image_fetcher(self, fetcher: Arc<dyn ImageFetcher>) -> Self {
        Self {
            fetcher: Some(fetcher),
            ..self
        }
    }

    pub fn plans(&self) -> watch::Receiver<Vec<ReplicaPlan>> {
        self.plans.subscribe()
    }

    pub async fn run(mut self) -> Result<(), AgentError> {
        let mut events = self.runtime.events().await?;
        loop {
            if let Err(error) = self.reconcile().await {
                tracing::warn!(%error, "reconcile failed");
            }
            tokio::select! {
                _ = self.source.changed() => {}
                Some(event) = events.recv() => self.record_event(event),
                _ = tokio::time::sleep(self.settings.resync_interval) => {}
            }
        }
    }

    pub async fn reconcile(&mut self) -> Result<(), AgentError> {
        let desired = self
            .source
            .plans()
            .await?
            .into_iter()
            .map(|plan| (plan.replica_id.clone(), plan))
            .collect::<BTreeMap<_, _>>();
        let actual = self
            .runtime
            .list()
            .await?
            .into_iter()
            .filter_map(|container| {
                let replica_id = container.labels.get(REPLICA_LABEL)?.as_str().into();
                Some((replica_id, container))
            })
            .collect::<BTreeMap<ReplicaId, ContainerStatus>>();

        while let Some(Ok((replica_id, result))) = self.retirements.try_join_next() {
            self.retiring.remove(&replica_id);
            self.finish_retirement(&replica_id, result);
        }
        desired.values().for_each(|plan| {
            let policy = StopPolicy {
                grace: plan
                    .stop_grace_secs
                    .map_or(self.settings.stop_grace, Duration::from_secs),
                drain_deadline_ms: plan.drain_deadline_ms,
            };
            self.stop_policies.insert(plan.replica_id.clone(), policy);
        });
        self.stop_policies.retain(|replica_id, _| {
            desired.contains_key(replica_id) || actual.contains_key(replica_id)
        });
        let leaving = actual
            .keys()
            .filter(|id| !desired.contains_key(*id) && !self.retiring.contains(*id))
            .cloned()
            .collect::<Vec<_>>();
        for replica_id in leaving {
            match self.stop_grace(&replica_id) > self.settings.stop_grace {
                true => self.retire_in_background(replica_id),
                false => self.retire(&replica_id).await,
            }
        }
        secret_files::sweep(
            &self.settings.secrets_dir,
            &actual.keys().cloned().collect::<BTreeSet<_>>(),
        );
        let stale_records = self
            .board
            .snapshot()
            .into_iter()
            .map(|status| status.replica_id)
            .filter(|id| !desired.contains_key(id) && !actual.contains_key(id))
            .collect::<Vec<_>>();
        stale_records.iter().for_each(|replica_id| {
            self.board.remove(replica_id);
            self.restarts.remove(replica_id);
        });
        // Published before launching: a pass that starts many replicas (each fetching its image
        // first) takes minutes, and health checks and firewall rules must cover every replica as
        // soon as it runs, not when the whole pass ends.
        self.plans.send_replace(desired.values().cloned().collect());
        for plan in desired.values() {
            self.converge(plan, actual.get(&plan.replica_id)).await;
        }
        Ok(())
    }

    async fn converge(&mut self, plan: &ReplicaPlan, container: Option<&ContainerStatus>) {
        let hash = plan_hash(plan);
        let state = container.map(|container| {
            let current = container.labels.get(PLAN_HASH_LABEL) == Some(&hash);
            (container.state, current)
        });
        match state {
            Some((ContainerState::Running, true)) => {
                if self.board.get(&plan.replica_id).is_none() {
                    self.adopt(plan, container.and_then(|container| container.ip));
                }
            }
            Some((ContainerState::Running, false)) => {
                self.replaced.insert(plan.replica_id.clone());
                self.retire(&plan.replica_id).await;
                self.restarts.remove(&plan.replica_id);
                self.launch(plan, &hash).await;
            }
            Some((ContainerState::Stopped { exit_code }, _)) => {
                self.handle_exit(plan, Some(exit_code)).await;
            }
            Some((ContainerState::Created | ContainerState::Unknown, _)) => {
                self.handle_exit(plan, None).await;
            }
            None => {
                let failed = self
                    .board
                    .get(&plan.replica_id)
                    .is_some_and(|status| status.phase == ReplicaPhase::Failed);
                let due = self
                    .restarts
                    .get(&plan.replica_id)
                    .is_none_or(|restart| restart.not_before <= Instant::now());
                if !failed && due {
                    self.launch(plan, &hash).await;
                }
            }
        }
    }

    async fn launch(&mut self, plan: &ReplicaPlan, hash: &str) {
        self.board.upsert(ReplicaStatus {
            phase: ReplicaPhase::Starting,
            ..self.status_for(plan)
        });
        let started = match self.prepare_mounts(plan) {
            Ok(mounts) => match self.obtain_image(plan).await {
                Ok(()) => self
                    .runtime
                    .start(&self.container_spec(plan, hash, mounts))
                    .await
                    .map_err(|error| error.to_string()),
                Err(error) => Err(error),
            },
            Err(error) => Err(error),
        };
        match started {
            Ok(container) => {
                let phase = match plan.healthcheck {
                    Some(_) => ReplicaPhase::Running,
                    None => ReplicaPhase::Healthy,
                };
                self.restarts
                    .entry(plan.replica_id.clone())
                    .or_insert(RestartState {
                        attempts: 0,
                        not_before: Instant::now(),
                        started_at: None,
                    })
                    .started_at = Some(Instant::now());
                self.board.update(&plan.replica_id, |status| {
                    status.phase = phase;
                    status.ip = container.ip;
                    status.consecutive_failures = 0;
                    status.last_error = None;
                });
            }
            Err(error) => {
                tracing::warn!(replica = %plan.replica_id, %error, "failed to start replica");
                let _ = self.runtime.remove(plan.replica_id.as_str()).await;
                self.schedule_restart(plan, error);
            }
        }
    }

    fn adopt(&mut self, plan: &ReplicaPlan, ip: Option<std::net::IpAddr>) {
        let phase = match plan.healthcheck {
            Some(_) => ReplicaPhase::Running,
            None => ReplicaPhase::Healthy,
        };
        self.restarts.insert(
            plan.replica_id.clone(),
            RestartState {
                attempts: 0,
                not_before: Instant::now(),
                started_at: Some(Instant::now()),
            },
        );
        self.board.upsert(ReplicaStatus {
            phase,
            ip,
            ..self.status_for(plan)
        });
    }

    async fn handle_exit(&mut self, plan: &ReplicaPlan, exit_code: Option<u32>) {
        let _ = self.runtime.remove(plan.replica_id.as_str()).await;
        secret_files::remove_for(&self.settings.secrets_dir, &plan.replica_id);
        let reason = exit_code.map_or_else(
            || "container is not running".to_string(),
            |code| format!("exited with code {code}"),
        );
        self.schedule_restart(plan, reason);
    }

    fn schedule_restart(&mut self, plan: &ReplicaPlan, reason: String) {
        let now = Instant::now();
        let settings = &self.settings;
        let previous = self.restarts.get(&plan.replica_id).copied();
        let was_stable = previous
            .and_then(|restart| restart.started_at)
            .is_some_and(|started_at| now.duration_since(started_at) >= settings.stable_after);
        let attempts = match was_stable {
            true => 1,
            false => previous.map_or(1, |restart| restart.attempts + 1),
        };
        let max_restarts = plan.max_restarts.unwrap_or(settings.default_max_restarts);
        let delay = settings
            .restart_base_delay
            .saturating_mul(2u32.saturating_pow(attempts.saturating_sub(1)))
            .min(settings.restart_max_delay);
        self.restarts.insert(
            plan.replica_id.clone(),
            RestartState {
                attempts,
                not_before: now + delay,
                started_at: None,
            },
        );
        let phase = match attempts > max_restarts {
            true => ReplicaPhase::Failed,
            false => ReplicaPhase::Restarting,
        };
        let template = self.status_for(plan);
        self.board.upsert(ReplicaStatus {
            phase,
            ip: None,
            restarts: attempts,
            last_error: Some(reason),
            ..template
        });
    }

    async fn retire(&mut self, replica_id: &ReplicaId) {
        let result = stop_and_remove(
            self.runtime.clone(),
            self.settings.secrets_dir.clone(),
            replica_id.clone(),
            self.stop_grace(replica_id),
        )
        .await;
        self.finish_retirement(replica_id, result);
    }

    /// Waiting out a long grace period would hold up every other replica on the node, so those
    /// replicas are stopped off the reconcile loop.
    fn retire_in_background(&mut self, replica_id: ReplicaId) {
        let grace = self.stop_grace(&replica_id);
        tracing::info!(replica = %replica_id, grace_secs = grace.as_secs(), "stopping replica");
        self.retiring.insert(replica_id.clone());
        let stopping = stop_and_remove(
            self.runtime.clone(),
            self.settings.secrets_dir.clone(),
            replica_id.clone(),
            grace,
        );
        self.retirements
            .spawn(async move { (replica_id, stopping.await) });
    }

    /// `deploy.stopGracePeriod`, or the default; a replica stopped while its deployment drains gets
    /// the rest of `deploy.drain.timeout` instead when that is longer.
    fn stop_grace(&self, replica_id: &ReplicaId) -> Duration {
        self.stop_policies
            .get(replica_id)
            .map_or(self.settings.stop_grace, |policy| {
                let remaining_drain = policy
                    .drain_deadline_ms
                    .map(|deadline| Duration::from_millis(deadline.saturating_sub(unix_now_ms())));
                remaining_drain.map_or(policy.grace, |remaining| remaining.max(policy.grace))
            })
    }

    fn finish_retirement(&mut self, replica_id: &ReplicaId, result: Result<(), RuntimeError>) {
        match result {
            Ok(()) => {
                self.board.remove(replica_id);
                self.restarts.remove(replica_id);
                self.stop_policies.remove(replica_id);
            }
            Err(error) => tracing::warn!(replica = %replica_id, %error, "failed to retire replica"),
        }
    }

    pub fn record_event(&mut self, event: RuntimeEvent) {
        match event {
            RuntimeEvent::OomKilled { id } => self.board.update(&id.as_str().into(), |status| {
                status.last_error = Some("killed: out of memory".to_string());
            }),
            RuntimeEvent::Exited { id, exit_code } => {
                let replica_id = ReplicaId::from(id.as_str());
                if !self.replaced.remove(&replica_id) {
                    self.board.update(&replica_id, |status| {
                        if status.phase.is_live() {
                            status.phase = ReplicaPhase::Restarting;
                            status.last_error = Some(format!("exited with code {exit_code}"));
                        }
                    })
                }
            }
        }
    }

    fn status_for(&self, plan: &ReplicaPlan) -> ReplicaStatus {
        let previous = self.board.get(&plan.replica_id);
        ReplicaStatus {
            replica_id: plan.replica_id.clone(),
            service_id: plan.service_id.clone(),
            deployment_id: plan.deployment_id.clone(),
            phase: ReplicaPhase::Starting,
            ip: None,
            restarts: previous.as_ref().map_or(0, |status| status.restarts),
            consecutive_failures: 0,
            last_error: previous.and_then(|status| status.last_error),
            drained: false,
        }
    }

    async fn obtain_image(&self, plan: &ReplicaPlan) -> Result<(), String> {
        match (&plan.image_source, &self.fetcher) {
            (Some(source), Some(fetcher)) => fetcher.fetch(&plan.image, source).await,
            _ => self
                .runtime
                .pull(&plan.image)
                .await
                .map_err(|error| error.to_string()),
        }
    }

    fn prepare_mounts(&self, plan: &ReplicaPlan) -> Result<Vec<BindMount>, String> {
        let volumes = plan
            .volumes
            .iter()
            .map(|volume| {
                let host_path = contained_volume(&self.settings.volume_roots, &volume.host_path)?;
                if let Some(owner) = &volume.owner {
                    std::os::unix::fs::chown(&host_path, Some(owner.uid), owner.gid).map_err(
                        |error| format!("failed to chown volume {}: {error}", volume.host_path),
                    )?;
                }
                Ok(BindMount {
                    source: host_path,
                    destination: volume.mount_path.clone(),
                    read_only: volume.read_only,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        let secrets = plan
            .secrets
            .as_ref()
            .map(|secrets| {
                secret_files::write(&self.settings.secrets_dir, &plan.replica_id, &secrets.items)
                    .map(|source| BindMount {
                        source,
                        destination: secrets.mount_path.clone(),
                        read_only: true,
                    })
                    .map_err(|error| format!("failed to write the secrets file: {error}"))
            })
            .transpose()?;
        Ok(volumes.into_iter().chain(secrets).collect())
    }

    fn container_spec(
        &self,
        plan: &ReplicaPlan,
        hash: &str,
        mounts: Vec<BindMount>,
    ) -> ContainerSpec {
        let labels = BTreeMap::from([
            (REPLICA_LABEL.to_string(), plan.replica_id.to_string()),
            (SERVICE_LABEL.to_string(), plan.service_id.to_string()),
            (DEPLOYMENT_LABEL.to_string(), plan.deployment_id.to_string()),
            (PLAN_HASH_LABEL.to_string(), hash.to_string()),
        ]);
        ContainerSpec {
            id: plan.replica_id.to_string(),
            image: plan.image.clone(),
            entrypoint: plan
                .command
                .as_ref()
                .map(|command| vec![command.command.clone()]),
            args: plan.command.as_ref().map(|command| command.args.clone()),
            env: plan.env.clone(),
            labels,
            limits: ContainerLimits {
                cpu: plan.resources.cpu_limit,
                memory: plan.resources.memory_limit,
                pids: self.settings.pids_limit,
                oom_score_adj: plan.oom_priority.oom_score_adj(),
            },
            cgroup_parent: self.settings.cgroup_parent.clone(),
            mounts,
            network: NetworkMode::Bridge,
        }
    }
}

impl Default for ReconcilerSettings {
    fn default() -> Self {
        Self {
            stop_grace: Duration::from_secs(10),
            restart_base_delay: Duration::from_secs(5),
            restart_max_delay: Duration::from_secs(300),
            stable_after: Duration::from_secs(60),
            default_max_restarts: 10,
            resync_interval: Duration::from_secs(10),
            pids_limit: 4096,
            cgroup_parent: "maestro-workloads.slice".to_string(),
            secrets_dir: PathBuf::from("/run/maestro/secrets"),
            volume_roots: maestro_core::ClusterConfig::default()
                .volume_roots
                .iter()
                .map(PathBuf::from)
                .collect(),
        }
    }
}

fn contained_volume(roots: &[PathBuf], host_path: &str) -> Result<PathBuf, String> {
    let requested = PathBuf::from(host_path);
    let outside = || {
        format!(
            "volume {host_path} is outside the allowed volume roots ({})",
            roots
                .iter()
                .map(|root| root.display().to_string())
                .collect::<Vec<_>>()
                .join(", ")
        )
    };
    let root = roots
        .iter()
        .find(|root| requested.starts_with(root) && requested != **root)
        .ok_or_else(outside)?;
    std::fs::create_dir_all(root)
        .map_err(|error| format!("failed to create {}: {error}", root.display()))?;
    let resolved_root = root
        .canonicalize()
        .map_err(|error| format!("failed to resolve {}: {error}", root.display()))?;
    let inside = |path: &Path| path.starts_with(&resolved_root) && path != resolved_root;
    let existing_ancestor = requested
        .ancestors()
        .find(|ancestor| ancestor.exists())
        .and_then(|ancestor| ancestor.canonicalize().ok())
        .filter(|ancestor| ancestor.starts_with(&resolved_root))
        .ok_or_else(outside)?;
    std::fs::create_dir_all(&requested)
        .map_err(|error| format!("failed to create volume {host_path}: {error}"))?;
    let resolved = requested
        .canonicalize()
        .map_err(|error| format!("failed to resolve volume {host_path}: {error}"))?;
    match inside(&resolved) && resolved.starts_with(&existing_ancestor) {
        true => Ok(resolved),
        false => Err(outside()),
    }
}

/// The drain check is only added once the deployment drains, which must not restart the replica.
async fn stop_and_remove<R: Runtime>(
    runtime: Arc<R>,
    secrets_dir: PathBuf,
    replica_id: ReplicaId,
    grace: Duration,
) -> Result<(), RuntimeError> {
    let stopped = runtime.stop(replica_id.as_str(), grace).await;
    let removed = runtime.remove(replica_id.as_str()).await;
    secret_files::remove_for(&secrets_dir, &replica_id);
    stopped.and(removed)
}

fn unix_now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis() as u64)
}

fn plan_hash(plan: &ReplicaPlan) -> String {
    let container = ReplicaPlan {
        drain_check: None,
        drain_deadline_ms: None,
        stop_grace_secs: None,
        ..plan.clone()
    };
    let encoded = serde_json::to_vec(&container).unwrap_or_default();
    hex::encode(Sha256::digest(encoded))[..16].to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn starting_to_drain_does_not_change_the_container() {
        let plan: ReplicaPlan = serde_json::from_value(serde_json::json!({
            "replicaId": "d1-0",
            "serviceId": "api",
            "deploymentId": "d1",
            "image": "nginx",
            "resources": {
                "cpuLimit": 1.0,
                "memoryLimit": "1Gi",
                "request": { "cpu": 0.25, "memory": "512Mi" }
            },
            "oomPriority": "normal"
        }))
        .unwrap();
        let draining = ReplicaPlan {
            drain_check: Some(maestro_core::HealthcheckPlan {
                path: "/drained".to_string(),
                port: 8080,
                interval_secs: 5,
            }),
            drain_deadline_ms: Some(1_800_000),
            stop_grace_secs: Some(120),
            ..plan.clone()
        };
        assert_eq!(plan_hash(&plan), plan_hash(&draining));
    }

    #[test]
    fn volumes_stay_inside_their_root_even_through_symlinks() {
        let scratch = tempfile::tempdir().unwrap();
        let root = scratch.path().join("volumes");
        let elsewhere = scratch.path().join("keys");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::create_dir_all(&elsewhere).unwrap();
        std::os::unix::fs::symlink(&elsewhere, root.join("escape")).unwrap();
        let roots = vec![root.clone()];
        let path = |relative: &str| root.join(relative).to_string_lossy().to_string();

        assert!(contained_volume(&roots, &path("api/data")).is_ok());
        assert!(contained_volume(&roots, &path("escape")).is_err());
        assert!(contained_volume(&roots, &path("escape/nested")).is_err());
        assert!(contained_volume(&roots, &root.to_string_lossy()).is_err());
        assert!(contained_volume(&roots, &elsewhere.to_string_lossy()).is_err());
        assert!(!elsewhere.join("nested").exists());
    }
}
