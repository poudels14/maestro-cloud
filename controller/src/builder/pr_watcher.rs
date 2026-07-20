use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Result, anyhow};
use tokio::sync::broadcast;

use super::{PullRequest, PullRequestApi, RateLimitError};
use crate::deployment::preview::{PreviewPullRequest, derive_preview_config};
use crate::deployment::store::ClusterStore;
use crate::deployment::types::{DeploymentStatus, ServiceConfig, ServiceDeployment, ServiceInfo};
use crate::logs::Logger;
use crate::signal::ShutdownEvent;

const INITIAL_BACKOFF: Duration = Duration::from_secs(30);
const MAX_BACKOFF: Duration = Duration::from_secs(5 * 60);
const DEFAULT_CLOSE_GRACE_PERIOD: Duration = Duration::from_secs(24 * 60 * 60);

#[derive(Debug, Clone)]
pub struct PrWatcherConfig {
    pub preview_domain: String,
    pub poll_interval: Duration,
    pub max_concurrent_previews: usize,
    pub homepage: Option<String>,
}

pub struct PrWatcher {
    store: Arc<dyn ClusterStore>,
    api: Arc<dyn PullRequestApi>,
    config: PrWatcherConfig,
    signal_rx: broadcast::Receiver<ShutdownEvent>,
    backoff: HashMap<String, (Instant, Duration)>,
    logger: Logger,
}

#[derive(Clone)]
struct BaseService {
    info: ServiceInfo,
    owner: String,
    repo: String,
    repo_key: String,
}

#[derive(Clone)]
struct Candidate {
    base: BaseService,
    pull_request: PullRequest,
}

impl PrWatcher {
    pub fn new(
        store: Arc<dyn ClusterStore>,
        api: Arc<dyn PullRequestApi>,
        config: PrWatcherConfig,
        signal_rx: broadcast::Receiver<ShutdownEvent>,
        logger: Logger,
    ) -> Self {
        Self {
            store,
            api,
            config,
            signal_rx,
            backoff: HashMap::new(),
            logger,
        }
    }

    pub async fn run(mut self) {
        self.logger.emit(
            "info",
            &format!(
                "PR preview watcher started (poll interval: {}s, max previews: {})",
                self.config.poll_interval.as_secs(),
                self.config.max_concurrent_previews
            ),
        );
        let mut interval = tokio::time::interval(self.config.poll_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                signal = self.signal_rx.recv() => {
                    match signal {
                        Ok(ShutdownEvent::Graceful)
                            | Ok(ShutdownEvent::Force)
                            | Ok(ShutdownEvent::Restart)
                            | Err(broadcast::error::RecvError::Closed) => {
                                self.logger.emit("info", "PR preview watcher shutting down");
                                return;
                            }
                        Err(broadcast::error::RecvError::Lagged(_)) => {}
                    }
                }
                _ = interval.tick() => {
                    let now_ms = crate::utils::time::current_time_millis().unwrap_or_default();
                    if let Err(error) = self.poll(now_ms).await {
                        self.logger.emit("error", &format!("PR preview watcher poll error: {error}"));
                    }
                }
            }
        }
    }

    async fn poll(&mut self, now_ms: u64) -> Result<()> {
        let service_ids = self.store.list_service_ids().await?;
        let mut infos = Vec::with_capacity(service_ids.len());
        for service_id in service_ids {
            if let Some(info) = self.store.read_service_info(&service_id).await? {
                infos.push(info);
            }
        }

        let mut bases = HashMap::new();
        for info in infos
            .iter()
            .filter(|info| info.config.preview_source.is_none())
        {
            if !info
                .config
                .preview
                .as_ref()
                .is_some_and(|preview| preview.enabled)
            {
                continue;
            }
            match self.base_service(info.clone()) {
                Ok(base) => {
                    bases.insert(info.config.id.clone(), base);
                }
                Err(error) => self.logger.emit(
                    "error",
                    &format!(
                        "cannot watch previews for service `{}`: {error}",
                        info.config.id
                    ),
                ),
            }
        }

        let mut previews = HashMap::new();
        for info in infos
            .into_iter()
            .filter(|info| info.config.preview_source.is_some())
        {
            let source = info.config.preview_source.as_ref().unwrap();
            previews.insert((source.base_service_id.clone(), source.pr_number), info);
        }

        let orphaned = previews
            .iter()
            .filter(|((base_id, _), _)| !bases.contains_key(base_id))
            .map(|(key, info)| (key.clone(), info.clone()))
            .collect::<Vec<_>>();
        for (key, info) in orphaned {
            if self
                .remove_preview(&info, "The base service no longer has previews enabled.")
                .await
            {
                previews.remove(&key);
            }
        }

        let mut repo_coordinates = HashMap::new();
        for base in bases.values() {
            repo_coordinates
                .entry(base.repo_key.clone())
                .or_insert_with(|| (base.owner.clone(), base.repo.clone()));
        }
        let mut pull_requests_by_repo = HashMap::new();
        for (repo_key, (owner, repo)) in repo_coordinates {
            if self
                .backoff
                .get(&repo_key)
                .is_some_and(|(retry_at, _)| Instant::now() < *retry_at)
            {
                continue;
            }
            match self.api.list_open(&owner, &repo).await {
                Ok(pull_requests) => {
                    self.backoff.remove(&repo_key);
                    pull_requests_by_repo.insert(repo_key, pull_requests);
                }
                Err(error) => {
                    self.record_backoff(&repo_key, &error);
                    self.logger.emit(
                        "error",
                        &format!("failed to list pull requests for `{owner}/{repo}`: {error}"),
                    );
                }
            }
        }

        let mut candidates = Vec::new();
        for base in bases.values() {
            let Some(repo_pull_requests) = pull_requests_by_repo.get(&base.repo_key) else {
                continue;
            };
            let open_pull_requests = repo_pull_requests
                .iter()
                .filter(|pull_request| {
                    !pull_request.draft
                        && pull_request
                            .head_repo_full_name
                            .as_deref()
                            .is_some_and(|repo| repo.eq_ignore_ascii_case(&base.repo_key))
                })
                .map(|pull_request| (pull_request.number, pull_request.clone()))
                .collect::<HashMap<_, _>>();
            let draft_pull_requests = repo_pull_requests
                .iter()
                .filter(|pull_request| {
                    pull_request.draft
                        && pull_request
                            .head_repo_full_name
                            .as_deref()
                            .is_some_and(|repo| repo.eq_ignore_ascii_case(&base.repo_key))
                })
                .map(|pull_request| pull_request.number)
                .collect::<HashSet<_>>();

            let existing_for_base = previews
                .iter()
                .filter(|((base_id, _), _)| base_id == &base.info.config.id)
                .map(|(key, info)| (key.clone(), info.clone()))
                .collect::<Vec<_>>();
            for (key, info) in existing_for_base {
                if draft_pull_requests.contains(&key.1) {
                    if self
                        .remove_preview(&info, "Draft pull requests do not receive previews.")
                        .await
                    {
                        previews.remove(&key);
                    }
                } else if let Some(pull_request) = open_pull_requests.get(&key.1) {
                    let updated = self
                        .reconcile_open_preview(base, &info, pull_request)
                        .await?;
                    previews.insert(key, updated);
                } else if self.reconcile_closed_preview(base, &info, now_ms).await? {
                    previews.remove(&key);
                }
            }

            if !base.info.deploy_frozen {
                for pull_request in open_pull_requests.into_values() {
                    let key = (base.info.config.id.clone(), pull_request.number);
                    if !previews.contains_key(&key) {
                        candidates.push(Candidate {
                            base: base.clone(),
                            pull_request,
                        });
                    }
                }
            }
        }

        candidates.sort_by(|left, right| {
            left.pull_request
                .created_at
                .cmp(&right.pull_request.created_at)
                .then_with(|| left.base.info.config.id.cmp(&right.base.info.config.id))
                .then_with(|| left.pull_request.number.cmp(&right.pull_request.number))
        });
        let slots = self
            .config
            .max_concurrent_previews
            .saturating_sub(previews.len());
        for (index, candidate) in candidates.into_iter().enumerate() {
            if index < slots {
                let info = self.create_preview(&candidate).await?;
                let source = info.config.preview_source.as_ref().unwrap();
                previews.insert((source.base_service_id.clone(), source.pr_number), info);
            } else {
                self.post_comment(
                    &candidate.base,
                    candidate.pull_request.number,
                    &quota_comment(self.config.max_concurrent_previews),
                )
                .await;
            }
        }
        Ok(())
    }

    fn base_service(&self, info: ServiceInfo) -> Result<BaseService> {
        let repo = info
            .config
            .build
            .as_ref()
            .and_then(|build| build.repo.as_deref())
            .ok_or_else(|| anyhow!("build.repo is missing"))?;
        let (owner, repo) =
            crate::validation::parse_github_repo(repo).map_err(anyhow::Error::msg)?;
        let repo_key = format!("{owner}/{repo}").to_ascii_lowercase();
        Ok(BaseService {
            info,
            owner,
            repo,
            repo_key,
        })
    }

    async fn create_preview(&self, candidate: &Candidate) -> Result<ServiceInfo> {
        let base_config = self.hydrate_deploy_secrets(&candidate.base.info).await?;
        let config = derive_preview_config(
            &base_config,
            &preview_pull_request(&candidate.pull_request),
            &self.config.preview_domain,
        )?;
        let deployment = ServiceDeployment::new(config.clone())?;
        self.store.queue_deployment(deployment).await?;
        self.logger.emit(
            "info",
            &format!(
                "created preview `{}` for PR #{}",
                config.id, candidate.pull_request.number
            ),
        );
        self.post_current_comment(&candidate.base, &config).await;
        Ok(ServiceInfo {
            config,
            deploy_frozen: false,
            replicas_override: None,
        })
    }

    async fn reconcile_open_preview(
        &self,
        base: &BaseService,
        existing: &ServiceInfo,
        pull_request: &PullRequest,
    ) -> Result<ServiceInfo> {
        if existing.replicas_override.is_some() {
            self.store
                .set_replicas_override(&existing.config.id, None)
                .await?;
        }
        if existing.deploy_frozen != base.info.deploy_frozen {
            self.store
                .set_deploy_frozen(&existing.config.id, base.info.deploy_frozen)
                .await?;
        }
        if base.info.deploy_frozen {
            let mut config = existing.config.clone();
            if let Some(source) = &mut config.preview_source {
                source.head_ref = pull_request.head_ref.clone();
                source.head_sha = pull_request.head_sha.clone();
                source.title = pull_request.title.clone();
                source.created_at = pull_request.created_at;
                source.closed_at = None;
            }
            if config != existing.config {
                self.store
                    .update_service_config(&config.id, config.clone())
                    .await?;
            }
            return Ok(ServiceInfo {
                config,
                deploy_frozen: true,
                replicas_override: None,
            });
        }

        let base_config = self.hydrate_deploy_secrets(&base.info).await?;
        let desired = derive_preview_config(
            &base_config,
            &preview_pull_request(pull_request),
            &self.config.preview_domain,
        )?;
        let head_changed = existing
            .config
            .preview_source
            .as_ref()
            .is_some_and(|source| source.head_sha != pull_request.head_sha);
        if existing.config.version != desired.version || head_changed {
            self.store
                .queue_deployment(ServiceDeployment::new(desired.clone())?)
                .await?;
            self.logger.emit(
                "info",
                &format!(
                    "queued updated preview `{}` for PR #{}",
                    desired.id, pull_request.number
                ),
            );
        } else if existing.config != desired {
            self.store
                .update_service_config(&desired.id, desired.clone())
                .await?;
        }
        self.post_current_comment(base, &desired).await;
        Ok(ServiceInfo {
            config: desired,
            deploy_frozen: false,
            replicas_override: None,
        })
    }

    async fn reconcile_closed_preview(
        &self,
        base: &BaseService,
        existing: &ServiceInfo,
        now_ms: u64,
    ) -> Result<bool> {
        let source = existing.config.preview_source.as_ref().unwrap();
        if existing.replicas_override.is_some() {
            self.store
                .set_replicas_override(&existing.config.id, None)
                .await?;
        }
        if !existing.deploy_frozen {
            self.store
                .set_deploy_frozen(&existing.config.id, true)
                .await?;
        }
        let close_grace_period = base
            .info
            .config
            .preview
            .as_ref()
            .and_then(|preview| {
                crate::validation::parse_duration(
                    &preview.close_grace_period,
                    "preview.closeGracePeriod",
                )
                .ok()
            })
            .unwrap_or(DEFAULT_CLOSE_GRACE_PERIOD);
        let closed_at = source.closed_at.unwrap_or(now_ms);
        let remove_at = closed_at.saturating_add(close_grace_period.as_millis() as u64);
        if source.closed_at.is_some() && now_ms >= remove_at {
            return Ok(self
                .remove_preview(existing, "The pull request is closed.")
                .await);
        }

        let mut config = existing.config.clone();
        config.preview_source.as_mut().unwrap().closed_at = Some(closed_at);
        if source.closed_at.is_none() {
            self.store
                .update_service_config(&config.id, config.clone())
                .await?;
        }
        self.post_comment(base, source.pr_number, &closing_comment(&config, remove_at))
            .await;
        Ok(false)
    }

    async fn remove_preview(&self, info: &ServiceInfo, reason: &str) -> bool {
        let source = info.config.preview_source.as_ref().unwrap();
        let repo = info
            .config
            .build
            .as_ref()
            .and_then(|build| build.repo.as_deref())
            .and_then(|repo| crate::validation::parse_github_repo(repo).ok());
        if let Err(error) = self.store.delete_service(&info.config.id).await {
            self.logger.emit(
                "error",
                &format!("failed to remove preview `{}`: {error}", info.config.id),
            );
            return false;
        }
        self.logger
            .emit("info", &format!("removed preview `{}`", info.config.id));
        if let Some((owner, repo)) = repo {
            let body = removed_comment(&info.config, reason);
            if let Err(error) = self
                .api
                .upsert_comment(
                    &owner,
                    &repo,
                    source.pr_number,
                    &source.base_service_id,
                    &body,
                )
                .await
            {
                self.logger.emit(
                    "warn",
                    &format!(
                        "failed to update preview comment for `{owner}/{repo}` PR #{}: {error}",
                        source.pr_number
                    ),
                );
            }
        }
        true
    }

    async fn hydrate_deploy_secrets(&self, info: &ServiceInfo) -> Result<ServiceConfig> {
        let mut config = info.config.clone();
        if config
            .deploy
            .secrets
            .as_ref()
            .is_some_and(|secrets| secrets.source.is_none())
            && let Some(latest) = self
                .store
                .list_service_deployments(&config.id)
                .await?
                .first()
        {
            let items = self
                .store
                .read_deployment_secrets(&config.id, &latest.id)
                .await?;
            if let Some(secrets) = &mut config.deploy.secrets {
                secrets.items = items;
            }
        }
        Ok(config)
    }

    async fn post_current_comment(&self, base: &BaseService, config: &ServiceConfig) {
        let body = match self.store.list_service_deployments(&config.id).await {
            Ok(deployments) => {
                current_comment(config, deployments.first(), self.config.homepage.as_deref())
            }
            Err(error) => {
                self.logger.emit(
                    "warn",
                    &format!(
                        "failed to load preview deployments for `{}`: {error}",
                        config.id
                    ),
                );
                current_comment(config, None, self.config.homepage.as_deref())
            }
        };
        let pr_number = config.preview_source.as_ref().unwrap().pr_number;
        self.post_comment(base, pr_number, &body).await;
    }

    async fn post_comment(&self, base: &BaseService, pr_number: u64, body: &str) {
        if let Err(error) = self
            .api
            .upsert_comment(
                &base.owner,
                &base.repo,
                pr_number,
                &base.info.config.id,
                body,
            )
            .await
        {
            self.logger.emit(
                "warn",
                &format!(
                    "failed to update preview comment for `{}` PR #{pr_number}: {error}",
                    base.repo_key
                ),
            );
        }
    }

    fn record_backoff(&mut self, repo_key: &str, error: &anyhow::Error) {
        let delay = error
            .downcast_ref::<RateLimitError>()
            .map(|error| error.retry_after)
            .unwrap_or_else(|| {
                self.backoff
                    .get(repo_key)
                    .map(|(_, delay)| *delay)
                    .unwrap_or(INITIAL_BACKOFF)
            });
        let next = (delay * 2).min(MAX_BACKOFF);
        self.backoff
            .insert(repo_key.to_string(), (Instant::now() + delay, next));
    }
}

fn preview_pull_request(pull_request: &PullRequest) -> PreviewPullRequest {
    PreviewPullRequest {
        number: pull_request.number,
        title: pull_request.title.clone(),
        head_ref: pull_request.head_ref.clone(),
        head_sha: pull_request.head_sha.clone(),
        created_at: pull_request.created_at,
    }
}

fn current_comment(
    config: &ServiceConfig,
    deployment: Option<&ServiceDeployment>,
    homepage: Option<&str>,
) -> String {
    let source = config.preview_source.as_ref().unwrap();
    let preview_url = config
        .ingress
        .as_ref()
        .and_then(|ingress| ingress.host.as_deref())
        .map(|host| format!("https://{host}"))
        .unwrap_or_default();
    let status = deployment
        .map(|deployment| &deployment.status)
        .unwrap_or(&DeploymentStatus::Queued);
    let sha = deployment
        .and_then(|deployment| deployment.git_commit.as_ref())
        .map(|commit| commit.reference.as_str())
        .unwrap_or(&source.head_sha);
    let short_sha = &sha[..sha.len().min(7)];
    let updated_at = deployment
        .map(|deployment| deployment.deployed_at.unwrap_or(deployment.created_at))
        .unwrap_or_default();
    let updated = format_time(updated_at);
    match status {
        DeploymentStatus::Ready => format!(
            "### Maestro preview: ready\n\n[Open preview]({preview_url}) · commit `{short_sha}` · updated {updated}"
        ),
        DeploymentStatus::Crashed | DeploymentStatus::Canceled => {
            let log_link = deployment.and_then(|deployment| {
                homepage.map(|homepage| {
                    format!(
                        "{homepage}/services/{}/deployments?deployment={}&tab=build",
                        config.id, deployment.id
                    )
                })
            });
            if let Some(log_link) = log_link {
                format!(
                    "### Maestro preview: failed\n\nBuild or deployment failed for commit `{short_sha}`. [View logs]({log_link})."
                )
            } else {
                format!(
                    "### Maestro preview: failed\n\nBuild or deployment failed for commit `{short_sha}`."
                )
            }
        }
        DeploymentStatus::PendingReady => format!(
            "### Maestro preview: deploying\n\nThe preview for commit `{short_sha}` is waiting to become ready."
        ),
        DeploymentStatus::Draining | DeploymentStatus::Removed | DeploymentStatus::Terminated => {
            removed_comment(config, "The preview deployment was removed.")
        }
        DeploymentStatus::Queued | DeploymentStatus::Building => format!(
            "### Maestro preview: building\n\nBuilding commit `{short_sha}`. The preview will be available at {preview_url}."
        ),
    }
}

fn closing_comment(config: &ServiceConfig, remove_at: u64) -> String {
    let source = config.preview_source.as_ref().unwrap();
    format!(
        "### Maestro preview: removal scheduled\n\nPR #{} is closed. The preview will remain available until {} and will be restored automatically if the PR is reopened before then.",
        source.pr_number,
        format_time(remove_at)
    )
}

fn removed_comment(config: &ServiceConfig, reason: &str) -> String {
    let source = config.preview_source.as_ref().unwrap();
    format!(
        "### Maestro preview: removed\n\nThe preview for PR #{} was removed. {reason}",
        source.pr_number
    )
}

fn quota_comment(max_concurrent_previews: usize) -> String {
    format!(
        "### Maestro preview: skipped (quota)\n\nThe cluster is already running its limit of {max_concurrent_previews} concurrent previews. This PR will be reconsidered automatically when a slot becomes available."
    )
}

fn format_time(timestamp_ms: u64) -> String {
    chrono::DateTime::from_timestamp_millis(timestamp_ms as i64)
        .map(|timestamp| timestamp.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
        .unwrap_or_else(|| "an unknown time".to_string())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use async_trait::async_trait;

    use super::*;
    use crate::deployment::types::{
        Deployment, EnvConfig, ForceQueueOutcome, IngressConfig, PreviewConfig, PreviewEnvConfig,
        QueuedDeployment, ServiceBuildConfig, ServiceDeployConfig,
    };

    #[derive(Default)]
    struct TestStore {
        state: Mutex<TestStoreState>,
    }

    #[derive(Default)]
    struct TestStoreState {
        infos: HashMap<String, ServiceInfo>,
        deployments: HashMap<String, Vec<ServiceDeployment>>,
    }

    #[async_trait]
    impl ClusterStore for TestStore {
        async fn list_service_ids(&self) -> Result<Vec<String>> {
            Ok(self.state.lock().unwrap().infos.keys().cloned().collect())
        }

        async fn list_queued_deployments(&self) -> Result<Vec<QueuedDeployment>> {
            Ok(Vec::new())
        }

        async fn claim_deployment_building(
            &self,
            _queued_deployment: &QueuedDeployment,
        ) -> Result<bool> {
            Ok(false)
        }

        async fn update_deployment_status(
            &self,
            _deployment: &Deployment,
            _status: DeploymentStatus,
        ) -> Result<()> {
            Ok(())
        }

        async fn read_service_info(&self, service_id: &str) -> Result<Option<ServiceInfo>> {
            Ok(self.state.lock().unwrap().infos.get(service_id).cloned())
        }

        async fn list_service_deployments(
            &self,
            service_id: &str,
        ) -> Result<Vec<ServiceDeployment>> {
            Ok(self
                .state
                .lock()
                .unwrap()
                .deployments
                .get(service_id)
                .cloned()
                .unwrap_or_default())
        }

        async fn read_deployment_secrets(
            &self,
            _service_id: &str,
            _deployment_id: &str,
        ) -> Result<HashMap<String, String>> {
            Ok(HashMap::new())
        }

        async fn queue_deployment(
            &self,
            deployment: ServiceDeployment,
        ) -> Result<ForceQueueOutcome> {
            let mut state = self.state.lock().unwrap();
            let service_id = deployment.config.id.clone();
            let deploy_frozen = state
                .infos
                .get(&service_id)
                .is_some_and(|info| info.deploy_frozen);
            state.infos.insert(
                service_id.clone(),
                ServiceInfo {
                    config: deployment.config.clone(),
                    deploy_frozen,
                    replicas_override: None,
                },
            );
            let deployments = state.deployments.entry(service_id).or_default();
            deployments.insert(0, deployment.clone());
            Ok(ForceQueueOutcome {
                deployment_index: deployments.len() - 1,
                deployment,
            })
        }

        async fn update_service_config(
            &self,
            service_id: &str,
            config: ServiceConfig,
        ) -> Result<()> {
            let mut state = self.state.lock().unwrap();
            state.infos.get_mut(service_id).unwrap().config = config.clone();
            if let Some(deployment) = state
                .deployments
                .get_mut(service_id)
                .and_then(|deployments| deployments.first_mut())
            {
                deployment.config = config;
            }
            Ok(())
        }

        async fn set_deploy_frozen(&self, service_id: &str, frozen: bool) -> Result<()> {
            self.state
                .lock()
                .unwrap()
                .infos
                .get_mut(service_id)
                .unwrap()
                .deploy_frozen = frozen;
            Ok(())
        }

        async fn set_replicas_override(
            &self,
            service_id: &str,
            override_value: Option<u32>,
        ) -> Result<()> {
            self.state
                .lock()
                .unwrap()
                .infos
                .get_mut(service_id)
                .unwrap()
                .replicas_override = override_value;
            Ok(())
        }

        async fn delete_service(&self, service_id: &str) -> Result<()> {
            let mut state = self.state.lock().unwrap();
            state.infos.remove(service_id);
            state.deployments.remove(service_id);
            Ok(())
        }
    }

    #[derive(Default)]
    struct TestApi {
        pull_requests: Mutex<Vec<PullRequest>>,
        comments: Mutex<HashMap<u64, String>>,
    }

    #[async_trait]
    impl PullRequestApi for TestApi {
        async fn list_open(&self, _owner: &str, _repo: &str) -> Result<Vec<PullRequest>> {
            Ok(self.pull_requests.lock().unwrap().clone())
        }

        async fn upsert_comment(
            &self,
            _owner: &str,
            _repo: &str,
            pr_number: u64,
            _comment_key: &str,
            body: &str,
        ) -> Result<()> {
            self.comments
                .lock()
                .unwrap()
                .insert(pr_number, body.to_string());
            Ok(())
        }
    }

    fn base_service(close_grace_period: &str) -> ServiceInfo {
        ServiceInfo {
            config: ServiceConfig {
                id: "app".to_string(),
                name: "App".to_string(),
                version: "base-v1".to_string(),
                build: Some(ServiceBuildConfig {
                    repo: Some("git@github.com:Baton-AI/baton.git".to_string()),
                    branch: Some("main".to_string()),
                    dockerfile: "Dockerfile".to_string(),
                    watch: true,
                    registry: None,
                    depot: None,
                    env: EnvConfig::default(),
                    secrets: EnvConfig::default(),
                }),
                image: None,
                deploy: ServiceDeployConfig {
                    flags: Vec::new(),
                    expose_ports: Vec::new(),
                    command: None,
                    healthcheck_path: None,
                    healthcheck_interval: 60,
                    replicas: 2,
                    exec: true,
                    max_restarts: None,
                    env: EnvConfig::default(),
                    secrets: None,
                    volumes: Vec::new(),
                    node_affinity: None,
                    egress: Default::default(),
                },
                ingress: Some(IngressConfig {
                    host: Some("app.example.com".to_string()),
                    hosts: Vec::new(),
                    port: Some(3000),
                    session_affinity: None,
                }),
                preview: Some(PreviewConfig {
                    enabled: true,
                    close_grace_period: close_grace_period.to_string(),
                    replicas: 1,
                    env: PreviewEnvConfig::default(),
                }),
                preview_source: None,
            },
            deploy_frozen: false,
            replicas_override: None,
        }
    }

    fn pull_request(number: u64, created_at: u64) -> PullRequest {
        PullRequest {
            number,
            title: format!("PR {number}"),
            draft: false,
            created_at,
            head_ref: format!("feature-{number}"),
            head_sha: format!("sha-{number}-1"),
            head_repo_full_name: Some("Baton-AI/baton".to_string()),
        }
    }

    fn watcher(
        store: Arc<TestStore>,
        api: Arc<TestApi>,
        max_concurrent_previews: usize,
    ) -> PrWatcher {
        let (_signal_tx, signal_rx) = broadcast::channel(1);
        PrWatcher::new(
            store,
            api,
            PrWatcherConfig {
                preview_domain: "preview.example.com".to_string(),
                poll_interval: Duration::from_secs(60),
                max_concurrent_previews,
                homepage: Some("http://maestro.example.com".to_string()),
            },
            signal_rx,
            Logger::noop(),
        )
    }

    #[test]
    fn feedback_bodies_cover_preview_lifecycle_states() {
        let config = derive_preview_config(
            &base_service("1d").config,
            &preview_pull_request(&pull_request(7, 1)),
            "preview.example.com",
        )
        .unwrap();
        let mut deployment = ServiceDeployment::new(config.clone()).unwrap();
        assert!(current_comment(&config, Some(&deployment), None).contains("building"));

        deployment.status = DeploymentStatus::Ready;
        deployment.deployed_at = Some(2_000);
        let ready = current_comment(&config, Some(&deployment), None);
        assert!(ready.contains("ready"));
        assert!(ready.contains("https://app-pr-7.preview.example.com"));

        deployment.status = DeploymentStatus::Crashed;
        let failed = current_comment(
            &config,
            Some(&deployment),
            Some("http://maestro.example.com"),
        );
        assert!(failed.contains("failed"));
        assert!(failed.contains("http://maestro.example.com/services/app-pr-7/deployments"));

        deployment.status = DeploymentStatus::Removed;
        assert!(current_comment(&config, Some(&deployment), None).contains("removed"));
        assert!(closing_comment(&config, 86_400_000).contains("removal scheduled"));
        assert!(quota_comment(10).contains("limit of 10"));
        assert_eq!(format_time(0), "1970-01-01T00:00:00Z");
    }

    #[test]
    fn candidates_sort_oldest_first() {
        let mut values = [(200_u64, "b"), (100_u64, "z"), (100_u64, "a")];
        values.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(right.1)));
        assert_eq!(values, [(100, "a"), (100, "z"), (200, "b")]);
    }

    #[tokio::test]
    async fn reconciles_create_push_close_delete_and_reopen() {
        let store = Arc::new(TestStore::default());
        store
            .state
            .lock()
            .unwrap()
            .infos
            .insert("app".to_string(), base_service("1s"));
        let api = Arc::new(TestApi::default());
        api.pull_requests.lock().unwrap().push(pull_request(7, 1));
        let mut watcher = watcher(store.clone(), api.clone(), 10);

        watcher.poll(1_000).await.unwrap();
        assert!(store.state.lock().unwrap().infos.contains_key("app-pr-7"));
        assert_eq!(store.state.lock().unwrap().deployments["app-pr-7"].len(), 1);
        assert!(api.comments.lock().unwrap()[&7].contains("building"));

        watcher.poll(1_250).await.unwrap();
        assert_eq!(store.state.lock().unwrap().deployments["app-pr-7"].len(), 1);

        api.pull_requests.lock().unwrap()[0].head_sha = "sha-7-2".to_string();
        watcher.poll(1_500).await.unwrap();
        assert_eq!(store.state.lock().unwrap().deployments["app-pr-7"].len(), 2);

        store
            .state
            .lock()
            .unwrap()
            .infos
            .get_mut("app")
            .unwrap()
            .config
            .build
            .as_mut()
            .unwrap()
            .dockerfile = "Dockerfile.preview".to_string();
        watcher.poll(1_750).await.unwrap();
        {
            let state = store.state.lock().unwrap();
            assert_eq!(state.deployments["app-pr-7"].len(), 3);
            assert_eq!(
                state.infos["app-pr-7"]
                    .config
                    .build
                    .as_ref()
                    .unwrap()
                    .dockerfile,
                "Dockerfile.preview"
            );
        }

        api.pull_requests.lock().unwrap().clear();
        watcher.poll(2_000).await.unwrap();
        let closed = store.state.lock().unwrap().infos["app-pr-7"].clone();
        assert!(closed.deploy_frozen);
        assert_eq!(closed.config.preview_source.unwrap().closed_at, Some(2_000));

        let mut reopened = pull_request(7, 1);
        reopened.head_sha = "sha-7-2".to_string();
        api.pull_requests.lock().unwrap().push(reopened);
        watcher.poll(2_500).await.unwrap();
        let reopened = store.state.lock().unwrap().infos["app-pr-7"].clone();
        assert!(!reopened.deploy_frozen);
        assert_eq!(reopened.config.preview_source.unwrap().closed_at, None);
        assert_eq!(store.state.lock().unwrap().deployments["app-pr-7"].len(), 3);

        api.pull_requests.lock().unwrap().clear();
        watcher.poll(3_000).await.unwrap();
        watcher.poll(4_001).await.unwrap();
        assert!(!store.state.lock().unwrap().infos.contains_key("app-pr-7"));
        assert!(api.comments.lock().unwrap()[&7].contains("removed"));

        api.pull_requests.lock().unwrap().push(pull_request(7, 1));
        watcher.poll(5_000).await.unwrap();
        assert!(store.state.lock().unwrap().infos.contains_key("app-pr-7"));
    }

    #[tokio::test]
    async fn ignores_forks_and_drafts_and_applies_global_quota_oldest_first() {
        let store = Arc::new(TestStore::default());
        store
            .state
            .lock()
            .unwrap()
            .infos
            .insert("app".to_string(), base_service("1d"));
        let api = Arc::new(TestApi::default());
        let mut draft = pull_request(1, 1);
        draft.draft = true;
        let mut fork = pull_request(2, 2);
        fork.head_repo_full_name = Some("someone/fork".to_string());
        api.pull_requests.lock().unwrap().extend([
            draft,
            fork,
            pull_request(4, 40),
            pull_request(3, 30),
        ]);
        let mut watcher = watcher(store.clone(), api.clone(), 1);

        watcher.poll(1_000).await.unwrap();
        let state = store.state.lock().unwrap();
        assert!(state.infos.contains_key("app-pr-3"));
        assert!(!state.infos.contains_key("app-pr-1"));
        assert!(!state.infos.contains_key("app-pr-2"));
        assert!(!state.infos.contains_key("app-pr-4"));
        drop(state);
        assert!(api.comments.lock().unwrap()[&4].contains("quota"));
    }

    #[tokio::test]
    async fn base_freeze_blocks_creation_and_follow_up_deployments() {
        let store = Arc::new(TestStore::default());
        let mut base = base_service("1d");
        base.deploy_frozen = true;
        store
            .state
            .lock()
            .unwrap()
            .infos
            .insert("app".to_string(), base);
        let api = Arc::new(TestApi::default());
        api.pull_requests.lock().unwrap().push(pull_request(8, 1));
        let mut watcher = watcher(store.clone(), api.clone(), 10);

        watcher.poll(1_000).await.unwrap();
        assert!(!store.state.lock().unwrap().infos.contains_key("app-pr-8"));

        store
            .state
            .lock()
            .unwrap()
            .infos
            .get_mut("app")
            .unwrap()
            .deploy_frozen = false;
        watcher.poll(2_000).await.unwrap();
        assert_eq!(store.state.lock().unwrap().deployments["app-pr-8"].len(), 1);

        store
            .state
            .lock()
            .unwrap()
            .infos
            .get_mut("app-pr-8")
            .unwrap()
            .replicas_override = Some(4);
        watcher.poll(2_500).await.unwrap();
        assert_eq!(
            store.state.lock().unwrap().infos["app-pr-8"].replicas_override,
            None
        );

        store
            .state
            .lock()
            .unwrap()
            .infos
            .get_mut("app")
            .unwrap()
            .deploy_frozen = true;
        api.pull_requests.lock().unwrap()[0].head_sha = "sha-8-2".to_string();
        watcher.poll(3_000).await.unwrap();
        let state = store.state.lock().unwrap();
        assert!(state.infos["app-pr-8"].deploy_frozen);
        assert_eq!(state.deployments["app-pr-8"].len(), 1);
    }
}
