use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};

use maestro_control::{StoreWriter, new_deployment, service_deletion};
use maestro_core::{
    Deployment, DeploymentStatus, PreviewSource, ServiceControls, ServiceId, ServiceSpec,
    parse_duration_secs,
};
use maestro_store::{Mutation, SecretKey};

use crate::github::{GitHub, GitHubError, PullRequest};

const PREVIEW_INFIX: &str = "-pr-";
const MAX_LABEL_LENGTH: usize = 63;
const INITIAL_BACKOFF: Duration = Duration::from_secs(30);
const MAX_BACKOFF: Duration = Duration::from_secs(300);
const SHORT_SHA: usize = 7;

pub struct PreviewSettings {
    pub domain: String,
    pub max_concurrent: Option<usize>,
    pub homepage: Option<String>,
}

pub struct PreviewWatcher<W: StoreWriter> {
    writer: Arc<W>,
    key: Arc<SecretKey>,
    github: GitHub,
    settings: PreviewSettings,
    backoff: HashMap<String, (Instant, Duration)>,
}

struct Base {
    id: ServiceId,
    spec: ServiceSpec,
    repo: String,
    frozen: bool,
}

struct Candidate<'a> {
    base: &'a Base,
    pull: PullRequest,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Report {
    Building,
    Deploying,
    Ready,
    Failed,
}

impl<W: StoreWriter> PreviewWatcher<W> {
    pub fn new(
        writer: Arc<W>,
        key: Arc<SecretKey>,
        github: GitHub,
        settings: PreviewSettings,
    ) -> Self {
        Self {
            writer,
            key,
            github,
            settings,
            backoff: HashMap::new(),
        }
    }

    pub async fn poll(&mut self) -> Result<(), String> {
        match self
            .writer
            .state()
            .cluster_freeze()
            .map_err(|error| error.to_string())?
        {
            Some(freeze) => {
                tracing::debug!(reason = %freeze.reason, "previews paused by the cluster freeze");
                Ok(())
            }
            None => self.reconcile().await,
        }
    }

    async fn reconcile(&mut self) -> Result<(), String> {
        let store = self.writer.state();
        let services = store.services().map_err(|error| error.to_string())?;
        let sources = store
            .preview_sources()
            .map_err(|error| error.to_string())?
            .into_iter()
            .collect::<BTreeMap<_, _>>();
        let bases = services
            .iter()
            .filter(|(id, spec)| {
                !sources.contains_key(id)
                    && spec.preview.as_ref().is_some_and(|preview| preview.enabled)
            })
            .filter_map(|(id, spec)| {
                let repo = spec.build.as_ref()?.github_repo()?;
                let frozen = store.service_controls(id).ok()?.frozen;
                Some(Base {
                    id: id.clone(),
                    spec: spec.clone(),
                    repo,
                    frozen,
                })
            })
            .collect::<Vec<_>>();
        for (id, source) in &sources {
            if bases.iter().all(|base| base.id != source.base_service_id) {
                self.remove(
                    id,
                    source,
                    None,
                    "The base service no longer has previews enabled.",
                )
                .await?;
            }
        }
        let pulls = self.fetch_pulls(&bases).await;
        let mut candidates = Vec::new();
        for base in &bases {
            if let Some(open) = pulls.get(&base.repo) {
                let existing = sources
                    .iter()
                    .filter(|(_, source)| source.base_service_id == base.id)
                    .collect::<Vec<_>>();
                for (id, source) in &existing {
                    match open.iter().find(|pull| pull.number == source.pr_number) {
                        Some(pull) if pull.draft => {
                            self.remove(
                                id,
                                source,
                                Some(base),
                                "Draft pull requests do not receive previews.",
                            )
                            .await?;
                        }
                        Some(pull) => self.reconcile_open(id, source, base, pull).await?,
                        None => self.reconcile_closed(id, source, base).await?,
                    }
                }
                candidates.extend(
                    open.iter()
                        .filter(|pull| eligible(pull, &base.repo))
                        .filter(|pull| {
                            existing
                                .iter()
                                .all(|(_, source)| source.pr_number != pull.number)
                        })
                        .filter(|_| !base.frozen)
                        .map(|pull| Candidate {
                            base,
                            pull: pull.clone(),
                        }),
                );
            }
        }
        candidates.sort_by(|left, right| {
            (&left.pull.created_at, &left.base.id, left.pull.number).cmp(&(
                &right.pull.created_at,
                &right.base.id,
                right.pull.number,
            ))
        });
        let running = self
            .writer
            .state()
            .preview_sources()
            .map_err(|error| error.to_string())?
            .len();
        let slots = self
            .settings
            .max_concurrent
            .map_or(usize::MAX, |limit| limit.saturating_sub(running));
        for (index, candidate) in candidates.iter().enumerate() {
            match index < slots {
                true => self.create(candidate).await?,
                false => {
                    let body = format!(
                        "### Maestro preview: skipped (quota)\n\nThe cluster is already running its limit of {} concurrent previews. This PR will be reconsidered automatically when a slot becomes available.",
                        self.settings.max_concurrent.unwrap_or_default()
                    );
                    self.comment(
                        &candidate.base.repo,
                        candidate.pull.number,
                        &candidate.base.id,
                        &body,
                        None,
                    )
                    .await;
                }
            }
        }
        self.report_statuses(&bases).await
    }

    async fn fetch_pulls(&mut self, bases: &[Base]) -> HashMap<String, Vec<PullRequest>> {
        let mut repos = bases
            .iter()
            .map(|base| base.repo.to_lowercase())
            .collect::<Vec<_>>();
        repos.sort();
        repos.dedup();
        let mut pulls = HashMap::new();
        for repo in repos {
            let waiting = self
                .backoff
                .get(&repo)
                .is_some_and(|(retry_at, _)| *retry_at > Instant::now());
            if !waiting {
                match self.github.open_pulls(&repo).await {
                    Ok(open) => {
                        self.backoff.remove(&repo);
                        pulls.insert(repo, open);
                    }
                    Err(error) => {
                        let next = self
                            .backoff
                            .get(&repo)
                            .map_or(INITIAL_BACKOFF, |(_, delay)| (*delay * 2).min(MAX_BACKOFF));
                        let delay = match &error {
                            GitHubError::RateLimited(retry_after) => *retry_after,
                            GitHubError::Failed(_) => next,
                        };
                        tracing::warn!(%repo, %error, retry_in = ?delay, "listing pull requests failed");
                        self.backoff.insert(repo, (Instant::now() + delay, next));
                    }
                }
            }
        }
        bases
            .iter()
            .filter_map(|base| {
                Some((
                    base.repo.clone(),
                    pulls.get(&base.repo.to_lowercase())?.clone(),
                ))
            })
            .collect()
    }

    async fn create(&mut self, candidate: &Candidate<'_>) -> Result<(), String> {
        let id = preview_id(&candidate.base.id, candidate.pull.number);
        match id {
            None => {
                tracing::warn!(base = %candidate.base.id, pr = candidate.pull.number, "preview id would exceed 63 characters");
                Ok(())
            }
            Some(id) => {
                let spec = self.derive(&candidate.base.spec, &id, &candidate.pull.head.branch)?;
                let github_deployment_id = self
                    .github
                    .create_deployment(
                        &candidate.base.repo,
                        &candidate.pull.head.sha,
                        &environment(&id),
                    )
                    .await
                    .map_err(
                        |error| tracing::warn!(%error, "creating the GitHub deployment failed"),
                    )
                    .ok();
                let source = PreviewSource {
                    base_service_id: candidate.base.id.clone(),
                    pr_number: candidate.pull.number,
                    head_ref: candidate.pull.head.branch.clone(),
                    head_sha: candidate.pull.head.sha.clone(),
                    title: candidate.pull.title.clone(),
                    created_at_ms: now_ms(),
                    closed_at_ms: None,
                    comment_id: None,
                    github_deployment_id,
                    reported: None,
                };
                tracing::info!(preview = %id, pr = candidate.pull.number, "creating preview");
                self.write(vec![
                    Mutation::PutService {
                        id: id.clone(),
                        spec: spec.clone(),
                    },
                    Mutation::SetPreviewSource {
                        id: id.clone(),
                        source: Some(source),
                    },
                    Mutation::CreateDeployment {
                        deployment: new_deployment(&id, spec),
                    },
                ])
                .await
            }
        }
    }

    async fn reconcile_open(
        &mut self,
        id: &ServiceId,
        source: &PreviewSource,
        base: &Base,
        pull: &PullRequest,
    ) -> Result<(), String> {
        let controls = self
            .writer
            .state()
            .service_controls(id)
            .map_err(|error| error.to_string())?;
        let moved = pull.head.sha != source.head_sha;
        let wrong_branch = self
            .writer
            .state()
            .service(id)
            .map_err(|error| error.to_string())?
            .and_then(|spec| spec.build)
            .is_some_and(|build| build.branch.as_deref() != Some(pull.head.branch.as_str()));
        let rebuild = (moved || wrong_branch) && !base.frozen;
        let mut updated = PreviewSource {
            head_ref: pull.head.branch.clone(),
            head_sha: pull.head.sha.clone(),
            title: pull.title.clone(),
            closed_at_ms: None,
            ..source.clone()
        };
        let mut mutations = Vec::new();
        if controls.frozen != base.frozen || controls.replicas_override.is_some() {
            mutations.push(Mutation::SetServiceControls {
                id: id.clone(),
                controls: ServiceControls {
                    frozen: base.frozen,
                    replicas_override: None,
                },
            });
        }
        if rebuild {
            let spec = self.derive(&base.spec, id, &pull.head.branch)?;
            updated.github_deployment_id = self
                .github
                .create_deployment(&base.repo, &pull.head.sha, &environment(id))
                .await
                .map_err(|error| tracing::warn!(%error, "creating the GitHub deployment failed"))
                .ok();
            updated.reported = None;
            tracing::info!(preview = %id, sha = %pull.head.sha, "rebuilding preview for a new commit");
            mutations.extend([
                Mutation::PutService {
                    id: id.clone(),
                    spec: spec.clone(),
                },
                Mutation::CreateDeployment {
                    deployment: new_deployment(id, spec),
                },
            ]);
        }
        if updated != *source {
            mutations.push(Mutation::SetPreviewSource {
                id: id.clone(),
                source: Some(updated),
            });
        }
        self.write(mutations).await
    }

    async fn reconcile_closed(
        &mut self,
        id: &ServiceId,
        source: &PreviewSource,
        base: &Base,
    ) -> Result<(), String> {
        let grace_ms = base
            .spec
            .preview
            .as_ref()
            .and_then(|preview| parse_duration_secs(&preview.close_grace_period))
            .unwrap_or(86_400)
            * 1000;
        match source.closed_at_ms {
            None => {
                let closed_at = now_ms();
                let remove_at = closed_at + grace_ms;
                let body = format!(
                    "### Maestro preview: removal scheduled\n\nPR #{} is closed. The preview will remain available until {} and will be restored automatically if the PR is reopened before then.",
                    source.pr_number,
                    rfc3339(remove_at)
                );
                let comment_id = self
                    .comment(
                        &base.repo,
                        source.pr_number,
                        &base.id,
                        &body,
                        source.comment_id,
                    )
                    .await;
                self.write(vec![
                    Mutation::SetServiceControls {
                        id: id.clone(),
                        controls: ServiceControls {
                            frozen: true,
                            replicas_override: None,
                        },
                    },
                    Mutation::SetPreviewSource {
                        id: id.clone(),
                        source: Some(PreviewSource {
                            closed_at_ms: Some(closed_at),
                            comment_id: comment_id.or(source.comment_id),
                            ..source.clone()
                        }),
                    },
                ])
                .await
            }
            Some(closed_at) if now_ms() >= closed_at + grace_ms => {
                self.remove(id, source, Some(base), "The pull request is closed.")
                    .await
            }
            Some(_) => Ok(()),
        }
    }

    async fn remove(
        &mut self,
        id: &ServiceId,
        source: &PreviewSource,
        base: Option<&Base>,
        reason: &str,
    ) -> Result<(), String> {
        tracing::info!(preview = %id, reason, "removing preview");
        if let Some(base) = base {
            let body = format!(
                "### Maestro preview: removed\n\nThe preview for PR #{} was removed. {reason}",
                source.pr_number
            );
            self.comment(
                &base.repo,
                source.pr_number,
                &base.id,
                &body,
                source.comment_id,
            )
            .await;
            if let Some(deployment) = source.github_deployment_id {
                let _ = self
                    .github
                    .deployment_status(&base.repo, deployment, "inactive", None, None)
                    .await;
            }
        }
        let mutations = service_deletion(self.writer.state(), std::slice::from_ref(id))
            .map_err(|error| error.to_string())?;
        self.write(mutations).await
    }

    async fn report_statuses(&mut self, bases: &[Base]) -> Result<(), String> {
        let sources = self
            .writer
            .state()
            .preview_sources()
            .map_err(|error| error.to_string())?;
        for (id, source) in sources {
            let base = bases.iter().find(|base| base.id == source.base_service_id);
            let latest = self
                .writer
                .state()
                .deployments(&id)
                .map_err(|error| error.to_string())?
                .into_iter()
                .max_by_key(|deployment| deployment.created_at_ms);
            let report = latest
                .as_ref()
                .and_then(|deployment| report_for(deployment.status));
            let label = report.map(|report| report_label(report).to_string());
            if let (Some(base), Some(report), Some(deployment), true) =
                (base, report, latest, label != source.reported)
            {
                let host = preview_host(&id, &self.settings.domain);
                let url = format!("https://{host}");
                let log_url = self.settings.homepage.as_ref().map(|homepage| {
                    format!(
                        "{homepage}/services/{id}/deployments?deployment={}&tab=build",
                        deployment.id
                    )
                });
                let comment_id = match source.closed_at_ms {
                    Some(_) => None,
                    None => {
                        let body =
                            comment_body(report, &source, &deployment, &url, log_url.as_deref());
                        self.comment(
                            &base.repo,
                            source.pr_number,
                            &base.id,
                            &body,
                            source.comment_id,
                        )
                        .await
                    }
                };
                if let Some(github_deployment) = source.github_deployment_id {
                    let (state, environment_url) = match report {
                        Report::Building | Report::Deploying => ("in_progress", None),
                        Report::Ready => ("success", Some(url.as_str())),
                        Report::Failed => ("failure", None),
                    };
                    if let Err(error) = self
                        .github
                        .deployment_status(
                            &base.repo,
                            github_deployment,
                            state,
                            environment_url,
                            log_url.as_deref(),
                        )
                        .await
                    {
                        tracing::warn!(%error, "updating the GitHub deployment status failed");
                    }
                }
                self.write(vec![Mutation::SetPreviewSource {
                    id: id.clone(),
                    source: Some(PreviewSource {
                        reported: label,
                        comment_id: comment_id.or(source.comment_id),
                        ..source
                    }),
                }])
                .await?;
            }
        }
        Ok(())
    }

    async fn comment(
        &self,
        repo: &str,
        pr: u64,
        base: &ServiceId,
        body: &str,
        known: Option<u64>,
    ) -> Option<u64> {
        let markers = [
            format!("<!-- maestro-preview:{base} -->"),
            format!("<!-- maestro-preview:preview-{base} -->"),
        ];
        match self
            .github
            .upsert_comment(repo, pr, &markers, body, known)
            .await
        {
            Ok(id) => Some(id),
            Err(error) => {
                tracing::warn!(%repo, pr, %error, "updating the preview comment failed");
                None
            }
        }
    }

    fn derive(
        &self,
        base: &ServiceSpec,
        id: &ServiceId,
        branch: &str,
    ) -> Result<ServiceSpec, String> {
        let opened = self
            .key
            .open_spec(base)
            .map_err(|error| error.to_string())?;
        let preview = opened
            .preview
            .clone()
            .ok_or_else(|| format!("service {id} has no preview configuration"))?;
        let mut spec = opened;
        spec.name = id.to_string();
        spec.preview = None;
        if let Some(build) = spec.build.as_mut() {
            build.watch = false;
            build.branch = Some(branch.to_string());
        }
        spec.deploy.replicas = preview.replicas;
        spec.deploy.volumes.clear();
        spec.deploy.env.items.extend(preview.env.items);
        if let Some(ingress) = spec.ingress.as_mut() {
            ingress.host = Some(preview_host(id, &self.settings.domain));
            ingress.hosts.clear();
        }
        Ok(self.key.seal_spec(&spec))
    }

    async fn write(&self, mutations: Vec<Mutation>) -> Result<(), String> {
        match mutations.is_empty() {
            true => Ok(()),
            false => self
                .writer
                .write(mutations)
                .await
                .map(|_| ())
                .map_err(|error| error.to_string()),
        }
    }
}

fn eligible(pull: &PullRequest, repo: &str) -> bool {
    !pull.draft
        && pull
            .head
            .repo
            .as_ref()
            .is_some_and(|head| head.full_name.eq_ignore_ascii_case(repo))
}

fn preview_id(base: &ServiceId, number: u64) -> Option<ServiceId> {
    let id = format!("{base}{PREVIEW_INFIX}{number}");
    (id.len() <= MAX_LABEL_LENGTH).then(|| ServiceId::new(id))
}

fn preview_host(id: &ServiceId, domain: &str) -> String {
    format!("{id}.{}", domain.trim_end_matches('.'))
}

fn environment(id: &ServiceId) -> String {
    format!("preview/{id}")
}

fn report_for(status: DeploymentStatus) -> Option<Report> {
    match status {
        DeploymentStatus::Queued | DeploymentStatus::Building => Some(Report::Building),
        DeploymentStatus::Deploying => Some(Report::Deploying),
        DeploymentStatus::Ready => Some(Report::Ready),
        DeploymentStatus::Failed | DeploymentStatus::Cancelled => Some(Report::Failed),
        DeploymentStatus::Draining | DeploymentStatus::Stopped => None,
    }
}

fn report_label(report: Report) -> &'static str {
    match report {
        Report::Building => "building",
        Report::Deploying => "deploying",
        Report::Ready => "ready",
        Report::Failed => "failed",
    }
}

fn comment_body(
    report: Report,
    source: &PreviewSource,
    deployment: &Deployment,
    url: &str,
    log_url: Option<&str>,
) -> String {
    let sha = deployment
        .git_commit
        .as_ref()
        .map_or(source.head_sha.as_str(), |commit| commit.reference.as_str());
    let short = &sha[..sha.len().min(SHORT_SHA)];
    match report {
        Report::Ready => format!(
            "### Maestro preview: ready\n\n[Open preview]({url}) · commit `{short}` · updated {}",
            rfc3339(deployment.updated_at_ms)
        ),
        Report::Failed => format!(
            "### Maestro preview: failed\n\nBuild or deployment failed for commit `{short}`.{}",
            log_url.map_or_else(String::new, |log_url| format!(" [View logs]({log_url})."))
        ),
        Report::Deploying => format!(
            "### Maestro preview: deploying\n\nThe preview for commit `{short}` is waiting to become ready."
        ),
        Report::Building => format!(
            "### Maestro preview: building\n\nBuilding commit `{short}`. The preview will be available at {url}."
        ),
    }
}

fn rfc3339(ms: u64) -> String {
    chrono::DateTime::from_timestamp_millis(ms as i64)
        .map(|time| time.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
        .unwrap_or_default()
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis() as u64)
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use axum::Json;
    use axum::extract::{Path, State};
    use axum::routing::{get, patch, post};
    use maestro_core::{BuildSpec, ClusterFreeze, IngressSpec, PreviewSpec};
    use maestro_store::{ConsensusError, StateMachine};
    use serde_json::{Value, json};

    use super::*;

    struct TestWriter(StateMachine);

    impl StoreWriter for TestWriter {
        async fn write(&self, mutations: Vec<Mutation>) -> Result<u64, ConsensusError> {
            Ok(self.0.apply(&mutations)?)
        }

        fn state(&self) -> &StateMachine {
            &self.0
        }
    }

    #[derive(Default)]
    struct FakeGitHub {
        pulls: Vec<Value>,
        comments: Vec<(u64, u64, String)>,
        deployments: u64,
        statuses: Vec<(u64, String)>,
    }

    type Shared = Arc<Mutex<FakeGitHub>>;

    async fn pulls(State(github): State<Shared>) -> Json<Value> {
        Json(Value::Array(github.lock().unwrap().pulls.clone()))
    }

    async fn list_comments(State(github): State<Shared>, Path(pr): Path<u64>) -> Json<Value> {
        let comments = github
            .lock()
            .unwrap()
            .comments
            .iter()
            .filter(|(_, number, _)| *number == pr)
            .map(|(id, _, body)| json!({ "id": id, "body": body }))
            .collect();
        Json(Value::Array(comments))
    }

    async fn create_comment(
        State(github): State<Shared>,
        Path(pr): Path<u64>,
        Json(body): Json<Value>,
    ) -> Json<Value> {
        let mut github = github.lock().unwrap();
        let id = github.comments.len() as u64 + 1;
        github
            .comments
            .push((id, pr, body["body"].as_str().unwrap().to_string()));
        Json(json!({ "id": id }))
    }

    async fn update_comment(
        State(github): State<Shared>,
        Path(id): Path<u64>,
        Json(body): Json<Value>,
    ) -> Json<Value> {
        let mut github = github.lock().unwrap();
        if let Some(comment) = github
            .comments
            .iter_mut()
            .find(|(existing, _, _)| *existing == id)
        {
            comment.2 = body["body"].as_str().unwrap().to_string();
        }
        Json(json!({ "id": id }))
    }

    async fn create_deployment(State(github): State<Shared>) -> Json<Value> {
        let mut github = github.lock().unwrap();
        github.deployments += 1;
        Json(json!({ "id": github.deployments }))
    }

    async fn create_status(
        State(github): State<Shared>,
        Path(id): Path<u64>,
        Json(body): Json<Value>,
    ) -> Json<Value> {
        github
            .lock()
            .unwrap()
            .statuses
            .push((id, body["state"].as_str().unwrap().to_string()));
        Json(json!({}))
    }

    fn pull(number: u64, sha: &str, draft: bool, repo: &str) -> Value {
        json!({
            "number": number,
            "title": format!("PR {number}"),
            "draft": draft,
            "created_at": format!("2026-10-0{number}T00:00:00Z"),
            "head": { "ref": format!("feature-{number}"), "sha": sha, "repo": { "full_name": repo } }
        })
    }

    async fn harness(
        max_concurrent: Option<usize>,
    ) -> (PreviewWatcher<TestWriter>, Shared, tempfile::TempDir) {
        let github = Shared::default();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = axum::Router::new()
            .route("/repos/acme/api/pulls", get(pulls))
            .route(
                "/repos/acme/api/issues/{pr}/comments",
                get(list_comments).post(create_comment),
            )
            .route(
                "/repos/acme/api/issues/comments/{id}",
                patch(update_comment),
            )
            .route("/repos/acme/api/deployments", post(create_deployment))
            .route(
                "/repos/acme/api/deployments/{id}/statuses",
                post(create_status),
            )
            .with_state(github.clone());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let directory = tempfile::tempdir().unwrap();
        let state = StateMachine::open(directory.path().join("state.redb")).unwrap();
        let key = Arc::new(SecretKey::from_bytes(&[9u8; 32]));
        let base = ServiceSpec {
            name: "API".to_string(),
            image: None,
            build: Some(BuildSpec {
                repo: Some("https://github.com/acme/api.git".to_string()),
                branch: Some("main".to_string()),
                dockerfile: "Dockerfile".to_string(),
                watch: true,
                registry: None,
                depot: None,
                env: Default::default(),
                secrets: Default::default(),
            }),
            deploy: Default::default(),
            ingress: Some(IngressSpec {
                host: Some("api.example.com".to_string()),
                hosts: Vec::new(),
                port: 80,
                session_affinity: None,
            }),
            preview: Some(PreviewSpec {
                enabled: true,
                close_grace_period: "1s".to_string(),
                replicas: 1,
                env: Default::default(),
            }),
        };
        state
            .apply(&[Mutation::PutService {
                id: ServiceId::from("api"),
                spec: key.seal_spec(&base),
            }])
            .unwrap();
        let watcher = PreviewWatcher::new(
            Arc::new(TestWriter(state)),
            key,
            GitHub::new(format!("http://{address}"), "token".to_string()),
            PreviewSettings {
                domain: "preview.example.com".to_string(),
                max_concurrent,
                homepage: Some("http://admin.example".to_string()),
            },
        );
        (watcher, github, directory)
    }

    fn previews(watcher: &PreviewWatcher<TestWriter>) -> Vec<(String, PreviewSource)> {
        watcher
            .writer
            .state()
            .preview_sources()
            .unwrap()
            .into_iter()
            .map(|(id, source)| (id.to_string(), source))
            .collect()
    }

    #[tokio::test]
    async fn creates_rebuilds_reports_and_removes_previews() {
        let (mut watcher, github, _directory) = harness(None).await;
        github.lock().unwrap().pulls = vec![
            pull(7, "aaaaaaaaa1", false, "acme/api"),
            pull(8, "bbbbbbbbb1", true, "acme/api"),
            pull(9, "ccccccccc1", false, "someone/fork"),
        ];
        watcher.poll().await.unwrap();
        let created = previews(&watcher);
        assert_eq!(created.len(), 1);
        assert_eq!(created[0].0, "api-pr-7");
        let spec = watcher
            .key
            .open_spec(
                &watcher
                    .writer
                    .state()
                    .service(&ServiceId::from("api-pr-7"))
                    .unwrap()
                    .unwrap(),
            )
            .unwrap();
        assert_eq!(
            spec.ingress.unwrap().host.as_deref(),
            Some("api-pr-7.preview.example.com")
        );
        assert!(spec.preview.is_none());
        assert_eq!(spec.build.unwrap().branch.as_deref(), Some("feature-7"));
        let deployments = watcher
            .writer
            .state()
            .deployments(&ServiceId::from("api-pr-7"))
            .unwrap();
        assert_eq!(deployments.len(), 1);
        assert!(
            github.lock().unwrap().comments[0]
                .2
                .contains("Maestro preview: building")
        );

        let deployment = deployments[0].clone();
        watcher
            .writer
            .state()
            .apply(&[Mutation::SetDeploymentStatus {
                service_id: deployment.service_id.clone(),
                deployment_id: deployment.id.clone(),
                status: DeploymentStatus::Ready,
                image: None,
                failure: None,
                at_ms: 1,
            }])
            .unwrap();
        watcher.poll().await.unwrap();
        {
            let github = github.lock().unwrap();
            assert_eq!(github.comments.len(), 1);
            assert!(github.comments[0].2.contains("Maestro preview: ready"));
            assert!(
                github.comments[0]
                    .2
                    .starts_with("<!-- maestro-preview:api -->")
            );
            assert_eq!(github.statuses.last().unwrap().1, "success");
        }

        github.lock().unwrap().pulls = vec![pull(7, "aaaaaaaaa2", false, "acme/api")];
        watcher.poll().await.unwrap();
        assert_eq!(
            watcher
                .writer
                .state()
                .deployments(&ServiceId::from("api-pr-7"))
                .unwrap()
                .len(),
            2
        );
        assert_eq!(previews(&watcher)[0].1.head_sha, "aaaaaaaaa2");

        github.lock().unwrap().pulls = Vec::new();
        watcher.poll().await.unwrap();
        let closed = previews(&watcher);
        assert!(closed[0].1.closed_at_ms.is_some());
        assert!(
            watcher
                .writer
                .state()
                .service_controls(&ServiceId::from("api-pr-7"))
                .unwrap()
                .frozen
        );
        tokio::time::sleep(Duration::from_millis(1100)).await;
        watcher.poll().await.unwrap();
        assert!(previews(&watcher).is_empty());
        assert!(
            watcher
                .writer
                .state()
                .service(&ServiceId::from("api-pr-7"))
                .unwrap()
                .is_none()
        );
        assert!(
            github.lock().unwrap().comments[0]
                .2
                .contains("Maestro preview: removed")
        );
        assert_eq!(
            github.lock().unwrap().statuses.last().unwrap().1,
            "inactive"
        );
    }

    #[tokio::test]
    async fn rebuilds_previews_built_from_the_wrong_branch_once() {
        let (mut watcher, github, _directory) = harness(None).await;
        github.lock().unwrap().pulls = vec![pull(2, "hhhhhhhhh1", false, "acme/api")];
        watcher.poll().await.unwrap();
        let id = ServiceId::from("api-pr-2");
        let mut spec = watcher.writer.state().service(&id).unwrap().unwrap();
        spec.build.as_mut().unwrap().branch = Some("main".to_string());
        watcher
            .writer
            .state()
            .apply(&[Mutation::PutService {
                id: id.clone(),
                spec,
            }])
            .unwrap();
        watcher.poll().await.unwrap();
        watcher.poll().await.unwrap();
        let state = watcher.writer.state();
        assert_eq!(state.deployments(&id).unwrap().len(), 2);
        assert_eq!(
            state.service(&id).unwrap().unwrap().build.unwrap().branch.as_deref(),
            Some("feature-2")
        );
    }

    #[tokio::test]
    async fn takes_over_the_comment_left_by_maestro_v1() {
        let (mut watcher, github, _directory) = harness(None).await;
        github.lock().unwrap().comments = vec![(
            1,
            4,
            "<!-- maestro-preview:preview-api -->\n### Maestro preview: updating".to_string(),
        )];
        github.lock().unwrap().pulls = vec![pull(4, "fffffffff1", false, "acme/api")];
        watcher.poll().await.unwrap();
        let github = github.lock().unwrap();
        assert_eq!(github.comments.len(), 1);
        assert!(
            github.comments[0]
                .2
                .starts_with("<!-- maestro-preview:api -->\n### Maestro preview: building")
        );
    }

    #[tokio::test]
    async fn reports_a_build_that_finishes_after_the_pr_closes() {
        let (mut watcher, github, _directory) = harness(None).await;
        github.lock().unwrap().pulls = vec![pull(6, "ggggggggg1", false, "acme/api")];
        watcher.poll().await.unwrap();
        github.lock().unwrap().pulls = Vec::new();
        watcher.poll().await.unwrap();
        let deployment = watcher
            .writer
            .state()
            .deployments(&ServiceId::from("api-pr-6"))
            .unwrap()
            .remove(0);
        watcher
            .writer
            .state()
            .apply(&[Mutation::SetDeploymentStatus {
                service_id: deployment.service_id.clone(),
                deployment_id: deployment.id.clone(),
                status: DeploymentStatus::Ready,
                image: None,
                failure: None,
                at_ms: 1,
            }])
            .unwrap();
        watcher.poll().await.unwrap();
        let github = github.lock().unwrap();
        assert_eq!(github.statuses.last().unwrap().1, "success");
        assert!(github.comments[0].2.contains("removal scheduled"));
    }

    #[tokio::test]
    async fn admits_the_oldest_pull_requests_within_the_quota() {
        let (mut watcher, github, _directory) = harness(Some(1)).await;
        github.lock().unwrap().pulls = vec![
            pull(5, "eeeeeeeee1", false, "acme/api"),
            pull(3, "ddddddddd1", false, "acme/api"),
        ];
        watcher.poll().await.unwrap();
        let created = previews(&watcher);
        assert_eq!(created.len(), 1);
        assert_eq!(created[0].0, "api-pr-3");
        let quota = github
            .lock()
            .unwrap()
            .comments
            .iter()
            .find(|(_, pr, _)| *pr == 5)
            .map(|(_, _, body)| body.clone())
            .unwrap();
        assert!(quota.contains("skipped (quota)"));
    }

    #[tokio::test]
    async fn previews_every_pull_request_without_a_limit() {
        let (mut watcher, github, _directory) = harness(None).await;
        github.lock().unwrap().pulls = (1..=12)
            .map(|number| pull(number, &format!("{number:0>9}a"), false, "acme/api"))
            .collect();
        watcher.poll().await.unwrap();
        assert_eq!(previews(&watcher).len(), 12);
    }

    #[tokio::test]
    async fn frozen_bases_get_no_new_previews() {
        let (mut watcher, github, _directory) = harness(None).await;
        watcher
            .writer
            .state()
            .apply(&[Mutation::SetServiceControls {
                id: ServiceId::from("api"),
                controls: ServiceControls {
                    frozen: true,
                    replicas_override: None,
                },
            }])
            .unwrap();
        github.lock().unwrap().pulls = vec![pull(4, "fffffffff1", false, "acme/api")];
        watcher.poll().await.unwrap();
        assert!(previews(&watcher).is_empty());
    }

    #[tokio::test]
    async fn pauses_during_a_cluster_freeze() {
        let (mut watcher, github, _directory) = harness(None).await;
        watcher
            .writer
            .state()
            .apply(&[Mutation::SetClusterFreeze {
                freeze: Some(ClusterFreeze {
                    reason: "rolling upgrade".to_string(),
                    upgrade_run_id: None,
                    at_ms: 1,
                }),
            }])
            .unwrap();
        github.lock().unwrap().pulls = vec![pull(4, "fffffffff1", false, "acme/api")];
        watcher.poll().await.unwrap();
        assert!(previews(&watcher).is_empty());
    }
}
