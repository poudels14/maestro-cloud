use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use reqwest::StatusCode;
use serde::Deserialize;
use serde_json::{Value, json};

const PAGE_SIZE: usize = 100;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(20);
const API_VERSION: &str = "2022-11-28";
const MIN_RETRY_AFTER: Duration = Duration::from_secs(1);

#[derive(Debug)]
pub enum GitHubError {
    RateLimited(Duration),
    Failed(String),
}

impl std::fmt::Display for GitHubError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RateLimited(retry_after) => write!(formatter, "rate limited for {retry_after:?}"),
            Self::Failed(message) => formatter.write_str(message),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
pub struct PullRequest {
    pub number: u64,
    pub title: String,
    #[serde(default)]
    pub draft: bool,
    pub created_at: String,
    pub head: PullHead,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
pub struct PullHead {
    #[serde(rename = "ref")]
    pub branch: String,
    pub sha: String,
    pub repo: Option<HeadRepo>,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
pub struct HeadRepo {
    pub full_name: String,
}

#[derive(Deserialize)]
struct Comment {
    id: u64,
    #[serde(default)]
    body: String,
}

#[derive(Deserialize)]
struct Created {
    id: u64,
}

pub struct GitHub {
    http: reqwest::Client,
    api: String,
    token: String,
    pull_cache: Mutex<HashMap<String, (String, Vec<PullRequest>)>>,
}

impl GitHub {
    pub fn new(api: String, token: String) -> Self {
        Self {
            http: reqwest::Client::new(),
            api: api.trim_end_matches('/').to_string(),
            token,
            pull_cache: Mutex::default(),
        }
    }

    pub async fn open_pulls(&self, repo: &str) -> Result<Vec<PullRequest>, GitHubError> {
        let cached = self.lock_cache().get(repo).cloned();
        let first_url = self.pulls_url(repo, 1);
        let mut request = self.request(reqwest::Method::GET, &first_url);
        if let Some((etag, _)) = &cached {
            request = request.header("If-None-Match", etag);
        }
        let response = send(request).await?;
        match (response.status(), cached) {
            (StatusCode::NOT_MODIFIED, Some((_, pulls))) => Ok(pulls),
            _ => {
                let etag = response
                    .headers()
                    .get("etag")
                    .and_then(|value| value.to_str().ok())
                    .map(str::to_string);
                let mut pulls = parse::<Vec<PullRequest>>(response).await?;
                let mut page = 1;
                let mut last_page_full = pulls.len() == PAGE_SIZE;
                while last_page_full {
                    page += 1;
                    let more = parse::<Vec<PullRequest>>(
                        send(self.request(reqwest::Method::GET, &self.pulls_url(repo, page)))
                            .await?,
                    )
                    .await?;
                    last_page_full = more.len() == PAGE_SIZE;
                    pulls.extend(more);
                }
                if let Some(etag) = etag {
                    self.lock_cache()
                        .insert(repo.to_string(), (etag, pulls.clone()));
                }
                Ok(pulls)
            }
        }
    }

    pub async fn upsert_comment(
        &self,
        repo: &str,
        pr: u64,
        marker: &str,
        body: &str,
        known: Option<u64>,
    ) -> Result<u64, GitHubError> {
        let existing = match known {
            Some(id) => Some(id),
            None => self.find_comment(repo, pr, marker).await?,
        };
        let payload = json!({ "body": format!("{marker}\n{body}") });
        match existing {
            Some(id) => {
                let response = send(
                    self.request(
                        reqwest::Method::PATCH,
                        &format!("{}/repos/{repo}/issues/comments/{id}", self.api),
                    )
                    .json(&payload),
                )
                .await?;
                match response.status() {
                    StatusCode::NOT_FOUND => {
                        Box::pin(self.upsert_comment(repo, pr, marker, body, None)).await
                    }
                    _ => parse::<Created>(response).await.map(|created| created.id),
                }
            }
            None => parse::<Created>(
                send(
                    self.request(
                        reqwest::Method::POST,
                        &format!("{}/repos/{repo}/issues/{pr}/comments", self.api),
                    )
                    .json(&payload),
                )
                .await?,
            )
            .await
            .map(|created| created.id),
        }
    }

    pub async fn create_deployment(
        &self,
        repo: &str,
        sha: &str,
        environment: &str,
    ) -> Result<u64, GitHubError> {
        let payload = json!({
            "ref": sha,
            "environment": environment,
            "auto_merge": false,
            "required_contexts": [],
            "transient_environment": true,
            "production_environment": false,
            "description": "Maestro preview",
        });
        parse::<Created>(
            send(
                self.request(
                    reqwest::Method::POST,
                    &format!("{}/repos/{repo}/deployments", self.api),
                )
                .json(&payload),
            )
            .await?,
        )
        .await
        .map(|created| created.id)
    }

    pub async fn deployment_status(
        &self,
        repo: &str,
        deployment: u64,
        state: &str,
        environment_url: Option<&str>,
        log_url: Option<&str>,
    ) -> Result<(), GitHubError> {
        let mut payload = json!({ "state": state, "auto_inactive": false });
        if let Some(url) = environment_url {
            payload["environment_url"] = Value::String(url.to_string());
        }
        if let Some(url) = log_url {
            payload["log_url"] = Value::String(url.to_string());
        }
        parse::<Value>(
            send(
                self.request(
                    reqwest::Method::POST,
                    &format!(
                        "{}/repos/{repo}/deployments/{deployment}/statuses",
                        self.api
                    ),
                )
                .json(&payload),
            )
            .await?,
        )
        .await
        .map(|_| ())
    }

    async fn find_comment(
        &self,
        repo: &str,
        pr: u64,
        marker: &str,
    ) -> Result<Option<u64>, GitHubError> {
        let mut page = 1;
        loop {
            let comments = parse::<Vec<Comment>>(
                send(self.request(
                    reqwest::Method::GET,
                    &format!(
                        "{}/repos/{repo}/issues/{pr}/comments?per_page={PAGE_SIZE}&page={page}",
                        self.api
                    ),
                ))
                .await?,
            )
            .await?;
            let found = comments
                .iter()
                .find(|comment| comment.body.contains(marker))
                .map(|comment| comment.id);
            match (found, comments.len() == PAGE_SIZE) {
                (Some(id), _) => break Ok(Some(id)),
                (None, true) => page += 1,
                (None, false) => break Ok(None),
            }
        }
    }

    fn pulls_url(&self, repo: &str, page: usize) -> String {
        format!(
            "{}/repos/{repo}/pulls?state=open&sort=created&direction=asc&per_page={PAGE_SIZE}&page={page}",
            self.api
        )
    }

    fn request(&self, method: reqwest::Method, url: &str) -> reqwest::RequestBuilder {
        self.http
            .request(method, url)
            .timeout(REQUEST_TIMEOUT)
            .bearer_auth(&self.token)
            .header("Accept", "application/vnd.github+json")
            .header("X-GitHub-Api-Version", API_VERSION)
            .header("User-Agent", concat!("maestro/", env!("CARGO_PKG_VERSION")))
    }

    fn lock_cache(&self) -> std::sync::MutexGuard<'_, HashMap<String, (String, Vec<PullRequest>)>> {
        self.pull_cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

async fn send(request: reqwest::RequestBuilder) -> Result<reqwest::Response, GitHubError> {
    let response = request
        .send()
        .await
        .map_err(|error| GitHubError::Failed(error.without_url().to_string()))?;
    let exhausted = response
        .headers()
        .get("x-ratelimit-remaining")
        .is_some_and(|remaining| remaining == "0");
    match (response.status(), exhausted) {
        (StatusCode::FORBIDDEN | StatusCode::TOO_MANY_REQUESTS, true) => {
            let reset = response
                .headers()
                .get("x-ratelimit-reset")
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or_default();
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |elapsed| elapsed.as_secs());
            Err(GitHubError::RateLimited(
                Duration::from_secs(reset.saturating_sub(now)).max(MIN_RETRY_AFTER),
            ))
        }
        _ => Ok(response),
    }
}

async fn parse<T: serde::de::DeserializeOwned>(
    response: reqwest::Response,
) -> Result<T, GitHubError> {
    let status = response.status();
    match status.is_success() {
        true => response
            .json()
            .await
            .map_err(|error| GitHubError::Failed(error.to_string())),
        false => Err(GitHubError::Failed(format!(
            "github returned {status}: {}",
            response.text().await.unwrap_or_default()
        ))),
    }
}
