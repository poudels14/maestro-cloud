use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use reqwest::{Method, StatusCode};
use serde::Deserialize;
use tokio::sync::Mutex;

use crate::utils::crypto::SecretString;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PullRequest {
    pub number: u64,
    pub title: String,
    pub draft: bool,
    pub created_at: u64,
    pub head_ref: String,
    pub head_sha: String,
    pub head_repo_full_name: Option<String>,
}

#[async_trait]
pub trait PullRequestApi: Send + Sync {
    async fn list_open(&self, owner: &str, repo: &str) -> Result<Vec<PullRequest>>;
    async fn upsert_comment(
        &self,
        owner: &str,
        repo: &str,
        pr_number: u64,
        comment_key: &str,
        body: &str,
    ) -> Result<()>;
}

#[derive(Debug)]
pub struct RateLimitError {
    pub retry_after: Duration,
}

impl std::fmt::Display for RateLimitError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "GitHub API rate limit exceeded; retry in {}s",
            self.retry_after.as_secs()
        )
    }
}

impl std::error::Error for RateLimitError {}

#[derive(Clone)]
pub struct GithubClient {
    client: reqwest::Client,
    token: SecretString,
    comments: Arc<Mutex<HashMap<CommentKey, CachedComment>>>,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct CommentKey {
    owner: String,
    repo: String,
    pr_number: u64,
    comment_key: String,
}

#[derive(Debug, Clone)]
struct CachedComment {
    id: u64,
    body: String,
}

#[derive(Debug, Deserialize)]
struct PullRequestResponse {
    number: u64,
    title: String,
    #[serde(default)]
    draft: bool,
    #[serde(deserialize_with = "deserialize_timestamp_millis")]
    created_at: u64,
    head: PullRequestHead,
}

#[derive(Debug, Deserialize)]
struct PullRequestHead {
    #[serde(rename = "ref")]
    reference: String,
    sha: String,
    repo: Option<PullRequestRepo>,
}

#[derive(Debug, Deserialize)]
struct PullRequestRepo {
    full_name: String,
}

#[derive(Debug, Deserialize)]
struct IssueComment {
    id: u64,
    body: Option<String>,
}

impl GithubClient {
    pub fn new(token: &str) -> Result<Self> {
        let client = reqwest::Client::builder()
            .user_agent(concat!("maestro/", env!("CARGO_PKG_VERSION")))
            .build()?;
        Ok(Self {
            client,
            token: SecretString::new(token.to_string()),
            comments: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    fn request(&self, method: Method, url: String) -> reqwest::RequestBuilder {
        self.client
            .request(method, url)
            .bearer_auth(self.token.as_str())
            .header("Accept", "application/vnd.github+json")
            .header("X-GitHub-Api-Version", "2022-11-28")
    }

    async fn send(&self, request: reqwest::RequestBuilder) -> Result<reqwest::Response> {
        let response = request.send().await?;
        if response.status().is_success() {
            return Ok(response);
        }
        if response.status() == StatusCode::FORBIDDEN
            && response
                .headers()
                .get("x-ratelimit-remaining")
                .and_then(|value| value.to_str().ok())
                == Some("0")
        {
            let reset_at = response
                .headers()
                .get("x-ratelimit-reset")
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or_default();
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            return Err(RateLimitError {
                retry_after: Duration::from_secs(reset_at.saturating_sub(now).max(1)),
            }
            .into());
        }
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        Err(anyhow!("GitHub API request failed with {status}: {body}"))
    }

    async fn find_comment(
        &self,
        owner: &str,
        repo: &str,
        pr_number: u64,
        marker: &str,
    ) -> Result<Option<IssueComment>> {
        let mut page = 1;
        loop {
            let url = format!(
                "https://api.github.com/repos/{owner}/{repo}/issues/{pr_number}/comments?per_page=100&page={page}"
            );
            let request = self.request(Method::GET, url);
            let comments = self
                .send(request)
                .await?
                .json::<Vec<IssueComment>>()
                .await?;
            let page_len = comments.len();
            if let Some(comment) = comments.into_iter().find(|comment| {
                comment
                    .body
                    .as_deref()
                    .is_some_and(|body| body.contains(marker))
            }) {
                return Ok(Some(comment));
            }
            if page_len < 100 {
                return Ok(None);
            }
            page += 1;
        }
    }
}

#[async_trait]
impl PullRequestApi for GithubClient {
    async fn list_open(&self, owner: &str, repo: &str) -> Result<Vec<PullRequest>> {
        let mut page = 1;
        let mut pull_requests = Vec::new();
        loop {
            let url = format!(
                "https://api.github.com/repos/{owner}/{repo}/pulls?state=open&sort=created&direction=asc&per_page=100&page={page}"
            );
            let request = self.request(Method::GET, url);
            let response = self
                .send(request)
                .await?
                .json::<Vec<PullRequestResponse>>()
                .await?;
            let page_len = response.len();
            pull_requests.extend(response.into_iter().map(|pull_request| PullRequest {
                number: pull_request.number,
                title: pull_request.title,
                draft: pull_request.draft,
                created_at: pull_request.created_at,
                head_ref: pull_request.head.reference,
                head_sha: pull_request.head.sha,
                head_repo_full_name: pull_request.head.repo.map(|repo| repo.full_name),
            }));
            if page_len < 100 {
                break;
            }
            page += 1;
        }
        Ok(pull_requests)
    }

    async fn upsert_comment(
        &self,
        owner: &str,
        repo: &str,
        pr_number: u64,
        comment_key: &str,
        body: &str,
    ) -> Result<()> {
        let marker = format!("<!-- maestro-preview:{comment_key} -->");
        let body = format!("{marker}\n{body}");
        let key = CommentKey {
            owner: owner.to_ascii_lowercase(),
            repo: repo.to_ascii_lowercase(),
            pr_number,
            comment_key: comment_key.to_string(),
        };
        let cached = self.comments.lock().await.get(&key).cloned();
        if cached.as_ref().is_some_and(|comment| comment.body == body) {
            return Ok(());
        }
        let existing = if cached.is_some() {
            cached
        } else {
            self.find_comment(owner, repo, pr_number, &marker)
                .await?
                .map(|comment| CachedComment {
                    id: comment.id,
                    body: comment.body.unwrap_or_default(),
                })
        };

        let (method, url) = if let Some(comment) = &existing {
            (
                Method::PATCH,
                format!(
                    "https://api.github.com/repos/{owner}/{repo}/issues/comments/{}",
                    comment.id
                ),
            )
        } else {
            (
                Method::POST,
                format!("https://api.github.com/repos/{owner}/{repo}/issues/{pr_number}/comments"),
            )
        };
        let request = self
            .request(method, url)
            .json(&serde_json::json!({ "body": body }));
        let comment = self.send(request).await?.json::<IssueComment>().await?;
        self.comments.lock().await.insert(
            key,
            CachedComment {
                id: comment.id,
                body,
            },
        );
        Ok(())
    }
}

fn deserialize_timestamp_millis<'de, D>(deserializer: D) -> std::result::Result<u64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error;

    let value = String::deserialize(deserializer)?;
    chrono::DateTime::parse_from_rfc3339(&value)
        .map(|timestamp| timestamp.timestamp_millis().max(0) as u64)
        .map_err(D::Error::custom)
}
