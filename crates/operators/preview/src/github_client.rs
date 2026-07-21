use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use kernel_api::{SecretValue, Timestamp};
use serde::Deserialize;
use tokio::sync::Mutex;

use crate::{PullRequest, PullRequestApi, PullRequestApiError, PullRequestReadiness};

const DEFAULT_API_BASE: &str = "https://api.github.com";
const MAX_RESPONSE_BYTES: usize = 4 * 1_024 * 1_024;
const MAX_PAGES: u32 = 100;
const PAGE_SIZE: usize = 100;

/// Production GitHub client construction failure.
#[derive(Debug, thiserror::Error)]
pub enum GithubClientError {
    /// A zero request timeout would permit unbounded calls.
    #[error("GitHub request timeout must be greater than zero")]
    ZeroTimeout,
    /// An empty token cannot authenticate GitHub API requests.
    #[error("GitHub token must not be empty")]
    EmptyToken,
    /// Whitespace or control characters would produce an unsafe authorization header.
    #[error("GitHub token cannot be represented as an authorization header")]
    InvalidToken,
    /// Reqwest rejected the bounded rustls client configuration.
    #[error("failed to build GitHub HTTP client: {0}")]
    Build(reqwest::Error),
}

/// Bounded, authenticated implementation of the pull-request API seam.
pub struct GithubPullRequestClient {
    transport: Arc<dyn GithubHttpTransport>,
    token: SecretValue,
    api_base: String,
    clock: Arc<dyn GithubEpochClock>,
    comments: Mutex<HashMap<CommentKey, CachedComment>>,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct CommentKey {
    owner: String,
    repository: String,
    pull_request_number: u64,
    comment_key: String,
}

#[derive(Debug, Clone)]
struct CachedComment {
    id: u64,
    body: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GithubHttpMethod {
    Get,
    Post,
    Patch,
}

/// This type intentionally omits `Debug` because its headers contain a token.
pub(crate) struct GithubHttpRequest {
    pub(crate) method: GithubHttpMethod,
    pub(crate) url: String,
    pub(crate) headers: BTreeMap<String, String>,
    pub(crate) body: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct GithubHttpResponse {
    pub(crate) status: u16,
    pub(crate) headers: BTreeMap<String, String>,
    pub(crate) body: Vec<u8>,
}

#[async_trait]
pub(crate) trait GithubHttpTransport: Send + Sync {
    async fn send(
        &self,
        request: GithubHttpRequest,
    ) -> Result<GithubHttpResponse, PullRequestApiError>;
}

pub(crate) trait GithubEpochClock: Send + Sync {
    fn unix_seconds(&self) -> u64;
}

struct SystemGithubEpochClock;

impl GithubEpochClock for SystemGithubEpochClock {
    fn unix_seconds(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }
}

struct ReqwestGithubTransport {
    client: reqwest::Client,
}

impl GithubPullRequestClient {
    /// Constructs a production rustls GitHub client with a total request timeout.
    pub fn new(token: SecretValue, timeout: Duration) -> Result<Self, GithubClientError> {
        if timeout.is_zero() {
            return Err(GithubClientError::ZeroTimeout);
        }
        if token.expose().trim().is_empty() {
            return Err(GithubClientError::EmptyToken);
        }
        let authorization = format!("Bearer {}", token.expose());
        if token.expose().trim() != token.expose()
            || reqwest::header::HeaderValue::from_str(&authorization).is_err()
        {
            return Err(GithubClientError::InvalidToken);
        }
        let client = reqwest::Client::builder()
            .https_only(true)
            .timeout(timeout)
            .user_agent(concat!("maestro/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(GithubClientError::Build)?;
        Ok(Self::with_transport(
            token,
            DEFAULT_API_BASE,
            Arc::new(ReqwestGithubTransport { client }),
            Arc::new(SystemGithubEpochClock),
        ))
    }

    pub(crate) fn with_transport(
        token: SecretValue,
        api_base: &str,
        transport: Arc<dyn GithubHttpTransport>,
        clock: Arc<dyn GithubEpochClock>,
    ) -> Self {
        Self {
            transport,
            token,
            api_base: api_base.trim_end_matches('/').to_string(),
            clock,
            comments: Mutex::new(HashMap::new()),
        }
    }

    async fn request(
        &self,
        method: GithubHttpMethod,
        path: &str,
        body: Vec<u8>,
    ) -> Result<GithubHttpResponse, PullRequestApiError> {
        let response = self
            .transport
            .send(GithubHttpRequest {
                method,
                url: format!("{}{path}", self.api_base),
                headers: BTreeMap::from([
                    (
                        "Accept".to_string(),
                        "application/vnd.github+json".to_string(),
                    ),
                    (
                        "Authorization".to_string(),
                        format!("Bearer {}", self.token.expose()),
                    ),
                    ("X-GitHub-Api-Version".to_string(), "2022-11-28".to_string()),
                ]),
                body,
            })
            .await?;
        classify_response(response, self.clock.unix_seconds())
    }

    async fn find_comment(
        &self,
        owner: &str,
        repository: &str,
        pull_request_number: u64,
        marker: &str,
    ) -> Result<Option<IssueComment>, PullRequestApiError> {
        for page in 1..=MAX_PAGES {
            let path = format!(
                "/repos/{owner}/{repository}/issues/{pull_request_number}/comments?per_page={PAGE_SIZE}&page={page}"
            );
            let response = self
                .request(GithubHttpMethod::Get, &path, Vec::new())
                .await?;
            let comments = decode::<Vec<IssueComment>>(&response.body, "issue comments")?;
            let page_len = comments.len();
            if let Some(comment) = comments.into_iter().find(|comment| {
                comment
                    .body
                    .as_deref()
                    .is_some_and(|body| body.contains(marker))
            }) {
                return Ok(Some(comment));
            }
            if page_len < PAGE_SIZE {
                return Ok(None);
            }
        }
        Err(PullRequestApiError::Rejected {
            message: "GitHub issue-comment pagination exceeded its safety bound".to_string(),
        })
    }
}

#[async_trait]
impl PullRequestApi for GithubPullRequestClient {
    async fn list_open(
        &self,
        owner: &str,
        repository: &str,
    ) -> Result<Vec<PullRequest>, PullRequestApiError> {
        validate_coordinate(owner, "owner")?;
        validate_coordinate(repository, "repository")?;
        let mut pull_requests = Vec::new();
        for page in 1..=MAX_PAGES {
            let path = format!(
                "/repos/{owner}/{repository}/pulls?state=open&sort=created&direction=asc&per_page={PAGE_SIZE}&page={page}"
            );
            let response = self
                .request(GithubHttpMethod::Get, &path, Vec::new())
                .await?;
            let page = decode::<Vec<PullRequestResponse>>(&response.body, "pull requests")?;
            let page_len = page.len();
            let page = page
                .into_iter()
                .map(PullRequestResponse::into_model)
                .collect::<Result<Vec<_>, _>>()?;
            pull_requests.extend(page);
            if page_len < PAGE_SIZE {
                return Ok(pull_requests);
            }
        }
        Err(PullRequestApiError::Rejected {
            message: "GitHub pull-request pagination exceeded its safety bound".to_string(),
        })
    }

    async fn upsert_comment(
        &self,
        owner: &str,
        repository: &str,
        pull_request_number: u64,
        comment_key: &str,
        body: &str,
    ) -> Result<(), PullRequestApiError> {
        validate_coordinate(owner, "owner")?;
        validate_coordinate(repository, "repository")?;
        validate_comment_key(comment_key)?;
        let marker = format!("<!-- maestro-preview:{comment_key} -->");
        let marked_body = format!("{marker}\n{body}");
        let key = CommentKey {
            owner: owner.to_ascii_lowercase(),
            repository: repository.to_ascii_lowercase(),
            pull_request_number,
            comment_key: comment_key.to_string(),
        };
        let cached = self.comments.lock().await.get(&key).cloned();
        if cached
            .as_ref()
            .is_some_and(|comment| comment.body == marked_body)
        {
            return Ok(());
        }
        let existing = match cached {
            Some(comment) => Some(comment),
            None => self
                .find_comment(owner, repository, pull_request_number, &marker)
                .await?
                .map(|comment| CachedComment {
                    id: comment.id,
                    body: comment.body.unwrap_or_default(),
                }),
        };
        let (method, path) = existing.as_ref().map_or_else(
            || {
                (
                    GithubHttpMethod::Post,
                    format!("/repos/{owner}/{repository}/issues/{pull_request_number}/comments"),
                )
            },
            |comment| {
                (
                    GithubHttpMethod::Patch,
                    format!("/repos/{owner}/{repository}/issues/comments/{}", comment.id),
                )
            },
        );
        let body =
            serde_json::to_vec(&serde_json::json!({ "body": marked_body })).map_err(|error| {
                PullRequestApiError::Rejected {
                    message: format!("cannot encode GitHub comment: {error}"),
                }
            })?;
        let response = self.request(method, &path, body).await?;
        let comment = decode::<IssueComment>(&response.body, "updated issue comment")?;
        self.comments.lock().await.insert(
            key,
            CachedComment {
                id: comment.id,
                body: marked_body,
            },
        );
        Ok(())
    }
}

#[async_trait]
impl GithubHttpTransport for ReqwestGithubTransport {
    async fn send(
        &self,
        request: GithubHttpRequest,
    ) -> Result<GithubHttpResponse, PullRequestApiError> {
        let method = match request.method {
            GithubHttpMethod::Get => reqwest::Method::GET,
            GithubHttpMethod::Post => reqwest::Method::POST,
            GithubHttpMethod::Patch => reqwest::Method::PATCH,
        };
        let mut builder = self.client.request(method, request.url).body(request.body);
        for (name, value) in request.headers {
            builder = builder.header(name, value);
        }
        let mut response =
            builder
                .send()
                .await
                .map_err(|error| PullRequestApiError::Unavailable {
                    message: error.to_string(),
                })?;
        let status = response.status().as_u16();
        let headers = response
            .headers()
            .iter()
            .filter_map(|(name, value)| {
                value
                    .to_str()
                    .ok()
                    .map(|value| (name.as_str().to_ascii_lowercase(), value.to_string()))
            })
            .collect();
        let mut body = Vec::new();
        while body.len() < MAX_RESPONSE_BYTES {
            let Some(chunk) =
                response
                    .chunk()
                    .await
                    .map_err(|error| PullRequestApiError::Unavailable {
                        message: error.to_string(),
                    })?
            else {
                break;
            };
            let remaining = MAX_RESPONSE_BYTES.saturating_sub(body.len());
            body.extend_from_slice(chunk.get(..remaining).unwrap_or(&chunk));
        }
        Ok(GithubHttpResponse {
            status,
            headers,
            body,
        })
    }
}

#[derive(Debug, Deserialize)]
struct PullRequestResponse {
    number: u64,
    title: String,
    #[serde(default)]
    draft: bool,
    created_at: String,
    head: PullRequestHead,
}

impl PullRequestResponse {
    fn into_model(self) -> Result<PullRequest, PullRequestApiError> {
        let created_at = chrono::DateTime::parse_from_rfc3339(&self.created_at)
            .map(|timestamp| Timestamp(timestamp.timestamp_millis()))
            .map_err(|error| PullRequestApiError::Unavailable {
                message: format!("GitHub returned an invalid pull-request timestamp: {error}"),
            })?;
        Ok(PullRequest {
            number: self.number,
            title: self.title,
            readiness: if self.draft {
                PullRequestReadiness::Draft
            } else {
                PullRequestReadiness::Ready
            },
            created_at,
            head_reference: self.head.reference,
            head_revision: self.head.sha,
            head_repository: self.head.repository.map(|repository| repository.full_name),
        })
    }
}

#[derive(Debug, Deserialize)]
struct PullRequestHead {
    #[serde(rename = "ref")]
    reference: String,
    sha: String,
    #[serde(rename = "repo")]
    repository: Option<PullRequestRepository>,
}

#[derive(Debug, Deserialize)]
struct PullRequestRepository {
    full_name: String,
}

#[derive(Debug, Deserialize)]
struct IssueComment {
    id: u64,
    body: Option<String>,
}

fn classify_response(
    response: GithubHttpResponse,
    now_seconds: u64,
) -> Result<GithubHttpResponse, PullRequestApiError> {
    if (200..300).contains(&response.status) {
        return Ok(response);
    }
    let rate_limited = response.status == 429
        || (response.status == 403
            && (response
                .headers
                .get("x-ratelimit-remaining")
                .map(String::as_str)
                == Some("0")
                || response.headers.contains_key("retry-after")));
    if rate_limited {
        let retry_after = response
            .headers
            .get("retry-after")
            .and_then(|value| value.parse::<u64>().ok())
            .map(Duration::from_secs)
            .or_else(|| {
                response
                    .headers
                    .get("x-ratelimit-reset")
                    .and_then(|value| value.parse::<u64>().ok())
                    .map(|reset| Duration::from_secs(reset.saturating_sub(now_seconds)))
            })
            .unwrap_or_else(|| Duration::from_secs(60))
            .max(Duration::from_secs(1));
        return Err(PullRequestApiError::RateLimited { retry_after });
    }
    let message = format!("GitHub API returned HTTP {}", response.status);
    if response.status >= 500 {
        Err(PullRequestApiError::Unavailable { message })
    } else {
        Err(PullRequestApiError::Rejected { message })
    }
}

fn decode<Value: serde::de::DeserializeOwned>(
    body: &[u8],
    resource: &str,
) -> Result<Value, PullRequestApiError> {
    serde_json::from_slice(body).map_err(|error| PullRequestApiError::Unavailable {
        message: format!("GitHub returned malformed {resource}: {error}"),
    })
}

fn validate_coordinate(value: &str, field: &str) -> Result<(), PullRequestApiError> {
    if value.is_empty()
        || matches!(value, "." | "..")
        || value.len() > 100
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        Err(PullRequestApiError::Rejected {
            message: format!("GitHub {field} contains unsupported characters"),
        })
    } else {
        Ok(())
    }
}

fn validate_comment_key(comment_key: &str) -> Result<(), PullRequestApiError> {
    if comment_key.is_empty()
        || comment_key.len() > 253
        || !comment_key
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        Err(PullRequestApiError::Rejected {
            message: "GitHub comment key contains unsupported characters".to_string(),
        })
    } else {
        Ok(())
    }
}
