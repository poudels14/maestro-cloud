use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use kernel_api::{SecretValue, Timestamp};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::{
    PullRequest, PullRequestApi, PullRequestApiError, PullRequestDeployment,
    PullRequestDeploymentState, PullRequestReadiness,
};

mod transport;

use transport::ReqwestGithubTransport;
pub(crate) use transport::{
    GithubHttpMethod, GithubHttpRequest, GithubHttpResponse, GithubHttpTransport,
};

const DEFAULT_API_BASE: &str = "https://api.github.com";
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
    deployments: Mutex<HashMap<DeploymentKey, CachedDeployment>>,
    publications: Mutex<HashMap<DeploymentKey, DesiredDeploymentStatus>>,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct DeploymentKey {
    owner: String,
    repository: String,
    head_revision: String,
    environment: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CachedDeployment {
    id: u64,
    status: DesiredDeploymentStatus,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DesiredDeploymentStatus {
    state: PullRequestDeploymentState,
    description: String,
    environment_url: Option<String>,
    log_url: Option<String>,
}

#[derive(Serialize)]
struct DeploymentStatusRequest<'a> {
    state: &'a str,
    description: &'a str,
    environment: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    environment_url: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    log_url: Option<&'a str>,
    auto_inactive: bool,
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
            .user_agent(format!("maestro/{}", kernel_api::MAESTRO_VERSION))
            .build()
            .map_err(GithubClientError::Build)?;
        Ok(Self::with_transport(
            token,
            DEFAULT_API_BASE,
            Arc::new(ReqwestGithubTransport::new(client)),
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
            deployments: Mutex::new(HashMap::new()),
            publications: Mutex::new(HashMap::new()),
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

    async fn list_deployments(
        &self,
        owner: &str,
        repository: &str,
        environment: &str,
    ) -> Result<Vec<GithubDeployment>, PullRequestApiError> {
        let mut deployments = Vec::new();
        for page in 1..=MAX_PAGES {
            let per_page = PAGE_SIZE.to_string();
            let page_number = page.to_string();
            let query = url::form_urlencoded::Serializer::new(String::new())
                .append_pair("environment", environment)
                .append_pair("per_page", &per_page)
                .append_pair("page", &page_number)
                .finish();
            let path = format!("/repos/{owner}/{repository}/deployments?{query}");
            let response = self
                .request(GithubHttpMethod::Get, &path, Vec::new())
                .await?;
            let page = decode::<Vec<GithubDeployment>>(&response.body, "deployments")?;
            let page_len = page.len();
            deployments.extend(page);
            if page_len < PAGE_SIZE {
                return Ok(deployments);
            }
        }
        Err(PullRequestApiError::Rejected {
            message: "GitHub deployment pagination exceeded its safety bound".to_string(),
        })
    }

    async fn create_deployment(
        &self,
        owner: &str,
        repository: &str,
        deployment: &PullRequestDeployment,
    ) -> Result<GithubDeployment, PullRequestApiError> {
        let body = encode(
            &serde_json::json!({
                "ref": deployment.head_revision,
                "task": "deploy",
                "auto_merge": false,
                "required_contexts": [],
                "environment": deployment.environment,
                "description": "Maestro pull-request preview",
                "transient_environment": true,
                "production_environment": false
            }),
            "deployment",
        )?;
        let response = self
            .request(
                GithubHttpMethod::Post,
                &format!("/repos/{owner}/{repository}/deployments"),
                body,
            )
            .await?;
        decode(&response.body, "created deployment")
    }

    async fn latest_deployment_status(
        &self,
        owner: &str,
        repository: &str,
        deployment_id: u64,
    ) -> Result<Option<GithubDeploymentStatus>, PullRequestApiError> {
        let response = self
            .request(
                GithubHttpMethod::Get,
                &format!(
                    "/repos/{owner}/{repository}/deployments/{deployment_id}/statuses?per_page={PAGE_SIZE}"
                ),
                Vec::new(),
            )
            .await?;
        Ok(
            decode::<Vec<GithubDeploymentStatus>>(&response.body, "deployment statuses")?
                .into_iter()
                .max_by_key(|status| status.id),
        )
    }

    async fn create_deployment_status(
        &self,
        owner: &str,
        repository: &str,
        deployment_id: u64,
        environment: &str,
        status: &DesiredDeploymentStatus,
    ) -> Result<(), PullRequestApiError> {
        let body = encode(
            &DeploymentStatusRequest {
                state: status.state.as_str(),
                description: &status.description,
                environment,
                environment_url: status.environment_url.as_deref(),
                log_url: status.log_url.as_deref(),
                auto_inactive: false,
            },
            "deployment status",
        )?;
        let response = self
            .request(
                GithubHttpMethod::Post,
                &format!("/repos/{owner}/{repository}/deployments/{deployment_id}/statuses"),
                body,
            )
            .await?;
        let _: GithubDeploymentStatus = decode(&response.body, "created deployment status")?;
        Ok(())
    }

    async fn ensure_deployment_status(
        &self,
        owner: &str,
        repository: &str,
        deployment: &GithubDeployment,
        environment: &str,
        desired: &DesiredDeploymentStatus,
    ) -> Result<(), PullRequestApiError> {
        let key = deployment_key(owner, repository, &deployment.sha, environment);
        if self
            .deployments
            .lock()
            .await
            .get(&key)
            .is_some_and(|cached| cached.id == deployment.id && cached.status == *desired)
        {
            return Ok(());
        }
        let current = self
            .latest_deployment_status(owner, repository, deployment.id)
            .await?;
        if !current
            .as_ref()
            .is_some_and(|current| deployment_status_matches(current, desired))
        {
            self.create_deployment_status(owner, repository, deployment.id, environment, desired)
                .await?;
        }
        self.deployments.lock().await.insert(
            key,
            CachedDeployment {
                id: deployment.id,
                status: desired.clone(),
            },
        );
        Ok(())
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

    async fn publish_deployment(
        &self,
        owner: &str,
        repository: &str,
        deployment: &PullRequestDeployment,
    ) -> Result<(), PullRequestApiError> {
        validate_coordinate(owner, "owner")?;
        validate_coordinate(repository, "repository")?;
        validate_deployment(deployment)?;
        let key = deployment_key(
            owner,
            repository,
            &deployment.head_revision,
            &deployment.environment,
        );
        let desired = DesiredDeploymentStatus {
            state: deployment.state,
            description: deployment.description.clone(),
            environment_url: deployment.environment_url.clone(),
            log_url: deployment.log_url.clone(),
        };
        if self.publications.lock().await.get(&key) == Some(&desired) {
            return Ok(());
        }

        let mut deployments = self
            .list_deployments(owner, repository, &deployment.environment)
            .await?;
        let current = match deployments
            .iter()
            .filter(|candidate| candidate.sha == deployment.head_revision)
            .max_by_key(|candidate| candidate.id)
            .cloned()
        {
            Some(current) => current,
            None => {
                let created = self
                    .create_deployment(owner, repository, deployment)
                    .await?;
                deployments.push(created.clone());
                created
            }
        };
        self.ensure_deployment_status(
            owner,
            repository,
            &current,
            &deployment.environment,
            &desired,
        )
        .await?;

        if deployment.state == PullRequestDeploymentState::Success {
            for previous in deployments
                .iter()
                .filter(|candidate| candidate.id != current.id)
            {
                self.ensure_deployment_status(
                    owner,
                    repository,
                    previous,
                    &deployment.environment,
                    &DesiredDeploymentStatus {
                        state: PullRequestDeploymentState::Inactive,
                        description: "Superseded by a newer Maestro preview.".to_string(),
                        environment_url: None,
                        log_url: deployment.log_url.clone(),
                    },
                )
                .await?;
            }
        }
        self.publications.lock().await.insert(key, desired);
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
struct PullRequestResponse {
    number: u64,
    title: String,
    user: Option<PullRequestUser>,
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
            author: self.user.map_or_else(String::new, |user| user.login),
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
struct PullRequestUser {
    login: String,
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

#[derive(Debug, Clone, Deserialize)]
struct GithubDeployment {
    id: u64,
    sha: String,
}

#[derive(Debug, Deserialize)]
struct GithubDeploymentStatus {
    id: u64,
    state: String,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    environment_url: Option<String>,
    #[serde(default)]
    log_url: Option<String>,
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

fn encode<Value: Serialize>(value: &Value, resource: &str) -> Result<Vec<u8>, PullRequestApiError> {
    serde_json::to_vec(value).map_err(|error| PullRequestApiError::Rejected {
        message: format!("cannot encode GitHub {resource}: {error}"),
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

fn deployment_key(
    owner: &str,
    repository: &str,
    head_revision: &str,
    environment: &str,
) -> DeploymentKey {
    DeploymentKey {
        owner: owner.to_ascii_lowercase(),
        repository: repository.to_ascii_lowercase(),
        head_revision: head_revision.to_ascii_lowercase(),
        environment: environment.to_string(),
    }
}

fn deployment_status_matches(
    current: &GithubDeploymentStatus,
    desired: &DesiredDeploymentStatus,
) -> bool {
    current.state == desired.state.as_str()
        && current.description.as_deref() == Some(desired.description.as_str())
        && current
            .environment_url
            .as_deref()
            .filter(|url| !url.is_empty())
            == desired.environment_url.as_deref()
        && current.log_url.as_deref().filter(|url| !url.is_empty()) == desired.log_url.as_deref()
}

fn validate_deployment(deployment: &PullRequestDeployment) -> Result<(), PullRequestApiError> {
    if deployment.head_revision.is_empty()
        || deployment.head_revision.len() > 128
        || !deployment
            .head_revision
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(PullRequestApiError::Rejected {
            message: "GitHub deployment revision must be a hexadecimal commit ID".to_string(),
        });
    }
    if deployment.environment.is_empty()
        || deployment.environment.len() > 255
        || deployment.environment.chars().any(char::is_control)
    {
        return Err(PullRequestApiError::Rejected {
            message: "GitHub deployment environment is invalid".to_string(),
        });
    }
    if deployment.description.is_empty()
        || deployment.description.len() > 140
        || deployment.description.chars().any(char::is_control)
    {
        return Err(PullRequestApiError::Rejected {
            message: "GitHub deployment description is invalid".to_string(),
        });
    }
    if let Some(environment_url) = deployment.environment_url.as_deref() {
        let parsed =
            url::Url::parse(environment_url).map_err(|_| PullRequestApiError::Rejected {
                message: "GitHub deployment environment URL is invalid".to_string(),
            })?;
        if parsed.scheme() != "https"
            || parsed.host_str().is_none()
            || !parsed.username().is_empty()
            || parsed.password().is_some()
        {
            return Err(PullRequestApiError::Rejected {
                message: "GitHub deployment environment URL must be an HTTPS origin".to_string(),
            });
        }
    }
    if let Some(log_url) = deployment.log_url.as_deref() {
        let parsed = url::Url::parse(log_url).map_err(|_| PullRequestApiError::Rejected {
            message: "GitHub deployment log URL is invalid".to_string(),
        })?;
        let private_http = parsed.scheme() == "http"
            && matches!(
                parsed.host(),
                Some(url::Host::Ipv4(address)) if address.is_private()
            );
        if (parsed.scheme() != "https" && !private_http)
            || parsed.host_str().is_none()
            || !parsed.username().is_empty()
            || parsed.password().is_some()
        {
            return Err(PullRequestApiError::Rejected {
                message: "GitHub deployment log URL must use HTTPS or private IPv4 HTTP"
                    .to_string(),
            });
        }
    }
    Ok(())
}
