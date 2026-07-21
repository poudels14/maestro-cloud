use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use kernel_api::{SecretValue, Timestamp};

use crate::github_client::{
    GithubEpochClock, GithubHttpMethod, GithubHttpRequest, GithubHttpResponse, GithubHttpTransport,
};
use crate::{
    GithubClientError, GithubPullRequestClient, PullRequestApi, PullRequestApiError,
    PullRequestReadiness,
};

#[test]
fn production_client_rejects_unbounded_or_unsafe_credentials() {
    assert!(matches!(
        GithubPullRequestClient::new(SecretValue::new("token"), Duration::ZERO),
        Err(GithubClientError::ZeroTimeout)
    ));
    assert!(matches!(
        GithubPullRequestClient::new(SecretValue::new(""), Duration::from_secs(1)),
        Err(GithubClientError::EmptyToken)
    ));
    assert!(matches!(
        GithubPullRequestClient::new(SecretValue::new("unsafe\nvalue"), Duration::from_secs(1)),
        Err(GithubClientError::InvalidToken)
    ));
}

#[tokio::test]
async fn lists_every_page_and_preserves_pull_request_eligibility_fields()
-> Result<(), Box<dyn std::error::Error>> {
    let first_page = (1..=100)
        .map(|number| pull_request_json(number, false, "acme/api"))
        .collect::<Vec<_>>();
    let second_page = vec![pull_request_json(101, true, "someone/fork")];
    let transport = Arc::new(FakeTransport::new(vec![
        json_response(200, serde_json::to_vec(&first_page)?),
        json_response(200, serde_json::to_vec(&second_page)?),
    ]));
    let client = client(transport.clone());

    let pull_requests = client.list_open("acme", "api").await?;

    assert_eq!(pull_requests.len(), 101);
    let first = pull_requests.first().unwrap();
    assert_eq!(first.number, 1);
    assert_eq!(first.created_at, Timestamp(1_704_067_200_000));
    assert_eq!(first.head_reference, "feature-1");
    assert_eq!(first.head_revision, format!("{:040x}", 1));
    assert_eq!(
        pull_requests.last().unwrap().readiness,
        PullRequestReadiness::Draft
    );
    assert_eq!(
        pull_requests.last().unwrap().head_repository.as_deref(),
        Some("someone/fork")
    );

    let requests = transport.requests();
    assert_eq!(requests.len(), 2);
    assert!(
        requests
            .first()
            .unwrap()
            .url
            .ends_with("per_page=100&page=1")
    );
    assert!(
        requests
            .last()
            .unwrap()
            .url
            .ends_with("per_page=100&page=2")
    );
    assert_eq!(requests.first().unwrap().method, GithubHttpMethod::Get);
    assert_eq!(
        requests
            .first()
            .unwrap()
            .headers
            .get("Authorization")
            .map(String::as_str),
        Some("Bearer github-secret")
    );
    Ok(())
}

#[tokio::test]
async fn finds_updates_and_caches_one_marker_keyed_comment()
-> Result<(), Box<dyn std::error::Error>> {
    let marker = "<!-- maestro-preview:preview-api -->";
    let transport = Arc::new(FakeTransport::new(vec![
        json_response(
            200,
            serde_json::to_vec(&serde_json::json!([
                {"id": 7, "body": format!("{marker}\nold")}
            ]))?,
        ),
        json_response(
            200,
            serde_json::to_vec(&serde_json::json!({"id": 7, "body": "updated"}))?,
        ),
        json_response(
            200,
            serde_json::to_vec(&serde_json::json!({"id": 7, "body": "changed"}))?,
        ),
    ]));
    let client = client(transport.clone());

    client
        .upsert_comment("acme", "api", 42, "preview-api", "current")
        .await?;
    client
        .upsert_comment("acme", "api", 42, "preview-api", "current")
        .await?;
    client
        .upsert_comment("acme", "api", 42, "preview-api", "next")
        .await?;

    let requests = transport.requests();
    assert_eq!(requests.len(), 3);
    assert_eq!(requests.first().unwrap().method, GithubHttpMethod::Get);
    assert_eq!(requests.get(1).unwrap().method, GithubHttpMethod::Patch);
    assert_eq!(requests.last().unwrap().method, GithubHttpMethod::Patch);
    assert!(requests.get(1).unwrap().url.ends_with("/issues/comments/7"));
    let body: serde_json::Value = serde_json::from_slice(&requests.get(1).unwrap().body)?;
    assert!(
        body.get("body")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|body| body == format!("{marker}\ncurrent"))
    );
    Ok(())
}

#[tokio::test]
async fn creates_a_comment_when_no_sticky_marker_exists() -> Result<(), Box<dyn std::error::Error>>
{
    let transport = Arc::new(FakeTransport::new(vec![
        json_response(200, b"[]".to_vec()),
        json_response(
            201,
            serde_json::to_vec(&serde_json::json!({"id": 9, "body": "created"}))?,
        ),
    ]));
    let client = client(transport.clone());

    client
        .upsert_comment("acme", "api", 42, "preview-api", "building")
        .await?;

    let requests = transport.requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests.last().unwrap().method, GithubHttpMethod::Post);
    assert!(
        requests
            .last()
            .unwrap()
            .url
            .ends_with("/issues/42/comments")
    );
    Ok(())
}

#[tokio::test]
async fn classifies_rate_limits_rejections_and_server_failures()
-> Result<(), Box<dyn std::error::Error>> {
    let transport = Arc::new(FakeTransport::new(vec![GithubHttpResponse {
        status: 403,
        headers: BTreeMap::from([
            ("x-ratelimit-remaining".to_string(), "0".to_string()),
            ("x-ratelimit-reset".to_string(), "103".to_string()),
        ]),
        body: Vec::new(),
    }]));
    let rate_limited = client(transport);
    assert_eq!(
        rate_limited.list_open("acme", "api").await.unwrap_err(),
        PullRequestApiError::RateLimited {
            retry_after: Duration::from_secs(3)
        }
    );

    let rejected = Arc::new(FakeTransport::new(vec![json_response(401, Vec::new())]));
    assert!(matches!(
        client(rejected).list_open("acme", "api").await,
        Err(PullRequestApiError::Rejected { .. })
    ));
    let unavailable = Arc::new(FakeTransport::new(vec![json_response(503, Vec::new())]));
    assert!(matches!(
        client(unavailable).list_open("acme", "api").await,
        Err(PullRequestApiError::Unavailable { .. })
    ));
    Ok(())
}

fn client(transport: Arc<dyn GithubHttpTransport>) -> GithubPullRequestClient {
    GithubPullRequestClient::with_transport(
        SecretValue::new("github-secret"),
        "https://github.test",
        transport,
        Arc::new(FixedClock),
    )
}

fn pull_request_json(number: u64, draft: bool, repository: &str) -> serde_json::Value {
    serde_json::json!({
        "number": number,
        "title": format!("Pull request {number}"),
        "draft": draft,
        "created_at": "2024-01-01T00:00:00Z",
        "head": {
            "ref": format!("feature-{number}"),
            "sha": format!("{number:040x}"),
            "repo": {"full_name": repository}
        }
    })
}

fn json_response(status: u16, body: Vec<u8>) -> GithubHttpResponse {
    GithubHttpResponse {
        status,
        headers: BTreeMap::new(),
        body,
    }
}

#[derive(Clone)]
struct RecordedRequest {
    method: GithubHttpMethod,
    url: String,
    headers: BTreeMap<String, String>,
    body: Vec<u8>,
}

struct FakeTransport {
    responses: Mutex<VecDeque<GithubHttpResponse>>,
    requests: Mutex<Vec<RecordedRequest>>,
}

impl FakeTransport {
    fn new(responses: Vec<GithubHttpResponse>) -> Self {
        Self {
            responses: Mutex::new(responses.into()),
            requests: Mutex::new(Vec::new()),
        }
    }

    fn requests(&self) -> Vec<RecordedRequest> {
        self.requests.lock().unwrap().clone()
    }
}

#[async_trait]
impl GithubHttpTransport for FakeTransport {
    async fn send(
        &self,
        request: GithubHttpRequest,
    ) -> Result<GithubHttpResponse, PullRequestApiError> {
        self.requests.lock().unwrap().push(RecordedRequest {
            method: request.method,
            url: request.url,
            headers: request.headers,
            body: request.body,
        });
        self.responses
            .lock()
            .unwrap()
            .pop_front()
            .ok_or_else(|| PullRequestApiError::Unavailable {
                message: "fake response queue exhausted".to_string(),
            })
    }
}

struct FixedClock;

impl GithubEpochClock for FixedClock {
    fn unix_seconds(&self) -> u64 {
        100
    }
}
