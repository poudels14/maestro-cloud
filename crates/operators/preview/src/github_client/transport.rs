use std::collections::BTreeMap;

use async_trait::async_trait;

use crate::PullRequestApiError;

const MAX_RESPONSE_BYTES: usize = 4 * 1_024 * 1_024;

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

pub(super) struct ReqwestGithubTransport {
    client: reqwest::Client,
}

impl ReqwestGithubTransport {
    pub(super) fn new(client: reqwest::Client) -> Self {
        Self { client }
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
