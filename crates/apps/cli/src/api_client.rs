use std::time::Duration;

use futures_util::StreamExt;
use kernel_api::{ArtifactArchiveId, ArtifactArchiveUploadResponse, RequestId};
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderValue};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::CliError;
use crate::contexts::Context;

const RESPONSE_LIMIT_BYTES: usize = 16 * 1_024 * 1_024;

pub(crate) struct ApiClient {
    origin: reqwest::Url,
    client: reqwest::Client,
}

impl ApiClient {
    pub(crate) fn new(context: Context) -> Result<Self, CliError> {
        let origin = reqwest::Url::parse(&context.host).map_err(|error| {
            CliError::invalid_contexts(format!("active context host is invalid: {error}"))
        })?;
        let mut headers = HeaderMap::new();
        if let Some(token) = context.token {
            let mut value = HeaderValue::from_str(&format!("Bearer {}", token.expose()))
                .map_err(|_| CliError::invalid_contexts("active context token is invalid"))?;
            value.set_sensitive(true);
            headers.insert(AUTHORIZATION, value);
        }
        let mut builder = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(30))
            .redirect(reqwest::redirect::Policy::none())
            .default_headers(headers)
            .user_agent(concat!("maestro-next/", env!("CARGO_PKG_VERSION")));
        if let Some(certificate) = context.ca_certificate_pem {
            let certificate =
                reqwest::Certificate::from_pem(certificate.as_bytes()).map_err(|_| {
                    CliError::invalid_contexts("active context CA certificate is invalid")
                })?;
            builder = builder.add_root_certificate(certificate);
        }
        let client = builder
            .build()
            .map_err(|source| CliError::transport("failed to construct API client", source))?;
        Ok(Self { origin, client })
    }

    pub(crate) async fn get<Response>(&self, path: &str) -> Result<Response, CliError>
    where
        Response: DeserializeOwned,
    {
        let endpoint = self.endpoint(path)?;
        let response = self
            .client
            .get(endpoint)
            .send()
            .await
            .map_err(|source| CliError::transport("API request failed", source))?;
        decode_response(response).await
    }

    pub(crate) async fn post<Request, Response>(
        &self,
        path: &str,
        request_id: &RequestId,
        body: &Request,
    ) -> Result<Response, CliError>
    where
        Request: Serialize,
        Response: DeserializeOwned,
    {
        self.mutate(reqwest::Method::POST, path, request_id, body)
            .await
    }

    pub(crate) async fn put<Request, Response>(
        &self,
        path: &str,
        request_id: &RequestId,
        body: &Request,
    ) -> Result<Response, CliError>
    where
        Request: Serialize,
        Response: DeserializeOwned,
    {
        self.mutate(reqwest::Method::PUT, path, request_id, body)
            .await
    }

    pub(crate) async fn delete<Request, Response>(
        &self,
        path: &str,
        request_id: &RequestId,
        body: &Request,
    ) -> Result<Response, CliError>
    where
        Request: Serialize,
        Response: DeserializeOwned,
    {
        self.mutate(reqwest::Method::DELETE, path, request_id, body)
            .await
    }

    pub(crate) async fn upload_artifact_archive(
        &self,
        archive_id: &ArtifactArchiveId,
        content: Vec<u8>,
    ) -> Result<ArtifactArchiveUploadResponse, CliError> {
        let endpoint = self.endpoint(&format!("/api/artifact-archives/{archive_id}"))?;
        let response = self
            .client
            .put(endpoint)
            .header(CONTENT_TYPE, "application/gzip")
            .body(content)
            .send()
            .await
            .map_err(|source| {
                CliError::transport(
                    format!("artifact upload failed; retry archive `{archive_id}`"),
                    source,
                )
            })?;
        decode_response(response).await
    }

    pub(crate) async fn post_query<Request, Response>(
        &self,
        path: &str,
        body: &Request,
    ) -> Result<Response, CliError>
    where
        Request: Serialize,
        Response: DeserializeOwned,
    {
        let endpoint = self.endpoint(path)?;
        let encoded = serde_json::to_vec(body)
            .map_err(|source| CliError::json("failed to encode API request", source))?;
        let response = self
            .client
            .post(endpoint)
            .header(CONTENT_TYPE, "application/json")
            .body(encoded)
            .send()
            .await
            .map_err(|source| CliError::transport("API query failed", source))?;
        decode_response(response).await
    }

    async fn mutate<Request, Response>(
        &self,
        method: reqwest::Method,
        path: &str,
        request_id: &RequestId,
        body: &Request,
    ) -> Result<Response, CliError>
    where
        Request: Serialize,
        Response: DeserializeOwned,
    {
        let endpoint = self.endpoint(path)?;
        let encoded = serde_json::to_vec(body)
            .map_err(|source| CliError::json("failed to encode API request", source))?;
        let response = self
            .client
            .request(method, endpoint)
            .header(CONTENT_TYPE, "application/json")
            .header("Idempotency-Key", request_id.as_str())
            .body(encoded)
            .send()
            .await
            .map_err(|source| {
                CliError::transport(
                    format!(
                        "API mutation failed; retry with --idempotency-key {}",
                        request_id.as_str()
                    ),
                    source,
                )
            })?;
        decode_response(response).await
    }

    pub(crate) fn endpoint(&self, path: &str) -> Result<reqwest::Url, CliError> {
        if !path.starts_with('/') {
            return Err(CliError::invalid_input(
                "API endpoint path must be absolute",
            ));
        }
        self.origin
            .join(path)
            .map_err(|error| CliError::invalid_input(format!("invalid API endpoint: {error}")))
    }
}

pub(crate) fn request_id(value: Option<String>) -> Result<RequestId, CliError> {
    match value {
        Some(value) => RequestId::new(value)
            .map_err(|error| CliError::invalid_input(format!("invalid idempotency key: {error}"))),
        None => RequestId::new(uuid::Uuid::new_v4().simple().to_string())
            .map_err(|error| CliError::invalid_input(error.to_string())),
    }
}

async fn decode_response<Response>(response: reqwest::Response) -> Result<Response, CliError>
where
    Response: DeserializeOwned,
{
    let status = response.status();
    let encoded = bounded_body(response).await?;
    if status.is_success() {
        serde_json::from_slice(&encoded)
            .map_err(|source| CliError::json("failed to decode API response", source))
    } else {
        let body =
            serde_json::from_slice::<ApiErrorBody>(&encoded).unwrap_or_else(|_| ApiErrorBody {
                code: "unexpectedResponse".to_string(),
                message: "API returned a non-JSON error response".to_string(),
            });
        Err(CliError::Api {
            status: status.as_u16(),
            code: body.code,
            message: body.message,
        })
    }
}

async fn bounded_body(response: reqwest::Response) -> Result<Vec<u8>, CliError> {
    if response
        .content_length()
        .is_some_and(|length| length > RESPONSE_LIMIT_BYTES as u64)
    {
        return Err(CliError::ResponseTooLarge {
            limit_bytes: RESPONSE_LIMIT_BYTES,
        });
    }
    let mut body = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk
            .map_err(|source| CliError::transport("failed while reading API response", source))?;
        if body.len().saturating_add(chunk.len()) > RESPONSE_LIMIT_BYTES {
            return Err(CliError::ResponseTooLarge {
                limit_bytes: RESPONSE_LIMIT_BYTES,
            });
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ApiErrorBody {
    code: String,
    message: String,
}
