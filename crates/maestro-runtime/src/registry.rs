use std::collections::BTreeMap;
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use reqwest::StatusCode;
use reqwest::header::{ACCEPT, AUTHORIZATION, WWW_AUTHENTICATE};
use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::docker_auth::{RegistryCredentials, registry_credentials};
use crate::reference;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(20);
const DOCKER_HUB_REGISTRY: &str = "registry-1.docker.io";
const MANIFEST_TYPES: &str = "application/vnd.oci.image.index.v1+json, \
    application/vnd.docker.distribution.manifest.list.v2+json, \
    application/vnd.oci.image.manifest.v1+json, \
    application/vnd.docker.distribution.manifest.v2+json";

#[derive(Debug, thiserror::Error)]
pub enum RegistryError {
    #[error("{reference}: {message}")]
    Resolve { reference: String, message: String },
}

#[derive(Clone)]
pub struct RegistryClient {
    http: reqwest::Client,
}

struct ImageName {
    registry: String,
    repository: String,
    tag: String,
}

#[derive(Deserialize)]
struct TokenResponse {
    token: Option<String>,
    access_token: Option<String>,
}

impl RegistryClient {
    pub fn new() -> Self {
        Self {
            http: reqwest::Client::builder()
                .timeout(REQUEST_TIMEOUT)
                .build()
                .unwrap_or_default(),
        }
    }

    pub async fn pin(&self, image: &str) -> Result<String, RegistryError> {
        let normalized = reference::normalize(image);
        match normalized.contains('@') {
            true => Ok(normalized),
            false => {
                let name = ImageName::parse(&normalized);
                let digest = self.manifest_digest(&name).await.map_err(|message| {
                    RegistryError::Resolve {
                        reference: normalized.clone(),
                        message,
                    }
                })?;
                Ok(format!("{normalized}@{digest}"))
            }
        }
    }

    async fn manifest_digest(&self, name: &ImageName) -> Result<String, String> {
        let url = format!(
            "{}/v2/{}/manifests/{}",
            name.base_url(),
            name.repository,
            name.tag
        );
        let anonymous = self.manifest(&url, None).await?;
        let response = match anonymous.status() {
            StatusCode::UNAUTHORIZED => {
                let challenge = anonymous
                    .headers()
                    .get(WWW_AUTHENTICATE)
                    .and_then(|value| value.to_str().ok())
                    .ok_or("registry requires authentication without a challenge")?
                    .to_string();
                let credentials = registry_credentials(&name.registry).await?;
                let authorization = match challenge.to_ascii_lowercase().starts_with("basic") {
                    true => credentials.as_ref().map(basic_authorization).ok_or(
                        "registry requires credentials; add them to the node's Docker config",
                    )?,
                    false => format!(
                        "Bearer {}",
                        self.bearer_token(&challenge, name, credentials.as_ref())
                            .await?
                    ),
                };
                self.manifest(&url, Some(&authorization)).await?
            }
            _ => anonymous,
        };
        match response.status() {
            status if status.is_success() => {
                let header_digest = response
                    .headers()
                    .get("docker-content-digest")
                    .and_then(|value| value.to_str().ok())
                    .map(str::to_string);
                match header_digest {
                    Some(digest) => Ok(digest),
                    None => {
                        let body = response.bytes().await.map_err(|error| error.to_string())?;
                        Ok(format!("sha256:{}", hex::encode(Sha256::digest(&body))))
                    }
                }
            }
            StatusCode::NOT_FOUND => Err("image or tag not found".to_string()),
            StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => Err(format!(
                "access denied; check the node's Docker credentials for {}",
                name.registry
            )),
            status => Err(format!("registry returned {status}")),
        }
    }

    async fn manifest(
        &self,
        url: &str,
        authorization: Option<&str>,
    ) -> Result<reqwest::Response, String> {
        let request = self.http.get(url).header(ACCEPT, MANIFEST_TYPES);
        let request = match authorization {
            Some(authorization) => request.header(AUTHORIZATION, authorization),
            None => request,
        };
        request.send().await.map_err(|error| error.to_string())
    }

    async fn bearer_token(
        &self,
        challenge: &str,
        name: &ImageName,
        credentials: Option<&RegistryCredentials>,
    ) -> Result<String, String> {
        let parameters = challenge_parameters(challenge);
        let realm = parameters
            .get("realm")
            .ok_or("registry challenge has no realm")?;
        let scope = parameters
            .get("scope")
            .cloned()
            .unwrap_or_else(|| format!("repository:{}:pull", name.repository));
        let query = parameters
            .get("service")
            .map(|service| vec![("service", service.clone()), ("scope", scope.clone())])
            .unwrap_or_else(|| vec![("scope", scope)]);
        let request = self.http.get(realm).query(&query);
        let request = match credentials {
            Some(credentials) => {
                request.basic_auth(&credentials.username, Some(&credentials.secret))
            }
            None => request,
        };
        let response: TokenResponse = request
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .map_err(|error| format!("token request failed: {error}"))?
            .json()
            .await
            .map_err(|error| format!("token response: {error}"))?;
        response
            .token
            .or(response.access_token)
            .ok_or_else(|| "token response has no token".to_string())
    }
}

fn basic_authorization(credentials: &RegistryCredentials) -> String {
    let pair = format!("{}:{}", credentials.username, credentials.secret);
    format!("Basic {}", BASE64.encode(pair))
}

impl Default for RegistryClient {
    fn default() -> Self {
        Self::new()
    }
}

impl ImageName {
    fn parse(normalized: &str) -> Self {
        let (registry, path) = normalized.split_once('/').unwrap_or(("", normalized));
        let (repository, tag) = path.rsplit_once(':').unwrap_or((path, "latest"));
        Self {
            registry: registry.to_string(),
            repository: repository.to_string(),
            tag: tag.to_string(),
        }
    }

    fn base_url(&self) -> String {
        let host = self.registry.split(':').next().unwrap_or_default();
        match (self.registry.as_str(), host) {
            ("docker.io", _) => format!("https://{DOCKER_HUB_REGISTRY}"),
            (_, "localhost" | "127.0.0.1") => format!("http://{}", self.registry),
            (registry, _) => format!("https://{registry}"),
        }
    }
}

fn challenge_parameters(challenge: &str) -> BTreeMap<String, String> {
    let parameters = challenge
        .strip_prefix("Bearer ")
        .or_else(|| challenge.strip_prefix("bearer "))
        .unwrap_or_default();
    let mut values = BTreeMap::new();
    let mut remaining = parameters.trim();
    while let Some((key, rest)) = remaining.split_once('=') {
        let (value, after) = match rest.strip_prefix('"') {
            Some(quoted) => quoted.split_once('"').unwrap_or((quoted, "")),
            None => rest.split_once(',').unwrap_or((rest, "")),
        };
        values.insert(key.trim().to_string(), value.to_string());
        remaining = after.trim_start_matches(',').trim();
    }
    values
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_bearer_challenges() {
        let parameters = challenge_parameters(
            r#"Bearer realm="https://auth.docker.io/token",service="registry.docker.io",scope="repository:library/nginx:pull""#,
        );
        assert_eq!(parameters["realm"], "https://auth.docker.io/token");
        assert_eq!(parameters["service"], "registry.docker.io");
        assert_eq!(parameters["scope"], "repository:library/nginx:pull");
    }

    #[test]
    fn splits_normalized_names() {
        let name = ImageName::parse("localhost:5000/team/api:v2");
        assert_eq!(name.registry, "localhost:5000");
        assert_eq!(name.repository, "team/api");
        assert_eq!(name.tag, "v2");
        assert_eq!(name.base_url(), "http://localhost:5000");
        assert_eq!(
            ImageName::parse("docker.io/library/nginx:latest").base_url(),
            "https://registry-1.docker.io"
        );
    }
}
