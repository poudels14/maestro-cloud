use std::{collections::BTreeMap, path::Path};

use serde::{Deserialize, Serialize};

use crate::service::{ServiceBuildConfig, ServiceDeployConfig};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ClusterConfig {
    services: BTreeMap<String, ServiceTemplate>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ServiceTemplate {
    name: String,
    #[serde(default)]
    build: Option<ServiceBuildConfig>,
    #[serde(default)]
    image: Option<String>,
    deploy: ServiceDeployConfig,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct PatchServiceRequest {
    id: String,
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    build: Option<ServiceBuildConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    image: Option<String>,
    deploy: ServiceDeployConfig,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PatchServiceResponse {
    queued: bool,
    service_id: String,
    version: String,
}

pub async fn run_rollout(config_path: &Path, host: &str) -> Result<(), String> {
    let raw = std::fs::read_to_string(config_path)
        .map_err(|err| format!("failed to read {}: {err}", config_path.display()))?;
    let cluster = parse_config(&raw)?;

    if cluster.services.is_empty() {
        return Err(format!(
            "no services configured in {}",
            config_path.display()
        ));
    }

    let patch_url = patch_endpoint(host)?;
    println!("[maestro]: rolling out services via {patch_url}");

    let client = reqwest::Client::new();

    for (service_id, service_template) in &cluster.services {
        let payload = service_payload(service_id, service_template)?;
        let response = call_patch_endpoint(&client, &patch_url, &payload, service_id).await?;

        if response.queued {
            println!(
                "[maestro]: queued service `{}` with version `{}`",
                response.service_id, response.version
            );
        } else {
            println!(
                "[maestro]: skipped service `{}`; config unchanged at version `{}`",
                response.service_id, response.version
            );
        }
    }

    Ok(())
}

async fn call_patch_endpoint(
    client: &reqwest::Client,
    endpoint: &str,
    payload: &PatchServiceRequest,
    service_id: &str,
) -> Result<PatchServiceResponse, String> {
    let response = client
        .patch(endpoint)
        .json(payload)
        .send()
        .await
        .map_err(|err| {
            format!("failed to call patch endpoint for service `{service_id}`: {err}")
        })?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!(
            "patch failed for service `{service_id}` with status {status}: {body}"
        ));
    }

    response
        .json::<PatchServiceResponse>()
        .await
        .map_err(|err| format!("failed to decode patch response for service `{service_id}`: {err}"))
}

fn parse_config(raw: &str) -> Result<ClusterConfig, String> {
    let json = strip_jsonc_comments(raw)?;
    serde_json::from_str(&json).map_err(|err| format!("failed to parse maestro.jsonc: {err}"))
}

fn normalize_base_url(host: &str) -> Result<String, String> {
    let host = host.trim();
    if host.is_empty() {
        return Err("host cannot be empty".to_string());
    }

    let base = if host.starts_with("http://") || host.starts_with("https://") {
        host.to_string()
    } else {
        format!("http://{host}")
    };

    Ok(base.trim_end_matches('/').to_string())
}

fn patch_endpoint(host: &str) -> Result<String, String> {
    Ok(format!("{}/api/services/patch", normalize_base_url(host)?))
}

fn service_payload(
    service_id: &str,
    service_template: &ServiceTemplate,
) -> Result<PatchServiceRequest, String> {
    let id = service_id.trim();
    if id.is_empty() {
        return Err("service id cannot be empty".to_string());
    }

    let name = service_template.name.trim();
    if name.is_empty() {
        return Err(format!("service `{service_id}` has empty name"));
    }
    let (build, image) = normalize_build_or_image(&service_template.build, &service_template.image)
        .map_err(|err| format!("service `{service_id}` {err}"))?;

    Ok(PatchServiceRequest {
        id: id.to_string(),
        name: name.to_string(),
        build,
        image,
        deploy: service_template.deploy.clone(),
    })
}

fn normalize_build_or_image(
    build: &Option<ServiceBuildConfig>,
    image: &Option<String>,
) -> Result<(Option<ServiceBuildConfig>, Option<String>), String> {
    match (build, image) {
        (Some(build), None) => {
            if build.repo.trim().is_empty() {
                return Err("has empty build.repo".to_string());
            }
            if build.dockerfile_path.trim().is_empty() {
                return Err("has empty build.dockerfilePath".to_string());
            }
            Ok((Some(build.clone()), None))
        }
        (None, Some(image)) => {
            if image.trim().is_empty() {
                return Err("has empty image".to_string());
            }
            Ok((None, Some(image.clone())))
        }
        (Some(_), Some(_)) => Err("must set either `build` or `image`, not both".to_string()),
        (None, None) => Err("must set either `build` or `image`".to_string()),
    }
}

fn strip_jsonc_comments(input: &str) -> Result<String, String> {
    enum State {
        Normal,
        InString(char),
        InLineComment,
        InBlockComment,
    }

    let mut output = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();
    let mut state = State::Normal;
    let mut escaped = false;

    while let Some(ch) = chars.next() {
        match state {
            State::Normal => {
                if ch == '"' || ch == '\'' {
                    state = State::InString(ch);
                    output.push(ch);
                    escaped = false;
                    continue;
                }

                if ch == '/' {
                    match chars.peek().copied() {
                        Some('/') => {
                            chars.next();
                            state = State::InLineComment;
                            continue;
                        }
                        Some('*') => {
                            chars.next();
                            state = State::InBlockComment;
                            continue;
                        }
                        _ => {}
                    }
                }

                output.push(ch);
            }
            State::InString(quote) => {
                output.push(ch);
                if escaped {
                    escaped = false;
                } else if ch == '\\' {
                    escaped = true;
                } else if ch == quote {
                    state = State::Normal;
                }
            }
            State::InLineComment => {
                if ch == '\n' {
                    output.push('\n');
                    state = State::Normal;
                }
            }
            State::InBlockComment => {
                if ch == '*' && matches!(chars.peek(), Some('/')) {
                    chars.next();
                    state = State::Normal;
                } else if ch == '\n' {
                    output.push('\n');
                }
            }
        }
    }

    match state {
        State::InBlockComment => Err("unterminated block comment in maestro.jsonc".to_string()),
        _ => Ok(output),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_config() -> &'static str {
        r#"{
          "services": {
            "service-1": {
              "name": "Service One",
              "build": {
                "repo": "https://example.com/org/repo.git",
                "dockerfilePath": "Dockerfile"
              },
              "deploy": {
                "healthcheckPath": "/_healthy"
              }
            }
          }
        }"#
    }

    #[test]
    fn parse_config_supports_jsonc() {
        let parsed = parse_config(sample_config()).expect("should parse");
        assert_eq!(parsed.services.len(), 1);
    }

    #[test]
    fn patch_endpoint_adds_http_when_missing() {
        let endpoint = patch_endpoint("127.0.0.1:3000").expect("should build endpoint");
        assert_eq!(endpoint, "http://127.0.0.1:3000/api/services/patch");
    }

    #[test]
    fn service_payload_uses_map_key_as_service_id() {
        let parsed = parse_config(sample_config()).expect("should parse");
        let template = parsed
            .services
            .get("service-1")
            .expect("service should exist");
        let payload = service_payload("service-1", template).expect("payload should build");
        assert_eq!(payload.id, "service-1");
        assert_eq!(payload.name, "Service One");
        assert_eq!(
            payload.build.as_ref().map(|build| build.repo.as_str()),
            Some("https://example.com/org/repo.git"),
        );
    }
}
