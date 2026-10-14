use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use serde::Deserialize;
use tokio::io::AsyncWriteExt;

const HELPER_TIMEOUT: Duration = Duration::from_secs(30);
const DOCKER_HUB_KEY: &str = "https://index.docker.io/v1/";
const DOCKER_HUB_HOSTS: [&str; 4] = [
    "docker.io",
    "index.docker.io",
    "registry-1.docker.io",
    "registry.hub.docker.com",
];

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RegistryCredentials {
    pub username: String,
    pub secret: String,
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DockerConfig {
    #[serde(default)]
    auths: BTreeMap<String, AuthEntry>,
    #[serde(default)]
    cred_helpers: BTreeMap<String, String>,
    #[serde(default)]
    creds_store: Option<String>,
}

#[derive(Default, Deserialize)]
struct AuthEntry {
    #[serde(default)]
    auth: Option<String>,
    #[serde(default)]
    username: Option<String>,
    #[serde(default)]
    password: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct HelperResponse {
    username: String,
    secret: String,
}

pub async fn registry_credentials(host: &str) -> Result<Option<RegistryCredentials>, String> {
    let config = match tokio::fs::read(config_path()).await {
        Ok(bytes) => serde_json::from_slice::<DockerConfig>(&bytes).map_err(|error| {
            format!("invalid Docker config {}: {error}", config_path().display())
        })?,
        Err(_) => DockerConfig::default(),
    };
    let registry = registry_key(host);
    let helper = config
        .cred_helpers
        .iter()
        .find(|(key, _)| registry_key(key) == registry)
        .map(|(_, helper)| helper.clone());
    let stored = config
        .auths
        .iter()
        .find(|(key, _)| registry_key(key) == registry)
        .and_then(|(_, entry)| stored_credentials(entry));
    match (helper, stored, config.creds_store) {
        (Some(helper), _, _) => helper_credentials(&helper, &registry).await,
        (None, Some(credentials), _) => Ok(Some(credentials)),
        (None, None, Some(store)) => helper_credentials(&store, &registry).await,
        (None, None, None) => Ok(None),
    }
}

fn config_path() -> PathBuf {
    let directory = std::env::var_os("DOCKER_CONFIG")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            std::env::var_os("HOME")
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("/root"))
                .join(".docker")
        });
    directory.join("config.json")
}

fn registry_key(host: &str) -> String {
    let trimmed = host
        .trim()
        .trim_start_matches("https://")
        .trim_start_matches("http://");
    let host = trimmed
        .split('/')
        .next()
        .unwrap_or(trimmed)
        .to_ascii_lowercase();
    match DOCKER_HUB_HOSTS.contains(&host.as_str()) {
        true => DOCKER_HUB_KEY.to_string(),
        false => host,
    }
}

fn stored_credentials(entry: &AuthEntry) -> Option<RegistryCredentials> {
    let decoded = entry
        .auth
        .as_deref()
        .and_then(|auth| BASE64.decode(auth.trim()).ok())
        .and_then(|bytes| String::from_utf8(bytes).ok())
        .and_then(|pair| {
            pair.split_once(':')
                .map(|(username, secret)| (username.to_string(), secret.to_string()))
        });
    let explicit = entry.username.clone().zip(entry.password.clone());
    decoded
        .or(explicit)
        .filter(|(username, secret)| !username.is_empty() || !secret.is_empty())
        .map(|(username, secret)| RegistryCredentials { username, secret })
}

async fn helper_credentials(
    helper: &str,
    registry: &str,
) -> Result<Option<RegistryCredentials>, String> {
    let program = format!("docker-credential-{helper}");
    let lookup = async {
        let mut child = tokio::process::Command::new(&program)
            .arg("get")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .map_err(|error| format!("{program}: {error}"))?;
        if let Some(mut stdin) = child.stdin.take() {
            stdin
                .write_all(registry.as_bytes())
                .await
                .map_err(|error| format!("{program}: {error}"))?;
        }
        let output = child
            .wait_with_output()
            .await
            .map_err(|error| format!("{program}: {error}"))?;
        let stdout = String::from_utf8_lossy(&output.stdout);
        match output.status.success() {
            true => serde_json::from_str::<HelperResponse>(&stdout)
                .map(|response| {
                    Some(RegistryCredentials {
                        username: response.username,
                        secret: response.secret,
                    })
                })
                .map_err(|error| format!("{program} returned invalid JSON: {error}")),
            false if stdout.contains("credentials not found") => Ok(None),
            false => Err(format!(
                "{program} failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            )),
        }
    };
    tokio::time::timeout(HELPER_TIMEOUT, lookup)
        .await
        .unwrap_or_else(|_| Err(format!("{program} timed out")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_registry_keys() {
        assert_eq!(registry_key("https://index.docker.io/v1/"), DOCKER_HUB_KEY);
        assert_eq!(registry_key("registry-1.docker.io"), DOCKER_HUB_KEY);
        assert_eq!(registry_key("https://GHCR.io"), "ghcr.io");
        assert_eq!(
            registry_key("123.dkr.ecr.us-east-1.amazonaws.com/v2/"),
            "123.dkr.ecr.us-east-1.amazonaws.com"
        );
    }

    #[test]
    fn decodes_stored_auths() {
        let entry = AuthEntry {
            auth: Some(BASE64.encode("robot:s3cret:with:colons")),
            ..AuthEntry::default()
        };
        assert_eq!(
            stored_credentials(&entry),
            Some(RegistryCredentials {
                username: "robot".to_string(),
                secret: "s3cret:with:colons".to_string(),
            })
        );
        assert_eq!(stored_credentials(&AuthEntry::default()), None);
    }

    #[tokio::test]
    async fn reads_credentials_from_the_docker_config() {
        let directory = tempfile::tempdir().unwrap();
        std::fs::write(
            directory.path().join("config.json"),
            serde_json::json!({
                "auths": { "https://ghcr.io": { "auth": BASE64.encode("me:token") } },
                "credHelpers": { "private.example": "maestro-missing-helper" }
            })
            .to_string(),
        )
        .unwrap();
        unsafe { std::env::set_var("DOCKER_CONFIG", directory.path()) };
        assert_eq!(
            registry_credentials("ghcr.io").await.unwrap(),
            Some(RegistryCredentials {
                username: "me".to_string(),
                secret: "token".to_string(),
            })
        );
        assert_eq!(registry_credentials("quay.io").await.unwrap(), None);
        assert!(registry_credentials("private.example").await.is_err());
    }
}
