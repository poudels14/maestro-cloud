use std::collections::BTreeMap;
use std::fs;
use std::io::Write;
use std::net::IpAddr;
use std::path::PathBuf;

use kernel_api::SecretValue;
use serde::{Deserialize, Serialize};

use crate::CliError;

const CONTEXTS_FILE_ENV: &str = "MAESTRO_CONTEXTS_FILE";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Context {
    pub(crate) host: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) token: Option<SecretValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) ca_certificate_pem: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ContextListing {
    pub(crate) name: String,
    pub(crate) host: String,
    pub(crate) active: bool,
}

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ContextsFile {
    #[serde(default)]
    active: Option<String>,
    #[serde(default)]
    contexts: BTreeMap<String, Context>,
}

pub(crate) struct ContextStore {
    path: PathBuf,
}

impl ContextStore {
    pub(crate) fn from_environment() -> Result<Self, CliError> {
        let path = match std::env::var_os(CONTEXTS_FILE_ENV) {
            Some(path) => PathBuf::from(path),
            None => {
                let home = std::env::var_os("HOME").ok_or_else(|| {
                    CliError::invalid_input(format!(
                        "HOME is not set; set {CONTEXTS_FILE_ENV} to choose a contexts file"
                    ))
                })?;
                PathBuf::from(home)
                    .join(".config")
                    .join("maestro")
                    .join("contexts.json")
            }
        };
        Ok(Self::at(path))
    }

    pub(crate) fn at(path: PathBuf) -> Self {
        Self { path }
    }

    pub(crate) fn set(
        &self,
        name: &str,
        host: &str,
        ca_certificate_pem: Option<String>,
    ) -> Result<String, CliError> {
        let name = validate_name(name)?;
        let host = normalize_origin(host)?;
        let mut contexts = self.load()?;
        let token = contexts
            .contexts
            .get(&name)
            .and_then(|context| context.token.clone());
        let ca_certificate_pem = ca_certificate_pem.or_else(|| {
            contexts
                .contexts
                .get(&name)
                .and_then(|context| context.ca_certificate_pem.clone())
        });
        contexts.contexts.insert(
            name.clone(),
            Context {
                host: host.clone(),
                token,
                ca_certificate_pem,
            },
        );
        if contexts.active.is_none() {
            contexts.active = Some(name);
        }
        self.save(&contexts)?;
        Ok(host)
    }

    pub(crate) fn use_context(&self, name: &str) -> Result<(), CliError> {
        let name = validate_name(name)?;
        let mut contexts = self.load()?;
        if !contexts.contexts.contains_key(&name) {
            return Err(CliError::not_found(format!(
                "context `{name}` does not exist"
            )));
        }
        contexts.active = Some(name);
        self.save(&contexts)
    }

    pub(crate) fn remove(&self, name: &str) -> Result<(), CliError> {
        let name = validate_name(name)?;
        let mut contexts = self.load()?;
        if contexts.contexts.remove(&name).is_none() {
            return Err(CliError::not_found(format!(
                "context `{name}` does not exist"
            )));
        }
        if contexts.active.as_deref() == Some(name.as_str()) {
            contexts.active = None;
        }
        self.save(&contexts)
    }

    pub(crate) fn list(&self) -> Result<Vec<ContextListing>, CliError> {
        let contexts = self.load()?;
        Ok(contexts
            .contexts
            .into_iter()
            .map(|(name, context)| ContextListing {
                active: contexts.active.as_deref() == Some(name.as_str()),
                name,
                host: context.host,
            })
            .collect())
    }

    pub(crate) fn active_name(&self) -> Result<String, CliError> {
        let contexts = self.load()?;
        let active = contexts.active.ok_or_else(no_active_context)?;
        if !contexts.contexts.contains_key(&active) {
            return Err(CliError::invalid_contexts(format!(
                "active context `{active}` is not present"
            )));
        }
        Ok(active)
    }

    pub(crate) fn active(&self) -> Result<Context, CliError> {
        let contexts = self.load()?;
        let active = contexts.active.ok_or_else(no_active_context)?;
        contexts.contexts.get(&active).cloned().ok_or_else(|| {
            CliError::invalid_contexts(format!("active context `{active}` is not present"))
        })
    }

    pub(crate) fn save_active_token(&self, token: SecretValue) -> Result<(), CliError> {
        let mut contexts = self.load()?;
        let active = contexts.active.clone().ok_or_else(no_active_context)?;
        let context = contexts.contexts.get_mut(&active).ok_or_else(|| {
            CliError::invalid_contexts(format!("active context `{active}` is not present"))
        })?;
        context.token = Some(token);
        self.save(&contexts)
    }

    fn load(&self) -> Result<ContextsFile, CliError> {
        let encoded = match crate::private_document::read_private(&self.path, "contexts file") {
            Ok(encoded) => encoded,
            Err(CliError::NotFound { .. }) => return Ok(ContextsFile::default()),
            Err(error) => return Err(error),
        };
        if encoded.iter().all(|byte| byte.is_ascii_whitespace()) {
            return Ok(ContextsFile::default());
        }
        serde_json::from_slice(&encoded).map_err(|source| {
            CliError::json(
                format!("failed to decode contexts file {}", self.path.display()),
                source,
            )
        })
    }

    fn save(&self, contexts: &ContextsFile) -> Result<(), CliError> {
        let parent = self.path.parent().ok_or_else(|| {
            CliError::invalid_input("contexts file path must have a parent directory")
        })?;
        fs::create_dir_all(parent).map_err(|source| {
            CliError::io(
                format!("failed to create contexts directory {}", parent.display()),
                source,
            )
        })?;
        let mut temporary = tempfile::NamedTempFile::new_in(parent).map_err(|source| {
            CliError::io(
                format!(
                    "failed to create temporary contexts file in {}",
                    parent.display()
                ),
                source,
            )
        })?;
        set_private_permissions(temporary.as_file())?;
        serde_json::to_writer_pretty(temporary.as_file_mut(), contexts)
            .map_err(|source| CliError::json("failed to encode contexts file", source))?;
        temporary
            .as_file_mut()
            .write_all(b"\n")
            .map_err(|source| CliError::io("failed to finish contexts file", source))?;
        temporary
            .as_file()
            .sync_all()
            .map_err(|source| CliError::io("failed to sync contexts file", source))?;
        temporary.persist(&self.path).map_err(|error| {
            CliError::io(
                format!("failed to replace contexts file {}", self.path.display()),
                error.error,
            )
        })?;
        Ok(())
    }
}

pub(crate) fn normalize_origin(host: &str) -> Result<String, CliError> {
    let host = host.trim();
    if host.is_empty() {
        return Err(CliError::invalid_input("context host cannot be empty"));
    }
    let has_explicit_scheme = host.starts_with("http://") || host.starts_with("https://");
    let candidate = if has_explicit_scheme {
        host.to_string()
    } else {
        format!("http://{host}")
    };
    let mut url = reqwest::Url::parse(&candidate)
        .map_err(|error| CliError::invalid_input(format!("invalid context host: {error}")))?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(CliError::invalid_input(
            "context host must use http or https",
        ));
    }
    if url.host_str().is_none() {
        return Err(CliError::invalid_input(
            "context host must include a hostname",
        ));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(CliError::invalid_input(
            "context host must not contain credentials",
        ));
    }
    if url.path() != "/" || url.query().is_some() || url.fragment().is_some() {
        return Err(CliError::invalid_input(
            "context host must be an origin without path, query, or fragment",
        ));
    }
    if url.scheme() == "http"
        && !is_loopback_host(url.host_str())
        && !is_tailnet_magic_dns_host(url.host_str())
        && !is_private_operator_ip(url.host_str())
    {
        if has_explicit_scheme {
            return Err(CliError::invalid_input(
                "HTTP context hosts must use loopback, a private or Tailscale IP, or a Tailscale \
                 .ts.net MagicDNS name",
            ));
        }
        url.set_scheme("https")
            .map_err(|()| CliError::invalid_input("failed to secure context host"))?;
    }
    Ok(url.as_str().trim_end_matches('/').to_string())
}

fn validate_name(name: &str) -> Result<String, CliError> {
    let name = name.trim();
    if name.is_empty() {
        return Err(CliError::invalid_input("context name cannot be empty"));
    }
    if !name
        .chars()
        .all(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.'))
    {
        return Err(CliError::invalid_input(
            "context name may contain only letters, numbers, '.', '-', and '_'",
        ));
    }
    Ok(name.to_string())
}

fn is_tailnet_magic_dns_host(host: Option<&str>) -> bool {
    host.and_then(|host| host.strip_suffix(".ts.net"))
        .is_some_and(|prefix| !prefix.is_empty() && !prefix.ends_with('.'))
}

fn is_loopback_host(host: Option<&str>) -> bool {
    host.is_some_and(|host| {
        host.eq_ignore_ascii_case("localhost")
            || parse_host_ip(host).is_some_and(|address| address.is_loopback())
    })
}

fn is_private_operator_ip(host: Option<&str>) -> bool {
    host.and_then(parse_host_ip)
        .is_some_and(|address| match address {
            IpAddr::V4(address) => address.is_private() || is_tailscale_ipv4(address),
            IpAddr::V6(address) => address.is_unique_local(),
        })
}

fn parse_host_ip(host: &str) -> Option<IpAddr> {
    host.trim_start_matches('[')
        .trim_end_matches(']')
        .parse()
        .ok()
}

fn is_tailscale_ipv4(address: std::net::Ipv4Addr) -> bool {
    let [first, second, _, _] = address.octets();
    first == 100 && second & 0b1100_0000 == 64
}

fn no_active_context() -> CliError {
    CliError::invalid_input("no active context; run `maestro contexts set <name> <host>` first")
}

#[cfg(unix)]
fn set_private_permissions(file: &fs::File) -> Result<(), CliError> {
    use std::os::unix::fs::PermissionsExt;

    file.set_permissions(fs::Permissions::from_mode(0o600))
        .map_err(|source| CliError::io("failed to protect temporary contexts file", source))
}

#[cfg(not(unix))]
fn set_private_permissions(_file: &fs::File) -> Result<(), CliError> {
    Ok(())
}
