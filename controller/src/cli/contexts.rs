use std::{
    collections::BTreeMap,
    io::{self, Write},
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

const CONTEXTS_FILE_ENV: &str = "MAESTRO_CONTEXTS_FILE";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Context {
    pub host: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ContextsFile {
    #[serde(default)]
    active: Option<String>,
    #[serde(default)]
    contexts: BTreeMap<String, Context>,
}

pub fn set_context(name: Option<&str>, host: Option<&str>) -> Result<()> {
    let path = contexts_path()?;
    let name = match name {
        Some(name) => name.trim().to_string(),
        None => prompt_required("Context name")?,
    };
    let host = match host {
        Some(host) => host.trim().to_string(),
        None => prompt_required("Maestro API host")?,
    };
    let normalized_host = set_context_at(&path, &name, &host)?;
    println!(
        "[maestro]: set context `{}` -> {normalized_host}",
        name.trim()
    );
    Ok(())
}

pub fn use_context(name: Option<&str>) -> Result<()> {
    let path = contexts_path()?;
    let name = match name {
        Some(name) => name.trim().to_string(),
        None => prompt_select_context(&path)?,
    };
    use_context_at(&path, &name)?;
    println!("[maestro]: active context set to `{name}`");
    Ok(())
}

pub fn remove_context(name: &str) -> Result<()> {
    let path = contexts_path()?;
    remove_context_at(&path, name)?;
    println!("[maestro]: removed context `{}`", name.trim());
    Ok(())
}

pub fn list_contexts() -> Result<()> {
    let path = contexts_path()?;
    let config = load_contexts(&path)?;
    if config.contexts.is_empty() {
        println!("[maestro]: no contexts configured");
        return Ok(());
    }

    for (name, context) in config.contexts {
        let marker = if config.active.as_deref() == Some(name.as_str()) {
            "*"
        } else {
            " "
        };
        println!("{marker} {name:<24} {}", context.host);
    }
    Ok(())
}

pub fn active_host() -> Result<String> {
    let path = contexts_path()?;
    active_host_at(&path)
}

pub fn active_token() -> Result<Option<String>> {
    let path = contexts_path()?;
    let config = load_contexts(&path)?;
    let Some(active) = config.active.as_deref() else {
        return Ok(None);
    };
    Ok(config.contexts.get(active).and_then(|c| c.token.clone()))
}

pub fn build_http_client() -> Result<reqwest::Client> {
    let mut builder = reqwest::Client::builder();
    if let Some(token) = active_token()? {
        let value =
            reqwest::header::HeaderValue::from_str(&format!("Bearer {token}")).map_err(|err| {
                Error::invalid_config(format!("invalid token in active context: {err}"))
            })?;
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(reqwest::header::AUTHORIZATION, value);
        builder = builder.default_headers(headers);
    }
    builder
        .build()
        .map_err(|err| Error::internal(format!("failed to build http client: {err}")))
}

pub fn save_active_token(token: &str) -> Result<()> {
    let path = contexts_path()?;
    let mut config = load_contexts(&path)?;
    let active = config.active.clone().ok_or_else(no_active_context)?;
    let context = config.contexts.get_mut(&active).ok_or_else(|| {
        Error::invalid_config(format!(
            "active context `{active}` is not present in contexts file"
        ))
    })?;
    context.token = Some(token.to_string());
    save_contexts(&path, &config)
}

pub fn set_context_at(path: &Path, name: &str, host: &str) -> Result<String> {
    let name = validate_context_name(name)?;
    let host = normalize_base_url(host)?;
    let mut config = load_contexts(path)?;
    let existing_token = config.contexts.get(&name).and_then(|c| c.token.clone());
    config.contexts.insert(
        name.clone(),
        Context {
            host: host.clone(),
            token: existing_token,
        },
    );
    if config.active.is_none() {
        config.active = Some(name);
    }
    save_contexts(path, &config)?;
    Ok(host)
}

pub fn use_context_at(path: &Path, name: &str) -> Result<()> {
    let name = validate_context_name(name)?;
    let mut config = load_contexts(path)?;
    if !config.contexts.contains_key(&name) {
        return Err(Error::not_found(format!("context `{name}` does not exist")));
    }
    config.active = Some(name);
    save_contexts(path, &config)
}

pub fn remove_context_at(path: &Path, name: &str) -> Result<()> {
    let name = validate_context_name(name)?;
    let mut config = load_contexts(path)?;
    if config.contexts.remove(&name).is_none() {
        return Err(Error::not_found(format!("context `{name}` does not exist")));
    }
    if config.active.as_deref() == Some(name.as_str()) {
        config.active = None;
    }
    save_contexts(path, &config)
}

pub fn active_host_at(path: &Path) -> Result<String> {
    let config = load_contexts(path)?;
    let active = config.active.as_deref().ok_or_else(no_active_context)?;
    let context = config.contexts.get(active).ok_or_else(|| {
        Error::invalid_config(format!(
            "active context `{active}` is not present in contexts file"
        ))
    })?;
    Ok(context.host.clone())
}

pub fn normalize_base_url(host: &str) -> Result<String> {
    let host = host.trim();
    if host.is_empty() {
        return Err(Error::invalid_input("context host cannot be empty"));
    }

    let candidate = if host.starts_with("http://") || host.starts_with("https://") {
        host.to_string()
    } else {
        format!("http://{host}")
    };

    let url = reqwest::Url::parse(&candidate)
        .map_err(|err| Error::invalid_input(format!("invalid context host: {err}")))?;
    if url.host_str().is_none() {
        return Err(Error::invalid_input("context host must include a hostname"));
    }
    if url.path() != "/" || url.query().is_some() || url.fragment().is_some() {
        return Err(Error::invalid_input(
            "context host must be an origin URL without path, query, or fragment",
        ));
    }

    Ok(url.as_str().trim_end_matches('/').to_string())
}

fn contexts_path() -> Result<PathBuf> {
    if let Some(path) = std::env::var_os(CONTEXTS_FILE_ENV) {
        return Ok(PathBuf::from(path));
    }

    let home = std::env::var_os("HOME").ok_or_else(|| {
        Error::invalid_input(format!(
            "HOME is not set; set {CONTEXTS_FILE_ENV} to choose a contexts file"
        ))
    })?;
    Ok(PathBuf::from(home)
        .join(".config")
        .join("maestro")
        .join("contexts.json"))
}

fn load_contexts(path: &Path) -> Result<ContextsFile> {
    if !path.exists() {
        return Ok(ContextsFile::default());
    }

    let raw = std::fs::read_to_string(path)
        .map_err(|err| Error::internal(format!("failed to read {}: {err}", path.display())))?;
    if raw.trim().is_empty() {
        return Ok(ContextsFile::default());
    }
    serde_json::from_str(&raw)
        .map_err(|err| Error::invalid_config(format!("failed to parse {}: {err}", path.display())))
}

fn save_contexts(path: &Path, config: &ContextsFile) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|err| {
            Error::internal(format!(
                "failed to create contexts directory {}: {err}",
                parent.display()
            ))
        })?;
    }

    let raw = serde_json::to_string_pretty(config)
        .map_err(|err| Error::internal(format!("failed to encode contexts file: {err}")))?;
    std::fs::write(path, format!("{raw}\n"))
        .map_err(|err| Error::internal(format!("failed to write {}: {err}", path.display())))
}

fn validate_context_name(name: &str) -> Result<String> {
    let name = name.trim();
    if name.is_empty() {
        return Err(Error::invalid_input("context name cannot be empty"));
    }
    if !name
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.'))
    {
        return Err(Error::invalid_input(
            "context name may only contain letters, numbers, '.', '-', and '_'",
        ));
    }
    Ok(name.to_string())
}

fn prompt_required(label: &str) -> Result<String> {
    print!("{label}: ");
    io::stdout()
        .flush()
        .map_err(|err| Error::internal(format!("failed to write prompt: {err}")))?;

    let mut value = String::new();
    io::stdin()
        .read_line(&mut value)
        .map_err(|err| Error::internal(format!("failed to read prompt input: {err}")))?;
    let value = value.trim().to_string();
    if value.is_empty() {
        return Err(Error::invalid_input(format!("{label} is required")));
    }
    Ok(value)
}

fn prompt_select_context(path: &Path) -> Result<String> {
    let config = load_contexts(path)?;
    if config.contexts.is_empty() {
        return Err(Error::not_found(
            "no contexts configured; run `maestro contexts set <name> <host>` first",
        ));
    }
    let names: Vec<String> = config.contexts.keys().cloned().collect();
    let starting_cursor = config
        .active
        .as_deref()
        .and_then(|active| names.iter().position(|name| name == active))
        .unwrap_or(0);
    inquire::Select::new("Select a context", names)
        .with_starting_cursor(starting_cursor)
        .prompt()
        .map_err(|err| Error::external(format!("context selection prompt failed: {err}")))
}

fn no_active_context() -> Error {
    Error::invalid_input(
        "no active context; run `maestro contexts set <name> <host>` or `maestro contexts use <name>`",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_context_path(name: &str) -> PathBuf {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("time")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "maestro-contexts-{name}-{}-{unique}.json",
            std::process::id()
        ))
    }

    #[test]
    fn set_context_normalizes_host_and_sets_first_active() {
        let path = temp_context_path("set");
        let host = set_context_at(&path, "dev", "127.0.0.1:3001").expect("set context");

        assert_eq!(host, "http://127.0.0.1:3001");
        assert_eq!(
            active_host_at(&path).expect("active host"),
            "http://127.0.0.1:3001"
        );

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn use_context_switches_active_context() {
        let path = temp_context_path("use");
        set_context_at(&path, "dev", "http://127.0.0.1:3001").expect("set dev");
        set_context_at(&path, "prod", "https://maestro.example.com").expect("set prod");
        use_context_at(&path, "prod").expect("use prod");

        assert_eq!(
            active_host_at(&path).expect("active host"),
            "https://maestro.example.com"
        );

        let _ = std::fs::remove_file(path);
    }
}
