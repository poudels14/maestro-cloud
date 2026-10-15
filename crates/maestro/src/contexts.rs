use std::collections::BTreeMap;
use std::path::PathBuf;

use std::time::Duration;

use anyhow::Context;
use clap::Subcommand;
use maestro_control::{ApiAuth, MAX_TOKEN_TTL};
use serde_json::{Value, json};

use crate::client::Endpoint;

const DEFAULT_API: &str = "http://127.0.0.1:3000";
const CONTEXTS_FILE_ENV: &str = "MAESTRO_CONTEXTS_FILE";
const JWT_SECRET_ENV: &str = "JWT_SECRET_KEY";

#[derive(Subcommand)]
pub enum ContextCommand {
    Set {
        name: String,
        #[arg(long)]
        api: String,
        #[arg(long)]
        token: Option<String>,
    },
    Use {
        name: String,
    },
    Ls,
    #[command(alias = "rm")]
    Remove {
        name: String,
    },
    Login {
        #[arg(long, value_parser = crate::parse_ttl, default_value = "24h")]
        ttl: u64,
    },
}

struct ContextFile {
    current: Option<String>,
    contexts: BTreeMap<String, (String, Option<String>)>,
}

pub fn run(command: ContextCommand) -> anyhow::Result<()> {
    let mut file = ContextFile::load()?;
    match command {
        ContextCommand::Set { name, api, token } => {
            file.contexts.insert(name.clone(), (api, token));
            file.current.get_or_insert(name.clone());
            file.save()?;
            println!("saved context {name}");
        }
        ContextCommand::Use { name } => {
            anyhow::ensure!(file.contexts.contains_key(&name), "unknown context {name}");
            file.current = Some(name.clone());
            file.save()?;
            println!("using context {name}");
        }
        ContextCommand::Ls => file.contexts.iter().for_each(|(name, (api, _))| {
            let marker = match file.current.as_deref() == Some(name.as_str()) {
                true => "*",
                false => " ",
            };
            println!("{marker} {name:<20} {api}");
        }),
        ContextCommand::Remove { name } => {
            anyhow::ensure!(
                file.contexts.remove(&name).is_some(),
                "unknown context {name}"
            );
            if file.current.as_deref() == Some(name.as_str()) {
                file.current = None;
            }
            file.save()?;
            println!("removed context {name}");
        }
        ContextCommand::Login { ttl } => {
            let name = file
                .current
                .clone()
                .context("no active context; run `maestro context set` first")?;
            let secret = jwt_secret()?;
            let lifetime = Duration::from_secs(ttl).min(MAX_TOKEN_TTL);
            let token =
                ApiAuth::new(secret.as_bytes()).issue(&format!("maestro-cli/{name}"), lifetime);
            if let Some((_, saved)) = file.contexts.get_mut(&name) {
                *saved = Some(token);
            }
            file.save()?;
            println!(
                "saved a token to context {name} (expires in {}h)",
                lifetime.as_secs() / 3600
            );
        }
    }
    Ok(())
}

pub fn jwt_secret() -> anyhow::Result<String> {
    match std::env::var(JWT_SECRET_ENV) {
        Ok(secret) => Ok(secret),
        Err(std::env::VarError::NotPresent) => {
            Ok(rpassword::prompt_password(format!("{JWT_SECRET_ENV}: "))?)
        }
        Err(std::env::VarError::NotUnicode(_)) => {
            anyhow::bail!("{JWT_SECRET_ENV} is not valid UTF-8")
        }
    }
}

pub fn save(name: &str, api: &str, token: Option<String>) -> anyhow::Result<()> {
    let mut file = ContextFile::load()?;
    file.contexts
        .insert(name.to_string(), (api.to_string(), token));
    file.current = Some(name.to_string());
    file.save()
}

pub fn resolve(api: Option<String>, token: Option<String>) -> anyhow::Result<Endpoint> {
    let file = ContextFile::load()?;
    let current = file
        .current
        .as_ref()
        .and_then(|name| file.contexts.get(name))
        .cloned();
    let (context_api, context_token) = current.unwrap_or((DEFAULT_API.to_string(), None));
    Ok(Endpoint {
        api: api.unwrap_or(context_api).trim_end_matches('/').to_string(),
        token: token.or(context_token),
    })
}

impl ContextFile {
    fn load() -> anyhow::Result<Self> {
        let path = path()?;
        let value = match std::fs::read(&path) {
            Ok(bytes) => serde_json::from_slice::<Value>(&bytes)
                .with_context(|| format!("{} is not valid JSON", path.display()))?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => json!({}),
            Err(error) => Err(error)?,
        };
        let contexts = value["contexts"]
            .as_object()
            .into_iter()
            .flatten()
            .map(|(name, context)| {
                let api = context["host"].as_str().unwrap_or(DEFAULT_API).to_string();
                let token = context["token"].as_str().map(str::to_string);
                (name.clone(), (api, token))
            })
            .collect();
        Ok(Self {
            current: value["active"].as_str().map(str::to_string),
            contexts,
        })
    }

    fn save(&self) -> anyhow::Result<()> {
        let path = path()?;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let contexts = self
            .contexts
            .iter()
            .map(|(name, (api, token))| {
                let context = match token {
                    Some(token) => json!({ "host": api, "token": token }),
                    None => json!({ "host": api }),
                };
                (name.clone(), context)
            })
            .collect::<serde_json::Map<_, _>>();
        let encoded = serde_json::to_vec_pretty(&json!({
            "active": self.current,
            "contexts": contexts,
        }))?;
        write_private(&path, &encoded)
    }
}

fn path() -> anyhow::Result<PathBuf> {
    match std::env::var_os(CONTEXTS_FILE_ENV) {
        Some(path) => Ok(PathBuf::from(path)),
        None => {
            let home = std::env::var_os("HOME").context("HOME is not set")?;
            Ok(PathBuf::from(home).join(".config/maestro/contexts.json"))
        }
    }
}

fn write_private(path: &std::path::Path, bytes: &[u8]) -> anyhow::Result<()> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(bytes)?;
    Ok(())
}
