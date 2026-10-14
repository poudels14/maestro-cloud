use std::collections::BTreeMap;
use std::path::PathBuf;

use anyhow::Context;
use clap::Subcommand;
use serde_json::{Value, json};

use crate::client::Endpoint;

const DEFAULT_API: &str = "http://127.0.0.1:3000";
const CONTEXTS_FILE_ENV: &str = "MAESTRO_CONTEXTS_FILE";

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
    }
    Ok(())
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
