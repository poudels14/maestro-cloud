use std::{
    collections::{BTreeMap, hash_map::DefaultHasher},
    hash::{Hash, Hasher},
    io::ErrorKind,
    path::PathBuf,
    time::Duration,
};

use async_trait::async_trait;
use serde::Deserialize;
use tokio::time::sleep;

use crate::supervisor::SupervisedJobConfig;

use super::ServiceConfigSource;

pub struct FileServiceConfigSource {
    path: PathBuf,
    poll_interval: Duration,
    last_seen_hash: Option<u64>,
}

impl FileServiceConfigSource {
    pub fn new(path: PathBuf, poll_interval: Duration) -> Self {
        Self {
            path,
            poll_interval,
            last_seen_hash: None,
        }
    }

    fn parse_config(contents: &str) -> Result<Vec<SupervisedJobConfig>, String> {
        #[derive(Debug, Clone, Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct RawTaskConfig {
            command: String,
            restart_delay_ms: u64,
            max_restarts: u32,
            shutdown_grace_period_ms: u64,
        }

        #[derive(Debug, Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct ServiceConfigWithName {
            name: String,
            #[serde(flatten)]
            config: RawTaskConfig,
        }

        #[derive(Debug, Deserialize)]
        #[serde(untagged)]
        enum RawConfigFile {
            NamedList(Vec<ServiceConfigWithName>),
            NamedListInObject {
                services: Vec<ServiceConfigWithName>,
            },
            NamedMap {
                services: BTreeMap<String, RawTaskConfig>,
            },
            FlatNamedMap(BTreeMap<String, RawTaskConfig>),
        }

        let parsed = json5::from_str::<RawConfigFile>(contents)
            .map_err(|err| format!("failed to parse JSON/JSONC config: {err}"))?;

        let services = match parsed {
            RawConfigFile::NamedList(list) => list
                .into_iter()
                .map(|service| SupervisedJobConfig {
                    id: service.name.clone(),
                    name: service.name,
                    command: service.config.command,
                    restart_delay_ms: service.config.restart_delay_ms,
                    max_restarts: service.config.max_restarts,
                    shutdown_grace_period_ms: service.config.shutdown_grace_period_ms,
                    logs_dir: None,
                })
                .collect::<Vec<_>>(),
            RawConfigFile::NamedListInObject { services } => services
                .into_iter()
                .map(|service| SupervisedJobConfig {
                    id: service.name.clone(),
                    name: service.name,
                    command: service.config.command,
                    restart_delay_ms: service.config.restart_delay_ms,
                    max_restarts: service.config.max_restarts,
                    shutdown_grace_period_ms: service.config.shutdown_grace_period_ms,
                    logs_dir: None,
                })
                .collect::<Vec<_>>(),
            RawConfigFile::NamedMap { services } | RawConfigFile::FlatNamedMap(services) => {
                services
                    .into_iter()
                    .map(|(name, config)| SupervisedJobConfig {
                        id: name.clone(),
                        name,
                        command: config.command,
                        restart_delay_ms: config.restart_delay_ms,
                        max_restarts: config.max_restarts,
                        shutdown_grace_period_ms: config.shutdown_grace_period_ms,
                        logs_dir: None,
                    })
                    .collect::<Vec<_>>()
            }
        };

        Ok(services)
    }

    fn hash_contents(contents: &str) -> u64 {
        let mut hasher = DefaultHasher::new();
        contents.hash(&mut hasher);
        hasher.finish()
    }
}

#[async_trait]
impl ServiceConfigSource for FileServiceConfigSource {
    async fn next_snapshot(&mut self) -> Result<Vec<SupervisedJobConfig>, String> {
        const MISSING_FILE_HASH: u64 = u64::MAX;

        loop {
            let contents = match tokio::fs::read_to_string(&self.path).await {
                Ok(contents) => contents,
                Err(err) if err.kind() == ErrorKind::NotFound => {
                    if self.last_seen_hash == Some(MISSING_FILE_HASH) {
                        sleep(self.poll_interval).await;
                        continue;
                    }

                    self.last_seen_hash = Some(MISSING_FILE_HASH);
                    return Ok(Vec::new());
                }
                Err(err) => {
                    return Err(format!("failed reading {}: {err}", self.path.display()));
                }
            };

            let file_hash = Self::hash_contents(&contents);
            if self.last_seen_hash == Some(file_hash) {
                sleep(self.poll_interval).await;
                continue;
            }

            self.last_seen_hash = Some(file_hash);
            return Self::parse_config(&contents);
        }
    }
}
