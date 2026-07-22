use std::collections::{BTreeMap, BTreeSet};

use serde::de::DeserializeOwned;

use crate::legacy_crypto::{LegacyCryptoError, LegacyDecryptor};
use crate::legacy_schema::{LegacyDeployment, LegacyServiceInfo};
use crate::{LegacyEntry, LegacySnapshot};

const SERVICES_PREFIX: &str = "/maetro/services/";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LegacyServiceCatalog {
    pub(crate) services: BTreeMap<String, LegacyServiceState>,
    pub(crate) unclaimed: Vec<LegacyEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LegacyServiceState {
    pub(crate) info: LegacyServiceInfo,
    pub(crate) next_history_index: u64,
    pub(crate) deployments: Vec<LegacyDeploymentRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LegacyDeploymentRecord {
    pub(crate) history_index: u64,
    pub(crate) deployment: LegacyDeployment,
    pub(crate) data: LegacyDeploymentData,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct LegacyDeploymentData {
    pub(crate) deploy_environment: BTreeMap<String, String>,
    pub(crate) deploy_secrets: BTreeMap<String, String>,
    pub(crate) build_environment: BTreeMap<String, String>,
    pub(crate) build_secrets: BTreeMap<String, String>,
    pub(crate) preview_environment: BTreeMap<String, String>,
}

impl LegacyServiceCatalog {
    pub(crate) fn decode(
        snapshot: &LegacySnapshot,
        master_secret: &str,
    ) -> Result<Self, LegacyServiceError> {
        let decryptor = LegacyDecryptor::new(master_secret)?;
        let mut builders = BTreeMap::<String, ServiceBuilder>::new();
        let mut unclaimed = Vec::new();

        for entry in snapshot.entries() {
            let Some(key) = classify_key(entry.key())? else {
                unclaimed.push(entry.clone());
                continue;
            };
            let service_id = key.service_id().to_owned();
            let builder = builders.entry(service_id).or_default();
            match key {
                ServiceKey::Info { .. } => {
                    builder.info = Some(decode_json(entry)?);
                }
                ServiceKey::HistoryCounter { .. } => {
                    builder.next_history_index = Some(decode_counter(entry)?);
                }
                ServiceKey::History { index, .. } => {
                    builder.history.insert(index, decode_json(entry)?);
                }
                ServiceKey::Sidecar {
                    deployment_id,
                    kind,
                    ..
                } => {
                    builder.sidecars.insert(
                        (deployment_id, kind),
                        decryptor.decode_json::<BTreeMap<String, String>>(entry)?,
                    );
                }
            }
        }

        let services = builders
            .into_iter()
            .map(|(service_id, builder)| {
                builder
                    .finish(&service_id)
                    .map(|service| (service_id, service))
            })
            .collect::<Result<_, _>>()?;
        Ok(Self {
            services,
            unclaimed,
        })
    }
}

#[derive(Default)]
struct ServiceBuilder {
    info: Option<LegacyServiceInfo>,
    next_history_index: Option<u64>,
    history: BTreeMap<u64, LegacyDeployment>,
    sidecars: BTreeMap<(String, SidecarKind), BTreeMap<String, String>>,
}

impl ServiceBuilder {
    fn finish(mut self, service_id: &str) -> Result<LegacyServiceState, LegacyServiceError> {
        let info = self.info.ok_or_else(|| LegacyServiceError::MissingInfo {
            service_id: service_id.to_owned(),
        })?;
        validate_service_identity(service_id, &info.config.id, "info")?;

        for (expected, actual) in (0_u64..).zip(self.history.keys().copied()) {
            if actual != expected {
                return Err(LegacyServiceError::MissingHistoryIndex {
                    service_id: service_id.to_owned(),
                    expected,
                    actual,
                });
            }
        }
        let expected_next_index = u64::try_from(self.history.len()).map_err(|_| {
            LegacyServiceError::HistoryIndexOverflow {
                service_id: service_id.to_owned(),
            }
        })?;
        let next_history_index =
            self.next_history_index
                .ok_or_else(|| LegacyServiceError::MissingHistoryCounter {
                    service_id: service_id.to_owned(),
                })?;
        if next_history_index != expected_next_index {
            return Err(LegacyServiceError::InvalidHistoryCounter {
                service_id: service_id.to_owned(),
                expected: expected_next_index,
                actual: next_history_index,
            });
        }

        let mut deployment_ids = BTreeSet::new();
        let mut deployments = Vec::with_capacity(self.history.len());
        for (history_index, deployment) in self.history {
            validate_service_identity(service_id, &deployment.config.id, "deployment history")?;
            if !deployment_ids.insert(deployment.id.clone()) {
                return Err(LegacyServiceError::DuplicateDeployment {
                    service_id: service_id.to_owned(),
                    deployment_id: deployment.id,
                });
            }
            let data = LegacyDeploymentData {
                deploy_environment: take_sidecar(
                    &mut self.sidecars,
                    &deployment.id,
                    SidecarKind::DeployEnvironment,
                ),
                deploy_secrets: take_sidecar(
                    &mut self.sidecars,
                    &deployment.id,
                    SidecarKind::DeploySecrets,
                ),
                build_environment: take_sidecar(
                    &mut self.sidecars,
                    &deployment.id,
                    SidecarKind::BuildEnvironment,
                ),
                build_secrets: take_sidecar(
                    &mut self.sidecars,
                    &deployment.id,
                    SidecarKind::BuildSecrets,
                ),
                preview_environment: take_sidecar(
                    &mut self.sidecars,
                    &deployment.id,
                    SidecarKind::PreviewEnvironment,
                ),
            };
            deployments.push(LegacyDeploymentRecord {
                history_index,
                deployment,
                data,
            });
        }
        if let Some(((deployment_id, _), _)) = self.sidecars.first_key_value() {
            return Err(LegacyServiceError::OrphanSidecar {
                service_id: service_id.to_owned(),
                deployment_id: deployment_id.clone(),
            });
        }
        Ok(LegacyServiceState {
            info,
            next_history_index,
            deployments,
        })
    }
}

fn take_sidecar(
    sidecars: &mut BTreeMap<(String, SidecarKind), BTreeMap<String, String>>,
    deployment_id: &str,
    kind: SidecarKind,
) -> BTreeMap<String, String> {
    sidecars
        .remove(&(deployment_id.to_owned(), kind))
        .unwrap_or_default()
}

fn decode_json<Value>(entry: &LegacyEntry) -> Result<Value, LegacyServiceError>
where
    Value: DeserializeOwned,
{
    serde_json::from_slice(entry.value()).map_err(|error| LegacyServiceError::InvalidJson {
        key: entry.key().to_owned(),
        message: error.to_string(),
    })
}

fn decode_counter(entry: &LegacyEntry) -> Result<u64, LegacyServiceError> {
    let value = std::str::from_utf8(entry.value())
        .ok()
        .and_then(|value| value.trim().parse().ok())
        .ok_or_else(|| LegacyServiceError::InvalidCounter {
            key: entry.key().to_owned(),
        })?;
    Ok(value)
}

fn validate_service_identity(
    key_service_id: &str,
    value_service_id: &str,
    record: &'static str,
) -> Result<(), LegacyServiceError> {
    if key_service_id == value_service_id {
        Ok(())
    } else {
        Err(LegacyServiceError::ServiceIdentityMismatch {
            record,
            key_service_id: key_service_id.to_owned(),
            value_service_id: value_service_id.to_owned(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ServiceKey {
    Info {
        service_id: String,
    },
    HistoryCounter {
        service_id: String,
    },
    History {
        service_id: String,
        index: u64,
    },
    Sidecar {
        service_id: String,
        deployment_id: String,
        kind: SidecarKind,
    },
}

impl ServiceKey {
    fn service_id(&self) -> &str {
        match self {
            Self::Info { service_id }
            | Self::HistoryCounter { service_id }
            | Self::History { service_id, .. }
            | Self::Sidecar { service_id, .. } => service_id,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum SidecarKind {
    DeployEnvironment,
    DeploySecrets,
    BuildEnvironment,
    BuildSecrets,
    PreviewEnvironment,
}

fn classify_key(key: &str) -> Result<Option<ServiceKey>, LegacyServiceError> {
    let Some(remainder) = key.strip_prefix(SERVICES_PREFIX) else {
        return Ok(None);
    };
    let Some((service_id, suffix)) = remainder.split_once('/') else {
        return Ok(None);
    };
    if service_id.is_empty() {
        return Err(LegacyServiceError::MalformedKey {
            key: key.to_owned(),
        });
    }
    match suffix {
        "info" => Ok(Some(ServiceKey::Info {
            service_id: service_id.to_owned(),
        })),
        "deployments/history-next-index" => Ok(Some(ServiceKey::HistoryCounter {
            service_id: service_id.to_owned(),
        })),
        _ => classify_history_or_sidecar(key, service_id, suffix),
    }
}

fn classify_history_or_sidecar(
    key: &str,
    service_id: &str,
    suffix: &str,
) -> Result<Option<ServiceKey>, LegacyServiceError> {
    if let Some(raw_index) = suffix.strip_prefix("deployments/history/") {
        if raw_index.len() != 10 || !raw_index.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(LegacyServiceError::MalformedKey {
                key: key.to_owned(),
            });
        }
        let index = raw_index
            .parse()
            .map_err(|_| LegacyServiceError::MalformedKey {
                key: key.to_owned(),
            })?;
        return Ok(Some(ServiceKey::History {
            service_id: service_id.to_owned(),
            index,
        }));
    }

    let Some((deployment_id, data_suffix)) = suffix.split_once('/') else {
        return Ok(None);
    };
    if deployment_id.is_empty() {
        return Err(LegacyServiceError::MalformedKey {
            key: key.to_owned(),
        });
    }
    let kind = match data_suffix {
        "deploy/env" => SidecarKind::DeployEnvironment,
        "deploy/secrets" => SidecarKind::DeploySecrets,
        "build/env" => SidecarKind::BuildEnvironment,
        "build/secrets" => SidecarKind::BuildSecrets,
        "preview/env" => SidecarKind::PreviewEnvironment,
        _ => return Ok(None),
    };
    Ok(Some(ServiceKey::Sidecar {
        service_id: service_id.to_owned(),
        deployment_id: deployment_id.to_owned(),
        kind,
    }))
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum LegacyServiceError {
    #[error(transparent)]
    Crypto(#[from] LegacyCryptoError),
    #[error("legacy service key is malformed: {key}")]
    MalformedKey { key: String },
    #[error("legacy JSON at `{key}` is invalid: {message}")]
    InvalidJson { key: String, message: String },
    #[error("legacy counter at `{key}` is not an unsigned integer")]
    InvalidCounter { key: String },
    #[error("legacy service `{service_id}` has state but no info record")]
    MissingInfo { service_id: String },
    #[error(
        "legacy {record} key names service `{key_service_id}` but its payload names \
         `{value_service_id}`"
    )]
    ServiceIdentityMismatch {
        record: &'static str,
        key_service_id: String,
        value_service_id: String,
    },
    #[error("legacy service `{service_id}` history index overflowed")]
    HistoryIndexOverflow { service_id: String },
    #[error("legacy service `{service_id}` has no history counter")]
    MissingHistoryCounter { service_id: String },
    #[error(
        "legacy service `{service_id}` history skips index {expected} before observed index {actual}"
    )]
    MissingHistoryIndex {
        service_id: String,
        expected: u64,
        actual: u64,
    },
    #[error("legacy service `{service_id}` history counter is {actual}, expected {expected}")]
    InvalidHistoryCounter {
        service_id: String,
        expected: u64,
        actual: u64,
    },
    #[error("legacy service `{service_id}` repeats deployment `{deployment_id}`")]
    DuplicateDeployment {
        service_id: String,
        deployment_id: String,
    },
    #[error(
        "legacy service `{service_id}` has encrypted sidecars for unknown deployment \
         `{deployment_id}`"
    )]
    OrphanSidecar {
        service_id: String,
        deployment_id: String,
    },
}
