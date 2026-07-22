use std::fmt::{Display, Formatter};

use kernel_api::{ClusterId, NodeId, RequestId, ResourceKind, ResourceName};

use crate::StoreError;

/// An exact persistence key under the canonical `/maestro/` namespace.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StoreKey(String);

impl StoreKey {
    /// Returns the backend key text.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub(crate) fn from_backend(bytes: &[u8]) -> Result<Self, StoreError> {
        let value = std::str::from_utf8(bytes).map_err(|error| StoreError::Contract {
            message: format!("store backend returned a non-UTF-8 key: {error}"),
        })?;
        if !value.starts_with("/maestro/clusters/") {
            Err(StoreError::Contract {
                message: format!(
                    "store backend returned a key outside the Maestro namespace: {value}"
                ),
            })
        } else {
            Ok(Self(value.to_string()))
        }
    }
}

impl Display for StoreKey {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// A key prefix under the canonical `/maestro/` namespace.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StorePrefix(String);

impl StorePrefix {
    /// Returns the backend prefix text, always ending in `/`.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Display for StorePrefix {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Centralized key construction scoped to one cluster.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Keyspace {
    root: String,
}

impl Keyspace {
    /// Creates the canonical keyspace for a validated cluster identity.
    pub fn new(cluster_id: &ClusterId) -> Self {
        Self {
            root: format!("/maestro/clusters/{cluster_id}"),
        }
    }

    /// Prefix containing every resource, control key, and liveness record.
    pub fn cluster(&self) -> StorePrefix {
        StorePrefix(format!("{}/", self.root))
    }

    /// Prefix containing every typed resource in the cluster.
    pub fn resources(&self) -> StorePrefix {
        self.prefix("resources")
    }

    /// Prefix containing every resource of one open kind.
    pub fn resource_kind(&self, kind: &ResourceKind) -> StorePrefix {
        self.prefix(&format!("resources/{kind}"))
    }

    /// Exact key of one typed resource.
    pub fn resource(&self, kind: &ResourceKind, id: &ResourceName) -> StoreKey {
        self.key(&format!("resources/{kind}/{id}"))
    }

    /// Leader campaign key used by the controller runtime.
    pub fn leader(&self) -> StoreKey {
        self.key("control/leader")
    }

    /// Session-bound liveness key for one node.
    pub fn node_liveness(&self, node_id: &NodeId) -> StoreKey {
        self.key(&format!("liveness/nodes/{node_id}"))
    }

    /// Exact internal command used to coordinate one node's staged upgrade.
    pub fn node_upgrade_command(&self, node_id: &NodeId) -> StoreKey {
        self.key(&format!("control/node-upgrades/{node_id}"))
    }

    /// Deduplication claim for one externally supplied request identity.
    pub fn request_claim(&self, request_id: &RequestId) -> StoreKey {
        self.key(&format!("control/requests/{request_id}"))
    }

    /// Scheduler-owned generation fence advanced with every assignment-set mutation.
    pub fn scheduler_generation(&self) -> StoreKey {
        self.key("control/scheduler-generation")
    }

    /// Latest successfully fenced scheduler failures exposed to operators.
    pub fn scheduler_observation(&self) -> StoreKey {
        self.key("observations/scheduler")
    }

    /// Root watched by Traefik's cluster-scoped dynamic configuration provider.
    pub fn traefik(&self) -> StorePrefix {
        self.prefix("integrations/traefik")
    }

    /// Exact dynamic-provider key below the cluster's Traefik root.
    pub fn traefik_entry(&self, relative: &str) -> Result<StoreKey, StoreError> {
        validate_relative_path(relative)?;
        Ok(self.key(&format!("integrations/traefik/{relative}")))
    }

    /// Dynamic-provider subtree below the cluster's Traefik root.
    pub fn traefik_prefix(&self, relative: &str) -> Result<StorePrefix, StoreError> {
        validate_relative_path(relative)?;
        Ok(self.prefix(&format!("integrations/traefik/{relative}")))
    }

    fn key(&self, suffix: &str) -> StoreKey {
        StoreKey(format!("{}/{suffix}", self.root))
    }

    fn prefix(&self, suffix: &str) -> StorePrefix {
        StorePrefix(format!("{}/{suffix}/", self.root))
    }
}

fn validate_relative_path(relative: &str) -> Result<(), StoreError> {
    if relative.is_empty()
        || relative.starts_with('/')
        || relative.ends_with('/')
        || relative
            .split('/')
            .any(|segment| segment.is_empty() || matches!(segment, "." | ".."))
    {
        Err(StoreError::Contract {
            message: format!("invalid Traefik provider path: {relative}"),
        })
    } else {
        Ok(())
    }
}
