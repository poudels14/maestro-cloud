use std::path::{Path, PathBuf};

use cluster::ClusterConfig;
use kernel_api::{ClusterId, NodeId, NodeRole};

use crate::DaemonError;

/// Long-lived responsibility hosted by one daemon process.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DaemonRole {
    /// Node-local networking and, where allowed, workload management.
    Agent,
    /// Leader-elected cluster operators on a control-plane-capable node.
    Controller,
}

impl std::fmt::Display for DaemonRole {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Agent => formatter.write_str("agent role"),
            Self::Controller => formatter.write_str("controller role"),
        }
    }
}

/// Exact inputs handed to one role factory during startup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoleSpec {
    /// Role being started.
    pub role: DaemonRole,
    /// Stable cluster identity shared by every role.
    pub cluster_id: ClusterId,
    /// Stable node identity owned by this process.
    pub node_id: NodeId,
    /// Public topology capability assigned to the node.
    pub node_role: NodeRole,
    /// Whether the agent may accept user workload assignments.
    pub workload_enabled: bool,
    /// Role-exclusive persistence directory.
    pub data_directory: PathBuf,
}

/// Validated, deterministic role wiring for one daemon instance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DaemonPlan {
    cluster: ClusterConfig,
    node_id: NodeId,
    data_directory: PathBuf,
    roles: Vec<RoleSpec>,
}

impl DaemonPlan {
    /// Validates topology and selects roles without touching local state.
    pub fn new(
        cluster: ClusterConfig,
        node_id: NodeId,
        data_directory: PathBuf,
    ) -> Result<Self, DaemonError> {
        cluster.preflight()?;
        if data_directory.as_os_str().is_empty() {
            return Err(DaemonError::EmptyDataDirectory);
        }
        let node = cluster
            .nodes
            .get(&node_id)
            .ok_or_else(|| DaemonError::UnknownNode {
                node_id: node_id.clone(),
            })?;
        let mut roles = vec![RoleSpec {
            role: DaemonRole::Agent,
            cluster_id: cluster.cluster_id.clone(),
            node_id: node_id.clone(),
            node_role: node.role,
            workload_enabled: node.role.runs_workloads(),
            data_directory: data_directory.join("agent"),
        }];
        if node.role.is_control_plane() {
            roles.push(RoleSpec {
                role: DaemonRole::Controller,
                cluster_id: cluster.cluster_id.clone(),
                node_id: node_id.clone(),
                node_role: node.role,
                workload_enabled: false,
                data_directory: data_directory.join("controller"),
            });
        }
        Ok(Self {
            cluster,
            node_id,
            data_directory,
            roles,
        })
    }

    /// Returns the validated cluster configuration shared with role adapters.
    pub fn cluster(&self) -> &ClusterConfig {
        &self.cluster
    }

    /// Returns the local stable node identity.
    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    /// Returns the daemon persistence root.
    pub fn data_directory(&self) -> &Path {
        &self.data_directory
    }

    /// Returns roles in dependency-safe startup order.
    pub fn roles(&self) -> &[RoleSpec] {
        &self.roles
    }
}
