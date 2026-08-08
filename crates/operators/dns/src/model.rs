use std::collections::BTreeSet;

use kernel_api::{Assignment, ClusterId, DnsRecord, DnsRecordId, NodeId, ReplicaState, Service};

/// Comma-separated short DNS names published for an annotated Service.
pub const DNS_ALIASES_ANNOTATION: &str = "dns.maestro.dev/aliases";

/// DNS publication policy applied to every generated service record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DnsSettings {
    /// Cache lifetime written into generated authoritative record sets.
    pub ttl_secs: u32,
}

/// Complete typed snapshot consumed by one deterministic DNS pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DnsInput {
    /// Cluster label used in the authoritative service domain.
    pub cluster_id: ClusterId,
    /// DNS publication settings.
    pub settings: DnsSettings,
    /// Desired services and their selected deployments.
    pub services: Vec<Service>,
    /// Scheduler-owned addresses for current placements.
    pub assignments: Vec<Assignment>,
    /// Nodes whose session-bound liveness records are currently present.
    pub live_nodes: BTreeSet<NodeId>,
    /// Exact readiness observations for assignment slots.
    pub replicas: Vec<ReplicaState>,
    /// Existing authoritative record resources.
    pub records: Vec<DnsRecord>,
}

/// Atomic DNS resource mutations from one projection pass.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DnsPlan {
    /// Missing operator-managed records to create.
    pub create_records: Vec<DnsRecord>,
    /// Existing records whose desired set changed.
    pub replace_records: Vec<DnsRecord>,
    /// Stale operator-managed records to delete.
    pub delete_records: Vec<DnsRecordId>,
}
