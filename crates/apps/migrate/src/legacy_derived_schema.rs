use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct LegacyControllerStats {
    pub(crate) reported_at_ms: i64,
    pub(crate) version: String,
    pub(crate) uptime_ms: u64,
    pub(crate) spool: LegacySpoolStats,
    pub(crate) sinks: Vec<LegacySinkStats>,
    pub(crate) dead_letters: LegacyDeadLetterStats,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct LegacySpoolStats {
    pub(crate) row_count: u64,
    pub(crate) high_watermark: i64,
    pub(crate) oldest_entry_at_ms: Option<i64>,
    pub(crate) database_bytes: u64,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct LegacySinkStats {
    pub(crate) id: String,
    pub(crate) cursor: i64,
    pub(crate) pending_entries: u64,
    pub(crate) oldest_pending_at_ms: Option<i64>,
    pub(crate) last_success_at_ms: Option<i64>,
    pub(crate) last_error_at_ms: Option<i64>,
    pub(crate) last_error: Option<String>,
    pub(crate) consecutive_failures: u64,
    pub(crate) last_cursor_advance_at_ms: Option<i64>,
    #[serde(default)]
    pub(crate) filtered_entries: u64,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct LegacyDeadLetterStats {
    pub(crate) count: u64,
    pub(crate) capacity: u64,
    pub(crate) payload_bytes: u64,
    pub(crate) latest_at_ms: Option<i64>,
    pub(crate) latest_status: Option<u16>,
    pub(crate) latest_error: Option<String>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct LegacyNodeDisk {
    pub(crate) name: String,
    pub(crate) mount_point: String,
    pub(crate) total_bytes: u64,
    pub(crate) available_bytes: u64,
    pub(crate) file_system: String,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct LegacyUnschedulableReplica {
    pub(crate) service_id: String,
    pub(crate) deployment_id: String,
    pub(crate) replica_index: u32,
    pub(crate) reason: String,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct LegacyDnsRecordSet {
    pub(crate) service_id: String,
    pub(crate) stable_fqdn: String,
    #[serde(default)]
    pub(crate) via_ingress: bool,
    pub(crate) addresses: Vec<String>,
    pub(crate) replica_records: Vec<(String, String)>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct LegacyTraefikServiceIdentity {
    pub(crate) service_id: String,
    pub(crate) deployment_id: String,
    #[serde(default)]
    pub(crate) node_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct LegacyImageHolder {
    pub(crate) image: String,
    pub(crate) node_id: String,
    pub(crate) available_at_ms: i64,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct LegacyRbacReady {
    pub(crate) version: u8,
    pub(crate) initialized_by: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct LegacyDerivedSummary {
    pub(crate) controller_stats: usize,
    pub(crate) disk_snapshots: usize,
    pub(crate) dns_record_sets: usize,
    pub(crate) image_holders: usize,
    pub(crate) traefik_service_mappings: usize,
    pub(crate) unschedulable_replicas: usize,
    pub(crate) rbac_version: Option<u8>,
}
