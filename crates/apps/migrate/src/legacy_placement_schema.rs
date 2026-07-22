use serde::Deserialize;

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct LegacyPlacementHistory {
    pub(crate) assignment_id: String,
    pub(crate) service_id: String,
    pub(crate) deployment_id: String,
    pub(crate) replica_index: u32,
    pub(crate) node_id: String,
    pub(crate) cluster_host_ip: String,
    pub(crate) cluster_api_port: u16,
    pub(crate) container_hostname: String,
    pub(crate) started_at_ms: i64,
    pub(crate) ended_at_ms: Option<i64>,
}
