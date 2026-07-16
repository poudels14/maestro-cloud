pub const SERVICES_ROOT: &str = "/maetro/services";
pub const SERVICES_PREFIX: &str = "/maetro/services/";
pub const SERVICE_HISTORY_NEXT_INDEX_SUFFIX: &str = "/deployments/history-next-index";

pub fn service_prefix(service_id: &str) -> String {
    format!("{SERVICES_ROOT}/{service_id}/")
}

pub fn service_info_key(service_id: &str) -> String {
    format!("{SERVICES_ROOT}/{service_id}/info")
}

pub const INGRESS_BLOCKLIST_PREFIX: &str = "/maetro/cluster/ingress-blocklist/";

pub fn ingress_blocklist_ip_key(address: &str) -> String {
    format!("{INGRESS_BLOCKLIST_PREFIX}{address}")
}

#[allow(dead_code)]
pub fn service_active_deployment_key(service_id: &str) -> String {
    format!("{SERVICES_ROOT}/{service_id}/deployments/active")
}

pub fn service_deployment_history_key(service_id: &str, index: usize) -> String {
    format!("{SERVICES_ROOT}/{service_id}/deployments/history/{index:010}")
}

pub fn service_history_next_index_key(service_id: &str) -> String {
    format!("{SERVICES_ROOT}/{service_id}{SERVICE_HISTORY_NEXT_INDEX_SUFFIX}")
}

pub fn service_deployment_history_prefix(service_id: &str) -> String {
    format!("{SERVICES_ROOT}/{service_id}/deployments/history/")
}

pub fn service_id_from_history_key(key: &str) -> Option<String> {
    let remainder = key.strip_prefix(&format!("{SERVICES_ROOT}/"))?;
    let (service_id, _) = remainder.split_once("/deployments/history/")?;
    Some(service_id.to_string())
}

pub fn replica_state_key(service_id: &str, deployment_id: &str, replica_index: u32) -> String {
    format!("{SERVICES_ROOT}/{service_id}/replicas/{deployment_id}/{replica_index}")
}

pub fn replica_states_prefix(service_id: &str, deployment_id: &str) -> String {
    format!("{SERVICES_ROOT}/{service_id}/replicas/{deployment_id}/")
}

pub fn deployment_prefix(service_id: &str, deployment_id: &str) -> String {
    format!("{SERVICES_ROOT}/{service_id}/{deployment_id}/")
}

pub fn deployment_build_env_key(service_id: &str, deployment_id: &str) -> String {
    format!("{SERVICES_ROOT}/{service_id}/{deployment_id}/build/env")
}

pub fn deployment_build_secrets_key(service_id: &str, deployment_id: &str) -> String {
    format!("{SERVICES_ROOT}/{service_id}/{deployment_id}/build/secrets")
}

pub fn deployment_deploy_env_key(service_id: &str, deployment_id: &str) -> String {
    format!("{SERVICES_ROOT}/{service_id}/{deployment_id}/deploy/env")
}

pub fn deployment_deploy_secrets_key(service_id: &str, deployment_id: &str) -> String {
    format!("{SERVICES_ROOT}/{service_id}/{deployment_id}/deploy/secrets")
}

pub fn deployment_preview_env_key(service_id: &str, deployment_id: &str) -> String {
    format!("{SERVICES_ROOT}/{service_id}/{deployment_id}/preview/env")
}

pub const SYSTEM_UPGRADE_REQUEST_KEY: &str = "/maetro/system/upgrade-request";
pub const SYSTEM_RESTART_REQUEST_KEY: &str = "/maetro/system/restart-request";
pub const CLUSTER_FREEZE_KEY: &str = "/maetro/system/cluster-freeze";
pub const CLUSTER_UPGRADE_KEY: &str = "/maetro/cluster/upgrade/current";
pub const SLACK_WEBHOOKS_KEY: &str = "/maetro/cluster/config/webhooks/slack";

pub fn system_upgrade_request_key(node_id: Option<&str>) -> String {
    node_id.map_or_else(
        || SYSTEM_UPGRADE_REQUEST_KEY.to_string(),
        |node_id| format!("{SYSTEM_UPGRADE_REQUEST_KEY}/{node_id}"),
    )
}

pub fn system_restart_request_key(node_id: Option<&str>) -> String {
    node_id.map_or_else(
        || SYSTEM_RESTART_REQUEST_KEY.to_string(),
        |node_id| format!("{SYSTEM_RESTART_REQUEST_KEY}/{node_id}"),
    )
}

pub fn service_id_from_info_key(key: &str) -> Option<String> {
    let remainder = key.strip_prefix(&format!("{SERVICES_ROOT}/"))?;
    let (service_id, suffix) = remainder.split_once('/')?;
    if suffix == "info" {
        Some(service_id.to_string())
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn system_requests_are_node_local_in_cluster_mode() {
        assert_eq!(
            system_upgrade_request_key(Some("node-a")),
            "/maetro/system/upgrade-request/node-a"
        );
        assert_eq!(
            system_restart_request_key(Some("node-b")),
            "/maetro/system/restart-request/node-b"
        );
        assert_eq!(system_upgrade_request_key(None), SYSTEM_UPGRADE_REQUEST_KEY);
    }

    #[test]
    fn ingress_blocklist_is_cluster_runtime_state() {
        assert_eq!(
            ingress_blocklist_ip_key("203.0.113.9"),
            "/maetro/cluster/ingress-blocklist/203.0.113.9"
        );
    }
}
