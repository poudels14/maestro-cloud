pub const SERVICES_ROOT: &str = "/maetro/services";
pub const SERVICES_PREFIX: &str = "/maetro/services/";
pub const SERVICE_HISTORY_NEXT_INDEX_SUFFIX: &str = "/deployments/history-next-index";

pub fn service_prefix(service_id: &str) -> String {
    format!("{SERVICES_ROOT}/{service_id}/")
}

pub fn service_info_key(service_id: &str) -> String {
    format!("{SERVICES_ROOT}/{service_id}/info")
}

#[allow(dead_code)]
pub fn service_active_deployment_key(service_id: &str) -> String {
    format!("{SERVICES_ROOT}/{service_id}/deployments/active")
}

pub fn service_deployment_history_key(service_id: &str, index: usize) -> String {
    format!("{SERVICES_ROOT}/{service_id}/deployments/history/{index}")
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

pub fn service_id_from_info_key(key: &str) -> Option<String> {
    let remainder = key.strip_prefix(&format!("{SERVICES_ROOT}/"))?;
    let (service_id, suffix) = remainder.split_once('/')?;
    if suffix == "info" {
        Some(service_id.to_string())
    } else {
        None
    }
}
