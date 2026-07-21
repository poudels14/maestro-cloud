use axum::extract::{Path, State};
use axum::routing::get;
use axum::{Json, Router};
use kernel_api::{
    BuiltinKind, DnsRecord, DnsRecordId, FirewallPolicy, FirewallPolicyId, IngressRoute,
    IngressRouteId, NodeFirewall, NodeFirewallId, NodeNetwork, NodeNetworkId, ServiceId,
    TrafficGeneration, TrafficGenerationId,
};

use super::deployments::{ensure_service, parse_service_id};
use crate::{ApiError, AppState, resource};

pub(super) fn router() -> Router<AppState> {
    Router::new()
        .route("/api/cluster/networks", get(list_networks))
        .route("/api/cluster/networks/{network_id}", get(get_network))
        .route("/api/cluster/node-firewalls", get(list_node_firewalls))
        .route(
            "/api/cluster/node-firewalls/{firewall_id}",
            get(get_node_firewall),
        )
        .route("/api/cluster/dns-records", get(list_dns_records))
        .route("/api/cluster/dns-records/{record_id}", get(get_dns_record))
        .route("/api/firewall/policies", get(list_firewall_policies))
        .route(
            "/api/firewall/policies/{policy_id}",
            get(get_firewall_policy),
        )
        .route(
            "/api/services/{service_id}/routes",
            get(list_ingress_routes),
        )
        .route(
            "/api/services/{service_id}/routes/{route_id}",
            get(get_ingress_route),
        )
        .route(
            "/api/services/{service_id}/traffic-generations",
            get(list_traffic_generations),
        )
        .route(
            "/api/services/{service_id}/traffic-generations/{generation_id}",
            get(get_traffic_generation),
        )
}

async fn list_networks(State(state): State<AppState>) -> Result<Json<Vec<NodeNetwork>>, ApiError> {
    Ok(Json(
        resource::list(&state, BuiltinKind::NodeNetwork).await?,
    ))
}

async fn get_network(
    State(state): State<AppState>,
    Path(network_id): Path<String>,
) -> Result<Json<NodeNetwork>, ApiError> {
    let network_id =
        NodeNetworkId::new(network_id).map_err(|error| ApiError::bad_request(error.to_string()))?;
    Ok(Json(
        resource::get(&state, BuiltinKind::NodeNetwork, network_id).await?,
    ))
}

async fn list_node_firewalls(
    State(state): State<AppState>,
) -> Result<Json<Vec<NodeFirewall>>, ApiError> {
    Ok(Json(
        resource::list(&state, BuiltinKind::NodeFirewall).await?,
    ))
}

async fn get_node_firewall(
    State(state): State<AppState>,
    Path(firewall_id): Path<String>,
) -> Result<Json<NodeFirewall>, ApiError> {
    let firewall_id = NodeFirewallId::new(firewall_id)
        .map_err(|error| ApiError::bad_request(error.to_string()))?;
    Ok(Json(
        resource::get(&state, BuiltinKind::NodeFirewall, firewall_id).await?,
    ))
}

async fn list_dns_records(State(state): State<AppState>) -> Result<Json<Vec<DnsRecord>>, ApiError> {
    Ok(Json(resource::list(&state, BuiltinKind::DnsRecord).await?))
}

async fn get_dns_record(
    State(state): State<AppState>,
    Path(record_id): Path<String>,
) -> Result<Json<DnsRecord>, ApiError> {
    let record_id =
        DnsRecordId::new(record_id).map_err(|error| ApiError::bad_request(error.to_string()))?;
    Ok(Json(
        resource::get(&state, BuiltinKind::DnsRecord, record_id).await?,
    ))
}

async fn list_firewall_policies(
    State(state): State<AppState>,
) -> Result<Json<Vec<FirewallPolicy>>, ApiError> {
    Ok(Json(
        resource::list(&state, BuiltinKind::FirewallPolicy).await?,
    ))
}

async fn get_firewall_policy(
    State(state): State<AppState>,
    Path(policy_id): Path<String>,
) -> Result<Json<FirewallPolicy>, ApiError> {
    let policy_id = FirewallPolicyId::new(policy_id)
        .map_err(|error| ApiError::bad_request(error.to_string()))?;
    Ok(Json(
        resource::get(&state, BuiltinKind::FirewallPolicy, policy_id).await?,
    ))
}

async fn list_ingress_routes(
    State(state): State<AppState>,
    Path(service_id): Path<String>,
) -> Result<Json<Vec<IngressRoute>>, ApiError> {
    let service_id = service_scope(&state, service_id).await?;
    let routes: Vec<IngressRoute> = resource::list(&state, BuiltinKind::IngressRoute).await?;
    Ok(Json(
        routes
            .into_iter()
            .filter(|route| route.spec.service_id == service_id)
            .collect(),
    ))
}

async fn get_ingress_route(
    State(state): State<AppState>,
    Path((service_id, route_id)): Path<(String, String)>,
) -> Result<Json<IngressRoute>, ApiError> {
    let service_id = service_scope(&state, service_id).await?;
    let route_id =
        IngressRouteId::new(route_id).map_err(|error| ApiError::bad_request(error.to_string()))?;
    let route: IngressRoute =
        resource::get(&state, BuiltinKind::IngressRoute, route_id.clone()).await?;
    ensure_service_owner(
        &route.spec.service_id,
        &service_id,
        "IngressRoute",
        &route_id,
    )?;
    Ok(Json(route))
}

async fn list_traffic_generations(
    State(state): State<AppState>,
    Path(service_id): Path<String>,
) -> Result<Json<Vec<TrafficGeneration>>, ApiError> {
    let service_id = service_scope(&state, service_id).await?;
    let generations: Vec<TrafficGeneration> =
        resource::list(&state, BuiltinKind::TrafficGeneration).await?;
    Ok(Json(
        generations
            .into_iter()
            .filter(|generation| generation.spec.service_id == service_id)
            .collect(),
    ))
}

async fn get_traffic_generation(
    State(state): State<AppState>,
    Path((service_id, generation_id)): Path<(String, String)>,
) -> Result<Json<TrafficGeneration>, ApiError> {
    let service_id = service_scope(&state, service_id).await?;
    let generation_id = TrafficGenerationId::new(generation_id)
        .map_err(|error| ApiError::bad_request(error.to_string()))?;
    let generation: TrafficGeneration = resource::get(
        &state,
        BuiltinKind::TrafficGeneration,
        generation_id.clone(),
    )
    .await?;
    ensure_service_owner(
        &generation.spec.service_id,
        &service_id,
        "TrafficGeneration",
        &generation_id,
    )?;
    Ok(Json(generation))
}

async fn service_scope(state: &AppState, service_id: String) -> Result<ServiceId, ApiError> {
    let service_id = parse_service_id(service_id)?;
    ensure_service(state, service_id.clone()).await?;
    Ok(service_id)
}

fn ensure_service_owner(
    actual: &ServiceId,
    expected: &ServiceId,
    kind: &str,
    id: &impl std::fmt::Display,
) -> Result<(), ApiError> {
    if actual == expected {
        Ok(())
    } else {
        Err(ApiError::not_found(format!(
            "{kind} `{id}` does not exist for Service `{expected}`"
        )))
    }
}
