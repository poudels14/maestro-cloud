use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

use axum::Json;
use axum::extract::{ConnectInfo, Path, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use maestro_agent::{DiscoveryDenied, DiscoveryState, StatusBoard};
use maestro_core::{DISCOVERY_PORT, ServiceId};
use serde_json::json;

const BIND_RETRY: Duration = Duration::from_secs(2);

#[derive(Clone)]
struct DiscoveryAccess {
    state: DiscoveryState,
    board: StatusBoard,
}

/// Serves `GET /v1/services/{service}` to this node's containers on the bridge gateway. The
/// caller is the service owning the request's source IP and must present that service's key.
pub async fn serve_discovery(
    gateway: IpAddr,
    state: DiscoveryState,
    board: StatusBoard,
) -> anyhow::Result<()> {
    let address = SocketAddr::new(gateway, DISCOVERY_PORT);
    let listener = loop {
        match tokio::net::TcpListener::bind(address).await {
            Ok(listener) => break listener,
            Err(error) => {
                tracing::debug!(%address, %error, "discovery bind failed; retrying");
                tokio::time::sleep(BIND_RETRY).await;
            }
        }
    };
    tracing::info!(%address, "service discovery listening");
    let app = axum::Router::new()
        .route(
            "/v1/services/{service_id}",
            axum::routing::get(service_replicas),
        )
        .with_state(DiscoveryAccess { state, board });
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await?;
    Ok(())
}

async fn service_replicas(
    State(access): State<DiscoveryAccess>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Path(service_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let caller = access
        .board
        .snapshot()
        .into_iter()
        .find(|status| status.ip == Some(peer.ip()))
        .map(|status| status.service_id);
    let key = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));
    let service_id = ServiceId::new(service_id);
    match access.state.lookup(caller.as_ref(), key, &service_id) {
        Ok(replicas) => {
            Json(json!({ "service": service_id, "replicas": replicas })).into_response()
        }
        Err(DiscoveryDenied::Unauthenticated) => (
            StatusCode::UNAUTHORIZED,
            Json(json!({ "error": "unknown caller or invalid MAESTRO_API_KEY" })),
        )
            .into_response(),
        Err(DiscoveryDenied::Forbidden) => (
            StatusCode::FORBIDDEN,
            Json(json!({ "error": format!("{service_id} does not list this service in network.from") })),
        )
            .into_response(),
    }
}
