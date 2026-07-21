mod cluster;
mod cluster_commands;
mod deployment_commands;
mod deployments;
mod observations;
mod service_commands;
mod services;
mod system;

use axum::Router;
use axum::middleware;

use crate::AppState;
use crate::auth::{AuthPolicy, require_operator};

pub(crate) fn router(state: AppState, auth: AuthPolicy) -> Router {
    let protected = Router::new()
        .merge(cluster::router())
        .merge(cluster_commands::router())
        .merge(deployment_commands::router())
        .merge(deployments::router())
        .merge(observations::router())
        .merge(service_commands::router())
        .merge(services::router())
        .route_layer(middleware::from_fn_with_state(auth, require_operator));
    Router::new()
        .merge(system::router())
        .merge(protected)
        .with_state(state)
}
