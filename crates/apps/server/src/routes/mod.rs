mod cluster;
mod deployment_commands;
mod deployments;
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
        .merge(deployment_commands::router())
        .merge(deployments::router())
        .merge(service_commands::router())
        .merge(services::router())
        .route_layer(middleware::from_fn_with_state(auth, require_operator));
    Router::new()
        .merge(system::router())
        .merge(protected)
        .with_state(state)
}
