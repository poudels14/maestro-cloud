mod artifact_archives;
mod automation;
mod cluster;
mod cluster_commands;
mod deployment_commands;
mod deployments;
mod exec;
mod firewall_dry_run;
mod firewall_policies;
mod logs;
mod metrics;
mod network_observations;
mod observations;
mod service_commands;
mod service_diff;
mod service_rollout_validation;
mod service_rollouts;
mod services;
mod system;
mod upgrades;
mod webhook_commands;

use axum::Router;
use axum::middleware;

use crate::AppState;
use crate::auth::{AuthPolicy, require_node, require_operator};

pub(crate) fn router(state: AppState, auth: AuthPolicy) -> Router {
    let protected = Router::new()
        .merge(automation::router())
        .merge(artifact_archives::router())
        .merge(cluster::router())
        .merge(cluster_commands::router())
        .merge(deployment_commands::router())
        .merge(deployments::router())
        .merge(exec::router())
        .merge(firewall_dry_run::router())
        .merge(firewall_policies::router())
        .merge(logs::router())
        .merge(metrics::router())
        .merge(network_observations::router())
        .merge(observations::router())
        .merge(service_commands::router())
        .merge(service_rollouts::router())
        .merge(services::router())
        .merge(upgrades::router())
        .merge(webhook_commands::router())
        .route_layer(middleware::from_fn_with_state(
            auth.clone(),
            require_operator,
        ));
    let node = logs::node_router()
        .merge(metrics::node_router())
        .merge(exec::node_router())
        .route_layer(middleware::from_fn_with_state(auth, require_node));
    Router::new()
        .merge(system::router())
        .merge(protected)
        .merge(node)
        .with_state(state)
}
