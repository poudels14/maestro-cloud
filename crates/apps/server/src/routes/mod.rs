mod artifact_archives;
mod automation;
mod cluster;
mod cluster_commands;
mod deployment_commands;
mod deployments;
mod firewall_dry_run;
mod firewall_policies;
mod logs;
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
use crate::auth::{AuthPolicy, require_operator};

pub(crate) fn router(state: AppState, auth: AuthPolicy) -> Router {
    let protected = Router::new()
        .merge(automation::router())
        .merge(artifact_archives::router())
        .merge(cluster::router())
        .merge(cluster_commands::router())
        .merge(deployment_commands::router())
        .merge(deployments::router())
        .merge(firewall_dry_run::router())
        .merge(firewall_policies::router())
        .merge(logs::router())
        .merge(network_observations::router())
        .merge(observations::router())
        .merge(service_commands::router())
        .merge(service_rollouts::router())
        .merge(services::router())
        .merge(upgrades::router())
        .merge(webhook_commands::router())
        .route_layer(middleware::from_fn_with_state(auth, require_operator));
    Router::new()
        .merge(system::router())
        .merge(protected)
        .with_state(state)
}
