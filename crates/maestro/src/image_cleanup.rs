use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use maestro_core::{DeploymentStatus, NodeState, retained_releases};
use maestro_runtime::{ContainerdRuntime, normalize_image_reference};
use maestro_store::{Consensus, StateMachine};
use serde_json::{Value, json};

use crate::node_service::{NodeClients, NodeRequest, call_nodes};

const CLEANUP_INTERVAL: Duration = Duration::from_secs(60 * 60);
const GRACE_SECONDS: i64 = 60 * 60;
const BUILT_IMAGE_PREFIX: &str = "maestro.local/";
const LIVE: [DeploymentStatus; 5] = [
    DeploymentStatus::Queued,
    DeploymentStatus::Building,
    DeploymentStatus::Deploying,
    DeploymentStatus::Ready,
    DeploymentStatus::Draining,
];

pub async fn run_image_cleanup(consensus: Arc<Consensus>, clients: NodeClients) {
    let mut ticker = tokio::time::interval(CLEANUP_INTERVAL);
    ticker.tick().await;
    loop {
        ticker.tick().await;
        let state = consensus.state();
        let maintaining = state
            .maintenance_run()
            .ok()
            .flatten()
            .is_some_and(|run| !run.is_terminal());
        match (consensus.is_leader() && !maintaining, keep_list(state)) {
            (false, _) => {}
            (true, Err(error)) => {
                tracing::warn!(%error, "image cleanup could not read cluster state")
            }
            (true, Ok(keep)) => {
                let nodes = state
                    .nodes()
                    .unwrap_or_default()
                    .into_iter()
                    .filter(|node| node.state != NodeState::Removed)
                    .collect();
                for (_, node_name, reply) in call_nodes(
                    &clients,
                    nodes,
                    NodeRequest::PruneImages { keep: keep.clone() },
                )
                .await
                {
                    match reply {
                        Ok(pruned) => {
                            tracing::info!(node = %node_name, removed = %pruned["removed"], "pruned container images")
                        }
                        Err(status) => {
                            tracing::warn!(node = %node_name, error = %status.message(), "image cleanup failed")
                        }
                    }
                }
            }
        }
    }
}

pub async fn prune_images(runtime: &ContainerdRuntime, keep: &[String]) -> Result<Value, String> {
    let images = runtime.images().await.map_err(|error| error.to_string())?;
    let in_use = runtime
        .container_images()
        .await
        .map_err(|error| error.to_string())?;
    let system = [
        maestro_agent::TAILSCALE_IMAGE.to_string(),
        maestro_agent::CLOUDFLARED_IMAGE.to_string(),
        maestro_agent::TRAEFIK_IMAGE.to_string(),
        crate::control_plane::admin_image(),
    ];
    let kept = keep
        .iter()
        .chain(&in_use)
        .chain(&system)
        .map(|image| normalize_image_reference(image))
        .collect::<BTreeSet<_>>();
    let now = chrono::Utc::now().timestamp();
    let mut removed = 0;
    for image in removable(&images, &kept, now) {
        match runtime.remove_image(&image).await {
            Ok(()) => removed += 1,
            Err(error) => tracing::warn!(%image, %error, "could not remove image"),
        }
    }
    Ok(json!({ "removed": removed }))
}

fn keep_list(state: &StateMachine) -> Result<Vec<String>, String> {
    let services = state.services().map_err(|error| error.to_string())?;
    let deployment_images = services
        .iter()
        .map(|(id, _)| state.deployments(id))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| error.to_string())?
        .into_iter()
        .flat_map(|deployments| {
            let live = deployments
                .iter()
                .filter(|deployment| LIVE.contains(&deployment.status))
                .filter_map(|deployment| deployment.image.clone())
                .collect::<Vec<_>>();
            retained_releases(&deployments)
                .into_iter()
                .filter_map(|deployment| deployment.image.clone())
                .chain(live)
                .collect::<Vec<_>>()
        });
    let assigned_images = state
        .nodes()
        .map_err(|error| error.to_string())?
        .iter()
        .map(|node| state.assignments(node.id))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| error.to_string())?
        .into_iter()
        .flatten()
        .map(|assignment| assignment.image);
    Ok(deployment_images
        .chain(assigned_images)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect())
}

/// Images built by Maestro, and older tags of any repository a kept image comes from (registry
/// builds, system images), are removable once they are past the grace period.
fn removable(images: &[(String, Option<i64>)], kept: &BTreeSet<String>, now: i64) -> Vec<String> {
    let repositories = kept
        .iter()
        .map(|image| repository(image))
        .collect::<BTreeSet<_>>();
    images
        .iter()
        .filter(|(name, _)| !kept.contains(name))
        .filter(|(name, _)| {
            name.starts_with(BUILT_IMAGE_PREFIX) || repositories.contains(&repository(name))
        })
        .filter(|(_, created)| created.is_some_and(|created| now - created >= GRACE_SECONDS))
        .map(|(name, _)| name.clone())
        .collect()
}

fn repository(image: &str) -> String {
    let name = image.split('@').next().unwrap_or(image);
    let last_segment = name.rfind('/').map_or(0, |index| index + 1);
    match name[last_segment..].rfind(':') {
        Some(colon) => name[..last_segment + colon].to_string(),
        None => name.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn removes_old_unkept_tags_of_managed_repositories_only() {
        let now = 10 * GRACE_SECONDS;
        let old = Some(now - GRACE_SECONDS);
        let fresh = Some(now - 60);
        let images = [
            ("maestro.local/api:d1".to_string(), old),
            ("maestro.local/api:d2".to_string(), old),
            ("maestro.local/api:d3".to_string(), fresh),
            ("docker.io/library/traefik:v3.5".to_string(), old),
            ("docker.io/library/traefik:v3.6".to_string(), old),
            ("docker.io/library/nginx:1.27".to_string(), old),
            ("maestro.local/web:d9".to_string(), None),
            (
                "123.dkr.ecr.us-west-2.amazonaws.com/team/app:d4".to_string(),
                old,
            ),
            (
                "123.dkr.ecr.us-west-2.amazonaws.com/team/app:d5".to_string(),
                old,
            ),
        ];
        let kept = BTreeSet::from([
            "maestro.local/api:d2".to_string(),
            "docker.io/library/traefik:v3.6".to_string(),
            "123.dkr.ecr.us-west-2.amazonaws.com/team/app:d5".to_string(),
        ]);
        assert_eq!(
            removable(&images, &kept, now),
            [
                "maestro.local/api:d1",
                "docker.io/library/traefik:v3.5",
                "123.dkr.ecr.us-west-2.amazonaws.com/team/app:d4"
            ]
        );
    }

    #[test]
    fn strips_tags_and_digests_to_get_the_repository() {
        assert_eq!(
            repository("docker.io/library/traefik:v3.6"),
            "docker.io/library/traefik"
        );
        assert_eq!(
            repository("localhost:5000/api@sha256:abc"),
            "localhost:5000/api"
        );
        assert_eq!(repository("localhost:5000/api"), "localhost:5000/api");
    }
}
