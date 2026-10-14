use std::path::PathBuf;

use maestro_core::ReplicaPlan;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LocalReplica {
    pub replica_id: String,
    pub service_id: String,
    pub deployment_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub healthcheck_path: Option<String>,
}

pub async fn publish_replica_manifest(path: PathBuf, mut plans: watch::Receiver<Vec<ReplicaPlan>>) {
    loop {
        let replicas = plans
            .borrow_and_update()
            .iter()
            .map(|plan| LocalReplica {
                replica_id: plan.replica_id.to_string(),
                service_id: plan.service_id.to_string(),
                deployment_id: plan.deployment_id.to_string(),
                healthcheck_path: plan.healthcheck.as_ref().map(|check| check.path.clone()),
            })
            .collect::<Vec<_>>();
        if let Err(error) = write_atomically(&path, &replicas) {
            tracing::warn!(%error, path = %path.display(), "failed to publish the replica manifest");
        }
        if plans.changed().await.is_err() {
            break;
        }
    }
}

fn write_atomically(path: &PathBuf, replicas: &[LocalReplica]) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let temporary = path.with_extension("json.tmp");
    std::fs::write(&temporary, serde_json::to_vec(replicas)?)?;
    std::fs::rename(temporary, path)
}
