use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use maestro_core::{Deployment, DeploymentStatus};
use maestro_store::{Consensus, Mutation, mask_spec};
use maestro_telemetry::S3Archive;
use serde_json::json;

const ARCHIVE_INTERVAL: Duration = Duration::from_secs(60);
const TERMINAL_STATUSES: [DeploymentStatus; 3] = [
    DeploymentStatus::Stopped,
    DeploymentStatus::Failed,
    DeploymentStatus::Cancelled,
];
const ARCHIVED_SUFFIX: &str = ".archived";

pub async fn run_archiver(consensus: Arc<Consensus>, archive: S3Archive, build_logs: PathBuf) {
    let mut ticker = tokio::time::interval(ARCHIVE_INTERVAL);
    loop {
        ticker.tick().await;
        let finished = TERMINAL_STATUSES
            .iter()
            .filter_map(|status| consensus.state().deployments_with_status(*status).ok())
            .flatten()
            .collect::<Vec<_>>();
        if consensus.is_leader() {
            for deployment in &finished {
                let archived = consensus
                    .state()
                    .is_archived(&deployment.service_id, &deployment.id)
                    .unwrap_or(true);
                if !archived {
                    archive_record(&consensus, &archive, deployment).await;
                }
            }
        }
        for deployment in &finished {
            archive_build_log(&archive, &build_logs, deployment).await;
        }
    }
}

async fn archive_record(consensus: &Consensus, archive: &S3Archive, deployment: &Deployment) {
    let now = now_ms();
    let record = json!({
        "archivedAtMs": now,
        "deployment": Deployment {
            spec: mask_spec(&deployment.spec),
            ..deployment.clone()
        },
    });
    let path = format!(
        "deployments/{}/{}.json.gz",
        deployment.service_id, deployment.id
    );
    let uploaded = archive
        .put(&path, gzip(record.to_string().as_bytes()))
        .await;
    let marked = match uploaded {
        Ok(()) => consensus
            .write(vec![Mutation::MarkDeploymentArchived {
                service_id: deployment.service_id.clone(),
                deployment_id: deployment.id.clone(),
                at_ms: now,
            }])
            .await
            .map(|_| ())
            .map_err(|error| error.to_string()),
        Err(error) => Err(error),
    };
    if let Err(error) = marked {
        tracing::warn!(deployment = %deployment.id, %error, "archiving the deployment record failed");
    }
}

async fn archive_build_log(
    archive: &S3Archive,
    build_logs: &std::path::Path,
    deployment: &Deployment,
) {
    let log = build_logs.join(format!("{}.log", deployment.id));
    let marker = build_logs.join(format!("{}.log{ARCHIVED_SUFFIX}", deployment.id));
    if log.exists() && !marker.exists() {
        let path = format!(
            "deployments/{}/{}.build.log.gz",
            deployment.service_id, deployment.id
        );
        let uploaded = match tokio::fs::read(&log).await {
            Ok(contents) => archive.put(&path, gzip(&contents)).await,
            Err(error) => Err(error.to_string()),
        };
        let marked = match uploaded {
            Ok(()) => tokio::fs::write(&marker, b"")
                .await
                .map_err(|error| error.to_string()),
            Err(error) => Err(error),
        };
        if let Err(error) = marked {
            tracing::warn!(deployment = %deployment.id, %error, "archiving the build log failed");
        }
    }
}

fn gzip(bytes: &[u8]) -> Vec<u8> {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    let _ = encoder.write_all(bytes);
    encoder.finish().unwrap_or_default()
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis() as u64)
}
