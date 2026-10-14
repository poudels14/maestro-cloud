use std::path::{Path, PathBuf};
use std::time::Duration;

use maestro_core::MaintenanceKind;
use serde::{Deserialize, Serialize};
use tokio::process::Command;

const PROGRESS_FILE: &str = "maintenance.json";
const SYSTEM_FLAKE: &str = "/etc/maestro";
const RESTART_DELAY: Duration = Duration::from_secs(1);
const MAX_ERROR_LENGTH: usize = 2000;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MaintenanceProgress {
    pub run_id: String,
    pub stage: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

pub fn start(node_dir: PathBuf, kind: MaintenanceKind, run_id: String) {
    tokio::spawn(async move {
        match kind {
            MaintenanceKind::Restart => {
                record(&node_dir, &run_id, "restarting", None);
                tokio::time::sleep(RESTART_DELAY).await;
                tracing::info!(%run_id, "restarting for a rolling restart");
                std::process::exit(0);
            }
            MaintenanceKind::Upgrade => {
                let steps: [(&str, &str, Vec<String>); 3] = [
                    (
                        "updating-source",
                        "nix",
                        vec![
                            "flake".into(),
                            "update".into(),
                            "--flake".into(),
                            SYSTEM_FLAKE.into(),
                        ],
                    ),
                    (
                        "rebuilding-system",
                        "nixos-rebuild",
                        vec![
                            "boot".into(),
                            "--flake".into(),
                            format!("{SYSTEM_FLAKE}#default"),
                        ],
                    ),
                    ("restarting", "systemctl", vec!["reboot".into()]),
                ];
                for (stage, program, args) in steps {
                    record(&node_dir, &run_id, stage, None);
                    tracing::info!(%run_id, stage, "upgrading the node");
                    if let Err(error) = run(program, &args).await {
                        tracing::warn!(%run_id, stage, %error, "node upgrade failed");
                        record(&node_dir, &run_id, "failed", Some(error));
                        break;
                    }
                }
            }
        }
    });
}

pub fn progress(node_dir: &Path) -> Option<MaintenanceProgress> {
    std::fs::read(node_dir.join(PROGRESS_FILE))
        .ok()
        .and_then(|bytes| serde_json::from_slice(&bytes).ok())
}

fn record(node_dir: &Path, run_id: &str, stage: &str, error: Option<String>) {
    let progress = MaintenanceProgress {
        run_id: run_id.to_string(),
        stage: stage.to_string(),
        error,
    };
    if let Ok(bytes) = serde_json::to_vec(&progress) {
        let _ = std::fs::write(node_dir.join(PROGRESS_FILE), bytes);
    }
}

async fn run(program: &str, args: &[String]) -> Result<(), String> {
    let output = Command::new(program)
        .args(args)
        .output()
        .await
        .map_err(|error| format!("failed to run {program}: {error}"))?;
    match output.status.success() {
        true => Ok(()),
        false => {
            let stderr = String::from_utf8_lossy(&output.stderr);
            Err(format!(
                "{program} {} failed: {}",
                args.join(" "),
                stderr
                    .chars()
                    .rev()
                    .take(MAX_ERROR_LENGTH)
                    .collect::<String>()
                    .chars()
                    .rev()
                    .collect::<String>()
            ))
        }
    }
}
