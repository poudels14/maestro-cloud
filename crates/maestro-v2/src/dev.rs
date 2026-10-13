use std::path::{Path, PathBuf};

use anyhow::Context;
use clap::Subcommand;
use tokio::process::Command;

use crate::contexts;

const VM_NAME: &str = "maestro-dev";
const TEMPLATE: &str = include_str!("../../../dev/lima/maestro-dev.yaml");
const API_URL: &str = "http://127.0.0.1:3000";
const API_WAIT_ATTEMPTS: u32 = 60;
const UNIT: &str = r#"[Unit]
Description=Maestro agent (dev)
After=containerd.service
Requires=containerd.service

[Service]
Slice=maestro-system.slice
ExecStart=/usr/local/bin/maestro-v2 agent --data-dir /var/lib/maestro --listen 127.0.0.1:3000 --admin-source {source}
Restart=always
RestartSec=2

[Install]
WantedBy=multi-user.target
"#;
const TELEMETRY_UNIT: &str = r#"[Unit]
Description=Maestro telemetry (dev)
After=maestro.service

[Service]
Slice=maestro-system.slice
MemoryMax=1G
ExecStart=/usr/local/bin/maestro-v2 telemetry
Restart=always
RestartSec=2

[Install]
WantedBy=multi-user.target
"#;

#[derive(Subcommand)]
pub enum DevCommand {
    Up {
        #[arg(long, default_value = ".")]
        source: PathBuf,
    },
    Reload {
        #[arg(long, default_value = ".")]
        source: PathBuf,
    },
    Down,
    Status,
}

pub async fn run(command: DevCommand) -> anyhow::Result<()> {
    match command {
        DevCommand::Up { source } => {
            ensure_vm().await?;
            install_agent(&source).await?;
            let token = limactl(&[
                "shell",
                VM_NAME,
                "--",
                "sudo",
                "/usr/local/bin/maestro-v2",
                "token",
                "create",
                "--data-dir",
                "/var/lib/maestro",
                "--subject",
                "dev",
            ])
            .await?;
            contexts::save("dev", API_URL, Some(token.trim().to_string()))?;
            wait_for_api().await?;
            println!("maestro is running; API at {API_URL} (context: dev)");
            Ok(())
        }
        DevCommand::Reload { source } => {
            install_agent(&source).await?;
            println!("maestro agent reloaded");
            Ok(())
        }
        DevCommand::Down => limactl(&["stop", VM_NAME]).await.map(|_| ()),
        DevCommand::Status => {
            println!(
                "vm: {}",
                vm_status().await?.unwrap_or_else(|| "missing".to_string())
            );
            let api = match reqwest::get(format!("{API_URL}/api/health")).await {
                Ok(response) if response.status().is_success() => "healthy",
                _ => "unreachable",
            };
            println!("api: {api} ({API_URL})");
            Ok(())
        }
    }
}

async fn ensure_vm() -> anyhow::Result<()> {
    match vm_status().await?.as_deref() {
        Some("Running") => Ok(()),
        Some(_) => limactl(&["start", VM_NAME]).await.map(|_| ()),
        None => {
            let template_path = std::env::temp_dir().join("maestro-dev.yaml");
            std::fs::write(&template_path, TEMPLATE)?;
            let template = template_path.to_string_lossy().to_string();
            println!("creating the {VM_NAME} VM (first run downloads an Ubuntu image)...");
            limactl(&["start", "--name", VM_NAME, "--tty=false", &template])
                .await
                .map(|_| ())
        }
    }
}

async fn install_agent(source: &Path) -> anyhow::Result<()> {
    let source = std::fs::canonicalize(source)
        .with_context(|| format!("source directory {} not found", source.display()))?;
    anyhow::ensure!(
        source.join("crates/maestro-v2").is_dir(),
        "{} is not the maestro repository root; pass --source",
        source.display()
    );
    let script = format!(
        r#"set -euo pipefail
source "$HOME/.cargo/env"
cd {source}
CARGO_TARGET_DIR="$HOME/target" cargo build --locked -p maestro-v2
sudo install -m 0755 "$HOME/target/debug/maestro-v2" /usr/local/bin/maestro-v2
[ -f /var/lib/maestro/node.json ] || sudo /usr/local/bin/maestro-v2 server init --data-dir /var/lib/maestro
sudo tee /etc/systemd/system/maestro.service > /dev/null <<'UNIT'
{unit}UNIT
sudo tee /etc/systemd/system/maestro-telemetry.service > /dev/null <<'UNIT'
{telemetry_unit}UNIT
sudo systemctl daemon-reload
sudo systemctl enable --now maestro.service maestro-telemetry.service
sudo systemctl restart maestro.service maestro-telemetry.service
"#,
        source = shell_quote(&source.to_string_lossy()),
        unit = UNIT.replace("{source}", &source.to_string_lossy()),
        telemetry_unit = TELEMETRY_UNIT,
    );
    limactl(&["shell", VM_NAME, "--", "bash", "-lc", &script])
        .await
        .map(|_| ())
}

async fn wait_for_api() -> anyhow::Result<()> {
    let mut attempts = 0;
    let mut healthy = false;
    while !healthy && attempts < API_WAIT_ATTEMPTS {
        healthy = reqwest::get(format!("{API_URL}/api/health"))
            .await
            .is_ok_and(|response| response.status().is_success());
        if !healthy {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
        attempts += 1;
    }
    match healthy {
        true => Ok(()),
        false => anyhow::bail!("the maestro API did not become healthy at {API_URL}"),
    }
}

async fn vm_status() -> anyhow::Result<Option<String>> {
    let output = limactl(&["list", "--format", "{{.Status}}", VM_NAME]).await;
    Ok(output
        .ok()
        .map(|status| status.trim().to_string())
        .filter(|status| !status.is_empty()))
}

async fn limactl(args: &[&str]) -> anyhow::Result<String> {
    let output = Command::new("limactl")
        .args(args)
        .stdin(std::process::Stdio::null())
        .stderr(std::process::Stdio::inherit())
        .output()
        .await
        .context("limactl is required for `maestro dev`; install it with `brew install lima`")?;
    anyhow::ensure!(
        output.status.success(),
        "limactl {} failed",
        args.first().unwrap_or(&"")
    );
    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', r"'\''"))
}
