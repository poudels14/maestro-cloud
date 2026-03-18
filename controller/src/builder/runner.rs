use std::path::Path;

use anyhow::{Result, anyhow};
use tokio::process::Command;

pub async fn sync_repo(repo: &str, branch: Option<&str>, target_dir: &Path) -> Result<()> {
    if target_dir.join(".git").exists() {
        eprintln!(
            "[maestro]: fetching repo {} into {}",
            repo,
            target_dir.display()
        );
        let output = Command::new("git")
            .args(["fetch", "origin"])
            .current_dir(target_dir)
            .output()
            .await
            .map_err(|err| anyhow!("failed to run git fetch: {err}"))?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(anyhow!("git fetch failed: {stderr}"));
        }

        let reset_ref = match branch {
            Some(branch) => format!("origin/{branch}"),
            None => "origin/HEAD".to_string(),
        };
        let output = Command::new("git")
            .args(["reset", "--hard", &reset_ref])
            .current_dir(target_dir)
            .output()
            .await
            .map_err(|err| anyhow!("failed to run git reset: {err}"))?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(anyhow!("git reset failed: {stderr}"));
        }
    } else {
        eprintln!(
            "[maestro]: cloning repo {} into {}",
            repo,
            target_dir.display()
        );
        if let Some(parent) = target_dir.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut args = vec!["clone"];
        if let Some(branch) = branch {
            args.extend(["--branch", branch]);
        }
        let target = target_dir.display().to_string();
        args.extend([repo, &target]);
        let output = Command::new("git")
            .args(&args)
            .output()
            .await
            .map_err(|err| anyhow!("failed to run git clone: {err}"))?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(anyhow!("git clone failed: {stderr}"));
        }
    }
    Ok(())
}

pub async fn get_head_commit(repo_dir: &Path) -> Result<(String, String)> {
    let output = Command::new("git")
        .args(["log", "-1", "--format=%H%n%s"])
        .current_dir(repo_dir)
        .output()
        .await
        .map_err(|err| anyhow!("failed to run git log: {err}"))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(anyhow!("git log failed: {stderr}"));
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut lines = stdout.trim().lines();
    let sha = lines.next().unwrap_or("").to_string();
    let message = lines.next().unwrap_or("").to_string();
    Ok((sha, message))
}
