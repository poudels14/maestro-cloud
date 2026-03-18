use std::path::Path;

use anyhow::Result;

use crate::utils::cmd;

pub async fn sync_repo(repo: &str, branch: Option<&str>, target_dir: &Path) -> Result<()> {
    if target_dir.join(".git").exists() {
        eprintln!(
            "[maestro]: fetching repo {} into {}",
            repo,
            target_dir.display()
        );
        cmd::run_in("git", &["fetch", "origin"], target_dir).await?;

        let reset_ref = match branch {
            Some(branch) => format!("origin/{branch}"),
            None => "origin/HEAD".to_string(),
        };
        cmd::run_in("git", &["reset", "--hard", &reset_ref], target_dir).await?;
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
        cmd::run("git", &args).await?;
    }
    Ok(())
}

pub async fn get_head_commit(repo_dir: &Path) -> Result<(String, String)> {
    let stdout = cmd::run_in("git", &["log", "-1", "--format=%H%n%s"], repo_dir).await?;
    let mut lines = stdout.trim().lines();
    let sha = lines.next().unwrap_or("").to_string();
    let message = lines.next().unwrap_or("").to_string();
    Ok((sha, message))
}
