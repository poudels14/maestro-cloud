use std::collections::HashMap;
use std::path::Path;

use anyhow::Result;

use crate::utils::cmd;
use crate::utils::crypto::SecretString;

pub async fn sync_repo(
    repo: &str,
    branch: Option<&str>,
    target_dir: &Path,
    env: &HashMap<String, SecretString>,
) -> Result<()> {
    if target_dir.join(".git").exists() {
        eprintln!(
            "[maestro]: fetching repo {} into {}",
            repo,
            target_dir.display()
        );
        cmd::exec("git", &["fetch", "origin"])
            .dir(target_dir)
            .env(env)
            .run()
            .await?;

        let reset_ref = match branch {
            Some(branch) => format!("origin/{branch}"),
            None => "origin/HEAD".to_string(),
        };
        cmd::exec("git", &["reset", "--hard", &reset_ref])
            .dir(target_dir)
            .run()
            .await?;
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
        cmd::exec("git", &args).env(env).run().await?;
    }
    Ok(())
}

pub async fn get_head_commit(repo_dir: &Path) -> Result<(String, String)> {
    let stdout = cmd::exec("git", &["log", "-1", "--format=%H%n%s"])
        .dir(repo_dir)
        .run()
        .await?;
    let mut lines = stdout.trim().lines();
    let sha = lines.next().unwrap_or("").to_string();
    let message = lines.next().unwrap_or("").to_string();
    Ok((sha, message))
}
