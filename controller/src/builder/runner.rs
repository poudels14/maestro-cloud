use std::collections::HashMap;
use std::path::Path;

use anyhow::Result;

use crate::logs::{LogEntry, LogOrigin};
use crate::utils::cmd;
use crate::utils::crypto::SecretString;

pub async fn sync_repo(
    repo: &str,
    branch: Option<&str>,
    target_dir: &Path,
    env: &HashMap<String, SecretString>,
    log_sender: Option<&flume::Sender<LogEntry>>,
    log_source: Option<&str>,
) -> Result<()> {
    if target_dir.join(".git").exists() {
        run_git(
            &["fetch", "origin"],
            Some(target_dir),
            env,
            log_sender,
            log_source,
        )
        .await?;

        let reset_ref = match branch {
            Some(branch) => format!("origin/{branch}"),
            None => "origin/HEAD".to_string(),
        };
        run_git(
            &["reset", "--hard", &reset_ref],
            Some(target_dir),
            &HashMap::new(),
            log_sender,
            log_source,
        )
        .await?;
    } else {
        if let Some(parent) = target_dir.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut args = vec!["clone"];
        if let Some(branch) = branch {
            args.extend(["--branch", branch]);
        }
        let target = target_dir.display().to_string();
        args.extend([repo, &target]);
        run_git(&args, None, env, log_sender, log_source).await?;
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

async fn run_git(
    args: &[&str],
    dir: Option<&Path>,
    env: &HashMap<String, SecretString>,
    log_sender: Option<&flume::Sender<LogEntry>>,
    log_source: Option<&str>,
) -> Result<()> {
    let mut command = cmd::exec("git", args).env(env);
    if let Some(dir) = dir {
        command = command.dir(dir);
    }
    if let (Some(sender), Some(source)) = (log_sender, log_source) {
        command
            .run_with_logs(sender, source, LogOrigin::Build)
            .await
    } else {
        command.run().await.map(|_| ())
    }
}
