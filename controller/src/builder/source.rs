use std::collections::HashMap;
use std::path::Path;

use anyhow::{Result, anyhow};
use async_trait::async_trait;

use crate::logs::{LogEntry, LogOrigin};
use crate::utils::cmd;
use crate::utils::crypto::SecretString;

pub struct HeadInfo {
    pub sha: String,
    pub message: String,
}

pub struct LogTarget<'a> {
    pub sender: &'a flume::Sender<LogEntry>,
    pub source: &'a str,
}

#[async_trait]
pub trait BuildSource: Send + Sync {
    async fn sync(&self, target_dir: &Path, log: Option<LogTarget<'_>>) -> Result<()>;

    async fn remote_head(&self) -> Result<String>;

    async fn head_info(&self, target_dir: &Path) -> Result<HeadInfo> {
        let stdout = cmd::exec("git", &["log", "-1", "--format=%H%n%s"])
            .dir(target_dir)
            .run()
            .await?;
        let mut lines = stdout.trim().lines();
        let sha = lines.next().unwrap_or("").to_string();
        let message = lines.next().unwrap_or("").to_string();
        Ok(HeadInfo { sha, message })
    }
}

pub struct GitSource {
    repo: String,
    branch: Option<String>,
    env: HashMap<String, SecretString>,
}

impl GitSource {
    pub fn new(repo: &str, branch: Option<&str>, env: HashMap<String, SecretString>) -> Self {
        Self {
            repo: to_https_url(repo),
            branch: branch.map(String::from),
            env,
        }
    }
}

#[async_trait]
impl BuildSource for GitSource {
    async fn sync(&self, target_dir: &Path, log: Option<LogTarget<'_>>) -> Result<()> {
        if target_dir.join(".git").exists() {
            self.run(&["fetch", "origin"], Some(target_dir), log.as_ref())
                .await?;
            let reset_ref = match &self.branch {
                Some(branch) => format!("origin/{branch}"),
                None => "origin/HEAD".to_string(),
            };
            self.run(
                &["reset", "--hard", &reset_ref],
                Some(target_dir),
                log.as_ref(),
            )
            .await?;
        } else {
            if let Some(parent) = target_dir.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let mut args = vec!["clone"];
            if let Some(branch) = &self.branch {
                args.extend(["--branch", branch]);
            }
            let target = target_dir.display().to_string();
            args.extend([self.repo.as_str(), &target]);
            self.run(&args, None, log.as_ref()).await?;
        }
        Ok(())
    }

    async fn remote_head(&self) -> Result<String> {
        let branch = self
            .branch
            .as_deref()
            .ok_or_else(|| anyhow!("watch requires a branch to be specified"))?;
        let refspec = format!("refs/heads/{branch}");
        let stdout = cmd::exec("git", &["ls-remote", &self.repo, &refspec])
            .env(&self.env)
            .run()
            .await?;
        let sha = stdout.split_whitespace().next().unwrap_or("").to_string();
        if sha.is_empty() {
            return Err(anyhow!(
                "branch `{branch}` not found on remote `{}`",
                self.repo
            ));
        }
        Ok(sha)
    }
}

impl GitSource {
    async fn run(
        &self,
        args: &[&str],
        dir: Option<&Path>,
        log: Option<&LogTarget<'_>>,
    ) -> Result<()> {
        let mut command = cmd::exec("git", args).env(&self.env);
        if let Some(dir) = dir {
            command = command.dir(dir);
        }
        if let Some(log) = log {
            command
                .run_with_logs(log.sender, log.source, LogOrigin::Build)
                .await
        } else {
            command.run().await.map(|_| ())
        }
    }
}

fn to_https_url(url: &str) -> String {
    if let Some(rest) = url.strip_prefix("git@") {
        if let Some((host, path)) = rest.split_once(':') {
            return format!("https://{host}/{path}");
        }
    }
    url.to_string()
}
