use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use tokio::io::AsyncBufReadExt;
use tokio::process::Command;

use crate::builder;
use crate::deployment::types::ServiceDeployment;
use crate::logs::LogEntry;
use crate::supervisor::SecretsMount;

#[derive(Debug)]
pub struct DeployOutput {
    pub command: String,
    pub secrets_mount: Option<SecretsMount>,
}

pub struct BuildOutput {
    pub image_tag: String,
    pub commit_sha: String,
    pub commit_message: String,
}

#[async_trait]
pub trait ServiceCommandPlanner: Send + Sync {
    async fn build(
        &self,
        deployment: &ServiceDeployment,
        build_dir: &Path,
        image_tag: &str,
        log_sender: Option<flume::Sender<LogEntry>>,
    ) -> Result<BuildOutput>;
    fn deploy(&self, deployment: &ServiceDeployment, replica_index: u32) -> Option<DeployOutput>;
}

pub struct DockerBuildConfig {
    pub context_dir: PathBuf,
    pub tag: String,
    pub dockerfile: Option<String>,
    pub build_args: HashMap<String, String>,
}

#[derive(Clone)]
pub struct DockerDeploymentProvider {
    pub network: String,
    pub dns_domain: Option<String>,
    pub secrets_dir: PathBuf,
}
pub struct ShellDeploymentProvider;

impl DockerDeploymentProvider {
    pub async fn build(
        config: &DockerBuildConfig,
        log_sender: Option<&flume::Sender<LogEntry>>,
        log_source: Option<&str>,
    ) -> Result<()> {
        eprintln!(
            "[maestro]: building docker image {} from {}",
            config.tag,
            config.context_dir.display()
        );

        let mut args = vec!["build".to_string(), "-t".to_string(), config.tag.clone()];
        if let Some(ref dockerfile) = config.dockerfile {
            let dockerfile_path = config.context_dir.join(dockerfile).display().to_string();
            args.push("-f".to_string());
            args.push(dockerfile_path);
        }
        for (key, value) in &config.build_args {
            args.push("--build-arg".to_string());
            args.push(format!("{key}={value}"));
        }
        args.push(config.context_dir.display().to_string());

        if let (Some(sender), Some(source)) = (log_sender, log_source) {
            let source: Arc<str> = Arc::from(source);
            let mut child = Command::new("docker")
                .args(&args)
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .spawn()
                .map_err(|err| anyhow!("failed to spawn docker build: {err}"))?;

            let stdout = child.stdout.take();
            let stderr = child.stderr.take();
            let sender_clone = sender.clone();
            let source_clone = source.clone();

            let stdout_task = tokio::spawn(async move {
                if let Some(stdout) = stdout {
                    let reader = tokio::io::BufReader::new(stdout);
                    let mut lines = reader.lines();
                    while let Ok(Some(line)) = lines.next_line().await {
                        let entry = build_log_entry(&line, "stdout", &source_clone);
                        if sender_clone.send_async(entry).await.is_err() {
                            break;
                        }
                    }
                }
            });

            let sender_clone = sender.clone();
            let source_clone = source.clone();
            let stderr_task = tokio::spawn(async move {
                if let Some(stderr) = stderr {
                    let reader = tokio::io::BufReader::new(stderr);
                    let mut lines = reader.lines();
                    while let Ok(Some(line)) = lines.next_line().await {
                        let entry = build_log_entry(&line, "stderr", &source_clone);
                        if sender_clone.send_async(entry).await.is_err() {
                            break;
                        }
                    }
                }
            });

            let _ = stdout_task.await;
            let _ = stderr_task.await;

            let status = child
                .wait()
                .await
                .map_err(|err| anyhow!("failed to wait for docker build: {err}"))?;
            if !status.success() {
                return Err(anyhow!("docker build failed for {}", config.tag));
            }
        } else {
            let output = Command::new("docker")
                .args(&args)
                .output()
                .await
                .map_err(|err| anyhow!("failed to run docker build: {err}"))?;

            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                return Err(anyhow!("docker build failed for {}: {stderr}", config.tag));
            }
        }

        eprintln!("[maestro]: docker image {} built successfully", config.tag);
        Ok(())
    }
}

#[async_trait]
impl ServiceCommandPlanner for DockerDeploymentProvider {
    async fn build(
        &self,
        deployment: &ServiceDeployment,
        build_dir: &Path,
        image_tag: &str,
        log_sender: Option<flume::Sender<LogEntry>>,
    ) -> Result<BuildOutput> {
        let build_config = deployment
            .config
            .build
            .as_ref()
            .ok_or_else(|| anyhow!("no build config"))?;
        builder::sync_repo(
            &build_config.repo,
            build_config.branch.as_deref(),
            build_dir,
        )
        .await?;
        let (commit_sha, commit_message) = builder::get_head_commit(build_dir).await?;
        let log_source = format!("{}/{}/build", deployment.config.id, deployment.id);
        DockerDeploymentProvider::build(
            &DockerBuildConfig {
                context_dir: build_dir.to_path_buf(),
                tag: image_tag.to_string(),
                dockerfile: Some(build_config.dockerfile_path.clone()),
                build_args: build_config.env.clone(),
            },
            log_sender.as_ref(),
            Some(&log_source),
        )
        .await?;
        Ok(BuildOutput {
            image_tag: image_tag.to_string(),
            commit_sha,
            commit_message,
        })
    }

    fn deploy(&self, deployment: &ServiceDeployment, replica_index: u32) -> Option<DeployOutput> {
        let built_image = deployment
            .build
            .as_ref()
            .map(|b| b.docker_image_id.as_str());
        let config_image = deployment
            .config
            .image
            .as_deref()
            .map(str::trim)
            .filter(|image| !image.is_empty());
        if let Some(image) = built_image.or(config_image) {
            let secrets_info = deployment.config.deploy.secrets.as_ref().and_then(|s| {
                if s.items.is_empty() {
                    return None;
                }
                let host_path = self
                    .secrets_dir
                    .join(&deployment.config.id)
                    .join(&deployment.id)
                    .join(format!("replica{replica_index}.env"));
                let content: String = s
                    .items
                    .iter()
                    .map(|(k, v)| format!("{k}={v}"))
                    .collect::<Vec<_>>()
                    .join("\n");
                Some((host_path, s.mount_path.clone(), content))
            });
            let mount_arg = secrets_info.as_ref().map(|(host_path, container_path, _)| {
                (host_path.display().to_string(), container_path.clone())
            });
            Some(DeployOutput {
                command: docker_run_command(
                    &deployment.config.id,
                    &deployment.id,
                    replica_index,
                    image,
                    &self.network,
                    self.dns_domain.as_deref(),
                    &deployment.config.deploy.expose_ports,
                    &deployment.config.deploy.env,
                    mount_arg.as_ref(),
                    &deployment.config.deploy.flags,
                ),
                secrets_mount: secrets_info.map(|(host_path, container_path, content)| {
                    SecretsMount {
                        host_path,
                        container_path,
                        content,
                    }
                }),
            })
        } else if let Some(deploy_command) = deployment.config.deploy.command.as_ref() {
            Some(DeployOutput {
                command: to_shell_command(&deploy_command.command, &deploy_command.args),
                secrets_mount: None,
            })
        } else {
            None
        }
    }
}

#[async_trait]
impl ServiceCommandPlanner for ShellDeploymentProvider {
    async fn build(
        &self,
        _deployment: &ServiceDeployment,
        _build_dir: &Path,
        _image_tag: &str,
        _log_sender: Option<flume::Sender<LogEntry>>,
    ) -> Result<BuildOutput> {
        Err(anyhow!("shell provider does not support build"))
    }

    fn deploy(&self, deployment: &ServiceDeployment, _replica_index: u32) -> Option<DeployOutput> {
        let deploy_command = deployment.config.deploy.command.as_ref()?;
        let command = deploy_command.command.trim();
        if command.is_empty() {
            return None;
        }
        Some(DeployOutput {
            command: to_shell_command(command, &deploy_command.args),
            secrets_mount: None,
        })
    }
}

fn to_shell_command(command: &str, args: &[String]) -> String {
    if args.is_empty() {
        command.to_string()
    } else {
        format!("{} {}", command, args.join(" "))
    }
}

fn docker_run_command(
    service_id: &str,
    deployment_id: &str,
    replica_index: u32,
    image: &str,
    network: &str,
    dns_domain: Option<&str>,
    expose_ports: &[u16],
    env: &HashMap<String, String>,
    secrets_mount: Option<&(String, String)>,
    flags: &[String],
) -> String {
    let short_deployment_id = deployment_id.chars().take(6).collect::<String>();
    let container_name = if replica_index == 0 {
        format!("{service_id}-{short_deployment_id}")
    } else {
        format!("{service_id}-{short_deployment_id}-{replica_index}")
    };

    let mut args = format!(
        "exec docker run --rm --name {container_name} --hostname {container_name} --network {network}"
    );
    if let Some(domain) = dns_domain {
        args.push_str(&format!(" --domainname {domain}"));
    }
    for port in expose_ports {
        args.push_str(&format!(" -p 0:{port}"));
    }
    for (key, value) in env {
        args.push_str(&format!(" -e {key}={value}"));
    }
    if let Some((host_path, container_path)) = secrets_mount {
        args.push_str(&format!(" -v {host_path}:{container_path}:ro"));
    }
    args.push_str(&format!(" {image}"));
    for flag in flags {
        args.push_str(&format!(" {flag}"));
    }
    args
}

fn build_log_entry(text: &str, stream: &str, source: &Arc<str>) -> LogEntry {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;
    LogEntry {
        seq: 0,
        ts: now,
        level: Arc::from("info"),
        stream: Arc::from(stream),
        text: text.to_string(),
        source: source.clone(),
        system: true,
        tags: Arc::new(serde_json::Value::Null),
    }
}
