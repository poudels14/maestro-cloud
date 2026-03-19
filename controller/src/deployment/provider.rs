use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{Result, anyhow};
use async_trait::async_trait;

use crate::builder;
use crate::deployment::types::ServiceDeployment;
use crate::logs::LogEntry;
use crate::supervisor::SecretsMount;
use crate::utils::cmd;

use crate::supervisor::JobCommand;

#[derive(Debug)]
pub struct DeployOutput {
    pub command: JobCommand,
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
            cmd::run_with_logs(
                "docker",
                &args,
                sender,
                source,
                crate::logs::LogOrigin::Build,
            )
            .await?;
        } else {
            cmd::run("docker", &args).await?;
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
            let shell_cmd = if deploy_command.args.is_empty() {
                deploy_command.command.clone()
            } else {
                format!(
                    "{} {}",
                    deploy_command.command,
                    deploy_command.args.join(" ")
                )
            };
            Some(DeployOutput {
                command: JobCommand::Shell(shell_cmd),
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
        let shell_cmd = if deploy_command.args.is_empty() {
            command.to_string()
        } else {
            format!("{} {}", command, deploy_command.args.join(" "))
        };
        Some(DeployOutput {
            command: JobCommand::Shell(shell_cmd),
            secrets_mount: None,
        })
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
) -> JobCommand {
    let short_deployment_id = deployment_id.chars().take(6).collect::<String>();
    let container_name = if replica_index == 0 {
        format!("{service_id}-{short_deployment_id}")
    } else {
        format!("{service_id}-{short_deployment_id}-{replica_index}")
    };

    let mut args = vec![
        "run".to_string(),
        "--rm".to_string(),
        "--name".to_string(),
        container_name.clone(),
        "--hostname".to_string(),
        container_name,
        "--network".to_string(),
        network.to_string(),
    ];
    if let Some(domain) = dns_domain {
        args.extend(["--domainname".to_string(), domain.to_string()]);
    }
    for port in expose_ports {
        args.extend(["-p".to_string(), format!("0:{port}")]);
    }
    for (key, value) in env {
        args.extend(["-e".to_string(), format!("{key}={value}")]);
    }
    if let Some((host_path, container_path)) = secrets_mount {
        args.extend(["-v".to_string(), format!("{host_path}:{container_path}:ro")]);
    }
    args.push(image.to_string());
    for flag in flags {
        args.push(flag.clone());
    }
    JobCommand::Exec {
        program: "docker".to_string(),
        args,
    }
}
