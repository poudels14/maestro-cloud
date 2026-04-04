use std::path::Path;
use std::sync::Arc;

use anyhow::{Result, anyhow};
use async_trait::async_trait;

use crate::builder::{BuildSource, LogTarget};
use crate::config::BuilderType;
use crate::deployment::types::{GitCommitInfo, ServiceDeployment};
use crate::logs::LogEntry;
use crate::runtime::{BuildSpec, RunSpec, RuntimeProvider};
use crate::supervisor::SecretsMount;

use crate::supervisor::JobCommand;

#[derive(Debug)]
pub struct DeployOutput {
    pub command: JobCommand,
    pub secrets_mount: Option<SecretsMount>,
}

pub struct BuildOutput {
    pub image_tag: String,
}

#[async_trait]
pub trait ServiceCommandPlanner: Send + Sync {
    async fn setup(
        &self,
        deployment: &ServiceDeployment,
        build_dir: &Path,
        log_sender: Option<&flume::Sender<LogEntry>>,
    ) -> Result<Option<GitCommitInfo>>;

    async fn build(
        &self,
        deployment: &ServiceDeployment,
        build_dir: &Path,
        image_tag: &str,
        log_sender: Option<flume::Sender<LogEntry>>,
    ) -> Result<BuildOutput>;

    fn deploy(&self, deployment: &ServiceDeployment, replica_index: u32) -> Option<DeployOutput>;
}

#[derive(Clone)]
pub struct ContainerDeploymentProvider {
    pub runtime: Arc<dyn RuntimeProvider>,
    pub builder: BuilderType,
    pub network: String,
    pub dns_domain: Option<String>,
    pub dns_server: Option<String>,
    pub secrets_dir: std::path::PathBuf,
}
pub struct ShellDeploymentProvider;

#[async_trait]
impl ServiceCommandPlanner for ContainerDeploymentProvider {
    async fn setup(
        &self,
        deployment: &ServiceDeployment,
        build_dir: &Path,
        log_sender: Option<&flume::Sender<LogEntry>>,
    ) -> Result<Option<GitCommitInfo>> {
        let Some(build_config) = deployment.config.build.as_ref() else {
            return Ok(None);
        };
        let source = build_config.source();
        let log_source_str = format!("{}/{}/build", deployment.config.id, deployment.id);
        let log = log_sender.map(|sender| LogTarget {
            sender,
            source: &log_source_str,
        });
        source.sync(build_dir, log).await?;
        let head = source.head_info(build_dir).await?;
        Ok(Some(GitCommitInfo {
            reference: head.sha,
            message: head.message,
        }))
    }

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
        let log_source_str = format!("{}/{}/build", deployment.config.id, deployment.id);
        let use_depot = self.builder == BuilderType::Depot;

        let (build_tag, depot_pushed) = if use_depot && build_config.registry.is_some() {
            let registry = build_config.registry.as_ref().unwrap();
            let registry_tag = format!(
                "{}/{}:{}",
                registry.trim_end_matches('/'),
                deployment.config.id,
                deployment.id
            );
            (registry_tag, true)
        } else {
            (image_tag.to_string(), false)
        };

        self.runtime
            .build_image(
                &BuildSpec {
                    context_dir: build_dir.to_path_buf(),
                    tag: build_tag.clone(),
                    dockerfile: Some(build_config.dockerfile.clone()),
                    build_args: build_config.env.items.clone(),
                    secrets: build_config.secrets.items.clone(),
                    builder: self.builder,
                    push_to_registry: depot_pushed,
                },
                log_sender.as_ref(),
                Some(&log_source_str),
            )
            .await?;

        let final_tag = if depot_pushed {
            build_tag
        } else if let Some(registry) = &build_config.registry {
            let registry_tag = format!(
                "{}/{}:{}",
                registry.trim_end_matches('/'),
                deployment.config.id,
                deployment.id
            );
            self.runtime.tag_image(image_tag, &registry_tag).await?;
            self.runtime.push_image(&registry_tag).await?;
            registry_tag
        } else {
            build_tag
        };

        Ok(BuildOutput {
            image_tag: final_tag,
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
                    .map(|(k, v)| {
                        let escaped = v.replace('\\', "\\\\").replace('"', "\\\"");
                        format!("{k}=\"{escaped}\"")
                    })
                    .collect::<Vec<_>>()
                    .join("\n");
                Some((host_path, s.mount_path.clone(), content))
            });
            let mount_arg = secrets_info.as_ref().map(|(host_path, container_path, _)| {
                (host_path.display().to_string(), container_path.clone())
            });

            let short_deployment_id = deployment.id.chars().take(6).collect::<String>();
            let container_name = if replica_index == 0 {
                format!("{}-{short_deployment_id}", deployment.config.id)
            } else {
                format!(
                    "{}-{short_deployment_id}-{replica_index}",
                    deployment.config.id
                )
            };

            let mut extra_flags = Vec::new();
            if let Some(dns) = &self.dns_server {
                extra_flags.extend(["--dns".to_string(), dns.clone()]);
            }
            for port in &deployment.config.deploy.expose_ports {
                extra_flags.extend(["-p".to_string(), format!("0:{port}")]);
            }
            for (key, value) in &deployment.config.deploy.env.items {
                extra_flags.extend(["-e".to_string(), format!("{key}={}", value.as_str())]);
            }
            if let Some((host_path, container_path)) = &mount_arg {
                extra_flags.extend(["-v".to_string(), format!("{host_path}:{container_path}:ro")]);
            }

            let mut image_and_args = vec![image.to_string()];
            for flag in &deployment.config.deploy.flags {
                image_and_args.push(flag.clone());
            }

            Some(DeployOutput {
                command: self.runtime.run_command(&RunSpec {
                    container_name: container_name.clone(),
                    hostname: container_name,
                    dns_domain: self.dns_domain.clone(),
                    network: self.network.clone(),
                    extra_flags,
                    image_and_args,
                }),
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
    async fn setup(
        &self,
        _deployment: &ServiceDeployment,
        _build_dir: &Path,
        _log_sender: Option<&flume::Sender<LogEntry>>,
    ) -> Result<Option<GitCommitInfo>> {
        Ok(None)
    }

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
