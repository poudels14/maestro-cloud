use std::path::PathBuf;

use anyhow::{Result, anyhow};
use tokio::process::Command;

use crate::deployment::types::ServiceDeployment;

pub trait ServiceCommandPlanner: Send + Sync {
    fn build(&self, deployment: &ServiceDeployment) -> Option<String>;
    fn deploy(&self, deployment: &ServiceDeployment, replica_index: u32) -> Option<String>;
}

pub struct DockerBuildConfig {
    pub context_dir: PathBuf,
    pub tag: String,
    pub dockerfile: Option<String>,
}

pub struct DockerDeploymentProvider {
    pub network: String,
}
pub struct ShellDeploymentProvider;

impl DockerDeploymentProvider {
    pub async fn build(config: &DockerBuildConfig) -> Result<()> {
        eprintln!(
            "[maestro]: building docker image {} from {}",
            config.tag,
            config.context_dir.display()
        );

        let mut args = vec!["build", "-t", &config.tag];
        let dockerfile_path;
        if let Some(ref dockerfile) = config.dockerfile {
            dockerfile_path = config.context_dir.join(dockerfile).display().to_string();
            args.extend(["-f", &dockerfile_path]);
        }
        let context = config.context_dir.display().to_string();
        args.push(&context);

        let output = Command::new("docker")
            .args(&args)
            .output()
            .await
            .map_err(|err| anyhow!("failed to run docker build: {err}"))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(anyhow!("docker build failed for {}: {stderr}", config.tag));
        }

        eprintln!("[maestro]: docker image {} built successfully", config.tag);
        Ok(())
    }
}

impl ServiceCommandPlanner for DockerDeploymentProvider {
    fn build(&self, deployment: &ServiceDeployment) -> Option<String> {
        let build = deployment.config.build.as_ref()?;
        Some(format!(
            "git clone {} && docker build -f {} .",
            build.repo, build.dockerfile_path
        ))
    }

    fn deploy(&self, deployment: &ServiceDeployment, replica_index: u32) -> Option<String> {
        if let Some(image) = deployment
            .config
            .image
            .as_deref()
            .map(str::trim)
            .filter(|image| !image.is_empty())
        {
            Some(docker_run_command(
                &deployment.config.id,
                &deployment.id,
                replica_index,
                image,
                &self.network,
                &deployment.config.deploy.expose_ports,
                &deployment.config.deploy.flags,
            ))
        } else if let Some(deploy_command) = deployment.config.deploy.command.as_ref() {
            Some(to_shell_command(
                &deploy_command.command,
                &deploy_command.args,
            ))
        } else {
            None
        }
    }
}

impl ServiceCommandPlanner for ShellDeploymentProvider {
    fn build(&self, _deployment: &ServiceDeployment) -> Option<String> {
        None
    }

    fn deploy(&self, deployment: &ServiceDeployment, _replica_index: u32) -> Option<String> {
        let deploy_command = deployment.config.deploy.command.as_ref()?;
        let command = deploy_command.command.trim();
        if command.is_empty() {
            return None;
        }
        Some(to_shell_command(command, &deploy_command.args))
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
    expose_ports: &[u16],
    flags: &[String],
) -> String {
    let short_deployment_id = deployment_id.chars().take(6).collect::<String>();
    let container_name = if replica_index == 0 {
        format!("{service_id}-{short_deployment_id}")
    } else {
        format!("{service_id}-{short_deployment_id}-{replica_index}")
    };

    let mut args = format!("exec docker run --rm --name {container_name} --network {network}");
    for port in expose_ports {
        args.push_str(&format!(" -p 0:{port}"));
    }
    args.push_str(&format!(" {image}"));
    for flag in flags {
        args.push_str(&format!(" {flag}"));
    }
    args
}
