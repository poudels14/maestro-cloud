use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};

use crate::builder::{BuildSource, LogTarget};
use crate::config::BuilderType;
use crate::deployment::types::{GitCommitInfo, ServiceBuildConfig, ServiceDeployment};
use crate::logs::LogEntry;
use crate::runtime::{
    BuildSpec, MANAGED_IMAGE_LABEL, RunSpec, RuntimeProvider, is_immutable_image_reference,
};
use crate::supervisor::SecretsMount;
use crate::utils::crypto::SecretString;

use crate::supervisor::JobCommand;

#[derive(Debug)]
pub struct DeployOutput {
    pub command: JobCommand,
    pub secrets_mount: Option<SecretsMount>,
}

pub struct BuildOutput {
    pub image_tag: String,
}

#[derive(Debug, Clone)]
pub struct ReplicaRuntimeIdentity {
    pub node_id: String,
    pub service_id: String,
    pub deployment_id: String,
    pub replica_index: u32,
    pub assignment_id: String,
    pub container_ip: std::net::Ipv4Addr,
    pub runtime_suffix: Option<String>,
}

pub(crate) fn replica_container_name(
    service_id: &str,
    deployment_id: &str,
    replica_index: u32,
    runtime_suffix: Option<&str>,
) -> String {
    let short_deployment_id = deployment_id.chars().take(6).collect::<String>();
    let mut name = if replica_index == 0 {
        format!("{service_id}-{short_deployment_id}")
    } else {
        format!("{service_id}-{short_deployment_id}-{replica_index}")
    };
    if let Some(suffix) = runtime_suffix {
        name.push('-');
        name.push_str(suffix);
    }
    name
}

#[derive(Clone)]
pub struct ContainerDeploymentProvider {
    pub runtime: Arc<dyn RuntimeProvider>,
    pub build_command_env: HashMap<String, SecretString>,
    pub network: String,
    pub dns_domain: Option<String>,
    pub dns_server: Option<String>,
    pub secrets_dir: std::path::PathBuf,
    pub uploads_dir: std::path::PathBuf,
}

impl ContainerDeploymentProvider {
    pub async fn setup(
        &self,
        deployment: &ServiceDeployment,
        build_dir: &Path,
        log_sender: Option<&flume::Sender<LogEntry>>,
    ) -> Result<Option<GitCommitInfo>> {
        if let Some(archive_filename) = deployment.upload_archive.as_deref() {
            let archive_path = self.uploads_dir.join(archive_filename);
            if build_dir.exists() {
                std::fs::remove_dir_all(build_dir)?;
            }
            crate::utils::archive::extract_context_file(&archive_path, build_dir)?;
            let _ = std::fs::remove_file(&archive_path);
            return Ok(Some(GitCommitInfo {
                reference: "upload".to_string(),
                message: format!("maestro up: {archive_filename}"),
            }));
        }
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

    pub async fn build(
        &self,
        deployment: &ServiceDeployment,
        build_dir: &Path,
        image_tag: &str,
        log_sender: Option<flume::Sender<LogEntry>>,
    ) -> Result<BuildOutput> {
        let Some(build_config) = deployment.config.build.as_ref() else {
            let image = deployment
                .config
                .image
                .as_deref()
                .map(str::trim)
                .filter(|image| !image.is_empty())
                .ok_or_else(|| anyhow!("no image to pull"))?;
            let log_source_str = format!("{}/{}/build", deployment.config.id, deployment.id);
            self.runtime
                .pull_image(image, log_sender.as_ref(), Some(&log_source_str))
                .await?;
            let image = self
                .runtime
                .resolve_immutable_image_reference(image)
                .await
                .with_context(|| {
                    format!(
                        "failed to pin registry image `{image}`; use an immutable digest if the registry does not expose one"
                    )
                })?;
            return Ok(BuildOutput { image_tag: image });
        };
        let log_source_str = format!("{}/{}/build", deployment.config.id, deployment.id);
        let depot_project = self.depot_project(build_config);
        let use_depot = depot_project.is_some();
        let builder = if use_depot {
            BuilderType::Depot
        } else {
            BuilderType::Default
        };

        let registry = build_config.registry.as_ref();
        let (build_tag, depot_pushed) = if let (true, Some(registry)) = (use_depot, registry) {
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

        let mut labels = std::collections::HashMap::new();
        let mut command_env = self.build_command_env.clone();
        labels.insert(
            MANAGED_IMAGE_LABEL.0.to_string(),
            MANAGED_IMAGE_LABEL.1.to_string(),
        );
        if !use_depot {
            command_env.clear();
        }

        self.runtime
            .build_image(
                &BuildSpec {
                    context_dir: build_dir.to_path_buf(),
                    tag: build_tag.clone(),
                    dockerfile: Some(build_config.dockerfile.clone()),
                    labels,
                    build_args: build_config.env.items.clone(),
                    secrets: build_config.secrets.items.clone(),
                    command_env,
                    builder,
                    depot_project,
                    push_to_registry: depot_pushed,
                },
                log_sender.as_ref(),
                Some(&log_source_str),
            )
            .await?;

        let final_tag = if depot_pushed {
            build_tag
        } else if let Some(registry) = registry {
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

    pub fn deploy(
        &self,
        deployment: &ServiceDeployment,
        replica_index: u32,
    ) -> Option<DeployOutput> {
        self.deploy_with_identity(deployment, replica_index, None)
    }

    pub fn deploy_with_identity(
        &self,
        deployment: &ServiceDeployment,
        replica_index: u32,
        identity: Option<&ReplicaRuntimeIdentity>,
    ) -> Option<DeployOutput> {
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

            let container_name = replica_container_name(
                &deployment.config.id,
                &deployment.id,
                replica_index,
                identity.and_then(|identity| identity.runtime_suffix.as_deref()),
            );

            let mut extra_flags = Vec::new();
            if built_image.is_some() && !is_immutable_image_reference(image) {
                extra_flags.push("--pull=never".to_string());
            }
            if let Some(dns) = &self.dns_server {
                extra_flags.extend(["--dns".to_string(), dns.clone()]);
            }
            if identity.is_none() {
                for port in &deployment.config.deploy.expose_ports {
                    extra_flags.extend(["-p".to_string(), format!("0:{port}")]);
                }
            }
            for (key, value) in &deployment.config.deploy.env.items {
                extra_flags.extend(["-e".to_string(), format!("{key}={}", value.as_str())]);
            }
            if let Some(identity) = identity {
                extra_flags.extend(["--ip".to_string(), identity.container_ip.to_string()]);
                for (key, value) in [
                    ("maestro.node-id", identity.node_id.as_str()),
                    ("maestro.service-id", identity.service_id.as_str()),
                    ("maestro.deployment-id", identity.deployment_id.as_str()),
                    ("maestro.replica-index", &identity.replica_index.to_string()),
                    ("maestro.assignment-id", identity.assignment_id.as_str()),
                ] {
                    extra_flags.extend(["--label".to_string(), format!("{key}={value}")]);
                }
                for (key, value) in [
                    ("MAESTRO_NODE_ID", identity.node_id.as_str()),
                    ("MAESTRO_REPLICA_INDEX", &identity.replica_index.to_string()),
                    ("MAESTRO_ASSIGNMENT_ID", identity.assignment_id.as_str()),
                ] {
                    extra_flags.extend(["-e".to_string(), format!("{key}={value}")]);
                }
            }
            if let Some((host_path, container_path)) = &mount_arg {
                extra_flags.extend(["-v".to_string(), format!("{host_path}:{container_path}:ro")]);
            }
            for volume in &deployment.config.deploy.volumes {
                let suffix = if volume.read_only { ":ro" } else { "" };
                extra_flags.extend([
                    "-v".to_string(),
                    format!("{}:{}{suffix}", volume.host_path, volume.mount_path),
                ]);
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
        } else {
            None
        }
    }
}

impl ContainerDeploymentProvider {
    fn depot_project(&self, build_config: &ServiceBuildConfig) -> Option<String> {
        if self.build_command_env.is_empty() {
            return None;
        }
        build_config
            .depot
            .as_ref()
            .map(|depot| depot.project.trim())
            .filter(|project| !project.is_empty())
            .map(ToString::to_string)
    }
}

#[cfg(test)]
mod tests {
    use super::replica_container_name;

    #[test]
    fn assignment_names_are_isolated_by_explicit_node_endpoint() {
        assert_eq!(
            replica_container_name("api", "deploy123", 0, Some("node-3001")),
            "api-deploy-node-3001"
        );
        assert_eq!(
            replica_container_name("api", "deploy123", 0, Some("node-3101")),
            "api-deploy-node-3101"
        );
    }

    #[test]
    fn legacy_and_standalone_replica_names_do_not_change() {
        assert_eq!(
            replica_container_name("api", "deploy123", 0, None),
            "api-deploy"
        );
        assert_eq!(
            replica_container_name("api", "deploy123", 2, None),
            "api-deploy-2"
        );
    }
}
