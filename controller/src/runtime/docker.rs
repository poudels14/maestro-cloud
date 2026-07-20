use std::time::Duration;

use anyhow::{Result, anyhow, bail};
use async_trait::async_trait;
use backon::{ConstantBuilder, Retryable};

use crate::logs::LogEntry;
use crate::supervisor::JobCommand;
use crate::utils::cmd;

use crate::config::BuilderType;

use super::{
    BuildSpec, ExecSession, InteractiveExecRequest, ManagedContainer, RunSpec, RuntimeProvider,
};

pub struct DockerRuntimeProvider;

#[async_trait]
impl RuntimeProvider for DockerRuntimeProvider {
    fn cli_name(&self) -> &str {
        "docker"
    }

    fn requires_explicit_dns(&self) -> bool {
        false
    }

    fn supports_dynamic_network_attachment(&self) -> bool {
        true
    }

    async fn ensure_network(&self, name: &str, subnet: Option<&str>) -> Result<()> {
        if cmd::run("docker", &["network", "inspect", name])
            .await
            .is_err()
        {
            let mut args = vec!["network", "create"];
            let subnet_flag = subnet.map(|s| format!("--subnet={s}"));
            if let Some(flag) = &subnet_flag {
                args.push(flag);
            }
            args.push(name);
            cmd::run("docker", &args).await.map_err(|err| {
                anyhow::anyhow!(
                    "failed to create docker network `{name}`: {err}. \
                     Check for subnet overlap/conflicts or set --subnet/--network explicitly."
                )
            })?;
        }
        Ok(())
    }

    async fn remove_network(&self, name: &str) -> Result<()> {
        let inspect = match cmd::run("docker", &["network", "inspect", name]).await {
            Ok(inspect) => inspect,
            Err(_) => return Ok(()),
        };
        for container in attached_container_names(&inspect)? {
            cmd::run("docker", &["rm", "-f", &container])
                .await
                .map_err(|error| {
                    anyhow!(
                        "failed to remove container `{container}` from network `{name}`: {error}"
                    )
                })?;
        }
        cmd::run("docker", &["network", "rm", name])
            .await
            .map_err(|error| anyhow!("failed to remove docker network `{name}`: {error}"))?;
        Ok(())
    }

    async fn remove_container(&self, name: &str) -> Result<()> {
        let _ = cmd::run("docker", &["rm", "-f", name]).await;
        Ok(())
    }

    async fn set_container_network_access(
        &self,
        name: &str,
        network: &str,
        enabled: bool,
        static_ip: Option<&str>,
    ) -> Result<()> {
        let attached = docker_network_attached(name, network).await?;
        if attached == enabled {
            return Ok(());
        }
        if enabled {
            let mut args = vec!["network", "connect"];
            if let Some(ip) = static_ip {
                args.extend(["--ip", ip]);
            }
            args.extend([network, name]);
            cmd::run("docker", &args).await?;
        } else {
            cmd::run("docker", &["network", "disconnect", "-f", network, name]).await?;
        }
        if docker_network_attached(name, network).await? != enabled {
            anyhow::bail!(
                "docker reported success but container `{name}` network `{network}` did not become {}",
                if enabled { "attached" } else { "detached" }
            );
        }
        Ok(())
    }

    fn run_command(&self, spec: &RunSpec) -> JobCommand {
        let mut args = vec![
            "run".to_string(),
            "--rm".to_string(),
            "--name".to_string(),
            spec.container_name.clone(),
            "--hostname".to_string(),
            spec.hostname.clone(),
        ];
        if let Some(domain) = &spec.dns_domain {
            args.extend(["--domainname".to_string(), domain.clone()]);
        }
        args.extend(["--network".to_string(), spec.network.clone()]);
        for flag in &spec.extra_flags {
            args.push(flag.clone());
        }
        for arg in &spec.image_and_args {
            args.push(arg.clone());
        }
        JobCommand::Exec {
            program: "docker".to_string(),
            args,
        }
    }

    async fn inspect_container_ip(&self, name: &str) -> Option<String> {
        let backoff = ConstantBuilder::default()
            .with_delay(Duration::from_millis(500))
            .with_max_times(20);
        let attempt = || async {
            let stdout = cmd::run(
                "docker",
                &[
                    "inspect",
                    "-f",
                    "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
                    name,
                ],
            )
            .await
            .map_err(|err| anyhow!("docker inspect failed for `{name}`: {err}"))?;
            let ip = stdout.trim().to_string();
            if ip.is_empty() {
                Err(anyhow!("ip not yet assigned for `{name}`"))
            } else {
                Ok::<String, anyhow::Error>(ip)
            }
        };
        attempt.retry(backoff).await.ok()
    }

    async fn inspect_network_cidr(&self, name: &str) -> Option<String> {
        let stdout = cmd::run(
            "docker",
            &[
                "network",
                "inspect",
                name,
                "--format",
                "{{range .IPAM.Config}}{{.Subnet}}{{end}}",
            ],
        )
        .await
        .ok()?;
        let cidr = stdout.trim().to_string();
        if cidr.is_empty() { None } else { Some(cidr) }
    }

    async fn list_managed_containers(&self, node_id: &str) -> Result<Vec<ManagedContainer>> {
        let filter = format!("label=maestro.node-id={node_id}");
        let output = cmd::run(
            "docker",
            &["ps", "--filter", &filter, "--format", "{{.Names}}"],
        )
        .await?;
        let mut containers = Vec::new();
        for name in output
            .lines()
            .map(str::trim)
            .filter(|name| !name.is_empty())
        {
            let labels = cmd::run(
                "docker",
                &["inspect", "-f", "{{json .Config.Labels}}", name],
            )
            .await?;
            containers.push(ManagedContainer {
                name: name.to_string(),
                labels: serde_json::from_str(labels.trim()).unwrap_or_default(),
            });
        }
        Ok(containers)
    }

    async fn prune_images(&self) -> Result<()> {
        cmd::run("docker", &["image", "prune", "-f"]).await?;
        Ok(())
    }

    async fn prune_containers(&self) -> Result<()> {
        cmd::run("docker", &["container", "prune", "-f"]).await?;
        Ok(())
    }

    async fn build_image(
        &self,
        spec: &BuildSpec,
        log_sender: Option<&flume::Sender<LogEntry>>,
        log_source: Option<&str>,
    ) -> Result<()> {
        let mut command_env = spec.command_env.clone();
        command_env.extend(spec.secrets.clone());
        let (cli, cli_label) = match spec.builder {
            BuilderType::Depot => ("depot", "depot"),
            BuilderType::Default => ("docker", "docker"),
        };
        eprintln!(
            "[maestro]: building {cli_label} image {} from {}",
            spec.tag,
            spec.context_dir.display()
        );
        let mut args = vec!["build".to_string(), "-t".to_string(), spec.tag.clone()];
        if spec.builder == BuilderType::Depot {
            let arch = match std::env::consts::ARCH {
                "aarch64" => "arm64",
                other => other,
            };
            if let Some(project) = &spec.depot_project {
                args.extend(["--project".to_string(), project.clone()]);
            }
            args.push(format!("--platform=linux/{arch}"));
            if spec.push_to_registry {
                args.push("--push".to_string());
            } else {
                args.push("--load".to_string());
            }
        }
        if let Some(ref dockerfile) = spec.dockerfile {
            let dockerfile_path = spec.context_dir.join(dockerfile).display().to_string();
            args.push("-f".to_string());
            args.push(dockerfile_path);
        }
        for (key, value) in &spec.build_args {
            args.push("--build-arg".to_string());
            args.push(format!("{key}={}", value.as_str()));
        }
        crate::runtime::append_build_secret_args(&mut args, &spec.secrets);
        for (key, value) in &spec.labels {
            args.push("--label".to_string());
            args.push(format!("{key}={value}"));
        }
        args.push(spec.context_dir.display().to_string());

        if let (Some(sender), Some(source)) = (log_sender, log_source) {
            cmd::exec(cli, &args)
                .env(&command_env)
                .run_with_logs(sender, source, crate::logs::LogOrigin::Build)
                .await?;
        } else {
            cmd::exec(cli, &args).env(&command_env).run().await?;
        }
        eprintln!(
            "[maestro]: {cli_label} image {} built successfully",
            spec.tag
        );
        Ok(())
    }

    async fn pull_image(
        &self,
        image: &str,
        log_sender: Option<&flume::Sender<LogEntry>>,
        log_source: Option<&str>,
    ) -> Result<()> {
        eprintln!("[maestro]: pulling docker image {image}");
        let args = vec!["pull".to_string(), image.to_string()];
        if let (Some(sender), Some(source)) = (log_sender, log_source) {
            cmd::exec("docker", &args)
                .run_with_logs(sender, source, crate::logs::LogOrigin::Build)
                .await?;
        } else {
            cmd::exec("docker", &args).run().await?;
        }
        eprintln!("[maestro]: docker image {image} pulled successfully");
        Ok(())
    }

    async fn export_image(&self, image: &str, output: crate::runtime::ImageOutput) -> Result<()> {
        crate::runtime::stream_command_output("docker", &["save", image], output).await
    }

    async fn import_image(&self, image: &str, input: crate::runtime::ImageInput) -> Result<()> {
        crate::runtime::stream_command_input("docker", &["load"], input).await?;
        if !self.image_exists(image).await? {
            bail!("imported archive did not contain expected image `{image}`");
        }
        Ok(())
    }

    async fn image_exists(&self, image: &str) -> Result<bool> {
        let status = tokio::process::Command::new("docker")
            .args(["image", "inspect", image])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .await?;
        Ok(status.success())
    }

    async fn resolve_immutable_image_reference(&self, image: &str) -> Result<String> {
        let inspect = cmd::run("docker", &["image", "inspect", image]).await?;
        crate::runtime::immutable_image_reference(image, &inspect)
    }

    async fn tag_image(&self, source: &str, target: &str) -> Result<()> {
        cmd::run("docker", &["tag", source, target]).await?;
        Ok(())
    }

    async fn push_image(&self, tag: &str) -> Result<()> {
        cmd::run("docker", &["push", tag]).await?;
        Ok(())
    }

    async fn exec_in_container(&self, container: &str, cmd_args: &[&str]) -> Result<String> {
        let mut args = vec!["exec", container];
        args.extend(cmd_args);
        cmd::run("docker", &args).await
    }

    async fn interactive_exec(&self, _request: InteractiveExecRequest) -> Result<ExecSession> {
        anyhow::bail!("interactive exec is not supported for the docker runtime")
    }

    async fn remove_image(&self, image_id: &str) -> Result<()> {
        let _ = cmd::run("docker", &["rmi", image_id]).await;
        Ok(())
    }
}

async fn docker_network_attached(name: &str, network: &str) -> Result<bool> {
    let backoff = ConstantBuilder::default()
        .with_delay(Duration::from_millis(100))
        .with_max_times(20);
    let inspect = || async {
        let networks = cmd::run(
            "docker",
            &["inspect", "-f", "{{json .NetworkSettings.Networks}}", name],
        )
        .await?;
        Ok::<bool, anyhow::Error>(
            serde_json::from_str::<serde_json::Value>(networks.trim())?
                .as_object()
                .is_some_and(|networks| networks.contains_key(network)),
        )
    };
    inspect.retry(backoff).await
}

fn attached_container_names(network_inspect: &str) -> Result<Vec<String>> {
    let value: serde_json::Value = serde_json::from_str(network_inspect)?;
    let containers = value
        .as_array()
        .and_then(|networks| networks.first())
        .and_then(|network| network.get("Containers"))
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| anyhow!("docker network inspect response has no Containers object"))?;
    Ok(containers
        .values()
        .filter_map(|container| container.get("Name"))
        .filter_map(serde_json::Value::as_str)
        .map(ToString::to_string)
        .collect())
}

#[cfg(test)]
mod tests {
    use super::{DockerRuntimeProvider, attached_container_names};
    use crate::runtime::{InteractiveExecRequest, RuntimeProvider};

    #[test]
    fn parses_attached_containers_from_network_inspect() {
        let inspect =
            r#"[{"Containers":{"abc":{"Name":"maestro-etcd-prod-a1b2"},"def":{"Name":"app-1"}}}]"#;
        let mut names = attached_container_names(inspect).unwrap();
        names.sort();
        assert_eq!(names, ["app-1", "maestro-etcd-prod-a1b2"]);
    }

    #[tokio::test]
    async fn interactive_exec_reports_docker_as_unsupported() {
        let error = DockerRuntimeProvider
            .interactive_exec(InteractiveExecRequest {
                container: "app".to_string(),
                command: vec!["/bin/sh".to_string()],
                tty: true,
                initial_size: None,
                session_root: std::env::temp_dir(),
            })
            .await
            .err()
            .expect("docker interactive exec must fail");
        assert_eq!(
            error.to_string(),
            "interactive exec is not supported for the docker runtime"
        );
    }
}
