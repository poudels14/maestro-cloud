use std::collections::HashSet;
use std::time::Duration;

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use backon::{ConstantBuilder, Retryable};

use crate::logs::LogEntry;
use crate::supervisor::JobCommand;
use crate::utils::cmd;
use crate::utils::nanoid;

use crate::config::BuilderType;

use super::{BuildSpec, ManagedContainer, RunSpec, RuntimeProvider};

pub struct NerdctlRuntimeProvider;

#[async_trait]
impl RuntimeProvider for NerdctlRuntimeProvider {
    fn cli_name(&self) -> &str {
        "nerdctl"
    }

    fn requires_explicit_dns(&self) -> bool {
        true
    }

    async fn ensure_network(&self, name: &str, subnet: Option<&str>) -> Result<()> {
        if cmd::run("nerdctl", &["network", "inspect", name])
            .await
            .is_err()
        {
            let mut args = vec!["network", "create"];
            let subnet_flag = subnet.map(|s| format!("--subnet={s}"));
            if let Some(flag) = &subnet_flag {
                args.push(flag);
            }
            args.push(name);
            cmd::run("nerdctl", &args).await.map_err(|err| {
                anyhow::anyhow!(
                    "failed to create nerdctl network `{name}`: {err}. \
                     Check for subnet overlap/conflicts or set --subnet/--network explicitly."
                )
            })?;
        }
        Ok(())
    }

    async fn remove_network(&self, name: &str) -> Result<()> {
        let inspect = match cmd::run("nerdctl", &["network", "inspect", name]).await {
            Ok(inspect) => inspect,
            Err(_) => return Ok(()),
        };
        for container in attached_container_names(&inspect)? {
            cmd::run("nerdctl", &["rm", "-f", &container])
                .await
                .map_err(|error| {
                    anyhow!(
                        "failed to remove container `{container}` from network `{name}`: {error}"
                    )
                })?;
        }
        cmd::run("nerdctl", &["network", "rm", name])
            .await
            .map_err(|error| anyhow!("failed to remove nerdctl network `{name}`: {error}"))?;
        let cni_state = std::path::Path::new("/var/lib/cni/networks").join(name);
        if cni_state.exists() {
            let _ = std::fs::remove_dir_all(&cni_state);
        }
        Ok(())
    }

    async fn remove_container(&self, name: &str) -> Result<()> {
        let _ = cmd::run("nerdctl", &["kill", name]).await;
        let _ = cmd::run("nerdctl", &["rm", "-f", name]).await;
        Ok(())
    }

    async fn set_container_network_access(
        &self,
        name: &str,
        _network: &str,
        enabled: bool,
        _static_ip: Option<&str>,
    ) -> Result<()> {
        let paused = nerdctl_container_paused(name).await?;
        if paused != enabled {
            return Ok(());
        }
        if enabled {
            cmd::run("nerdctl", &["unpause", name]).await?;
        } else {
            cmd::run("nerdctl", &["pause", name]).await?;
        }
        if nerdctl_container_paused(name).await? == enabled {
            anyhow::bail!(
                "nerdctl reported success but container `{name}` did not become {}",
                if enabled { "unpaused" } else { "paused" }
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
            program: "nerdctl".to_string(),
            args,
        }
    }

    async fn inspect_container_ip(&self, name: &str) -> Option<String> {
        let backoff = ConstantBuilder::default()
            .with_delay(Duration::from_millis(500))
            .with_max_times(20);
        let attempt = || async {
            let stdout = cmd::run(
                "nerdctl",
                &[
                    "inspect",
                    "-f",
                    "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
                    name,
                ],
            )
            .await
            .map_err(|err| anyhow!("nerdctl inspect failed for `{name}`: {err}"))?;
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
            "nerdctl",
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
            "nerdctl",
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
                "nerdctl",
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

    async fn remove_conflicting_containers(
        &self,
        network: &str,
        names: &[String],
        ips: &[String],
    ) -> Result<()> {
        let tracked_names = names.iter().map(String::as_str).collect::<HashSet<_>>();
        let tracked_ips = ips.iter().map(String::as_str).collect::<HashSet<_>>();

        let containers = cmd::run("nerdctl", &["ps", "-a", "--format", "{{.Names}}"])
            .await
            .unwrap_or_default();
        for container in containers.lines().map(str::trim).filter(|s| !s.is_empty()) {
            let on_network = cmd::run(
                "nerdctl",
                &[
                    "inspect",
                    "--format",
                    "{{json .NetworkSettings.Networks}}",
                    container,
                ],
            )
            .await
            .is_ok_and(|out| out.contains(network));

            if !on_network && !tracked_names.contains(container) {
                continue;
            }

            let ip = cmd::run(
                "nerdctl",
                &[
                    "inspect",
                    "-f",
                    "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
                    container,
                ],
            )
            .await
            .unwrap_or_default();
            let ip = ip.trim();

            if tracked_names.contains(container) || (on_network && tracked_ips.contains(ip)) {
                let _ = cmd::run("nerdctl", &["rm", "-f", container]).await;
            }
        }

        Ok(())
    }

    async fn prune_images(&self) -> Result<()> {
        cmd::run("nerdctl", &["image", "prune", "-f"]).await?;
        Ok(())
    }

    async fn prune_containers(&self) -> Result<()> {
        cmd::run("nerdctl", &["container", "prune", "-f"]).await?;
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
            BuilderType::Default => ("nerdctl", "nerdctl"),
        };
        eprintln!(
            "[maestro]: building image {} from {} ({cli_label})",
            spec.tag,
            spec.context_dir.display()
        );
        let mut args = vec!["build".to_string(), "-t".to_string(), spec.tag.clone()];
        let mut archive_path = None;
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
                let path = std::env::temp_dir()
                    .join(format!("maestro-depot-{}.tar", nanoid::unique_id(10)));
                let output = format!("type=docker,dest={}", path.display());
                args.extend(["--output".to_string(), output]);
                archive_path = Some(path);
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
        for (key, value) in &spec.labels {
            args.push("--label".to_string());
            args.push(format!("{key}={value}"));
        }
        args.push(spec.context_dir.display().to_string());

        let build_result = if let (Some(sender), Some(source)) = (log_sender, log_source) {
            cmd::exec(cli, &args)
                .env(&command_env)
                .run_with_logs(sender, source, crate::logs::LogOrigin::Build)
                .await
        } else {
            cmd::exec(cli, &args)
                .env(&command_env)
                .run()
                .await
                .map(|_| ())
        };
        if let Err(err) = build_result {
            if let Some(path) = &archive_path {
                let _ = std::fs::remove_file(path);
            }
            return Err(err);
        }
        if let Some(path) = &archive_path {
            let load_result =
                cmd::run("nerdctl", &["load", "-i", path.to_string_lossy().as_ref()]).await;
            let _ = std::fs::remove_file(path);
            load_result?;
        }
        eprintln!(
            "[maestro]: image {} built successfully ({cli_label})",
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
        eprintln!("[maestro]: pulling nerdctl image {image}");
        let args = vec!["pull".to_string(), image.to_string()];
        if let (Some(sender), Some(source)) = (log_sender, log_source) {
            cmd::exec("nerdctl", &args)
                .run_with_logs(sender, source, crate::logs::LogOrigin::Build)
                .await?;
        } else {
            cmd::exec("nerdctl", &args).run().await?;
        }
        eprintln!("[maestro]: nerdctl image {image} pulled successfully");
        Ok(())
    }

    async fn image_exists(&self, image: &str) -> Result<bool> {
        let status = tokio::process::Command::new("nerdctl")
            .args(["image", "inspect", image])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .await?;
        Ok(status.success())
    }

    async fn tag_image(&self, source: &str, target: &str) -> Result<()> {
        cmd::run("nerdctl", &["tag", source, target]).await?;
        Ok(())
    }

    async fn push_image(&self, tag: &str) -> Result<()> {
        cmd::run("nerdctl", &["push", tag]).await?;
        Ok(())
    }

    async fn exec_in_container(&self, container: &str, cmd_args: &[&str]) -> Result<String> {
        let mut args = vec!["exec", container];
        args.extend(cmd_args);
        cmd::run("nerdctl", &args).await
    }

    async fn remove_image(&self, image_id: &str) -> Result<()> {
        let _ = cmd::run("nerdctl", &["rmi", image_id]).await;
        Ok(())
    }
}

async fn nerdctl_container_paused(name: &str) -> Result<bool> {
    Ok(
        cmd::run("nerdctl", &["inspect", "-f", "{{.State.Paused}}", name])
            .await?
            .trim()
            .eq_ignore_ascii_case("true"),
    )
}

fn attached_container_names(network_inspect: &str) -> Result<Vec<String>> {
    let value: serde_json::Value = serde_json::from_str(network_inspect)?;
    let containers = value
        .as_array()
        .and_then(|networks| networks.first())
        .and_then(|network| network.get("Containers"))
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| anyhow!("nerdctl network inspect response has no Containers object"))?;
    Ok(containers
        .values()
        .filter_map(|container| container.get("Name"))
        .filter_map(serde_json::Value::as_str)
        .map(ToString::to_string)
        .collect())
}

#[cfg(test)]
mod tests {
    use super::attached_container_names;

    #[test]
    fn parses_attached_containers_without_relying_on_interface_names() {
        let inspect = r#"[{"Containers":{"abc":{"Name":"maestro-ingress-prod-a1b2"},"def":{"Name":"app-1"}}}]"#;
        let mut names = attached_container_names(inspect).unwrap();
        names.sort();
        assert_eq!(names, ["app-1", "maestro-ingress-prod-a1b2"]);
    }
}
