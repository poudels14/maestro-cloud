use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashSet;

use crate::logs::LogEntry;
use crate::supervisor::JobCommand;
use crate::utils::cmd;
use crate::utils::nanoid;

use crate::config::BuilderType;

use super::{BuildSpec, MANAGED_IMAGE_LABEL, RunSpec, RuntimeProvider};

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
        if let Ok(output) = cmd::run("nerdctl", &["ps", "-a", "--format", "{{.Names}}"]).await {
            for container in output.lines().map(str::trim).filter(|s| !s.is_empty()) {
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
                .is_ok_and(|out| out.contains(name));
                if on_network {
                    let _ = cmd::run("nerdctl", &["rm", "-f", container]).await;
                }
            }
        }
        let _ = cmd::run("nerdctl", &["network", "rm", name]).await;
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
        for _ in 0..10 {
            if let Ok(stdout) = cmd::run(
                "nerdctl",
                &[
                    "inspect",
                    "-f",
                    "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
                    name,
                ],
            )
            .await
            {
                let ip = stdout.trim().to_string();
                if !ip.is_empty() {
                    return Some(ip);
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
        None
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
        cmd::run(
            "nerdctl",
            &[
                "image",
                "prune",
                "-a",
                "-f",
                "--filter",
                "until=12h",
                "--filter",
                &format!("label={}={}", MANAGED_IMAGE_LABEL.0, MANAGED_IMAGE_LABEL.1),
            ],
        )
        .await?;
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
