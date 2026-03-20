use anyhow::Result;
use async_trait::async_trait;

use crate::logs::LogEntry;
use crate::supervisor::JobCommand;
use crate::utils::cmd;

use super::{BuildSpec, RunSpec, RuntimeProvider};

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
        let _ = cmd::run("nerdctl", &["network", "rm", name]).await;
        Ok(())
    }

    async fn remove_container(&self, name: &str) -> Result<()> {
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

    async fn build_image(
        &self,
        spec: &BuildSpec,
        log_sender: Option<&flume::Sender<LogEntry>>,
        log_source: Option<&str>,
    ) -> Result<()> {
        eprintln!(
            "[maestro]: building image {} from {} (nerdctl)",
            spec.tag,
            spec.context_dir.display()
        );
        let mut args = vec!["build".to_string(), "-t".to_string(), spec.tag.clone()];
        if let Some(ref dockerfile) = spec.dockerfile {
            let dockerfile_path = spec.context_dir.join(dockerfile).display().to_string();
            args.push("-f".to_string());
            args.push(dockerfile_path);
        }
        for (key, value) in &spec.build_args {
            args.push("--build-arg".to_string());
            args.push(format!("{key}={value}"));
        }
        args.push(spec.context_dir.display().to_string());

        if let (Some(sender), Some(source)) = (log_sender, log_source) {
            cmd::run_with_logs(
                "nerdctl",
                &args,
                sender,
                source,
                crate::logs::LogOrigin::Build,
            )
            .await?;
        } else {
            cmd::run("nerdctl", &args).await?;
        }
        eprintln!("[maestro]: image {} built successfully (nerdctl)", spec.tag);
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
