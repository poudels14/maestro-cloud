use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use tokio::sync::broadcast;

use crate::cluster::registry::{EtcdNodeRegistry, NodeRegistry};
use crate::cluster::types::ClusterRuntime;
use crate::logs::Logger;
use crate::runtime::RuntimeProvider;

#[derive(Debug, Clone)]
pub struct IngressConnectorGate {
    pub network: String,
    pub containers: Vec<IngressConnector>,
}

#[derive(Debug, Clone)]
pub struct IngressConnector {
    pub name: String,
    pub static_ip: String,
}

pub fn spawn(
    registry: Arc<EtcdNodeRegistry>,
    runtime: Arc<dyn RuntimeProvider>,
    cluster: ClusterRuntime,
    certs_dir: std::path::PathBuf,
    ingress_gate: Option<IngressConnectorGate>,
    mut shutdown: broadcast::Receiver<crate::signal::ShutdownEvent>,
    logger: Logger,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let gateway_client = cluster
            .role
            .runs_workloads()
            .then(|| gateway_client(&certs_dir));
        loop {
            let mut status = match &gateway_client {
                Some(Ok(client)) => check(client, &cluster).await,
                Some(Err(error)) => Err(anyhow::anyhow!(error.to_string())),
                None => Ok(()),
            };
            if status.is_ok()
                && let Some(gate) = &ingress_gate
                && let Err(error) = set_ingress_connectors(runtime.as_ref(), gate, true).await
            {
                status = Err(error.context("failed to enable ingress connectors"));
            }
            let (ready, error) = match status {
                Ok(()) => (true, None),
                Err(err) => {
                    if let Some(gate) = &ingress_gate
                        && let Err(gate_error) =
                            set_ingress_connectors(runtime.as_ref(), gate, false).await
                    {
                        logger.emit(
                            "error",
                            &format!("failed to disable unready ingress connectors: {gate_error}"),
                        );
                    }
                    (false, Some(err.to_string()))
                }
            };
            let checked_at_ms = crate::utils::time::current_time_millis()
                .ok()
                .and_then(|value| i64::try_from(value).ok())
                .unwrap_or_default();
            if let Err(err) = registry
                .update_data_plane_status(&cluster.instance_id, ready, checked_at_ms, error.clone())
                .await
            {
                logger.emit(
                    "warn",
                    &format!("failed to publish data-plane status: {err}"),
                );
            }
            if let Some(error) = error {
                logger.emit("warn", &format!("workload data plane is unready: {error}"));
            }
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(5)) => {}
                _ = shutdown.recv() => break,
            }
        }
    })
}

async fn set_ingress_connectors(
    runtime: &dyn RuntimeProvider,
    gate: &IngressConnectorGate,
    enabled: bool,
) -> Result<()> {
    let mut errors = Vec::new();
    for container in &gate.containers {
        if let Err(error) = runtime
            .set_container_network_access(
                &container.name,
                &gate.network,
                enabled,
                Some(&container.static_ip),
            )
            .await
            .with_context(|| {
                format!(
                    "failed to {} cloudflared container `{}`",
                    if enabled { "enable" } else { "disable" },
                    container.name
                )
            })
        {
            errors.push(error.to_string());
        }
    }
    if !errors.is_empty() {
        if enabled {
            for container in &gate.containers {
                if let Err(error) = runtime
                    .set_container_network_access(
                        &container.name,
                        &gate.network,
                        false,
                        Some(&container.static_ip),
                    )
                    .await
                {
                    errors.push(format!(
                        "failed to roll back cloudflared container `{}`: {error}",
                        container.name
                    ));
                }
            }
        }
        bail!(errors.join("; "));
    }
    Ok(())
}

async fn check(client: &reqwest::Client, cluster: &ClusterRuntime) -> Result<()> {
    let url = format!(
        "https://{}:{}{}",
        cluster.host_ip,
        cluster.gateway_port,
        crate::cluster::traefik::GATEWAY_HEALTH_PATH
    );
    let response = client
        .get(&url)
        .send()
        .await
        .with_context(|| format!("node gateway probe `{url}` failed"))?;
    if !response.status().is_success() {
        bail!("node gateway probe `{url}` returned {}", response.status());
    }
    Ok(())
}

fn gateway_client(certs_dir: &Path) -> Result<reqwest::Client> {
    let ca = std::fs::read(certs_dir.join("ca.pem"))?;
    let mut identity = std::fs::read(certs_dir.join("traefik-client.pem"))?;
    identity.extend_from_slice(b"\n");
    identity.extend_from_slice(&std::fs::read(certs_dir.join("traefik-client-key.pem"))?);
    Ok(reqwest::Client::builder()
        .add_root_certificate(reqwest::Certificate::from_pem(&ca)?)
        .identity(reqwest::Identity::from_pem(&identity)?)
        .https_only(true)
        .timeout(Duration::from_secs(2))
        .build()?)
}

pub async fn inspect_tailscale_ip(
    runtime: &dyn RuntimeProvider,
    container_name: &str,
) -> Option<std::net::Ipv4Addr> {
    let status = tailscale_status(runtime, container_name).await.ok()?;
    status
        .get("Self")?
        .get("TailscaleIPs")?
        .as_array()?
        .iter()
        .filter_map(serde_json::Value::as_str)
        .find_map(|value| value.parse().ok())
}

async fn tailscale_status(
    runtime: &dyn RuntimeProvider,
    container_name: &str,
) -> Result<serde_json::Value> {
    let status = runtime
        .exec_in_container(container_name, &["tailscale", "status", "--json"])
        .await?;
    Ok(serde_json::from_str(&status)?)
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use async_trait::async_trait;

    use super::*;

    type NetworkTransition = (String, String, bool, Option<String>);

    struct RecordingRuntime {
        transitions: Mutex<Vec<NetworkTransition>>,
    }

    #[async_trait]
    impl RuntimeProvider for RecordingRuntime {
        fn cli_name(&self) -> &str {
            "test"
        }

        fn requires_explicit_dns(&self) -> bool {
            false
        }

        async fn ensure_network(&self, _name: &str, _subnet: Option<&str>) -> Result<()> {
            Ok(())
        }

        async fn remove_network(&self, _name: &str) -> Result<()> {
            Ok(())
        }

        async fn remove_container(&self, _name: &str) -> Result<()> {
            Ok(())
        }

        async fn set_container_network_access(
            &self,
            name: &str,
            network: &str,
            enabled: bool,
            static_ip: Option<&str>,
        ) -> Result<()> {
            self.transitions.lock().unwrap().push((
                name.to_string(),
                network.to_string(),
                enabled,
                static_ip.map(str::to_string),
            ));
            Ok(())
        }

        fn run_command(&self, _spec: &crate::runtime::RunSpec) -> crate::supervisor::JobCommand {
            unreachable!()
        }

        async fn inspect_container_ip(&self, _name: &str) -> Option<String> {
            None
        }

        async fn inspect_network_cidr(&self, _name: &str) -> Option<String> {
            None
        }

        async fn build_image(
            &self,
            _spec: &crate::runtime::BuildSpec,
            _log_sender: Option<&flume::Sender<crate::logs::LogEntry>>,
            _log_source: Option<&str>,
        ) -> Result<()> {
            Ok(())
        }

        async fn pull_image(
            &self,
            _image: &str,
            _log_sender: Option<&flume::Sender<crate::logs::LogEntry>>,
            _log_source: Option<&str>,
        ) -> Result<()> {
            Ok(())
        }

        async fn tag_image(&self, _source: &str, _target: &str) -> Result<()> {
            Ok(())
        }

        async fn push_image(&self, _tag: &str) -> Result<()> {
            Ok(())
        }

        async fn exec_in_container(&self, _container: &str, _cmd: &[&str]) -> Result<String> {
            Ok(String::new())
        }

        async fn remove_image(&self, _image_id: &str) -> Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn data_plane_gate_controls_every_cloudflare_connector() {
        let runtime = RecordingRuntime {
            transitions: Mutex::new(Vec::new()),
        };
        let gate = IngressConnectorGate {
            network: "maestro".to_string(),
            containers: vec![
                IngressConnector {
                    name: "cloudflared-1".to_string(),
                    static_ip: "10.100.0.248".to_string(),
                },
                IngressConnector {
                    name: "cloudflared-2".to_string(),
                    static_ip: "10.100.0.247".to_string(),
                },
            ],
        };

        set_ingress_connectors(&runtime, &gate, false)
            .await
            .unwrap();
        set_ingress_connectors(&runtime, &gate, true).await.unwrap();

        assert_eq!(
            runtime.transitions.into_inner().unwrap(),
            vec![
                (
                    "cloudflared-1".to_string(),
                    "maestro".to_string(),
                    false,
                    Some("10.100.0.248".to_string()),
                ),
                (
                    "cloudflared-2".to_string(),
                    "maestro".to_string(),
                    false,
                    Some("10.100.0.247".to_string()),
                ),
                (
                    "cloudflared-1".to_string(),
                    "maestro".to_string(),
                    true,
                    Some("10.100.0.248".to_string()),
                ),
                (
                    "cloudflared-2".to_string(),
                    "maestro".to_string(),
                    true,
                    Some("10.100.0.247".to_string()),
                ),
            ]
        );
    }

    #[test]
    fn gateway_probe_client_accepts_cluster_node_certificates() {
        let directory = std::env::temp_dir().join(format!(
            "maestro-gateway-client-test-{}",
            crate::utils::nanoid::unique_id(8)
        ));
        let ca = crate::utils::certs::generate_cluster_ca().unwrap();
        let certs = crate::utils::certs::generate_cluster_node_certs(
            &ca,
            "10.20.0.11".parse().unwrap(),
            crate::cluster::NodeRole::Voter,
        )
        .unwrap();
        crate::utils::certs::write_etcd_certs(&directory, &certs).unwrap();

        gateway_client(&directory).unwrap();
        let _ = std::fs::remove_dir_all(directory);
    }
}
