use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use maestro_core::{ByteSize, Millicores};
use maestro_runtime::{
    BindMount, ContainerLimits, ContainerSpec, ContainerState, NetworkMode, Runtime,
};

use crate::ingress::entry_point_name;

const SYSTEM_LABEL: &str = "maestro.system";
const SYSTEM_SLICE: &str = "maestro-system.slice";
const CHECK_INTERVAL: Duration = Duration::from_secs(5);
const STOP_GRACE: Duration = Duration::from_secs(10);
const TRAEFIK_IMAGE: &str = "traefik:v3.6";
const TRAEFIK_CONFIG_MOUNT: &str = "/etc/maestro-traefik";
const SYSTEM_OOM_SCORE_ADJ: i32 = -900;
const ADMIN_API_SOCKET: &str = "/run/maestro/api.sock";
pub const ADMIN_CONTAINER_ID: &str = "maestro-admin";

pub struct SystemSupervisor<R: Runtime> {
    runtime: Arc<R>,
    containers: Vec<ContainerSpec>,
}

impl<R: Runtime> SystemSupervisor<R> {
    pub fn new(runtime: Arc<R>, containers: Vec<ContainerSpec>) -> Self {
        Self {
            runtime,
            containers,
        }
    }

    pub async fn run(self) {
        let mut ticker = tokio::time::interval(CHECK_INTERVAL);
        loop {
            ticker.tick().await;
            self.ensure_running().await;
        }
    }

    pub async fn ensure_running(&self) {
        let running = match self.runtime.list().await {
            Ok(containers) => containers
                .into_iter()
                .filter(|container| container.state == ContainerState::Running)
                .map(|container| container.id)
                .collect::<Vec<_>>(),
            Err(error) => {
                tracing::warn!(%error, "failed to list system containers");
                Vec::new()
            }
        };
        for spec in self
            .containers
            .iter()
            .filter(|spec| !running.contains(&spec.id))
        {
            let started = match self.runtime.pull(&spec.image).await {
                Ok(()) => self.runtime.start(spec).await.map(|_| ()),
                Err(error) => Err(error),
            };
            match started {
                Ok(()) => tracing::info!(container = %spec.id, "started system container"),
                Err(error) => {
                    tracing::warn!(container = %spec.id, %error, "failed to start system container")
                }
            }
        }
    }

    pub async fn stop_all(&self) {
        for spec in &self.containers {
            let _ = self.runtime.stop(&spec.id, STOP_GRACE).await;
            let _ = self.runtime.remove(&spec.id).await;
        }
    }
}

pub fn traefik_container(config_dir: &Path, ports: &[u16]) -> ContainerSpec {
    let entry_points = ports
        .iter()
        .map(|port| format!("--entryPoints.{}.address=:{port}", entry_point_name(*port)));
    let args = [
        format!("--providers.file.directory={TRAEFIK_CONFIG_MOUNT}"),
        "--providers.file.watch=true".to_string(),
        "--accesslog=true".to_string(),
        "--accesslog.format=json".to_string(),
        "--log.level=WARN".to_string(),
    ]
    .into_iter()
    .chain(entry_points)
    .collect();
    ContainerSpec {
        id: "maestro-traefik".to_string(),
        image: TRAEFIK_IMAGE.to_string(),
        entrypoint: Some(vec!["traefik".to_string()]),
        args: Some(args),
        env: BTreeMap::new(),
        labels: BTreeMap::from([(SYSTEM_LABEL.to_string(), "traefik".to_string())]),
        limits: ContainerLimits {
            cpu: Millicores::new(2000),
            memory: ByteSize::new(1 << 30),
            pids: 4096,
            oom_score_adj: SYSTEM_OOM_SCORE_ADJ,
        },
        cgroup_parent: SYSTEM_SLICE.to_string(),
        mounts: vec![BindMount {
            source: config_dir.to_path_buf(),
            destination: TRAEFIK_CONFIG_MOUNT.to_string(),
            read_only: true,
        }],
        network: NetworkMode::Host,
    }
}

pub fn admin_container(image: &str, api_socket: &Path) -> ContainerSpec {
    ContainerSpec {
        id: ADMIN_CONTAINER_ID.to_string(),
        image: image.to_string(),
        entrypoint: None,
        args: None,
        env: BTreeMap::from([
            ("PORT".to_string(), "80".to_string()),
            (
                "MAESTRO_API_SOCKET".to_string(),
                ADMIN_API_SOCKET.to_string(),
            ),
        ]),
        labels: BTreeMap::from([(SYSTEM_LABEL.to_string(), "admin".to_string())]),
        limits: ContainerLimits {
            cpu: Millicores::new(1000),
            memory: ByteSize::new(512 << 20),
            pids: 1024,
            oom_score_adj: SYSTEM_OOM_SCORE_ADJ,
        },
        cgroup_parent: SYSTEM_SLICE.to_string(),
        mounts: vec![BindMount {
            source: api_socket.to_path_buf(),
            destination: ADMIN_API_SOCKET.to_string(),
            read_only: false,
        }],
        network: NetworkMode::Bridge,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn admin_only_mounts_the_api_socket() {
        let spec = admin_container(
            "maestro.local/maestro-admin:0.1.0",
            Path::new("/run/maestro/api.sock"),
        );
        assert_eq!(spec.network, NetworkMode::Bridge);
        assert_eq!(spec.mounts.len(), 1);
        assert_eq!(spec.mounts[0].destination, ADMIN_API_SOCKET);
        assert_eq!(spec.env["MAESTRO_API_SOCKET"], ADMIN_API_SOCKET);
    }

    #[test]
    fn traefik_listens_on_configured_ports_in_system_slice() {
        let spec = traefik_container(Path::new("/var/lib/maestro/traefik"), &[80, 443]);
        let args = spec.args.unwrap();
        assert!(args.contains(&"--entryPoints.web80.address=:80".to_string()));
        assert!(args.contains(&"--entryPoints.web443.address=:443".to_string()));
        assert_eq!(spec.cgroup_parent, "maestro-system.slice");
        assert_eq!(spec.network, NetworkMode::Host);
        assert_eq!(spec.mounts[0].destination, TRAEFIK_CONFIG_MOUNT);
    }
}
