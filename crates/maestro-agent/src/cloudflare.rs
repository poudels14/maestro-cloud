use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use maestro_core::{ByteSize, Millicores};
use maestro_runtime::{ContainerLimits, ContainerSpec, ContainerState, NetworkMode, Runtime};
use tokio::sync::watch;

pub const CLOUDFLARED_IMAGE: &str = "cloudflare/cloudflared:2026.10.4";
const CONNECTOR_PREFIX: &str = "maestro-cloudflared-";
const CHECK_INTERVAL: Duration = Duration::from_secs(5);
const STOP_GRACE: Duration = Duration::from_secs(10);
const INGRESS_PROBE_TIMEOUT: Duration = Duration::from_secs(2);
const DETACHED_GRACE: Duration = Duration::from_secs(5 * 60);
const SYSTEM_SLICE: &str = "maestro-system.slice";
const SYSTEM_LABEL: &str = "maestro.system";
const SYSTEM_OOM_SCORE_ADJ: i32 = -900;

pub struct TunnelConnectors<R: Runtime> {
    runtime: Arc<R>,
    connectors: Vec<ContainerSpec>,
    attached: watch::Receiver<bool>,
    ingress_address: String,
}

impl<R: Runtime> TunnelConnectors<R> {
    pub fn new(
        runtime: Arc<R>,
        token: &str,
        replicas: u32,
        attached: watch::Receiver<bool>,
        ingress_port: u16,
    ) -> Self {
        Self {
            runtime,
            connectors: (0..replicas).map(|index| connector(token, index)).collect(),
            attached,
            ingress_address: format!("127.0.0.1:{ingress_port}"),
        }
    }

    pub async fn run(self) {
        let mut ticker = tokio::time::interval(CHECK_INTERVAL);
        let mut was_ready = None;
        let mut last_attached = None;
        loop {
            ticker.tick().await;
            if *self.attached.borrow() {
                last_attached = Some(Instant::now());
            }
            let ready =
                recently_attached(last_attached, Instant::now()) && self.ingress_accepts().await;
            if was_ready != Some(ready) {
                tracing::info!(ready, "cloudflare tunnel connectors follow node readiness");
                was_ready = Some(ready);
            }
            self.converge(ready).await;
        }
    }

    pub async fn converge(&self, ready: bool) {
        match self.runtime.list().await {
            Err(error) => tracing::warn!(%error, "failed to list tunnel connectors"),
            Ok(containers) => {
                let running = containers
                    .into_iter()
                    .filter(|container| container.state == ContainerState::Running)
                    .map(|container| container.id)
                    .filter(|id| id.starts_with(CONNECTOR_PREFIX))
                    .collect::<Vec<_>>();
                let unwanted = running
                    .iter()
                    .filter(|id| !ready || self.connectors.iter().all(|spec| spec.id != **id))
                    .cloned()
                    .collect::<Vec<_>>();
                self.stop_running(&unwanted).await;
                if ready {
                    self.start_missing(&running).await;
                }
            }
        }
    }

    async fn start_missing(&self, running: &[String]) {
        for spec in self
            .connectors
            .iter()
            .filter(|spec| !running.contains(&spec.id))
        {
            let started = match self.runtime.pull(&spec.image).await {
                Ok(()) => self.runtime.start(spec).await.map(|_| ()),
                Err(error) => Err(error),
            };
            if let Err(error) = started {
                tracing::warn!(connector = %spec.id, %error, "failed to start tunnel connector");
            }
        }
    }

    async fn stop_running(&self, running: &[String]) {
        for id in running {
            let _ = self.runtime.stop(id, STOP_GRACE).await;
            let _ = self.runtime.remove(id).await;
        }
    }

    async fn ingress_accepts(&self) -> bool {
        tokio::time::timeout(
            INGRESS_PROBE_TIMEOUT,
            tokio::net::TcpStream::connect(&self.ingress_address),
        )
        .await
        .is_ok_and(|connected| connected.is_ok())
    }
}

fn recently_attached(last_attached: Option<Instant>, now: Instant) -> bool {
    last_attached.is_some_and(|attached_at| now.duration_since(attached_at) < DETACHED_GRACE)
}

fn connector(token: &str, index: u32) -> ContainerSpec {
    ContainerSpec {
        id: format!("{CONNECTOR_PREFIX}{index}"),
        image: CLOUDFLARED_IMAGE.to_string(),
        entrypoint: None,
        args: Some(
            ["tunnel", "--no-autoupdate", "run"]
                .into_iter()
                .map(str::to_string)
                .collect(),
        ),
        env: BTreeMap::from([("TUNNEL_TOKEN".to_string(), token.to_string())]),
        labels: BTreeMap::from([(SYSTEM_LABEL.to_string(), "cloudflared".to_string())]),
        limits: ContainerLimits {
            cpu: Millicores::new(500),
            memory: ByteSize::new(256 << 20),
            pids: 1024,
            oom_score_adj: SYSTEM_OOM_SCORE_ADJ,
        },
        cgroup_parent: SYSTEM_SLICE.to_string(),
        mounts: Vec::new(),
        network: NetworkMode::Host,
    }
}

#[cfg(test)]
mod tests {
    use maestro_runtime::FakeRuntime;

    use super::*;

    #[test]
    fn keeps_connectors_through_a_leader_change_but_not_a_long_detachment() {
        let now = Instant::now();
        assert!(!recently_attached(None, now));
        assert!(recently_attached(Some(now), now));
        assert!(recently_attached(
            Some(now),
            now + DETACHED_GRACE - Duration::from_secs(1)
        ));
        assert!(!recently_attached(Some(now), now + DETACHED_GRACE));
    }

    #[tokio::test]
    async fn runs_connectors_only_while_the_node_is_ready() {
        let runtime = Arc::new(FakeRuntime::default());
        let (_attached, receiver) = watch::channel(true);
        let connectors = TunnelConnectors::new(runtime.clone(), "token", 2, receiver, 80);
        connectors.converge(true).await;
        assert_eq!(runtime.start_count("maestro-cloudflared-0"), 1);
        assert_eq!(runtime.start_count("maestro-cloudflared-1"), 1);
        assert_eq!(
            runtime.spec("maestro-cloudflared-0").unwrap().env["TUNNEL_TOKEN"],
            "token"
        );
        connectors.converge(true).await;
        assert_eq!(runtime.start_count("maestro-cloudflared-0"), 1);

        let (_attached, receiver) = watch::channel(true);
        TunnelConnectors::new(runtime.clone(), "", 1, receiver, 80)
            .converge(true)
            .await;
        assert!(
            runtime
                .list()
                .await
                .unwrap()
                .iter()
                .all(|container| container.id != "maestro-cloudflared-1"
                    || container.state != ContainerState::Running)
        );

        connectors.converge(false).await;
        let running = runtime
            .list()
            .await
            .unwrap()
            .into_iter()
            .filter(|container| container.state == ContainerState::Running)
            .count();
        assert_eq!(running, 0);
    }
}
