use std::future::Future;
use std::net::IpAddr;
use std::time::Duration;

use maestro_core::{AgentMessage, ControlMessage, NodeId, ReplicaPlan};
use tokio::sync::{mpsc, watch};

use crate::dns::DnsRecords;
use crate::error::AgentError;
use crate::ingress::IngressWriter;
use crate::source::PlanSource;
use crate::status::StatusBoard;
use crate::wireguard::{MeshConfig, WireGuardMesh};

const HEARTBEAT: Duration = Duration::from_secs(5);
const RECONNECT_MIN: Duration = Duration::from_millis(500);
const RECONNECT_MAX: Duration = Duration::from_secs(10);

pub trait ControlLink: Send + Sync + 'static {
    fn connect(
        &self,
        target: &str,
    ) -> impl Future<
        Output = Result<(mpsc::Sender<AgentMessage>, mpsc::Receiver<ControlMessage>), String>,
    > + Send;
}

pub struct StreamPlanSource {
    plans: watch::Receiver<Vec<ReplicaPlan>>,
    initialized: watch::Receiver<bool>,
}

pub struct Attachment<L: ControlLink> {
    link: L,
    node_id: NodeId,
    seeds: Vec<String>,
    board: StatusBoard,
    plans: watch::Sender<Vec<ReplicaPlan>>,
    initialized: watch::Sender<bool>,
    attached: watch::Sender<bool>,
    members: watch::Sender<Option<Vec<NodeId>>>,
    node_addresses: watch::Sender<Vec<IpAddr>>,
    ingress: IngressWriter,
    dns: DnsRecords,
    mesh: Option<(WireGuardMesh, MeshConfig)>,
}

enum SessionEnd {
    Disconnected,
    Redirected(Option<String>),
}

impl<L: ControlLink> Attachment<L> {
    pub fn new(
        link: L,
        node_id: NodeId,
        seeds: Vec<String>,
        board: StatusBoard,
        ingress: IngressWriter,
        dns: DnsRecords,
        mesh: Option<(WireGuardMesh, MeshConfig)>,
    ) -> (Self, StreamPlanSource) {
        let (plans, plans_receiver) = watch::channel(Vec::new());
        let (initialized, initialized_receiver) = watch::channel(false);
        let attachment = Self {
            link,
            node_id,
            seeds,
            board,
            plans,
            initialized,
            attached: watch::channel(false).0,
            members: watch::channel(None).0,
            node_addresses: watch::channel(Vec::new()).0,
            ingress,
            dns,
            mesh,
        };
        let source = StreamPlanSource {
            plans: plans_receiver,
            initialized: initialized_receiver,
        };
        (attachment, source)
    }

    pub fn attached(&self) -> watch::Receiver<bool> {
        self.attached.subscribe()
    }

    pub fn node_addresses(&self) -> watch::Receiver<Vec<IpAddr>> {
        self.node_addresses.subscribe()
    }

    pub fn members(&self) -> watch::Receiver<Option<Vec<NodeId>>> {
        self.members.subscribe()
    }

    pub async fn run(mut self) {
        let mut leader_hint: Option<String> = None;
        let mut seed_index = 0;
        let mut backoff = RECONNECT_MIN;
        loop {
            let target = leader_hint.take().unwrap_or_else(|| {
                seed_index = (seed_index + 1) % self.seeds.len().max(1);
                self.seeds.get(seed_index).cloned().unwrap_or_default()
            });
            match self.link.connect(&target).await {
                Ok((sender, receiver)) => {
                    tracing::info!(%target, "attached to the control plane");
                    backoff = RECONNECT_MIN;
                    let ended = self.session(sender, receiver).await;
                    self.attached.send_replace(false);
                    match ended {
                        SessionEnd::Redirected(leader) => leader_hint = leader,
                        SessionEnd::Disconnected => {
                            tracing::info!(%target, "control stream closed; reconnecting")
                        }
                    }
                }
                Err(error) => {
                    tracing::debug!(%target, %error, "failed to attach to control plane");
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(RECONNECT_MAX);
                }
            }
        }
    }

    async fn session(
        &mut self,
        sender: mpsc::Sender<AgentMessage>,
        mut receiver: mpsc::Receiver<ControlMessage>,
    ) -> SessionEnd {
        let mut board_changes = self.board.subscribe();
        let mut heartbeat = tokio::time::interval(HEARTBEAT);
        let hello = AgentMessage::Hello {
            node_id: self.node_id,
        };
        match sender.send(hello).await {
            Err(_) => SessionEnd::Disconnected,
            Ok(()) => loop {
                tokio::select! {
                    _ = heartbeat.tick() => {
                        if self.report(&sender).await.is_err() {
                            break SessionEnd::Disconnected;
                        }
                    }
                    _ = board_changes.changed() => {
                        if self.report(&sender).await.is_err() {
                            break SessionEnd::Disconnected;
                        }
                    }
                    message = receiver.recv() => match message {
                        None => break SessionEnd::Disconnected,
                        Some(ControlMessage::NotLeader { leader }) => break SessionEnd::Redirected(leader),
                        Some(message) => self.apply(message).await,
                    },
                }
            },
        }
    }

    async fn report(
        &self,
        sender: &mpsc::Sender<AgentMessage>,
    ) -> Result<(), mpsc::error::SendError<AgentMessage>> {
        sender
            .send(AgentMessage::Replicas {
                statuses: self.board.snapshot(),
            })
            .await
    }

    async fn apply(&mut self, message: ControlMessage) {
        match message {
            ControlMessage::Plans { plans } => {
                self.plans.send_replace(plans);
                self.initialized.send_replace(true);
                self.attached.send_replace(true);
            }
            ControlMessage::Routes { table, dns } => {
                if let Err(error) = self.ingress.write(&table).await {
                    tracing::warn!(%error, "failed to write ingress config");
                }
                self.dns.replace(dns.into_iter().collect());
            }
            ControlMessage::Mesh { peers, members } => {
                if !members.is_empty() {
                    self.members.send_replace(Some(members));
                }
                self.node_addresses
                    .send_replace(peers.iter().map(|peer| peer.endpoint.ip()).collect());
                if let Some((mesh, template)) = &self.mesh {
                    let config = MeshConfig {
                        peers,
                        ..template.clone()
                    };
                    if let Err(error) = mesh.apply(&config).await {
                        tracing::warn!(%error, "failed to apply the WireGuard mesh");
                    }
                }
            }
            ControlMessage::NotLeader { .. } => {}
        }
    }
}

impl PlanSource for StreamPlanSource {
    async fn plans(&self) -> Result<Vec<ReplicaPlan>, AgentError> {
        let mut initialized = self.initialized.clone();
        match *initialized.borrow_and_update() {
            true => Ok(self.plans.borrow().clone()),
            false => Err(AgentError::Host(
                "waiting for the first plan from the control plane".to_string(),
            )),
        }
    }

    async fn changed(&self) {
        let mut plans = self.plans.clone();
        let _ = plans.changed().await;
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};

    use maestro_core::{
        ByteSize, DeploymentId, EffectiveResources, Millicores, OomPriority, ReplicaId,
        ResolvedRequest, RouteTable, ServiceId,
    };

    use super::*;

    type ControlEnd = (mpsc::Receiver<AgentMessage>, mpsc::Sender<ControlMessage>);

    #[derive(Clone, Default)]
    struct FakeLink {
        attempts: Arc<Mutex<Vec<String>>>,
        sessions: Arc<Mutex<Vec<ControlEnd>>>,
    }

    impl ControlLink for FakeLink {
        async fn connect(
            &self,
            target: &str,
        ) -> Result<(mpsc::Sender<AgentMessage>, mpsc::Receiver<ControlMessage>), String> {
            self.attempts.lock().unwrap().push(target.to_string());
            let (agent_sender, agent_receiver) = mpsc::channel(16);
            let (control_sender, control_receiver) = mpsc::channel(16);
            self.sessions
                .lock()
                .unwrap()
                .push((agent_receiver, control_sender));
            Ok((agent_sender, control_receiver))
        }
    }

    impl FakeLink {
        async fn next_session(&self) -> ControlEnd {
            loop {
                let session = self.sessions.lock().unwrap().pop();
                match session {
                    Some(session) => break session,
                    None => tokio::task::yield_now().await,
                }
            }
        }
    }

    fn plan() -> ReplicaPlan {
        ReplicaPlan {
            replica_id: ReplicaId::from("d1-0"),
            service_id: ServiceId::from("api"),
            deployment_id: DeploymentId::from("d1"),
            image: "nginx".to_string(),
            resources: EffectiveResources {
                cpu_limit: Millicores::new(1000),
                memory_limit: ByteSize::new(1 << 30),
                request: ResolvedRequest {
                    cpu: Millicores::new(250),
                    memory: ByteSize::new(512 << 20),
                },
            },
            oom_priority: OomPriority::Normal,
            command: None,
            env: BTreeMap::new(),
            healthcheck: None,
            max_restarts: None,
            volumes: Vec::new(),
            secrets: None,
            image_source: None,
            egress: Vec::new(),
        }
    }

    fn attachment(
        link: FakeLink,
    ) -> (
        Attachment<FakeLink>,
        StreamPlanSource,
        DnsRecords,
        tempfile::TempDir,
    ) {
        let directory = tempfile::tempdir().unwrap();
        let dns = DnsRecords::default();
        let (attachment, source) = Attachment::new(
            link,
            NodeId(7),
            vec!["10.0.0.1:7400".to_string()],
            StatusBoard::new(),
            IngressWriter::new(directory.path().to_path_buf(), &[80]),
            dns.clone(),
            None,
        );
        (attachment, source, dns, directory)
    }

    #[tokio::test]
    async fn applies_plans_and_routes_from_the_leader() {
        let link = FakeLink::default();
        let (attachment, source, dns, _directory) = attachment(link.clone());
        assert!(source.plans().await.is_err());
        tokio::spawn(attachment.run());

        let (mut from_agent, to_agent) = link.next_session().await;
        assert_eq!(
            from_agent.recv().await,
            Some(AgentMessage::Hello { node_id: NodeId(7) })
        );
        to_agent
            .send(ControlMessage::Plans {
                plans: vec![plan()],
            })
            .await
            .unwrap();
        to_agent
            .send(ControlMessage::Routes {
                table: RouteTable::default(),
                dns: BTreeMap::from([(
                    "api.maestro.internal".to_string(),
                    vec!["10.200.1.2".parse().unwrap()],
                )]),
            })
            .await
            .unwrap();
        source.changed().await;
        assert_eq!(source.plans().await.unwrap(), vec![plan()]);
        while dns.lookup("api.maestro.internal").is_none() {
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test]
    async fn follows_leader_redirects_and_keeps_plans_on_disconnect() {
        let link = FakeLink::default();
        let (attachment, source, _dns, _directory) = attachment(link.clone());
        tokio::spawn(attachment.run());

        let (_from_agent, to_agent) = link.next_session().await;
        to_agent
            .send(ControlMessage::Plans {
                plans: vec![plan()],
            })
            .await
            .unwrap();
        source.changed().await;
        to_agent
            .send(ControlMessage::NotLeader {
                leader: Some("10.0.0.2:7400".to_string()),
            })
            .await
            .unwrap();

        let _second = link.next_session().await;
        assert_eq!(
            link.attempts.lock().unwrap().last().unwrap(),
            "10.0.0.2:7400"
        );
        assert_eq!(source.plans().await.unwrap(), vec![plan()]);
    }
}
