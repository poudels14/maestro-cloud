use std::collections::{BTreeMap, VecDeque};
use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use kernel_api::{
    Assignment, AssignmentId, AssignmentPhase, AssignmentSpec, AssignmentStatus, CommandSpec,
    DeploymentId, ExecStreamFrame, Generation, NodeId, Object, SecretValue, ServiceId, WorkloadId,
};
use runtime::{ExecInput, ExecMode, ExecOutput, ExecRequest, ExecSession, RuntimeError};
use tokio::sync::Notify;
use tokio_tungstenite::tungstenite::{Error as WebSocketError, Message};

use crate::{
    ApiServer, ClusterExecSessions, ExecSessionOpenError, HttpClusterExecSessions, ServerSettings,
    TlsIdentity,
};

use super::deployments::deployment;
use super::{metadata, put, seeded_store};

#[tokio::test]
async fn public_exec_routes_the_scoped_assignment_and_preserves_stream_frames()
-> Result<(), Box<dyn std::error::Error>> {
    let (store, cluster_id) = seeded_store().await?;
    let deployment = deployment("api-deployment", "api")?;
    let assignment = assignment("assignment-1", "api-deployment", "node-remote")?;
    put(
        &store,
        &cluster_id,
        "Deployment",
        deployment.meta.id.as_str(),
        &deployment,
    )
    .await?;
    put(
        &store,
        &cluster_id,
        "Assignment",
        assignment.meta.id.as_str(),
        &assignment,
    )
    .await?;
    let sessions = Arc::new(TestExecSessions::default());
    let server = ApiServer::new(
        store,
        cluster_id,
        ServerSettings::new("127.0.0.1:0".parse()?, None),
    )?
    .with_exec_sessions(sessions.clone())
    .bind()
    .await?;
    let address = server.local_address();
    let (shutdown, receiver) = tokio::sync::watch::channel(false);
    let task = tokio::spawn(server.serve(receiver));

    let mut endpoint = reqwest::Url::parse(&format!(
        "ws://{address}/api/services/api/deployments/api-deployment/assignments/assignment-1/exec"
    ))?;
    endpoint.query_pairs_mut().append_pair(
        "command",
        &serde_json::to_string(&vec!["/bin/echo", "hello world"])?,
    );
    let (mut socket, _) = tokio_tungstenite::connect_async(endpoint.as_str()).await?;
    assert_eq!(
        receive_frame(&mut socket).await?,
        ExecStreamFrame::Stdout(b"node-remote:/bin/echo".to_vec())
    );
    socket
        .send(Message::Binary(
            ExecStreamFrame::Stdin(b"input".to_vec()).encode()?.into(),
        ))
        .await?;
    assert_eq!(
        receive_frame(&mut socket).await?,
        ExecStreamFrame::Stdout(b"input".to_vec())
    );
    socket
        .send(Message::Binary(ExecStreamFrame::Kill.encode()?.into()))
        .await?;
    assert_eq!(
        receive_frame(&mut socket).await?,
        ExecStreamFrame::Exited { code: Some(137) }
    );
    assert_eq!(sessions.opened.load(Ordering::SeqCst), 1);

    shutdown.send(true)?;
    task.await??;
    Ok(())
}

#[tokio::test]
async fn exec_relay_cap_rejects_a_ninth_open_websocket() -> Result<(), Box<dyn std::error::Error>> {
    let (store, cluster_id) = seeded_store().await?;
    let deployment = deployment("api-deployment", "api")?;
    let assignment = assignment("assignment-1", "api-deployment", "node-remote")?;
    put(
        &store,
        &cluster_id,
        "Deployment",
        deployment.meta.id.as_str(),
        &deployment,
    )
    .await?;
    put(
        &store,
        &cluster_id,
        "Assignment",
        assignment.meta.id.as_str(),
        &assignment,
    )
    .await?;
    let server = ApiServer::new(
        store,
        cluster_id,
        ServerSettings::new("127.0.0.1:0".parse()?, None),
    )?
    .with_exec_sessions(Arc::new(TestExecSessions::default()))
    .bind()
    .await?;
    let address = server.local_address();
    let (shutdown, receiver) = tokio::sync::watch::channel(false);
    let task = tokio::spawn(server.serve(receiver));
    let endpoint = format!(
        "ws://{address}/api/services/api/deployments/api-deployment/assignments/assignment-1/exec?tty=false"
    );
    let mut sockets = Vec::new();
    for _ in 0..8 {
        sockets.push(tokio_tungstenite::connect_async(&endpoint).await?.0);
    }
    let ninth = tokio_tungstenite::connect_async(&endpoint).await;
    assert!(matches!(
        ninth,
        Err(WebSocketError::Http(response))
            if response.status() == tokio_tungstenite::tungstenite::http::StatusCode::SERVICE_UNAVAILABLE
    ));
    for mut socket in sockets {
        socket.close(None).await?;
    }
    shutdown.send(true)?;
    task.await??;
    Ok(())
}

#[tokio::test]
async fn node_exec_client_uses_mutual_tls_node_scope_and_typed_relay()
-> Result<(), Box<dyn std::error::Error>> {
    let secret = SecretValue::new("cluster-exec-test-secret-that-is-long-enough");
    let certified = rcgen::generate_simple_self_signed(vec!["127.0.0.1".to_owned()])?;
    let certificate_pem = certified.cert.pem();
    let identity = TlsIdentity::new(
        certificate_pem.clone(),
        SecretValue::new(certified.signing_key.serialize_pem()),
    );
    let (store, cluster_id) = seeded_store().await?;
    let remote = ApiServer::new(
        store,
        cluster_id,
        ServerSettings::new("127.0.0.1:0".parse()?, Some(secret.clone()))
            .with_tls_identity(identity.clone())
            .with_cluster_trust_root(certificate_pem.clone()),
    )?
    .with_exec_sessions(Arc::new(TestExecSessions::default()))
    .bind()
    .await?;
    let remote_address = remote.local_address();
    let (shutdown, receiver) = tokio::sync::watch::channel(false);
    let task = tokio::spawn(remote.serve(receiver));

    let local_node = NodeId::new("node-local")?;
    let remote_node = NodeId::new("node-remote")?;
    let client = HttpClusterExecSessions::new(
        local_node.clone(),
        BTreeMap::from([
            (local_node, "127.0.0.1:1".parse()?),
            (remote_node.clone(), remote_address),
        ]),
        &certificate_pem,
        &identity,
        &secret,
        local_exec_service()?,
    )?;
    let mut session = client
        .open(
            &remote_node,
            &AssignmentId::new("assignment-remote")?,
            request(),
        )
        .await?;
    assert_eq!(
        session.next().await?,
        Some(ExecOutput::Stdout(b"local:/bin/echo".to_vec()))
    );
    session
        .send(ExecInput::Stdin(b"peer input".to_vec()))
        .await?;
    assert_eq!(
        session.next().await?,
        Some(ExecOutput::Stdout(b"peer input".to_vec()))
    );
    session.kill().await?;
    assert_eq!(
        session.next().await?,
        Some(ExecOutput::Exited { code: Some(137) })
    );

    shutdown.send(true)?;
    task.await??;
    Ok(())
}

fn assignment(
    assignment_id: &str,
    deployment_id: &str,
    node_id: &str,
) -> Result<Assignment, kernel_api::InvalidIdentifier> {
    Ok(Object {
        meta: metadata(AssignmentId::new(assignment_id)?),
        spec: AssignmentSpec {
            service_id: ServiceId::new("api")?,
            deployment_id: DeploymentId::new(deployment_id)?,
            restart_generation: Generation(1),
            replica_index: 0,
            node_id: NodeId::new(node_id)?,
            placement_epoch: 1,
            workload_address: IpAddr::V4(Ipv4Addr::new(10, 80, 0, 10)),
            replaces_assignment_id: None,
        },
        status: AssignmentStatus {
            phase: AssignmentPhase::Running,
            workload_id: Some(WorkloadId::new(assignment_id)?),
            conditions: Vec::new(),
        },
    })
}

fn request() -> ExecRequest {
    ExecRequest {
        command: CommandSpec {
            executable: "/bin/echo".to_owned(),
            arguments: vec!["hello".to_owned()],
        },
        environment: BTreeMap::new(),
        mode: ExecMode::Pipes,
    }
}

fn local_exec_service() -> Result<Arc<node_agent::NodeExecService>, Box<dyn std::error::Error>> {
    let store = Arc::new(kernel_store::InMemoryStore::new(Arc::new(
        kernel_store::TokioClock::new(),
    )));
    Ok(Arc::new(node_agent::NodeExecService::new(
        store,
        Arc::new(runtime::FakeRuntime::new()),
        node_agent::NodeExecSettings {
            cluster_id: kernel_api::ClusterId::new("unused-local")?,
            node_id: NodeId::new("node-local")?,
            maximum_sessions: 8,
        },
    )?))
}

async fn receive_frame(
    socket: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
) -> Result<ExecStreamFrame, Box<dyn std::error::Error>> {
    loop {
        match socket.next().await {
            Some(Ok(Message::Binary(encoded))) => return Ok(ExecStreamFrame::decode(&encoded)?),
            Some(Ok(Message::Ping(payload))) => socket.send(Message::Pong(payload)).await?,
            Some(Ok(Message::Pong(_))) => {}
            Some(Ok(message)) => {
                return Err(format!("unexpected WebSocket message: {message:?}").into());
            }
            Some(Err(error)) => return Err(error.into()),
            None => return Err("exec WebSocket closed before a protocol frame".into()),
        }
    }
}

#[derive(Default)]
struct TestExecSessions {
    opened: AtomicUsize,
}

#[async_trait]
impl ClusterExecSessions for TestExecSessions {
    async fn open(
        &self,
        node_id: &NodeId,
        _assignment_id: &AssignmentId,
        request: ExecRequest,
    ) -> Result<Box<dyn ExecSession>, ExecSessionOpenError> {
        self.opened.fetch_add(1, Ordering::SeqCst);
        Ok(test_session(node_id.as_str(), request))
    }

    async fn open_local(
        &self,
        _assignment_id: &AssignmentId,
        request: ExecRequest,
    ) -> Result<Box<dyn ExecSession>, ExecSessionOpenError> {
        self.opened.fetch_add(1, Ordering::SeqCst);
        Ok(test_session("local", request))
    }
}

fn test_session(label: &str, request: ExecRequest) -> Box<dyn ExecSession> {
    Box::new(TestExecSession {
        outputs: VecDeque::from([ExecOutput::Stdout(
            format!("{label}:{}", request.command.executable).into_bytes(),
        )]),
        notify: Arc::new(Notify::new()),
    })
}

struct TestExecSession {
    outputs: VecDeque<ExecOutput>,
    notify: Arc<Notify>,
}

#[async_trait]
impl ExecSession for TestExecSession {
    async fn send(&mut self, input: ExecInput) -> Result<(), RuntimeError> {
        if let ExecInput::Stdin(bytes) = input {
            self.outputs.push_back(ExecOutput::Stdout(bytes));
            self.notify.notify_one();
        }
        Ok(())
    }

    async fn next(&mut self) -> Result<Option<ExecOutput>, RuntimeError> {
        loop {
            if let Some(output) = self.outputs.pop_front() {
                return Ok(Some(output));
            }
            self.notify.notified().await;
        }
    }

    async fn kill(&mut self) -> Result<(), RuntimeError> {
        self.outputs.clear();
        self.outputs
            .push_back(ExecOutput::Exited { code: Some(137) });
        self.notify.notify_one();
        Ok(())
    }
}
