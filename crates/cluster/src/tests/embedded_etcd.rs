use std::collections::{BTreeMap, BTreeSet};
use std::net::{Ipv4Addr, TcpListener};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use kernel_api::{ClusterId, NodeId, NodeRole, SecretValue};
use kernel_store::{CasOutcome, ExpectedVersion, Keyspace, PutRequest, TokioClock};
use tracing_subscriber::util::SubscriberInitExt;

use crate::{
    ClusterCertificateAuthority, ClusterPorts, EmbeddedEtcdProvider, EmbeddedEtcdSettings,
    MemberActivation, MemberState, StoreJoinTicket, StoreMember, StoreProvider,
    StoreProviderConfig, StoreProviderError, StoreRecoveryPermit, StoreShutdown, StoreStartMode,
    embedded_etcd::prepare_local_state,
    embedded_etcd_plan::{
        EtcdJoinTicketData, EtcdLaunchMode, EtcdMemberPlan, EtcdReadiness, EtcdSecurityPaths,
        EtcdStartPlan, TICKET_FORMAT_VERSION,
    },
};

use super::fixtures::{provider_config, validity};

#[test]
fn launch_plan_separates_new_existing_and_recovery_modes() -> Result<(), Box<dyn std::error::Error>>
{
    let directory = tempfile::tempdir()?;
    let config = provider_config(directory.path().join("node-1"), "node-1")?;
    let security = security_paths(directory.path());
    let bootstrap = EtcdStartPlan::build(
        &config,
        EtcdLaunchMode::Start(StoreStartMode::Bootstrap),
        &security,
    )?;
    assert!(has_argument(&bootstrap, "--initial-cluster-state=new"));
    assert!(has_argument(
        &bootstrap,
        "--initial-cluster=maestro-node-1=https://10.20.0.11:2380"
    ));
    assert!(!has_argument(&bootstrap, "--force-new-cluster=true"));
    assert_eq!(bootstrap.readiness, EtcdReadiness::WritableQuorum);

    let restart = EtcdStartPlan::build(
        &config,
        EtcdLaunchMode::Start(StoreStartMode::Restart),
        &security,
    )?;
    assert!(has_argument(&restart, "--initial-cluster-state=existing"));
    assert!(has_argument(
        &restart,
        "--initial-cluster=maestro-node-1=https://10.20.0.11:2380,maestro-node-2=https://10.20.0.12:2380,maestro-node-3=https://10.20.0.13:2380"
    ));

    let recovery = EtcdStartPlan::build(&config, EtcdLaunchMode::Recover, &security)?;
    assert!(has_argument(&recovery, "--force-new-cluster=true"));
    Ok(())
}

#[test]
fn join_ticket_is_bound_to_cluster_node_and_known_peers() -> Result<(), Box<dyn std::error::Error>>
{
    let directory = tempfile::tempdir()?;
    let config = provider_config(directory.path().join("node-2"), "node-2")?;
    let ticket = EtcdJoinTicketData {
        format_version: TICKET_FORMAT_VERSION,
        cluster_id: config.cluster_id().clone(),
        node_id: NodeId::new("node-2")?,
        member_id: 42,
        members: vec![
            EtcdMemberPlan {
                node_id: NodeId::new("node-1")?,
                peer_url: "https://10.20.0.11:2380".to_owned(),
            },
            EtcdMemberPlan {
                node_id: NodeId::new("node-2")?,
                peer_url: "https://10.20.0.12:2380".to_owned(),
            },
        ],
    }
    .encode()?;
    let plan = EtcdStartPlan::build(
        &config,
        EtcdLaunchMode::Start(StoreStartMode::Join(ticket.clone())),
        &security_paths(directory.path()),
    )?;
    assert!(has_argument(
        &plan,
        "--initial-cluster=maestro-node-1=https://10.20.0.11:2380,maestro-node-2=https://10.20.0.12:2380"
    ));
    assert_eq!(
        plan.readiness,
        EtcdReadiness::JoinedLearner { member_id: 42 }
    );

    let wrong_config = provider_config(directory.path().join("node-3"), "node-3")?;
    assert!(
        EtcdStartPlan::build(
            &wrong_config,
            EtcdLaunchMode::Start(StoreStartMode::Join(ticket)),
            &security_paths(directory.path()),
        )
        .is_err()
    );
    Ok(())
}

#[test]
fn local_state_allows_idempotent_start_but_not_implicit_restart()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let config = provider_config(directory.path().join("node-1"), "node-1")?;
    assert!(prepare_local_state(&config, &EtcdLaunchMode::Start(StoreStartMode::Restart)).is_err());
    prepare_local_state(&config, &EtcdLaunchMode::Start(StoreStartMode::Bootstrap))?;
    prepare_local_state(&config, &EtcdLaunchMode::Start(StoreStartMode::Bootstrap))?;
    prepare_local_state(&config, &EtcdLaunchMode::Start(StoreStartMode::Restart))?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires MAESTRO_ETCD_BIN and MAESTRO_ETCD_TEST_IP"]
async fn real_embedded_etcd_bootstraps_and_restarts() -> Result<(), Box<dyn std::error::Error>> {
    let _tracing = test_tracing();
    let binary = PathBuf::from(std::env::var("MAESTRO_ETCD_BIN")?);
    let host_address = std::env::var("MAESTRO_ETCD_TEST_IP")?.parse::<Ipv4Addr>()?;
    let directory = tempfile::tempdir()?;
    let ports = allocate_ports()?;
    let config = one_member_config(directory.path(), host_address, ports)?;
    let settings = EmbeddedEtcdSettings::new(
        Duration::from_secs(30),
        Duration::from_secs(2),
        Duration::from_millis(100),
        Duration::from_secs(1),
    )?;
    let provider = EmbeddedEtcdProvider::new(
        config.clone(),
        binary.clone(),
        Arc::new(TokioClock::new()),
        settings,
    )?;
    let runtime = provider.start(StoreStartMode::Bootstrap).await?;
    let key = Keyspace::new(config.cluster_id()).leader();
    assert!(matches!(
        runtime
            .store()
            .put_cas(PutRequest {
                key: key.clone(),
                value: b"before-restart".to_vec(),
                expected: ExpectedVersion::Missing,
                session: None,
            })
            .await?,
        CasOutcome::Applied(_)
    ));
    runtime.shutdown(StoreShutdown::Immediate).await?;

    let provider =
        EmbeddedEtcdProvider::new(config, binary, Arc::new(TokioClock::new()), settings)?;
    let runtime = provider.start(StoreStartMode::Restart).await?;
    assert_eq!(
        runtime.store().get(&key).await?.map(|value| value.value),
        Some(b"before-restart".to_vec())
    );
    runtime.shutdown(StoreShutdown::Immediate).await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires MAESTRO_ETCD_BIN"]
async fn real_members_stage_activate_rejoin_and_recover_after_loss()
-> Result<(), Box<dyn std::error::Error>> {
    let _tracing = test_tracing();
    let binary = PathBuf::from(std::env::var("MAESTRO_ETCD_BIN")?);
    let directory = tempfile::tempdir()?;
    let ports = allocate_ports()?;
    let configs = three_member_configs(directory.path(), ports)?;
    let [config_1, config_2, config_3]: [StoreProviderConfig; 3] = configs
        .try_into()
        .map_err(|_| "expected three provider configurations")?;
    let settings = EmbeddedEtcdSettings::new(
        Duration::from_secs(30),
        Duration::from_secs(2),
        Duration::from_millis(100),
        Duration::from_secs(1),
    )?;
    let provider_1 = real_provider(config_1, binary.clone(), settings)?;
    let provider_2 = real_provider(config_2, binary.clone(), settings)?;
    let provider_3 = real_provider(config_3, binary, settings)?;

    let runtime_1 = provider_1.start(StoreStartMode::Bootstrap).await?;
    let (ticket_2, staged_2) = await_staging(
        &provider_1,
        StoreMember {
            node_id: NodeId::new("node-2")?,
            host_address: Ipv4Addr::new(127, 0, 0, 2),
        },
    )
    .await?;
    assert_eq!(staged_2.state, MemberState::Staged);
    let runtime_2 = provider_2
        .start(StoreStartMode::Join(ticket_2.clone()))
        .await?;
    await_activation(&provider_1, &ticket_2).await?;

    let (ticket_3, staged_3) = await_staging(
        &provider_1,
        StoreMember {
            node_id: NodeId::new("node-3")?,
            host_address: Ipv4Addr::new(127, 0, 0, 3),
        },
    )
    .await?;
    assert_eq!(staged_3.state, MemberState::Staged);
    let runtime_3 = provider_3
        .start(StoreStartMode::Join(ticket_3.clone()))
        .await?;
    await_activation(&provider_1, &ticket_3).await?;

    runtime_2.shutdown(StoreShutdown::Immediate).await?;
    let key = Keyspace::new(&ClusterId::new("embedded-provider-three-node")?).leader();
    assert!(matches!(
        runtime_1
            .store()
            .put_cas(PutRequest {
                key: key.clone(),
                value: b"survived-node-loss".to_vec(),
                expected: ExpectedVersion::Missing,
                session: None,
            })
            .await?,
        CasOutcome::Applied(_)
    ));
    let runtime_2 = provider_2.start(StoreStartMode::Restart).await?;
    assert_eq!(
        runtime_2.store().get(&key).await?.map(|value| value.value),
        Some(b"survived-node-loss".to_vec())
    );

    runtime_3.shutdown(StoreShutdown::Immediate).await?;
    runtime_2.shutdown(StoreShutdown::Immediate).await?;
    runtime_1.shutdown(StoreShutdown::Immediate).await?;
    let expected_members = BTreeSet::from([
        NodeId::new("node-1")?,
        NodeId::new("node-2")?,
        NodeId::new("node-3")?,
    ]);
    let recovery = provider_1
        .recover(StoreRecoveryPermit::new(
            ClusterId::new("embedded-provider-three-node")?,
            NodeId::new("node-1")?,
            expected_members,
            1_750_000_000_000,
        )?)
        .await?;
    assert_eq!(recovery.report.retained_node, NodeId::new("node-1")?);
    assert_eq!(
        recovery.report.members_to_rejoin,
        vec![NodeId::new("node-2")?, NodeId::new("node-3")?]
    );
    assert_eq!(
        recovery
            .runtime
            .store()
            .get(&key)
            .await?
            .map(|value| value.value),
        Some(b"survived-node-loss".to_vec())
    );
    recovery.runtime.shutdown(StoreShutdown::Immediate).await?;
    Ok(())
}

fn security_paths(root: &Path) -> EtcdSecurityPaths {
    EtcdSecurityPaths {
        certificate_authority: root.join("ca.pem"),
        certificate: root.join("identity.pem"),
        private_key: root.join("identity-key.pem"),
    }
}

fn test_tracing() -> tracing::subscriber::DefaultGuard {
    tracing_subscriber::fmt()
        .with_test_writer()
        .with_max_level(tracing::Level::INFO)
        .set_default()
}

fn has_argument(plan: &EtcdStartPlan, expected: &str) -> bool {
    plan.arguments.iter().any(|argument| argument == expected)
}

fn allocate_ports() -> Result<ClusterPorts, Box<dyn std::error::Error>> {
    let mut listeners = Vec::new();
    let mut ports = Vec::new();
    while ports.len() < 4 {
        let listener = TcpListener::bind((Ipv4Addr::UNSPECIFIED, 0))?;
        let port = listener.local_addr()?.port();
        if !ports.contains(&port) {
            ports.push(port);
            listeners.push(listener);
        }
    }
    let values: [u16; 4] = ports.try_into().map_err(|_| "failed to allocate ports")?;
    let [gateway, store_client, store_peer, wireguard] = values;
    drop(listeners);
    ClusterPorts::new(gateway, store_client, store_peer, wireguard).map_err(Into::into)
}

async fn await_activation(
    provider: &EmbeddedEtcdProvider,
    ticket: &StoreJoinTicket,
) -> Result<(), StoreProviderError> {
    for _attempt in 0..100 {
        match provider.activate_member(ticket).await {
            Ok(activation) if activation.state == MemberState::Active => return Ok(()),
            Ok(_) | Err(StoreProviderError::MemberNotReady { .. }) => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(error) => return Err(error),
        }
    }
    Err(StoreProviderError::MemberNotReady {
        node_id: ticket.node_id().clone(),
    })
}

async fn await_staging(
    provider: &EmbeddedEtcdProvider,
    member: StoreMember,
) -> Result<(StoreJoinTicket, MemberActivation), StoreProviderError> {
    for _attempt in 0..100 {
        match provider.stage_member(member.clone()).await {
            Ok(staged) => return Ok(staged),
            Err(StoreProviderError::Unavailable { .. }) => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(error) => return Err(error),
        }
    }
    Err(StoreProviderError::Unavailable {
        reason: format!(
            "membership staging for `{}` did not converge",
            member.node_id
        ),
    })
}

fn real_provider(
    config: StoreProviderConfig,
    binary: PathBuf,
    settings: EmbeddedEtcdSettings,
) -> Result<EmbeddedEtcdProvider, StoreProviderError> {
    EmbeddedEtcdProvider::new(config, binary, Arc::new(TokioClock::new()), settings)
}

fn three_member_configs(
    root: &Path,
    ports: ClusterPorts,
) -> Result<Vec<StoreProviderConfig>, Box<dyn std::error::Error>> {
    let cluster_id = ClusterId::new("embedded-provider-three-node")?;
    let definitions = [
        ("node-1", Ipv4Addr::new(127, 0, 0, 1), NodeRole::Master),
        ("node-2", Ipv4Addr::new(127, 0, 0, 2), NodeRole::Hybrid),
        (
            "node-3",
            Ipv4Addr::new(127, 0, 0, 3),
            NodeRole::ControlPlane,
        ),
    ];
    let members = definitions
        .iter()
        .map(|(node_id, host_address, _)| {
            let node_id = NodeId::new(*node_id)?;
            Ok((
                node_id.clone(),
                StoreMember {
                    node_id,
                    host_address: *host_address,
                },
            ))
        })
        .collect::<Result<BTreeMap<_, _>, Box<dyn std::error::Error>>>()?;
    let authority = ClusterCertificateAuthority::generate(&cluster_id.to_string(), validity()?)?;
    definitions
        .into_iter()
        .map(|(node_id, host_address, role)| {
            let node_id = NodeId::new(node_id)?;
            let local = members.get(&node_id).ok_or("missing test member")?.clone();
            // Linux selects 127.0.0.1 as the source address when etcd peers
            // connect between loopback aliases, so peer authentication must
            // recognize both the listener alias and the observed source.
            let security = authority.issue_node_certificate_with_additional_ip_sans(
                &node_id,
                &format!("{node_id}.internal"),
                host_address,
                &[Ipv4Addr::LOCALHOST],
                role,
                validity()?,
            )?;
            StoreProviderConfig::new(
                cluster_id.clone(),
                local,
                members.clone(),
                ports,
                root.join(node_id.as_str()),
                SecretValue::new("test-store-encryption-secret-with-32-characters"),
                security,
            )
            .map_err(Into::into)
        })
        .collect()
}

fn one_member_config(
    root: &Path,
    host_address: Ipv4Addr,
    ports: ClusterPorts,
) -> Result<StoreProviderConfig, Box<dyn std::error::Error>> {
    let cluster_id = ClusterId::new("embedded-provider-test")?;
    let node_id = NodeId::new("node-1")?;
    let member = StoreMember {
        node_id: node_id.clone(),
        host_address,
    };
    let authority = ClusterCertificateAuthority::generate("embedded-provider-test", validity()?)?;
    let security = authority.issue_node_certificate(
        &node_id,
        "node-1.internal",
        host_address,
        NodeRole::Master,
        validity()?,
    )?;
    StoreProviderConfig::new(
        cluster_id,
        member.clone(),
        BTreeMap::from([(node_id, member)]),
        ports,
        root.join("store"),
        SecretValue::new("test-store-encryption-secret-with-32-characters"),
        security,
    )
    .map_err(Into::into)
}
