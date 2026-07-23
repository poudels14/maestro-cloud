use std::net::Ipv4Addr;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use kernel_api::{NodeId, NodeRole, SecretValue};
use kernel_store::{ExpectedVersion, InMemoryStore, Keyspace, PutRequest, Store, TokioClock};

use crate::{
    AdmissionCoordinator, AdmissionCoordinatorError, JoinPrivateKey, JoinRequest,
    JoinResponseStatus, MemberActivation, MemberState, NodeJoinApprovalState, StoreJoinTicket,
    StoreMember, StoreProvider, StoreProviderError, StoreRecovery, StoreRecoveryPermit,
    StoreRuntime, StoreStartMode, decrypt_join_response, public_key_fingerprint, sign_join_request,
};

use super::fixtures::{valid_config, validity};

struct RecordingProvider {
    staged: Mutex<Vec<NodeId>>,
}

#[async_trait]
impl StoreProvider for RecordingProvider {
    async fn start(
        &self,
        _mode: StoreStartMode,
    ) -> Result<Box<dyn StoreRuntime>, StoreProviderError> {
        Err(unused())
    }

    async fn stage_member(
        &self,
        member: StoreMember,
    ) -> Result<(StoreJoinTicket, MemberActivation), StoreProviderError> {
        self.staged
            .lock()
            .map_err(|_| unused())?
            .push(member.node_id.clone());
        Ok((
            StoreJoinTicket::from_provider_data(member.node_id.clone(), b"membership-ticket"),
            MemberActivation {
                node_id: member.node_id,
                state: MemberState::Staged,
            },
        ))
    }

    async fn activate_member(
        &self,
        _ticket: &StoreJoinTicket,
    ) -> Result<MemberActivation, StoreProviderError> {
        Err(unused())
    }

    async fn remove_member(&self, _node_id: &NodeId) -> Result<(), StoreProviderError> {
        Err(unused())
    }

    async fn recover(
        &self,
        _permit: StoreRecoveryPermit,
    ) -> Result<StoreRecovery, StoreProviderError> {
        Err(unused())
    }
}

#[tokio::test]
async fn approval_admits_and_replays_only_one_exact_control_plane_request()
-> Result<(), Box<dyn std::error::Error>> {
    let mut config = valid_config()?;
    let worker_id = NodeId::new("node-4")?;
    let mut worker = config
        .nodes
        .get(&NodeId::new("node-3")?)
        .ok_or("missing fixture node")?
        .clone();
    worker.hostname = "node-4.internal".to_string();
    worker.endpoint.host_address = Ipv4Addr::new(10, 20, 0, 14);
    worker.workload_subnet = "172.22.4.0/24".parse()?;
    worker.role = NodeRole::Worker;
    config.nodes.insert(worker_id.clone(), worker);
    let authority = crate::ClusterCertificateAuthority::generate(&config.name, validity()?)?;
    let store = Arc::new(InMemoryStore::new(Arc::new(TokioClock::new())));
    let provider = Arc::new(RecordingProvider {
        staged: Mutex::new(Vec::new()),
    });
    let operator_secret = SecretValue::new("operator-test-secret-with-at-least-32-characters");
    let storage_secret = SecretValue::new("storage-test-secret-with-at-least-32-characters");
    let coordinator = AdmissionCoordinator::new(
        config.clone(),
        authority,
        operator_secret.clone(),
        storage_secret.clone(),
        provider.clone(),
        store.clone(),
    )?;
    let node_id = NodeId::new("node-2")?;
    let join_key = JoinPrivateKey::generate();
    let fingerprint = public_key_fingerprint(&join_key.public_key_hex())?;
    let approval = coordinator
        .approve(node_id.clone(), fingerprint.clone(), 900)
        .await?;
    assert_eq!(approval.state, NodeJoinApprovalState::Approved);
    assert_eq!(
        coordinator
            .approve(node_id.clone(), fingerprint, 950)
            .await?,
        approval
    );

    let request = JoinRequest::from_config(&join_key, &config, &node_id, 1_000)?;
    let signature = sign_join_request(&config.join_secret, &request)?;
    assert!(matches!(
        coordinator
            .admit(
                &request,
                &signature,
                request.endpoint.host_address,
                1_000 + 10 * 60 * 1_000,
                validity()?,
            )
            .await,
        Err(AdmissionCoordinatorError::Admission(
            crate::AdmissionError::Protocol(
                crate::JoinProtocolError::TimestampOutsideWindow { .. }
            )
        ))
    ));
    assert!(
        provider
            .staged
            .lock()
            .map_err(|_| "staged lock poisoned")?
            .is_empty()
    );
    let first = coordinator
        .admit(
            &request,
            &signature,
            request.endpoint.host_address,
            1_000,
            validity()?,
        )
        .await?;
    let first_payload = decrypt_join_response(
        &config.join_secret,
        &join_key,
        &request,
        &first,
        JoinResponseStatus::ACCEPTED,
    )?;
    assert_eq!(first_payload.operator_jwt_secret, operator_secret);
    assert_eq!(first_payload.store_encryption_secret, storage_secret);
    assert!(first_payload.store_join_ticket.is_some());
    assert!(first_payload.certificate_issuer.is_some());

    let replay = coordinator
        .admit(
            &request,
            &signature,
            request.endpoint.host_address,
            1_000 + 10 * 60 * 1_000,
            validity()?,
        )
        .await?;
    assert_eq!(
        decrypt_join_response(
            &config.join_secret,
            &join_key,
            &request,
            &replay,
            JoinResponseStatus::ACCEPTED,
        )?,
        first_payload
    );
    assert_eq!(
        provider
            .staged
            .lock()
            .map_err(|_| "staged lock poisoned")?
            .as_slice(),
        std::slice::from_ref(&node_id)
    );
    let approvals = coordinator.list().await?;
    assert_eq!(approvals.len(), 1);
    assert_eq!(
        approvals.first().map(|item| item.state),
        Some(NodeJoinApprovalState::Admitted)
    );
    let public_json = serde_json::to_string(&approvals)?;
    assert!(!public_json.contains(operator_secret.expose()));
    assert!(!public_json.contains(storage_secret.expose()));

    let changed_request = JoinRequest::from_config(&join_key, &config, &node_id, 1_002)?;
    let changed_signature = sign_join_request(&config.join_secret, &changed_request)?;
    assert!(matches!(
        coordinator
            .admit(
                &changed_request,
                &changed_signature,
                changed_request.endpoint.host_address,
                1_002,
                validity()?,
            )
            .await,
        Err(AdmissionCoordinatorError::AdmissionConflict { .. })
    ));

    let other_key = JoinPrivateKey::generate();
    let other_request = JoinRequest::from_config(&other_key, &config, &node_id, 1_000)?;
    let other_signature = sign_join_request(&config.join_secret, &other_request)?;
    let result = coordinator
        .admit(
            &other_request,
            &other_signature,
            other_request.endpoint.host_address,
            1_000,
            validity()?,
        )
        .await;
    assert!(matches!(
        result,
        Err(AdmissionCoordinatorError::ApprovalKeyMismatch { .. })
    ));

    store
        .put_cas(PutRequest {
            key: Keyspace::new(&config.cluster_id).node_tombstone(&node_id),
            value: b"removed".to_vec(),
            expected: ExpectedVersion::Missing,
            session: None,
        })
        .await?;
    assert!(matches!(
        coordinator
            .admit(
                &request,
                &signature,
                request.endpoint.host_address,
                1_001,
                validity()?,
            )
            .await,
        Err(AdmissionCoordinatorError::NodeRemoved { .. })
    ));

    let worker_key = JoinPrivateKey::generate();
    coordinator
        .approve(
            worker_id.clone(),
            public_key_fingerprint(&worker_key.public_key_hex())?,
            1_100,
        )
        .await?;
    let worker_request = JoinRequest::from_config(&worker_key, &config, &worker_id, 1_100)?;
    let worker_response = coordinator
        .admit(
            &worker_request,
            &sign_join_request(&config.join_secret, &worker_request)?,
            worker_request.endpoint.host_address,
            1_100,
            validity()?,
        )
        .await?;
    let worker_payload = decrypt_join_response(
        &config.join_secret,
        &worker_key,
        &worker_request,
        &worker_response,
        JoinResponseStatus::ACCEPTED,
    )?;
    assert!(worker_payload.store_join_ticket.is_none());
    assert!(worker_payload.certificate_issuer.is_none());
    assert_eq!(
        provider
            .staged
            .lock()
            .map_err(|_| "staged lock poisoned")?
            .as_slice(),
        std::slice::from_ref(&node_id)
    );
    Ok(())
}

#[tokio::test]
async fn approval_rejects_unknown_master_and_conflicting_keys()
-> Result<(), Box<dyn std::error::Error>> {
    let config = valid_config()?;
    let authority = crate::ClusterCertificateAuthority::generate(&config.name, validity()?)?;
    let store = Arc::new(InMemoryStore::new(Arc::new(TokioClock::new())));
    let coordinator = AdmissionCoordinator::new(
        config.clone(),
        authority,
        SecretValue::new("operator-test-secret-with-at-least-32-characters"),
        SecretValue::new("storage-test-secret-with-at-least-32-characters"),
        Arc::new(RecordingProvider {
            staged: Mutex::new(Vec::new()),
        }),
        store.clone(),
    )?;
    assert!(matches!(
        coordinator
            .approve(NodeId::new("node-1")?, "00".repeat(32), 1_000)
            .await,
        Err(AdmissionCoordinatorError::MasterCannotJoin)
    ));
    assert!(matches!(
        coordinator
            .approve(NodeId::new("missing")?, "00".repeat(32), 1_000)
            .await,
        Err(AdmissionCoordinatorError::UnknownNode { .. })
    ));
    assert!(matches!(
        coordinator
            .approve(NodeId::new("node-3")?, "AA".repeat(32), 1_000)
            .await,
        Err(AdmissionCoordinatorError::InvalidFingerprint)
    ));
    let node_id = NodeId::new("node-3")?;
    coordinator
        .approve(node_id.clone(), "00".repeat(32), 1_000)
        .await?;
    assert!(matches!(
        coordinator.approve(node_id, "11".repeat(32), 1_001).await,
        Err(AdmissionCoordinatorError::ApprovalConflict { .. })
    ));
    let removing = NodeId::new("node-2")?;
    store
        .put_cas(PutRequest {
            key: Keyspace::new(&config.cluster_id).node_removal(&removing),
            value: b"removing".to_vec(),
            expected: ExpectedVersion::Missing,
            session: None,
        })
        .await?;
    assert!(matches!(
        coordinator.approve(removing, "22".repeat(32), 1_002).await,
        Err(AdmissionCoordinatorError::NodeRemovalInProgress { .. })
    ));
    Ok(())
}

fn unused() -> StoreProviderError {
    StoreProviderError::InvalidConfiguration {
        reason: "unused test operation".to_string(),
    }
}
