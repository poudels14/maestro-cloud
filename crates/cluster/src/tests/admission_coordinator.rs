use std::net::Ipv4Addr;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use kernel_api::{NodeId, NodeRole};
use kernel_store::{ExpectedVersion, InMemoryStore, Keyspace, PutRequest, Store, TokioClock};

use crate::{
    AdmissionCoordinator, AdmissionCoordinatorError, JoinPrivateKey, JoinRequest,
    JoinResponseStatus, MemberActivation, MemberState, StoreJoinTicket, StoreMember, StoreProvider,
    StoreProviderError, StoreRecovery, StoreRecoveryPermit, StoreRuntime, StoreStartMode,
    decrypt_join_response, sign_join_request,
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
async fn configured_node_admits_and_replays_only_one_exact_control_plane_request()
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
    let expected_authority = authority.clone();
    let store = Arc::new(InMemoryStore::new(Arc::new(TokioClock::new())));
    let provider = Arc::new(RecordingProvider {
        staged: Mutex::new(Vec::new()),
    });
    let coordinator =
        AdmissionCoordinator::new(config.clone(), authority, provider.clone(), store.clone())?;
    let master_id = NodeId::new("node-1")?;
    let master_key = JoinPrivateKey::generate();
    let master_request = JoinRequest::from_config(&master_key, &config, &master_id, 1_000)?;
    assert!(matches!(
        coordinator
            .admit(
                &master_request,
                &sign_join_request(&config.join_secret, &master_request)?,
                master_request.endpoint.host_address,
                1_000,
                validity()?,
            )
            .await,
        Err(AdmissionCoordinatorError::MasterCannotJoin)
    ));

    let node_id = NodeId::new("node-2")?;
    let join_key = JoinPrivateKey::generate();
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
    assert!(first_payload.store_join_ticket.is_some());
    assert_eq!(first_payload.certificate_issuer, Some(expected_authority));

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
        Err(AdmissionCoordinatorError::JoinKeyConflict { .. })
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

fn unused() -> StoreProviderError {
    StoreProviderError::InvalidConfiguration {
        reason: "unused test operation".to_string(),
    }
}
