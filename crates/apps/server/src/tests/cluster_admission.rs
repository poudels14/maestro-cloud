use std::collections::BTreeMap;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration as StdDuration, SystemTime};

use async_trait::async_trait;
use axum::body::Body;
use axum::extract::ConnectInfo;
use axum::http::{Method, Request, StatusCode, header};
use cluster::{
    AdmissionCoordinator, CaDiscoveryRequest, CertificateValidity, ClusterCertificateAuthority,
    ClusterConfig, ClusterPorts, EncryptedJoinResponse, Ipv4Cidr, JoinPrivateKey, JoinRequest,
    JoinResponseStatus, MemberActivation, NodeDefinition, NodeEndpoint, SignedJoinRequest,
    StoreJoinTicket, StoreMember, StoreProvider, StoreProviderError, StoreRecovery,
    StoreRecoveryPermit, StoreRuntime, StoreStartMode, decrypt_join_response, sign_join_request,
    verify_ca_discovery_response,
};
use kernel_api::{ClusterId, NodeId, NodeRole, SecretValue};
use kernel_store::{InMemoryStore, TokioClock};
use time::{Duration, OffsetDateTime};
use tower::ServiceExt;

use crate::{ApiServer, ServerSettings};

use super::decode;

#[tokio::test]
async fn declared_node_discovers_and_joins_without_operator_approval()
-> Result<(), Box<dyn std::error::Error>> {
    let (config, worker_id) = cluster_config()?;
    let store = Arc::new(InMemoryStore::new(Arc::new(TokioClock::new())));
    let authority = ClusterCertificateAuthority::generate(&config.name, authority_validity()?)?;
    let coordinator = Arc::new(AdmissionCoordinator::new(
        config.clone(),
        authority,
        Arc::new(UnusedProvider),
        store.clone(),
    )?);
    let server = ApiServer::new(
        store,
        config.cluster_id.clone(),
        ServerSettings::new("127.0.0.1:3000".parse()?, None),
    )?
    .with_admission_coordinator(coordinator);
    let now = now_unix_ms();
    let join_key = JoinPrivateKey::generate();
    let discovery_request = CaDiscoveryRequest::new(config.name.clone());
    let discovered = send_json(
        &server,
        Method::POST,
        "/api/cluster/ca",
        &discovery_request,
        None,
        worker_address(),
    )
    .await?;
    assert_eq!(discovered.status(), StatusCode::OK);
    let discovered = decode(discovered).await?;
    verify_ca_discovery_response(
        &config.join_secret,
        &config.name,
        &discovery_request,
        &discovered,
    )?;

    let join_request = JoinRequest::from_config(&join_key, &config, &worker_id, now)?;
    let signed = SignedJoinRequest {
        signature: sign_join_request(&config.join_secret, &join_request)?,
        request: join_request.clone(),
    };
    let admitted = send_json(
        &server,
        Method::POST,
        "/api/cluster/join",
        &signed,
        None,
        worker_address(),
    )
    .await?;
    assert_eq!(admitted.status(), StatusCode::OK);
    let encrypted: EncryptedJoinResponse = decode(admitted).await?;
    let payload = decrypt_join_response(
        &config.join_secret,
        &join_key,
        &join_request,
        &encrypted,
        JoinResponseStatus::ACCEPTED,
    )?;
    assert!(payload.store_join_ticket.is_none());
    assert!(payload.certificate_issuer.is_none());

    let changed_request = JoinRequest::from_config(&join_key, &config, &worker_id, now)?;
    let changed = SignedJoinRequest {
        signature: sign_join_request(&config.join_secret, &changed_request)?,
        request: changed_request,
    };
    let conflict = send_json(
        &server,
        Method::POST,
        "/api/cluster/join",
        &changed,
        None,
        worker_address(),
    )
    .await?;
    assert_eq!(conflict.status(), StatusCode::CONFLICT);
    Ok(())
}

#[tokio::test]
async fn join_rejects_a_transport_source_other_than_the_signed_endpoint()
-> Result<(), Box<dyn std::error::Error>> {
    let (config, worker_id) = cluster_config()?;
    let store = Arc::new(InMemoryStore::new(Arc::new(TokioClock::new())));
    let coordinator = Arc::new(AdmissionCoordinator::new(
        config.clone(),
        ClusterCertificateAuthority::generate(&config.name, authority_validity()?)?,
        Arc::new(UnusedProvider),
        store.clone(),
    )?);
    let join_key = JoinPrivateKey::generate();
    let server = ApiServer::new(
        store,
        config.cluster_id.clone(),
        ServerSettings::new("127.0.0.1:3000".parse()?, None),
    )?
    .with_admission_coordinator(coordinator);
    let request = JoinRequest::from_config(&join_key, &config, &worker_id, now_unix_ms())?;
    let signed = SignedJoinRequest {
        signature: sign_join_request(&config.join_secret, &request)?,
        request,
    };

    let response = send_json(
        &server,
        Method::POST,
        "/api/cluster/join",
        &signed,
        None,
        Ipv4Addr::new(10, 20, 0, 99),
    )
    .await?;
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    Ok(())
}

async fn send_json(
    server: &ApiServer,
    method: Method,
    uri: &str,
    payload: &impl serde::Serialize,
    token: Option<&str>,
    source: Ipv4Addr,
) -> Result<axum::response::Response, Box<dyn std::error::Error>> {
    let mut request = Request::builder()
        .method(method)
        .uri(uri)
        .header(header::CONTENT_TYPE, "application/json");
    if let Some(token) = token {
        request = request.header(header::AUTHORIZATION, format!("Bearer {token}"));
    }
    let mut request = request.body(Body::from(serde_json::to_vec(payload)?))?;
    request
        .extensions_mut()
        .insert(ConnectInfo(SocketAddr::from((source, 40_000))));
    Ok(server.router().oneshot(request).await?)
}

fn cluster_config() -> Result<(ClusterConfig, NodeId), Box<dyn std::error::Error>> {
    let master_id = NodeId::new("node-1")?;
    let worker_id = NodeId::new("node-2")?;
    let nodes = BTreeMap::from([
        (
            master_id,
            NodeDefinition {
                hostname: "node-1.internal".to_string(),
                endpoint: NodeEndpoint {
                    host_address: Ipv4Addr::new(10, 20, 0, 11),
                    api_port: 3_000,
                },
                workload_subnet: "172.22.1.0/24".parse::<Ipv4Cidr>()?,
                role: NodeRole::Master,
            },
        ),
        (
            worker_id.clone(),
            NodeDefinition {
                hostname: "node-2.internal".to_string(),
                endpoint: NodeEndpoint {
                    host_address: worker_address(),
                    api_port: 3_000,
                },
                workload_subnet: "172.22.2.0/24".parse::<Ipv4Cidr>()?,
                role: NodeRole::Worker,
            },
        ),
    ]);
    Ok((
        ClusterConfig {
            cluster_id: ClusterId::new("server-test")?,
            name: "server-test".to_string(),
            nodes,
            control_allow_cidrs: vec!["10.20.0.0/24".parse()?],
            ports: ClusterPorts::new(3_001, 2_379, 2_380, 51_820)?,
            join_secret: SecretValue::new("join-test-secret-with-at-least-32-characters"),
            tailscale: None,
            cloudflare: None,
        },
        worker_id,
    ))
}

fn authority_validity() -> Result<CertificateValidity, Box<dyn std::error::Error>> {
    let now = OffsetDateTime::now_utc();
    Ok(CertificateValidity::new(
        now - Duration::days(1),
        now + Duration::days(3_650),
    )?)
}

fn now_unix_ms() -> i64 {
    let duration = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or(StdDuration::ZERO);
    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
}

fn worker_address() -> Ipv4Addr {
    Ipv4Addr::new(10, 20, 0, 12)
}

struct UnusedProvider;

#[async_trait]
impl StoreProvider for UnusedProvider {
    async fn start(
        &self,
        _mode: StoreStartMode,
    ) -> Result<Box<dyn StoreRuntime>, StoreProviderError> {
        Err(unused_provider_error())
    }

    async fn stage_member(
        &self,
        _member: StoreMember,
    ) -> Result<(StoreJoinTicket, MemberActivation), StoreProviderError> {
        Err(unused_provider_error())
    }

    async fn activate_member(
        &self,
        _ticket: &StoreJoinTicket,
    ) -> Result<MemberActivation, StoreProviderError> {
        Err(unused_provider_error())
    }

    async fn remove_member(&self, _node_id: &NodeId) -> Result<(), StoreProviderError> {
        Err(unused_provider_error())
    }

    async fn recover(
        &self,
        _permit: StoreRecoveryPermit,
    ) -> Result<StoreRecovery, StoreProviderError> {
        Err(unused_provider_error())
    }
}

fn unused_provider_error() -> StoreProviderError {
    StoreProviderError::InvalidConfiguration {
        reason: "store provider is unused for worker admission".to_string(),
    }
}
