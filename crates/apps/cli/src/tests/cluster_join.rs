use std::sync::Mutex;

use cluster::{
    CaDiscoveryRequest, CaDiscoveryResponse, CertificateValidity, ClusterCertificateAuthority,
    ClusterConfig, JoinPayload, JoinResponseStatus, SignedJoinRequest, StoreJoinTicket,
    create_ca_discovery_response, encrypt_join_response, verify_join_request_signature,
};
use kernel_api::SecretValue;
use time::{Duration, OffsetDateTime};

use crate::CliError;
use crate::cluster_join::{AdmissionResponse, JoinOptions, JoinTransport, join_with_transport};
use crate::config::load_cluster;
use crate::config_source::ConfigSourceReader;

struct MemoryReader {
    source: String,
}

impl ConfigSourceReader for MemoryReader {
    async fn read(&self, _source: &str) -> Result<String, CliError> {
        Ok(self.source.clone())
    }
}

struct FakeJoinTransport {
    config: ClusterConfig,
    authority: ClusterCertificateAuthority,
    payload: JoinPayload,
    requests: Mutex<Vec<SignedJoinRequest>>,
}

impl JoinTransport for FakeJoinTransport {
    async fn discover(
        &self,
        origin: &reqwest::Url,
        request: &CaDiscoveryRequest,
    ) -> Result<CaDiscoveryResponse, CliError> {
        if origin.as_str() != "https://10.20.0.11:3000/" {
            return Err(CliError::invalid_input("unexpected test origin"));
        }
        create_ca_discovery_response(
            &self.config.join_secret,
            &self.config.name,
            &self.config.cluster_id,
            &self.authority.certificate_pem,
            request,
        )
        .map_err(|error| CliError::cluster("failed test discovery", error.to_string()))
    }

    async fn admit(
        &self,
        _origin: &reqwest::Url,
        ca_certificate_pem: &str,
        signed: &SignedJoinRequest,
    ) -> Result<AdmissionResponse, CliError> {
        if ca_certificate_pem != self.authority.certificate_pem {
            return Err(CliError::invalid_input("unexpected test CA"));
        }
        verify_join_request_signature(&self.config.join_secret, &signed.request, &signed.signature)
            .map_err(|error| CliError::cluster("invalid test join signature", error.to_string()))?;
        self.requests
            .lock()
            .map_err(|_| CliError::invalid_input("test request lock poisoned"))?
            .push(signed.clone());
        let envelope = encrypt_join_response(
            &self.config.join_secret,
            &signed.request,
            &self.payload,
            JoinResponseStatus::ACCEPTED,
        )
        .map_err(|error| CliError::cluster("failed test join encryption", error.to_string()))?;
        Ok(AdmissionResponse {
            status: JoinResponseStatus::ACCEPTED,
            envelope,
        })
    }
}

#[tokio::test]
async fn authenticated_join_persists_a_replayable_private_worker_launch_document()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let reader = MemoryReader {
        source: cluster_document(),
    };
    let loaded = load_cluster("maestro.jsonc", &reader).await?;
    let now = OffsetDateTime::now_utc();
    let validity = CertificateValidity::new(
        now.checked_sub(Duration::minutes(1))
            .ok_or("certificate start underflow")?,
        now.checked_add(Duration::days(1))
            .ok_or("certificate end overflow")?,
    )?;
    let authority = ClusterCertificateAuthority::generate(&loaded.cluster.name, validity)?;
    let node = loaded
        .cluster
        .nodes
        .get(&loaded.node_id)
        .ok_or("worker missing")?;
    let payload = JoinPayload {
        cluster_id: loaded.cluster.cluster_id.clone(),
        cluster_name: loaded.cluster.name.clone(),
        nodes: loaded.cluster.nodes.clone(),
        control_allow_cidrs: loaded.cluster.control_allow_cidrs.clone(),
        ports: loaded.cluster.ports,
        certificates: authority.issue_node_certificate(
            &loaded.node_id,
            &node.hostname,
            node.endpoint.host_address,
            node.role,
            validity,
        )?,
        operator_jwt_secret: SecretValue::new("operator-test-secret-with-at-least-32-characters"),
        store_encryption_secret: SecretValue::new(
            "storage-test-secret-with-at-least-32-characters",
        ),
        store_join_ticket: None,
        certificate_issuer: None,
    };
    let transport = FakeJoinTransport {
        config: loaded.cluster,
        authority,
        payload,
        requests: Mutex::new(Vec::new()),
    };
    let options = || {
        JoinOptions::new(
            "https://10.20.0.11:3000".to_string(),
            "maestro.jsonc".to_string(),
            directory.path().to_path_buf(),
        )
    };

    let mut first_output = Vec::new();
    join_with_transport(options(), &mut first_output, &reader, &transport).await?;
    let request_path = directory.path().join("security/join-request.json");
    let first_request = std::fs::read(&request_path)?;
    let mut second_output = Vec::new();
    join_with_transport(options(), &mut second_output, &reader, &transport).await?;
    assert_eq!(std::fs::read(&request_path)?, first_request);
    let requests = transport
        .requests
        .lock()
        .map_err(|_| "test request lock poisoned")?;
    assert_eq!(requests.len(), 2);
    assert_eq!(requests.first(), requests.get(1));

    let launch_path = directory.path().join("launch.json");
    let launch: serde_json::Value = serde_json::from_slice(&std::fs::read(&launch_path)?)?;
    assert_eq!(launch.pointer("/nodeId"), Some(&"node-2".into()));
    assert_eq!(launch.pointer("/storeMode/kind"), Some(&"client".into()));
    assert_eq!(
        launch.pointer("/cluster/controlAllowCidrs/0"),
        Some(&"10.20.0.0/24".into())
    );
    assert!(launch.pointer("/security/identity/privateKeyPem").is_some());
    assert!(launch.pointer("/operatorJwtSecret").is_some());
    assert!(launch.pointer("/storeEncryptionSecret").is_some());
    assert!(launch.pointer("/certificateIssuer").is_none());
    assert!(launch.pointer("/etcdBinary").is_none());
    let first_output = String::from_utf8(first_output)?;
    assert!(first_output.contains("created joined launch document for node `node-2`"));
    assert!(!first_output.contains("operator-test-secret"));
    let second_output = String::from_utf8(second_output)?;
    assert!(second_output.contains("verified existing joined launch document"));

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        for path in [
            directory.path().join("security/join.key"),
            request_path,
            launch_path,
        ] {
            assert_eq!(std::fs::metadata(path)?.permissions().mode() & 0o777, 0o600);
        }
    }
    Ok(())
}

#[tokio::test]
async fn join_rejects_non_https_leader_before_transport() -> Result<(), Box<dyn std::error::Error>>
{
    let directory = tempfile::tempdir()?;
    let reader = MemoryReader {
        source: cluster_document(),
    };
    let loaded = load_cluster("maestro.jsonc", &reader).await?;
    let now = OffsetDateTime::now_utc();
    let validity = CertificateValidity::new(now, now + Duration::days(1))?;
    let authority = ClusterCertificateAuthority::generate(&loaded.cluster.name, validity)?;
    let transport = FakeJoinTransport {
        payload: unusable_payload(&loaded.cluster, &authority, validity)?,
        config: loaded.cluster,
        authority,
        requests: Mutex::new(Vec::new()),
    };
    let error = join_with_transport(
        JoinOptions::new(
            "http://10.20.0.11:3000".to_string(),
            "maestro.jsonc".to_string(),
            directory.path().to_path_buf(),
        ),
        &mut Vec::new(),
        &reader,
        &transport,
    )
    .await
    .expect_err("plaintext leader must be rejected");
    assert!(error.to_string().contains("leader must be an HTTPS origin"));
    assert!(
        transport
            .requests
            .lock()
            .map_err(|_| "lock poisoned")?
            .is_empty()
    );
    Ok(())
}

#[tokio::test]
async fn control_plane_join_writes_its_bound_ticket_issuer_and_etcd_path()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let reader = MemoryReader {
        source: control_plane_cluster_document(),
    };
    let loaded = load_cluster("maestro.jsonc", &reader).await?;
    let now = OffsetDateTime::now_utc();
    let validity = CertificateValidity::new(now, now + Duration::days(1))?;
    let authority = ClusterCertificateAuthority::generate(&loaded.cluster.name, validity)?;
    let node = loaded
        .cluster
        .nodes
        .get(&loaded.node_id)
        .ok_or("control-plane node missing")?;
    let payload = JoinPayload {
        cluster_id: loaded.cluster.cluster_id.clone(),
        cluster_name: loaded.cluster.name.clone(),
        nodes: loaded.cluster.nodes.clone(),
        control_allow_cidrs: loaded.cluster.control_allow_cidrs.clone(),
        ports: loaded.cluster.ports,
        certificates: authority.issue_node_certificate(
            &loaded.node_id,
            &node.hostname,
            node.endpoint.host_address,
            node.role,
            validity,
        )?,
        operator_jwt_secret: SecretValue::new("operator-test-secret-with-at-least-32-characters"),
        store_encryption_secret: SecretValue::new(
            "storage-test-secret-with-at-least-32-characters",
        ),
        store_join_ticket: Some(StoreJoinTicket::from_provider_data(
            loaded.node_id.clone(),
            b"opaque-test-ticket",
        )),
        certificate_issuer: Some(authority.clone()),
    };
    let transport = FakeJoinTransport {
        config: loaded.cluster,
        authority,
        payload,
        requests: Mutex::new(Vec::new()),
    };
    let mut options = JoinOptions::new(
        "https://10.20.0.11:3000".to_string(),
        "maestro.jsonc".to_string(),
        directory.path().to_path_buf(),
    );
    options.etcd_binary = Some("/run/current-system/sw/bin/etcd".into());

    join_with_transport(options, &mut Vec::new(), &reader, &transport).await?;
    let launch: serde_json::Value =
        serde_json::from_slice(&std::fs::read(directory.path().join("launch.json"))?)?;
    assert_eq!(
        launch
            .pointer("/storeMode/kind")
            .and_then(serde_json::Value::as_str),
        Some("join")
    );
    assert_eq!(
        launch
            .pointer("/storeMode/ticket/nodeId")
            .and_then(serde_json::Value::as_str),
        Some("node-2")
    );
    assert_eq!(
        launch
            .pointer("/etcdBinary")
            .and_then(serde_json::Value::as_str),
        Some("/run/current-system/sw/bin/etcd")
    );
    assert!(launch.pointer("/certificateIssuer/privateKeyPem").is_some());
    Ok(())
}

fn unusable_payload(
    config: &ClusterConfig,
    authority: &ClusterCertificateAuthority,
    validity: CertificateValidity,
) -> Result<JoinPayload, Box<dyn std::error::Error>> {
    let node_id = kernel_api::NodeId::new("node-2")?;
    let node = config.nodes.get(&node_id).ok_or("worker missing")?;
    Ok(JoinPayload {
        cluster_id: config.cluster_id.clone(),
        cluster_name: config.name.clone(),
        nodes: config.nodes.clone(),
        control_allow_cidrs: config.control_allow_cidrs.clone(),
        ports: config.ports,
        certificates: authority.issue_node_certificate(
            &node_id,
            &node.hostname,
            node.endpoint.host_address,
            node.role,
            validity,
        )?,
        operator_jwt_secret: SecretValue::new("operator-test-secret-with-at-least-32-characters"),
        store_encryption_secret: SecretValue::new(
            "storage-test-secret-with-at-least-32-characters",
        ),
        store_join_ticket: None,
        certificate_issuer: None,
    })
}

fn cluster_document() -> String {
    r#"{
            cluster: {
                name: "test-cluster",
                nodes: {
                    "node-1": {
                        hostname: "node-1.internal",
                        endpoint: "10.20.0.11",
                        subnet: "172.22.1.0/24",
                        role: "master"
                    },
                    "node-2": {
                        hostname: "node-2.internal",
                        endpoint: "10.20.0.12",
                        subnet: "172.22.2.0/24",
                        role: "worker"
                    }
                },
                controlAllowCidrs: ["10.20.0.0/24"],
                joinSecret: "a-test-join-secret-with-at-least-32-characters"
            },
            node: "node-2"
        }"#
    .to_string()
}

fn control_plane_cluster_document() -> String {
    r#"{
            cluster: {
                name: "test-cluster",
                nodes: {
                    "node-1": {
                        endpoint: "10.20.0.11",
                        subnet: "172.22.1.0/24",
                        role: "master"
                    },
                    "node-2": {
                        endpoint: "10.20.0.12",
                        subnet: "172.22.2.0/24",
                        role: "control-plane"
                    },
                    "node-3": {
                        endpoint: "10.20.0.13",
                        subnet: "172.22.3.0/24",
                        role: "control-plane"
                    }
                },
                controlAllowCidrs: ["10.20.0.0/24"],
                joinSecret: "a-test-join-secret-with-at-least-32-characters"
            },
            node: "node-2"
        }"#
    .to_string()
}
