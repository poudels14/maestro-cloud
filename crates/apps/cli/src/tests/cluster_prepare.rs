use std::sync::Mutex;

use cluster::{
    CaDiscoveryRequest, CaDiscoveryResponse, CertificateValidity, ClusterCertificateAuthority,
    ClusterConfig, JoinPayload, JoinResponseStatus, SignedJoinRequest,
    create_ca_discovery_response, encrypt_join_response, verify_join_request_signature,
};
use time::{Duration, OffsetDateTime};

use crate::CliError;
use crate::cluster_join::{AdmissionResponse, JoinTransport};
use crate::cluster_prepare::{NodeLaunchOptions, prepare_node_launch_with_transport};
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

struct FailingReader;

impl ConfigSourceReader for FailingReader {
    async fn read(&self, _source: &str) -> Result<String, CliError> {
        Err(CliError::invalid_input(
            "existing launch preparation unexpectedly fetched config",
        ))
    }
}

struct FakeJoinTransport {
    config: ClusterConfig,
    authority: ClusterCertificateAuthority,
    payload: JoinPayload,
    origins: Mutex<Vec<String>>,
}

impl JoinTransport for FakeJoinTransport {
    async fn discover(
        &self,
        origin: &reqwest::Url,
        request: &CaDiscoveryRequest,
    ) -> Result<CaDiscoveryResponse, CliError> {
        self.origins
            .lock()
            .map_err(|_| CliError::invalid_input("test origin lock poisoned"))?
            .push(origin.to_string());
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
async fn fresh_master_data_directory_is_prepared_before_daemon_start()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let reader = MemoryReader {
        source: super::cluster_formation::cluster_document(),
    };
    let loaded = load_cluster("maestro.jsonc", &reader).await?;
    let transport = unused_transport(&loaded.cluster)?;
    let options = NodeLaunchOptions::new(
        "maestro.jsonc".to_string(),
        directory.path().to_path_buf(),
        "/run/containerd/containerd.sock".into(),
        "/run/current-system/sw/bin/etcd".into(),
    );

    let mut output = Vec::new();
    prepare_node_launch_with_transport(&options, &mut output, &reader, &transport).await?;
    let launch_path = directory.path().join("launch.json");
    let launch: serde_json::Value = serde_json::from_slice(&std::fs::read(&launch_path)?)?;
    assert_eq!(launch.pointer("/nodeId"), Some(&"node-1".into()));
    assert_eq!(launch.pointer("/storeMode/kind"), Some(&"bootstrap".into()));
    assert!(String::from_utf8(output)?.contains("created bootstrap launch document"));

    prepare_node_launch_with_transport(&options, &mut Vec::new(), &FailingReader, &transport)
        .await?;
    Ok(())
}

#[tokio::test]
async fn fresh_follower_uses_the_declared_master_and_persists_join_state()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let reader = MemoryReader {
        source: super::cluster_formation::cluster_document()
            .replace("node: \"node-1\"", "node: \"node-2\""),
    };
    let loaded = load_cluster("maestro.jsonc", &reader).await?;
    let node = loaded
        .cluster
        .nodes
        .get(&loaded.node_id)
        .ok_or("selected follower missing")?;
    let now = OffsetDateTime::now_utc();
    let validity = CertificateValidity::new(now - Duration::minutes(1), now + Duration::days(1))?;
    let authority = ClusterCertificateAuthority::generate(&loaded.cluster.name, validity)?;
    let payload = JoinPayload {
        cluster_id: loaded.cluster.cluster_id.clone(),
        cluster_name: loaded.cluster.name.clone(),
        nodes: loaded.cluster.nodes.clone(),
        ports: loaded.cluster.ports,
        certificates: authority.issue_node_certificate_for_definition(
            &loaded.node_id,
            node,
            validity,
        )?,
        certificate_issuer: None,
        store_join_ticket: None,
    };
    let transport = FakeJoinTransport {
        config: loaded.cluster,
        authority,
        payload,
        origins: Mutex::new(Vec::new()),
    };
    let options = NodeLaunchOptions::new(
        "maestro.jsonc".to_string(),
        directory.path().to_path_buf(),
        "/run/containerd/containerd.sock".into(),
        "/run/current-system/sw/bin/etcd".into(),
    );

    prepare_node_launch_with_transport(&options, &mut Vec::new(), &reader, &transport).await?;
    let launch: serde_json::Value =
        serde_json::from_slice(&std::fs::read(directory.path().join("launch.json"))?)?;
    assert_eq!(launch.pointer("/nodeId"), Some(&"node-2".into()));
    assert_eq!(launch.pointer("/storeMode/kind"), Some(&"client".into()));
    assert_eq!(
        transport
            .origins
            .lock()
            .map_err(|_| "test origin lock poisoned")?
            .as_slice(),
        ["https://10.20.0.11:3000/"]
    );
    Ok(())
}

fn unused_transport(
    config: &ClusterConfig,
) -> Result<FakeJoinTransport, Box<dyn std::error::Error>> {
    let now = OffsetDateTime::now_utc();
    let validity = CertificateValidity::new(now - Duration::minutes(1), now + Duration::days(1))?;
    let authority = ClusterCertificateAuthority::generate(&config.name, validity)?;
    let node_id = kernel_api::NodeId::new("node-2")?;
    let node = config.nodes.get(&node_id).ok_or("test follower missing")?;
    let payload = JoinPayload {
        cluster_id: config.cluster_id.clone(),
        cluster_name: config.name.clone(),
        nodes: config.nodes.clone(),
        ports: config.ports,
        certificates: authority.issue_node_certificate_for_definition(&node_id, node, validity)?,
        certificate_issuer: None,
        store_join_ticket: None,
    };
    Ok(FakeJoinTransport {
        config: config.clone(),
        authority,
        payload,
        origins: Mutex::new(Vec::new()),
    })
}
