use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use axum::http::StatusCode;
use kernel_api::{NodeId, SecretValue};
use runtime::{
    ArtifactBuildRequest, ArtifactByteStream, ArtifactDigest, ArtifactPrunePolicy,
    ArtifactPruneReport, ArtifactReference, ArtifactStore, ArtifactStoreError,
};

use super::{request, seeded_store, token};
use crate::{ApiServer, HttpNodeArtifactClient, ServerSettings, TlsIdentity};

const DIGEST: &str = "sha256:0123456789abcdef";

#[tokio::test]
async fn node_artifact_route_streams_only_to_node_credentials()
-> Result<(), Box<dyn std::error::Error>> {
    let secret = "node-artifact-test-secret-that-is-long-enough";
    let (store, cluster_id) = seeded_store().await?;
    let server = ApiServer::new(
        store,
        cluster_id,
        ServerSettings::new("127.0.0.1:3000".parse()?, Some(SecretValue::new(secret))),
    )?
    .with_artifact_store(Arc::new(TestArtifacts));
    let path = format!("/api/node/artifacts?digest={DIGEST}");

    assert_eq!(
        request(&server, &path, None).await?.status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        request(&server, &path, Some(&token(secret, "operator")?))
            .await?
            .status(),
        StatusCode::FORBIDDEN
    );
    let response = request(&server, &path, Some(&token(secret, "node")?)).await?;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        http_body_util::BodyExt::collect(response.into_body())
            .await?
            .to_bytes(),
        "artifact-bytes"
    );
    Ok(())
}

#[tokio::test]
async fn node_artifact_route_fails_closed_without_local_artifact_storage()
-> Result<(), Box<dyn std::error::Error>> {
    let (store, cluster_id) = seeded_store().await?;
    let server = ApiServer::new(
        store,
        cluster_id,
        ServerSettings::new("127.0.0.1:3000".parse()?, None),
    )?;
    assert_eq!(
        request(
            &server,
            &format!("/api/node/artifacts?digest={DIGEST}"),
            None,
        )
        .await?
        .status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    Ok(())
}

#[tokio::test]
async fn node_artifact_client_streams_over_authenticated_mutual_tls()
-> Result<(), Box<dyn std::error::Error>> {
    let secret = SecretValue::new("node-artifact-client-secret-that-is-long-enough");
    let certified = rcgen::generate_simple_self_signed(vec!["127.0.0.1".to_owned()])?;
    let certificate_pem = certified.cert.pem();
    let identity = TlsIdentity::new(
        certificate_pem.clone(),
        SecretValue::new(certified.signing_key.serialize_pem()),
    );
    let remote_node = NodeId::new("node-remote")?;
    let local_node = NodeId::new("node-local")?;
    let (store, cluster_id) = seeded_store().await?;
    let remote = ApiServer::new(
        store,
        cluster_id,
        ServerSettings::new("127.0.0.1:0".parse()?, Some(secret.clone()))
            .with_tls_identity(identity.clone())
            .with_cluster_trust_root(certificate_pem.clone()),
    )?
    .with_artifact_store(Arc::new(TestArtifacts))
    .bind()
    .await?;
    let remote_address = remote.local_address();
    let (shutdown, shutdown_receiver) = tokio::sync::watch::channel(false);
    let task = tokio::spawn(remote.serve(shutdown_receiver));
    let client = HttpNodeArtifactClient::new(
        local_node.clone(),
        BTreeMap::from([
            (local_node, "127.0.0.1:1".parse()?),
            (remote_node.clone(), remote_address),
        ]),
        &certificate_pem,
        &identity,
        &secret,
    )?;
    let digest = ArtifactDigest::new(DIGEST)?;
    let mut stream = client.export(&remote_node, &digest).await?;
    let mut received = Vec::new();
    while let Some(chunk) = stream.next().await? {
        received.extend_from_slice(&chunk);
    }
    assert_eq!(received, b"artifact-bytes");
    shutdown.send(true)?;
    task.await??;
    Ok(())
}

struct TestArtifacts;

#[async_trait]
impl ArtifactStore for TestArtifacts {
    async fn build(
        &self,
        _request: &ArtifactBuildRequest,
    ) -> Result<ArtifactDigest, ArtifactStoreError> {
        Err(unused("build"))
    }

    async fn pull(
        &self,
        _reference: &ArtifactReference,
    ) -> Result<ArtifactDigest, ArtifactStoreError> {
        Err(unused("pull"))
    }

    async fn push(
        &self,
        _digest: &ArtifactDigest,
        _destination: &ArtifactReference,
    ) -> Result<(), ArtifactStoreError> {
        Err(unused("push"))
    }

    async fn resolve_digest(
        &self,
        _reference: &ArtifactReference,
    ) -> Result<ArtifactDigest, ArtifactStoreError> {
        Err(unused("resolve"))
    }

    async fn contains(&self, digest: &ArtifactDigest) -> Result<bool, ArtifactStoreError> {
        Ok(digest.as_str() == DIGEST)
    }

    async fn export(
        &self,
        digest: &ArtifactDigest,
    ) -> Result<Box<dyn ArtifactByteStream>, ArtifactStoreError> {
        if digest.as_str() != DIGEST {
            return Err(ArtifactStoreError::NotFound {
                reference: digest.as_str().to_owned(),
            });
        }
        Ok(Box::new(TestStream(Some(b"artifact-bytes".to_vec()))))
    }

    async fn import(
        &self,
        _source: Box<dyn ArtifactByteStream>,
    ) -> Result<ArtifactDigest, ArtifactStoreError> {
        Err(unused("import"))
    }

    async fn prune(
        &self,
        _policy: &ArtifactPrunePolicy,
    ) -> Result<ArtifactPruneReport, ArtifactStoreError> {
        Err(unused("prune"))
    }
}

struct TestStream(Option<Vec<u8>>);

#[async_trait]
impl ArtifactByteStream for TestStream {
    async fn next(&mut self) -> Result<Option<Vec<u8>>, ArtifactStoreError> {
        Ok(self.0.take())
    }
}

fn unused(operation: &str) -> ArtifactStoreError {
    ArtifactStoreError::Rejected {
        message: format!("test artifact store does not support {operation}"),
    }
}
