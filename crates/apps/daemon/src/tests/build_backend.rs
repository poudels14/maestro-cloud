use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard};

use async_trait::async_trait;
use build::{BuildRevisionResolver, BuildSourceError, BuildSourceProvider, PreparedBuildSource};
use ingress::IngressBackend;
use kernel_api::{BuildId, BuildSource, SecretValue, WebhookFormat};
use runtime::{
    ArtifactBuildRequest, ArtifactByteStream, ArtifactDigest, ArtifactPrunePolicy,
    ArtifactPruneReport, ArtifactReference, ArtifactSource, ArtifactStore, ArtifactStoreError,
};
use webhook::{WebhookDelivery, WebhookDeliveryBackend, WebhookDeliveryError};

use crate::OperatorBackends;

const SOURCE_REVISION: &str = "0123456789abcdef0123456789abcdef01234567";

pub(super) struct FakeBuildBackend {
    builds: Mutex<Vec<ArtifactBuildRequest>>,
    source_tokens: Mutex<Vec<Option<String>>>,
    revision_tokens: Mutex<Vec<Option<String>>>,
    revision: Mutex<String>,
}

impl Default for FakeBuildBackend {
    fn default() -> Self {
        Self {
            builds: Mutex::new(Vec::new()),
            source_tokens: Mutex::new(Vec::new()),
            revision_tokens: Mutex::new(Vec::new()),
            revision: Mutex::new(SOURCE_REVISION.to_string()),
        }
    }
}

impl FakeBuildBackend {
    pub(super) fn operator_backends(
        ingress: Arc<dyn IngressBackend>,
    ) -> (OperatorBackends, Arc<Self>) {
        let build = Arc::new(Self::default());
        (
            OperatorBackends {
                ingress,
                build_source: build.clone(),
                build_revisions: build.clone(),
                artifacts: build.clone(),
                pull_requests: None,
                upgrades: None,
                webhooks: Arc::new(AcceptingWebhookBackend),
            },
            build,
        )
    }

    pub(super) fn builds(&self) -> Vec<ArtifactBuildRequest> {
        lock(&self.builds).clone()
    }

    pub(super) fn source_tokens(&self) -> Vec<Option<String>> {
        lock(&self.source_tokens).clone()
    }

    pub(super) fn set_revision(&self, revision: impl Into<String>) {
        *lock(&self.revision) = revision.into();
    }
}

struct AcceptingWebhookBackend;

#[async_trait]
impl WebhookDeliveryBackend for AcceptingWebhookBackend {
    async fn deliver(
        &self,
        _endpoint: &str,
        _format: WebhookFormat,
        _signing_secret: Option<&SecretValue>,
        _delivery: &WebhookDelivery,
    ) -> Result<(), WebhookDeliveryError> {
        Ok(())
    }
}

#[async_trait]
impl BuildSourceProvider for FakeBuildBackend {
    async fn prepare(
        &self,
        _build_id: &BuildId,
        _source: &BuildSource,
        resolved_revision: Option<&str>,
        github_token: Option<&SecretValue>,
    ) -> Result<PreparedBuildSource, BuildSourceError> {
        lock(&self.source_tokens).push(github_token.map(|token| token.expose().to_owned()));
        let revision = resolved_revision
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| lock(&self.revision).clone());
        Ok(PreparedBuildSource {
            artifact_source: ArtifactSource::Directory {
                root: PathBuf::from("/var/lib/maestro/test-build"),
                definition: PathBuf::new(),
            },
            revision,
        })
    }

    async fn cleanup(&self, _build_id: &BuildId) -> Result<(), BuildSourceError> {
        Ok(())
    }
}

#[async_trait]
impl BuildRevisionResolver for FakeBuildBackend {
    async fn resolve_revision(
        &self,
        _source: &BuildSource,
        github_token: Option<&SecretValue>,
    ) -> Result<Option<String>, BuildSourceError> {
        lock(&self.revision_tokens).push(github_token.map(|token| token.expose().to_owned()));
        Ok(Some(lock(&self.revision).clone()))
    }
}

#[async_trait]
impl ArtifactStore for FakeBuildBackend {
    async fn build(
        &self,
        request: &ArtifactBuildRequest,
    ) -> Result<ArtifactDigest, ArtifactStoreError> {
        lock(&self.builds).push(request.clone());
        ArtifactDigest::new(
            "maestro.test/build@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        )
    }

    async fn pull(
        &self,
        reference: &ArtifactReference,
    ) -> Result<ArtifactDigest, ArtifactStoreError> {
        Err(unused("pull", reference.as_str()))
    }

    async fn push(
        &self,
        digest: &ArtifactDigest,
        _destination: &ArtifactReference,
    ) -> Result<(), ArtifactStoreError> {
        Err(unused("push", digest.as_str()))
    }

    async fn resolve_digest(
        &self,
        reference: &ArtifactReference,
    ) -> Result<ArtifactDigest, ArtifactStoreError> {
        Err(unused("resolve", reference.as_str()))
    }

    async fn contains(&self, _digest: &ArtifactDigest) -> Result<bool, ArtifactStoreError> {
        Ok(false)
    }

    async fn export(
        &self,
        digest: &ArtifactDigest,
    ) -> Result<Box<dyn ArtifactByteStream>, ArtifactStoreError> {
        Err(unused("export", digest.as_str()))
    }

    async fn import(
        &self,
        _source: Box<dyn ArtifactByteStream>,
    ) -> Result<ArtifactDigest, ArtifactStoreError> {
        Err(unused("import", "stream"))
    }

    async fn prune(
        &self,
        _policy: &ArtifactPrunePolicy,
    ) -> Result<ArtifactPruneReport, ArtifactStoreError> {
        Ok(ArtifactPruneReport {
            removed: Vec::new(),
        })
    }
}

fn unused(operation: &str, target: &str) -> ArtifactStoreError {
    ArtifactStoreError::Rejected {
        message: format!("fake build backend does not support {operation} for `{target}`"),
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}
