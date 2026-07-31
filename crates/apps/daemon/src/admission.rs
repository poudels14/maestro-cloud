use std::sync::Arc;

use cluster::{
    AdmissionCoordinator, AdmissionCoordinatorError, ClusterCertificateAuthority, ClusterConfig,
    StoreProvider,
};
use kernel_store::{CasOutcome, ExpectedVersion, Keyspace, PutRequest, Store, StoreError};

/// Secret material retained only by control-plane nodes that can admit members.
#[derive(Clone)]
pub struct AdmissionDependencies {
    /// First-bootstrap signer used only to initialize the encrypted store record.
    pub authority_seed: Option<ClusterCertificateAuthority>,
}

impl AdmissionDependencies {
    pub(crate) async fn coordinator(
        &self,
        config: ClusterConfig,
        provider: Arc<dyn StoreProvider>,
        store: Arc<dyn Store>,
    ) -> Result<Arc<AdmissionCoordinator>, AdmissionBootstrapError> {
        let authority = load_or_seed_authority(
            store.as_ref(),
            &Keyspace::new(&config.cluster_id),
            self.authority_seed.as_ref(),
        )
        .await?;
        AdmissionCoordinator::new(config, authority, provider, store)
            .map(Arc::new)
            .map_err(Into::into)
    }
}

async fn load_or_seed_authority(
    store: &dyn Store,
    keys: &Keyspace,
    seed: Option<&ClusterCertificateAuthority>,
) -> Result<ClusterCertificateAuthority, AdmissionBootstrapError> {
    let key = keys.certificate_authority();
    if let Some(stored) = store.get(&key).await? {
        return decode_authority(&stored.value, seed);
    }
    let seed = seed.ok_or(AdmissionBootstrapError::MissingAuthority)?;
    seed.validate()?;
    let encoded = serde_json::to_vec(seed)?;
    match store
        .put_cas(PutRequest {
            key: key.clone(),
            value: encoded,
            expected: ExpectedVersion::Missing,
            session: None,
        })
        .await?
    {
        CasOutcome::Applied(_) => Ok(seed.clone()),
        CasOutcome::Conflict { .. } => {
            let stored = store
                .get(&key)
                .await?
                .ok_or(AdmissionBootstrapError::MissingAuthority)?;
            decode_authority(&stored.value, Some(seed))
        }
    }
}

fn decode_authority(
    encoded: &[u8],
    expected: Option<&ClusterCertificateAuthority>,
) -> Result<ClusterCertificateAuthority, AdmissionBootstrapError> {
    let authority: ClusterCertificateAuthority = serde_json::from_slice(encoded)?;
    authority.validate()?;
    if expected.is_some_and(|expected| expected != &authority) {
        Err(AdmissionBootstrapError::AuthorityConflict)
    } else {
        Ok(authority)
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum AdmissionBootstrapError {
    #[error("cluster certificate authority is absent from encrypted cluster state")]
    MissingAuthority,
    #[error("node bootstrap authority does not match encrypted cluster state")]
    AuthorityConflict,
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Certificate(#[from] cluster::CertificateError),
    #[error("cluster certificate authority record is invalid: {0}")]
    Decode(#[from] serde_json::Error),
    #[error(transparent)]
    Coordinator(#[from] AdmissionCoordinatorError),
}
