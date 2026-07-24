use kernel_api::{Generation, Service, ServiceId, Timestamp};
use kernel_controller::{ControllerError, FencedStore};
use kernel_store::{
    Compare, ExpectedVersion, Keyspace, Mutation, StoreKey, Transaction, TransactionOutcome,
    Version,
};

use crate::traefik_resources::{
    TRAEFIK_SERVICE_ID, TraefikResourceError, TraefikSystemResources, is_managed,
};

const MAXIMUM_CONFLICT_RETRIES: usize = 8;

pub(crate) struct TraefikResourceReconciler {
    service_id: ServiceId,
    service_key: StoreKey,
    desired: Option<TraefikSystemResources>,
}

impl TraefikResourceReconciler {
    pub(crate) fn new(
        cluster_id: &kernel_api::ClusterId,
        desired: Option<TraefikSystemResources>,
    ) -> Result<Self, TraefikReconcileError> {
        let service_id = ServiceId::new(TRAEFIK_SERVICE_ID)?;
        let service_kind = kernel_api::ResourceKind::new("Service")?;
        let service_key =
            Keyspace::new(cluster_id).resource(&service_kind, &service_id.clone().into());
        Ok(Self {
            service_id,
            service_key,
            desired,
        })
    }

    pub(crate) async fn reconcile(
        &self,
        store: &FencedStore,
        now: Timestamp,
    ) -> Result<(), TraefikReconcileError> {
        for _ in 0..MAXIMUM_CONFLICT_RETRIES {
            let current = self.read(store).await?;
            if let Some(current) = current.as_ref()
                && current.resource.meta.id != self.service_id
            {
                return Err(TraefikReconcileError::IdentityMismatch {
                    expected: self.service_id.to_string(),
                    actual: current.resource.meta.id.to_string(),
                });
            }
            let expected = current
                .as_ref()
                .map_or(ExpectedVersion::Missing, |current| {
                    ExpectedVersion::Exact(current.version)
                });
            let replacement = converge(
                current,
                self.desired.as_ref().map(|desired| &desired.service),
                now,
            )?;
            let Some(replacement) = replacement else {
                store.verify_leadership().await?;
                return Ok(());
            };
            let value = serde_json::to_vec(&replacement).map_err(|error| {
                TraefikReconcileError::Serialize {
                    message: error.to_string(),
                }
            })?;
            if matches!(
                store
                    .txn(Transaction {
                        compares: vec![Compare {
                            key: self.service_key.clone(),
                            expected,
                        }],
                        mutations: vec![Mutation::Put {
                            key: self.service_key.clone(),
                            value,
                            session: None,
                        }],
                    })
                    .await?,
                TransactionOutcome::Applied { .. }
            ) {
                return Ok(());
            }
        }
        Err(TraefikReconcileError::ConflictExhausted)
    }

    async fn read(
        &self,
        store: &FencedStore,
    ) -> Result<Option<StoredService>, TraefikReconcileError> {
        let Some(stored) = store.get(&self.service_key).await? else {
            return Ok(None);
        };
        let resource = serde_json::from_slice(&stored.value).map_err(|error| {
            TraefikReconcileError::Malformed {
                key: self.service_key.to_string(),
                message: error.to_string(),
            }
        })?;
        Ok(Some(StoredService {
            resource,
            version: stored.version,
        }))
    }
}

struct StoredService {
    resource: Service,
    version: Version,
}

fn converge(
    current: Option<StoredService>,
    desired: Option<&Service>,
    now: Timestamp,
) -> Result<Option<Service>, TraefikReconcileError> {
    match (current, desired) {
        (None, Some(desired)) => Ok(Some(desired.clone())),
        (None, None) => Ok(None),
        (Some(current), None) => {
            if !is_managed(&current.resource.meta.annotations)
                || current.resource.meta.deletion_timestamp.is_some()
            {
                return Ok(None);
            }
            let mut replacement = current.resource;
            replacement.meta.revision = current.version.resource_revision();
            replacement.meta.deletion_timestamp = Some(now);
            Ok(Some(replacement))
        }
        (Some(current), Some(desired)) => {
            if !is_managed(&current.resource.meta.annotations) {
                return Err(TraefikReconcileError::ResourceCollision);
            }
            if current.resource.meta.deletion_timestamp.is_some() {
                return Err(TraefikReconcileError::ResourceTerminating);
            }
            let mut replacement = current.resource.clone();
            replacement.meta.revision = current.version.resource_revision();
            replacement.meta.labels = desired.meta.labels.clone();
            replacement.meta.annotations = desired.meta.annotations.clone();
            replacement.meta.owner_refs = desired.meta.owner_refs.clone();
            if replacement.spec != desired.spec {
                replacement.meta.generation = Generation(
                    replacement
                        .meta
                        .generation
                        .0
                        .checked_add(1)
                        .ok_or(TraefikReconcileError::GenerationExhausted)?,
                );
                replacement.spec = desired.spec.clone();
            }
            Ok((replacement != current.resource).then_some(replacement))
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum TraefikReconcileError {
    #[error(transparent)]
    Controller(#[from] ControllerError),
    #[error(transparent)]
    Store(#[from] kernel_store::StoreError),
    #[error(transparent)]
    Resource(#[from] TraefikResourceError),
    #[error(transparent)]
    Identifier(#[from] kernel_api::InvalidIdentifier),
    #[error("stored Traefik Service `{key}` is malformed: {message}")]
    Malformed { key: String, message: String },
    #[error("stored Traefik Service identity is `{actual}`, expected `{expected}`")]
    IdentityMismatch { expected: String, actual: String },
    #[error("built-in Traefik Service is owned by another writer")]
    ResourceCollision,
    #[error("built-in Traefik Service is still terminating")]
    ResourceTerminating,
    #[error("built-in Traefik Service exhausted its generation")]
    GenerationExhausted,
    #[error("failed to serialize built-in Traefik Service: {message}")]
    Serialize { message: String },
    #[error("built-in Traefik Service changed during every reconciliation attempt")]
    ConflictExhausted,
}
