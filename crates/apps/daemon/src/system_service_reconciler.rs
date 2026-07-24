use kernel_api::{AnnotationKey, Generation, Service, ServiceId, Timestamp};
use kernel_controller::{ControllerError, FencedStore};
use kernel_store::{
    Compare, ExpectedVersion, Keyspace, Mutation, StoreKey, Transaction, TransactionOutcome,
    Version,
};

const MANAGED_ANNOTATION: &str = "system.maestro.dev/owner";
const MAXIMUM_CONFLICT_RETRIES: usize = 8;

/// Fenced create, update, and retirement for one daemon-owned ordinary Service.
pub(crate) struct SystemServiceReconciler {
    name: &'static str,
    owner: &'static str,
    service_id: ServiceId,
    service_key: StoreKey,
    desired: Option<Service>,
}

impl SystemServiceReconciler {
    pub(crate) fn new(
        cluster_id: &kernel_api::ClusterId,
        name: &'static str,
        service_id: &str,
        owner: &'static str,
        desired: Option<Service>,
    ) -> Result<Self, SystemServiceReconcileError> {
        let service_id = ServiceId::new(service_id)?;
        if let Some(desired) = &desired
            && desired.meta.id != service_id
        {
            return Err(SystemServiceReconcileError::DesiredIdentityMismatch {
                name,
                expected: service_id.to_string(),
                actual: desired.meta.id.to_string(),
            });
        }
        let service_kind = kernel_api::ResourceKind::new("Service")?;
        let service_key =
            Keyspace::new(cluster_id).resource(&service_kind, &service_id.clone().into());
        Ok(Self {
            name,
            owner,
            service_id,
            service_key,
            desired,
        })
    }

    pub(crate) async fn reconcile(
        &self,
        store: &FencedStore,
        now: Timestamp,
    ) -> Result<(), SystemServiceReconcileError> {
        for _ in 0..MAXIMUM_CONFLICT_RETRIES {
            let current = self.read(store).await?;
            if let Some(current) = current.as_ref()
                && current.resource.meta.id != self.service_id
            {
                return Err(SystemServiceReconcileError::StoredIdentityMismatch {
                    name: self.name,
                    expected: self.service_id.to_string(),
                    actual: current.resource.meta.id.to_string(),
                });
            }
            let expected = current
                .as_ref()
                .map_or(ExpectedVersion::Missing, |current| {
                    ExpectedVersion::Exact(current.version)
                });
            let replacement = converge(current, self.desired.as_ref(), self.name, self.owner, now)?;
            let Some(replacement) = replacement else {
                store.verify_leadership().await?;
                return Ok(());
            };
            let value = serde_json::to_vec(&replacement).map_err(|error| {
                SystemServiceReconcileError::Serialize {
                    name: self.name,
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
        Err(SystemServiceReconcileError::ConflictExhausted { name: self.name })
    }

    async fn read(
        &self,
        store: &FencedStore,
    ) -> Result<Option<StoredService>, SystemServiceReconcileError> {
        let Some(stored) = store.get(&self.service_key).await? else {
            return Ok(None);
        };
        let resource = serde_json::from_slice(&stored.value).map_err(|error| {
            SystemServiceReconcileError::Malformed {
                name: self.name,
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
    name: &'static str,
    owner: &str,
    now: Timestamp,
) -> Result<Option<Service>, SystemServiceReconcileError> {
    match (current, desired) {
        (None, Some(desired)) => Ok(Some(desired.clone())),
        (None, None) => Ok(None),
        (Some(current), None) => {
            if !is_managed(&current.resource, owner)
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
            if !is_managed(&current.resource, owner) {
                return Err(SystemServiceReconcileError::ResourceCollision { name });
            }
            if current.resource.meta.deletion_timestamp.is_some() {
                return Err(SystemServiceReconcileError::ResourceTerminating { name });
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
                        .ok_or(SystemServiceReconcileError::GenerationExhausted { name })?,
                );
                replacement.spec = desired.spec.clone();
            }
            Ok((replacement != current.resource).then_some(replacement))
        }
    }
}

fn is_managed(service: &Service, owner: &str) -> bool {
    service
        .meta
        .annotations
        .get(&AnnotationKey(MANAGED_ANNOTATION.to_owned()))
        .is_some_and(|value| value == owner)
}

/// Invalid state while reconciling one daemon-owned system Service.
#[derive(Debug, thiserror::Error)]
pub(crate) enum SystemServiceReconcileError {
    #[error(transparent)]
    Controller(#[from] ControllerError),
    #[error(transparent)]
    Store(#[from] kernel_store::StoreError),
    #[error(transparent)]
    Identifier(#[from] kernel_api::InvalidIdentifier),
    #[error("desired {name} Service identity is `{actual}`, expected `{expected}`")]
    DesiredIdentityMismatch {
        name: &'static str,
        expected: String,
        actual: String,
    },
    #[error("stored {name} Service `{key}` is malformed: {message}")]
    Malformed {
        name: &'static str,
        key: String,
        message: String,
    },
    #[error("stored {name} Service identity is `{actual}`, expected `{expected}`")]
    StoredIdentityMismatch {
        name: &'static str,
        expected: String,
        actual: String,
    },
    #[error("built-in {name} Service is owned by another writer")]
    ResourceCollision { name: &'static str },
    #[error("built-in {name} Service is still terminating")]
    ResourceTerminating { name: &'static str },
    #[error("built-in {name} Service exhausted its generation")]
    GenerationExhausted { name: &'static str },
    #[error("failed to serialize built-in {name} Service: {message}")]
    Serialize { name: &'static str, message: String },
    #[error("built-in {name} Service changed during every reconciliation attempt")]
    ConflictExhausted { name: &'static str },
}
