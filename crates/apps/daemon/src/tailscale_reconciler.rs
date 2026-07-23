use std::fmt::Display;

use kernel_api::{
    FirewallPolicy, FirewallPolicyId, Generation, Object, Service, ServiceId, Timestamp,
};
use kernel_controller::{ControllerError, FencedStore};
use kernel_store::{
    Compare, ExpectedVersion, Keyspace, Mutation, StoreKey, Transaction, TransactionOutcome,
    Version,
};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::tailscale_resources::TailscaleSystemResources;
use crate::tailscale_resources::{is_managed, resource_ids};

const MAXIMUM_CONFLICT_RETRIES: usize = 8;

pub(crate) struct TailscaleResourceReconciler {
    service_id: ServiceId,
    policy_id: FirewallPolicyId,
    service_key: StoreKey,
    policy_key: StoreKey,
    desired: Option<TailscaleSystemResources>,
}

impl TailscaleResourceReconciler {
    pub(crate) fn new(
        cluster_id: &kernel_api::ClusterId,
        desired: Option<TailscaleSystemResources>,
    ) -> Result<Self, TailscaleReconcileError> {
        let keyspace = Keyspace::new(cluster_id);
        let (service_id, policy_id) = resource_ids()?;
        let service_kind = kernel_api::ResourceKind::new("Service")?;
        let policy_kind = kernel_api::ResourceKind::new("FirewallPolicy")?;
        Ok(Self {
            service_key: keyspace.resource(&service_kind, &service_id.clone().into()),
            policy_key: keyspace.resource(&policy_kind, &policy_id.clone().into()),
            service_id,
            policy_id,
            desired,
        })
    }

    pub(crate) async fn reconcile(
        &self,
        store: &FencedStore,
        now: Timestamp,
    ) -> Result<(), TailscaleReconcileError> {
        for _ in 0..MAXIMUM_CONFLICT_RETRIES {
            let current_service =
                read_resource::<Service>(store, &self.service_key, "Service").await?;
            let current_policy =
                read_resource::<FirewallPolicy>(store, &self.policy_key, "FirewallPolicy").await?;
            validate_id(current_service.as_ref(), &self.service_id, "Service")?;
            validate_id(current_policy.as_ref(), &self.policy_id, "FirewallPolicy")?;

            let compares = vec![
                compare(&self.service_key, current_service.as_ref()),
                compare(&self.policy_key, current_policy.as_ref()),
            ];
            let service_write = converge(
                current_service,
                self.desired.as_ref().map(|desired| &desired.service),
                now,
                "Service",
            )?;
            let policy_write = converge(
                current_policy,
                self.desired
                    .as_ref()
                    .map(|desired| &desired.firewall_policy),
                now,
                "FirewallPolicy",
            )?;
            let mut mutations = Vec::new();
            if let Some(service) = service_write {
                mutations.push(put(&self.service_key, &service, "Service")?);
            }
            if let Some(policy) = policy_write {
                mutations.push(put(&self.policy_key, &policy, "FirewallPolicy")?);
            }
            if mutations.is_empty() {
                store.verify_leadership().await?;
                return Ok(());
            }
            if matches!(
                store
                    .txn(Transaction {
                        compares,
                        mutations
                    })
                    .await?,
                TransactionOutcome::Applied { .. }
            ) {
                return Ok(());
            }
        }
        Err(TailscaleReconcileError::ConflictExhausted)
    }
}

struct StoredResource<Resource> {
    resource: Resource,
    version: Version,
}

async fn read_resource<Resource>(
    store: &FencedStore,
    key: &StoreKey,
    kind: &'static str,
) -> Result<Option<StoredResource<Resource>>, TailscaleReconcileError>
where
    Resource: DeserializeOwned,
{
    let Some(stored) = store.get(key).await? else {
        return Ok(None);
    };
    let resource = serde_json::from_slice(&stored.value).map_err(|error| {
        TailscaleReconcileError::Malformed {
            kind,
            key: key.to_string(),
            message: error.to_string(),
        }
    })?;
    Ok(Some(StoredResource {
        resource,
        version: stored.version,
    }))
}

fn validate_id<Id, Spec, Status>(
    current: Option<&StoredResource<Object<Id, Spec, Status>>>,
    expected: &Id,
    kind: &'static str,
) -> Result<(), TailscaleReconcileError>
where
    Id: PartialEq + Display,
{
    if let Some(current) = current
        && current.resource.meta.id != *expected
    {
        return Err(TailscaleReconcileError::IdentityMismatch {
            kind,
            expected: expected.to_string(),
            actual: current.resource.meta.id.to_string(),
        });
    }
    Ok(())
}

fn compare<Resource>(key: &StoreKey, current: Option<&StoredResource<Resource>>) -> Compare {
    Compare {
        key: key.clone(),
        expected: current.map_or(ExpectedVersion::Missing, |current| {
            ExpectedVersion::Exact(current.version)
        }),
    }
}

fn converge<Id, Spec, Status>(
    current: Option<StoredResource<Object<Id, Spec, Status>>>,
    desired: Option<&Object<Id, Spec, Status>>,
    now: Timestamp,
    kind: &'static str,
) -> Result<Option<Object<Id, Spec, Status>>, TailscaleReconcileError>
where
    Id: Clone + Display + PartialEq,
    Spec: Clone + PartialEq,
    Status: Clone + PartialEq,
{
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
                return Err(TailscaleReconcileError::ResourceCollision {
                    kind,
                    resource_id: current.resource.meta.id.to_string(),
                });
            }
            if current.resource.meta.deletion_timestamp.is_some() {
                return Err(TailscaleReconcileError::ResourceTerminating {
                    kind,
                    resource_id: current.resource.meta.id.to_string(),
                });
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
                        .ok_or_else(|| TailscaleReconcileError::GenerationExhausted {
                            kind,
                            resource_id: replacement.meta.id.to_string(),
                        })?,
                );
                replacement.spec = desired.spec.clone();
            }
            Ok((replacement != current.resource).then_some(replacement))
        }
    }
}

fn put<Resource>(
    key: &StoreKey,
    resource: &Resource,
    kind: &'static str,
) -> Result<Mutation, TailscaleReconcileError>
where
    Resource: Serialize,
{
    let value =
        serde_json::to_vec(resource).map_err(|error| TailscaleReconcileError::Serialize {
            kind,
            message: error.to_string(),
        })?;
    Ok(Mutation::Put {
        key: key.clone(),
        value,
        session: None,
    })
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum TailscaleReconcileError {
    #[error(transparent)]
    Controller(#[from] ControllerError),
    #[error(transparent)]
    Identifier(#[from] kernel_api::InvalidIdentifier),
    #[error("stored {kind} `{key}` is malformed: {message}")]
    Malformed {
        kind: &'static str,
        key: String,
        message: String,
    },
    #[error("stored {kind} identity is `{actual}`, expected `{expected}`")]
    IdentityMismatch {
        kind: &'static str,
        expected: String,
        actual: String,
    },
    #[error("built-in Tailscale {kind} `{resource_id}` is owned by another writer")]
    ResourceCollision {
        kind: &'static str,
        resource_id: String,
    },
    #[error("built-in Tailscale {kind} `{resource_id}` is still terminating")]
    ResourceTerminating {
        kind: &'static str,
        resource_id: String,
    },
    #[error("built-in Tailscale {kind} `{resource_id}` exhausted its generation")]
    GenerationExhausted {
        kind: &'static str,
        resource_id: String,
    },
    #[error("failed to serialize built-in Tailscale {kind}: {message}")]
    Serialize { kind: &'static str, message: String },
    #[error("built-in Tailscale resources changed during every reconciliation attempt")]
    ConflictExhausted,
}
