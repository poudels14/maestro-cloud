use kernel_api::{IngressRoute, Preview, PreviewId, ResourceKind, ResourceName, Service};
use kernel_controller::FencedStore;
use kernel_store::{
    Compare, ExpectedVersion, Keyspace, Mutation, StoreKey, StoredValue, Transaction,
    TransactionOutcome, Version,
};

use crate::PreviewError;
use crate::snapshot::{PreviewSnapshot, StoredResource};

pub(crate) struct PreviewWriter {
    keyspace: Keyspace,
    preview_kind: ResourceKind,
    service_kind: ResourceKind,
    route_kind: ResourceKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PreviewWriteOutcome {
    Noop,
    Applied,
    Conflict,
}

impl PreviewWriter {
    pub(crate) fn new(
        cluster_id: &kernel_api::ClusterId,
    ) -> Result<Self, kernel_api::InvalidIdentifier> {
        Ok(Self {
            keyspace: Keyspace::new(cluster_id),
            preview_kind: ResourceKind::new("Preview")?,
            service_kind: ResourceKind::new("Service")?,
            route_kind: ResourceKind::new("IngressRoute")?,
        })
    }

    pub(crate) async fn apply(
        &self,
        store: &FencedStore,
        observed_preview: Version,
        preview_id: &PreviewId,
        snapshot: &PreviewSnapshot,
        preview: Option<&Preview>,
        service: Option<&Service>,
        routes: &[IngressRoute],
        delete_routes: &[StoredResource<IngressRoute>],
    ) -> Result<PreviewWriteOutcome, PreviewError> {
        let preview_key = self
            .keyspace
            .resource(&self.preview_kind, &ResourceName::from(preview_id.clone()));
        let mut compares = vec![Compare {
            key: preview_key.clone(),
            expected: ExpectedVersion::Exact(observed_preview),
        }];
        let mut mutations = Vec::new();
        if let Some(base) = &snapshot.base {
            compare(&mut compares, &base.stored);
        }
        for base_route in &snapshot.base_routes {
            compare(&mut compares, &base_route.stored);
        }
        if let Some(service) = service {
            let service_key = self.keyspace.resource(
                &self.service_kind,
                &ResourceName::from(service.meta.id.clone()),
            );
            replace_optional(
                &mut compares,
                &mut mutations,
                snapshot.child.as_ref(),
                Some(service),
                service_key,
            )?;
        }
        for route in routes {
            let current = snapshot.child_routes.get(&route.meta.id);
            let key = self
                .keyspace
                .resource(&self.route_kind, &ResourceName::from(route.meta.id.clone()));
            replace_optional(&mut compares, &mut mutations, current, Some(route), key)?;
        }
        for route in delete_routes {
            compare(&mut compares, &route.stored);
            mutations.push(Mutation::Delete {
                key: route.stored.key.clone(),
            });
        }
        if let Some(preview) = preview {
            let stored =
                store
                    .get(&preview_key)
                    .await?
                    .ok_or_else(|| PreviewError::InvalidDefinition {
                        message: format!(
                            "preview `{preview_id}` disappeared during reconciliation"
                        ),
                    })?;
            let mut current: Preview = serde_json::from_slice(&stored.value).map_err(|error| {
                PreviewError::MalformedResource {
                    kind: "Preview",
                    key: stored.key.to_string(),
                    message: error.to_string(),
                }
            })?;
            current.meta.revision = preview.meta.revision;
            if current != *preview {
                mutations.push(Mutation::Put {
                    key: preview_key,
                    value: serialize(preview)?,
                    session: None,
                });
            }
        }
        if mutations.is_empty() {
            return Ok(PreviewWriteOutcome::Noop);
        }
        let outcome = store
            .txn(Transaction {
                compares,
                mutations,
            })
            .await?;
        Ok(match outcome {
            TransactionOutcome::Applied { .. } => PreviewWriteOutcome::Applied,
            TransactionOutcome::Conflict => PreviewWriteOutcome::Conflict,
        })
    }
}

fn replace_optional<Resource: serde::Serialize>(
    compares: &mut Vec<Compare>,
    mutations: &mut Vec<Mutation>,
    current: Option<&StoredResource<Resource>>,
    desired: Option<&Resource>,
    key: StoreKey,
) -> Result<(), PreviewError> {
    let Some(desired) = desired else {
        return Ok(());
    };
    let key = current.map_or(key, |current| current.stored.key.clone());
    compares.push(Compare {
        key: key.clone(),
        expected: current.map_or(ExpectedVersion::Missing, |current| {
            ExpectedVersion::Exact(current.stored.version)
        }),
    });
    let encoded = serialize(desired)?;
    let changed = match current {
        Some(current) => serialize(&current.resource)? != encoded,
        None => true,
    };
    if changed {
        mutations.push(Mutation::Put {
            key,
            value: encoded,
            session: None,
        });
    }
    Ok(())
}

fn compare(compares: &mut Vec<Compare>, stored: &StoredValue) {
    compares.push(Compare {
        key: stored.key.clone(),
        expected: ExpectedVersion::Exact(stored.version),
    });
}

fn serialize(value: &impl serde::Serialize) -> Result<Vec<u8>, PreviewError> {
    serde_json::to_vec(value).map_err(|error| PreviewError::Serialize {
        message: error.to_string(),
    })
}
