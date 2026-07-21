use std::fmt::Display;

use kernel_api::{
    BuiltinKind, Deployment, Node, Preview, ResourceKind, ResourceName, UpgradeRun, WebhookEvent,
    WebhookNodeAvailability, WebhookObservation, WebhookObservedState,
};
use kernel_controller::{ControllerError, FencedStore};
use kernel_store::{Compare, ExpectedVersion, Keyspace, StoredValue};
use serde::de::DeserializeOwned;

const MAX_RESOURCE_BYTES: usize = 1024 * 1_024;

pub(crate) struct CurrentObservation {
    pub(crate) observation: WebhookObservation,
    pub(crate) source_compare: Compare,
}

pub(crate) struct WebhookSnapshot {
    pub(crate) current: Vec<CurrentObservation>,
}

impl WebhookSnapshot {
    pub(crate) async fn load(
        store: &FencedStore,
        keys: &Keyspace,
        events: &[WebhookEvent],
    ) -> Result<Self, WebhookSnapshotError> {
        let mut current = Vec::new();
        if events.contains(&WebhookEvent::DeploymentTransition) {
            for (stored, deployment) in
                resources::<Deployment>(store, keys, BuiltinKind::Deployment).await?
            {
                current.push(observation(
                    &stored,
                    deployment.meta.id.into(),
                    WebhookObservedState::DeploymentTransition(deployment.status.phase),
                ));
            }
        }
        if events.contains(&WebhookEvent::NodeAvailability) {
            for (stored, node) in resources::<Node>(store, keys, BuiltinKind::Node).await? {
                let liveness_key = keys.node_liveness(&node.meta.id);
                let liveness = store.get(&liveness_key).await?;
                let (availability, revision, expected) = match liveness {
                    Some(liveness) => (
                        WebhookNodeAvailability::Available,
                        liveness.version.resource_revision(),
                        ExpectedVersion::Exact(liveness.version),
                    ),
                    None => (
                        WebhookNodeAvailability::Unavailable,
                        stored.version.resource_revision(),
                        ExpectedVersion::Missing,
                    ),
                };
                current.push(CurrentObservation {
                    observation: WebhookObservation {
                        resource_id: node.meta.id.into(),
                        state: WebhookObservedState::NodeAvailability(availability),
                        resource_revision: revision,
                    },
                    source_compare: Compare {
                        key: liveness_key,
                        expected,
                    },
                });
            }
        }
        if events.contains(&WebhookEvent::PreviewTransition) {
            for (stored, preview) in resources::<Preview>(store, keys, BuiltinKind::Preview).await?
            {
                current.push(observation(
                    &stored,
                    preview.meta.id.into(),
                    WebhookObservedState::PreviewTransition(preview.status.phase),
                ));
            }
        }
        if events.contains(&WebhookEvent::UpgradeTransition) {
            for (stored, upgrade) in
                resources::<UpgradeRun>(store, keys, BuiltinKind::UpgradeRun).await?
            {
                current.push(observation(
                    &stored,
                    upgrade.meta.id.into(),
                    WebhookObservedState::UpgradeTransition(upgrade.status.phase),
                ));
            }
        }
        current.sort_by(|left, right| {
            event_rank(left.observation.state.event())
                .cmp(&event_rank(right.observation.state.event()))
                .then_with(|| {
                    left.observation
                        .resource_id
                        .cmp(&right.observation.resource_id)
                })
        });
        Ok(Self { current })
    }

    pub(crate) fn baseline(&self) -> Vec<WebhookObservation> {
        self.current
            .iter()
            .map(|current| current.observation.clone())
            .collect()
    }

    pub(crate) fn first_transition<'a>(
        &'a self,
        previous: &[WebhookObservation],
    ) -> Option<(&'a CurrentObservation, Option<WebhookObservedState>)> {
        self.current.iter().find_map(|current| {
            let previous = previous
                .iter()
                .find(|previous| same_resource(previous, &current.observation));
            match previous {
                Some(previous) if previous.state == current.observation.state => None,
                Some(previous) => Some((current, Some(previous.state))),
                None => Some((current, None)),
            }
        })
    }

    pub(crate) fn retain_present(&self, observations: &mut Vec<WebhookObservation>) -> bool {
        let before = observations.len();
        observations.retain(|previous| {
            self.current
                .iter()
                .any(|current| same_resource(previous, &current.observation))
        });
        observations.len() != before
    }

    pub(crate) fn acknowledge(
        &self,
        previous: &[WebhookObservation],
        acknowledged: &WebhookObservation,
    ) -> Vec<WebhookObservation> {
        self.current
            .iter()
            .filter_map(|current| {
                if same_resource(&current.observation, acknowledged) {
                    Some(acknowledged.clone())
                } else {
                    previous
                        .iter()
                        .find(|previous| same_resource(previous, &current.observation))
                        .cloned()
                }
            })
            .collect()
    }
}

fn observation(
    stored: &StoredValue,
    resource_id: ResourceName,
    state: WebhookObservedState,
) -> CurrentObservation {
    CurrentObservation {
        observation: WebhookObservation {
            resource_id,
            state,
            resource_revision: stored.version.resource_revision(),
        },
        source_compare: Compare {
            key: stored.key.clone(),
            expected: ExpectedVersion::Exact(stored.version),
        },
    }
}

async fn resources<Resource>(
    store: &FencedStore,
    keys: &Keyspace,
    kind: BuiltinKind,
) -> Result<Vec<(StoredValue, Resource)>, WebhookSnapshotError>
where
    Resource: DeserializeOwned + ResourceIdentity,
{
    let resource_kind = ResourceKind::new(kind.as_str())?;
    store
        .list(&keys.resource_kind(&resource_kind))
        .await?
        .values
        .into_iter()
        .map(|stored| {
            if stored.value.len() > MAX_RESOURCE_BYTES {
                return Err(WebhookSnapshotError::MalformedResource {
                    kind,
                    key: stored.key.to_string(),
                    message: format!("document exceeds {MAX_RESOURCE_BYTES} bytes"),
                });
            }
            let resource: Resource = serde_json::from_slice(&stored.value).map_err(|error| {
                WebhookSnapshotError::MalformedResource {
                    kind,
                    key: stored.key.to_string(),
                    message: error.to_string(),
                }
            })?;
            let expected = keys.resource(&resource_kind, &resource.resource_name());
            if expected != stored.key {
                return Err(WebhookSnapshotError::IdentityMismatch {
                    kind,
                    key: stored.key.to_string(),
                    resource_id: resource.resource_name().to_string(),
                });
            }
            Ok((stored, resource))
        })
        .collect()
}

trait ResourceIdentity {
    fn resource_name(&self) -> ResourceName;
}

impl<Id, Spec, Status> ResourceIdentity for kernel_api::Object<Id, Spec, Status>
where
    Id: Clone + Into<ResourceName> + Display,
{
    fn resource_name(&self) -> ResourceName {
        self.meta.id.clone().into()
    }
}

fn same_resource(left: &WebhookObservation, right: &WebhookObservation) -> bool {
    left.state.event() == right.state.event() && left.resource_id == right.resource_id
}

const fn event_rank(event: WebhookEvent) -> u8 {
    match event {
        WebhookEvent::DeploymentTransition => 0,
        WebhookEvent::NodeAvailability => 1,
        WebhookEvent::PreviewTransition => 2,
        WebhookEvent::UpgradeTransition => 3,
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum WebhookSnapshotError {
    #[error(transparent)]
    InvalidIdentifier(#[from] kernel_api::InvalidIdentifier),
    #[error(transparent)]
    Controller(#[from] ControllerError),
    #[error("malformed {kind} resource at `{key}`: {message}")]
    MalformedResource {
        kind: BuiltinKind,
        key: String,
        message: String,
    },
    #[error("{kind} `{resource_id}` does not match store key `{key}`")]
    IdentityMismatch {
        kind: BuiltinKind,
        key: String,
        resource_id: String,
    },
}
