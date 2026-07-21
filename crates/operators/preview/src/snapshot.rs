use std::collections::BTreeMap;
use std::fmt::Display;

use kernel_api::{
    IngressRoute, IngressRouteId, IngressRouteSpec, IngressRouteStatus, Object, Preview, Service,
    ServiceId, ServiceSpec, ServiceStatus,
};
use kernel_controller::FencedStore;
use kernel_store::{Keyspace, StorePrefix, StoredValue};
use serde::de::DeserializeOwned;

use crate::PreviewError;
use crate::resource::owned_by_preview;

pub(crate) struct PreviewSnapshot {
    pub(crate) base: Option<StoredResource<Service>>,
    pub(crate) child: Option<StoredResource<Service>>,
    pub(crate) base_routes: Vec<StoredResource<IngressRoute>>,
    pub(crate) routes: BTreeMap<IngressRouteId, StoredResource<IngressRoute>>,
    pub(crate) child_routes: BTreeMap<IngressRouteId, StoredResource<IngressRoute>>,
}

impl PreviewSnapshot {
    pub(crate) async fn load(
        store: &FencedStore,
        keyspace: &Keyspace,
        preview: &Preview,
    ) -> Result<Self, PreviewError> {
        let services = decode_prefix::<ServiceId, ServiceSpec, ServiceStatus>(
            store,
            keyspace.resource_kind(&kind("Service")?),
            "Service",
        )
        .await?;
        let routes = decode_prefix::<IngressRouteId, IngressRouteSpec, IngressRouteStatus>(
            store,
            keyspace.resource_kind(&kind("IngressRoute")?),
            "IngressRoute",
        )
        .await?;
        let base = services.get(&preview.spec.base_service_id).cloned();
        let child = services.get(&preview.spec.service_id).cloned();
        let base_routes = routes
            .values()
            .filter(|route| route.resource.spec.service_id == preview.spec.base_service_id)
            .cloned()
            .collect();
        let child_routes = routes
            .iter()
            .filter(|(_, route)| owned_by_preview(preview, &route.resource.meta))
            .map(|(id, route)| (id.clone(), route.clone()))
            .collect();
        Ok(Self {
            base,
            child,
            base_routes,
            routes,
            child_routes,
        })
    }
}

#[derive(Clone)]
pub(crate) struct StoredResource<Resource> {
    pub(crate) resource: Resource,
    pub(crate) stored: StoredValue,
}

pub(crate) async fn decode_prefix<Id, Spec, Status>(
    store: &FencedStore,
    prefix: StorePrefix,
    kind: &'static str,
) -> Result<BTreeMap<Id, StoredResource<Object<Id, Spec, Status>>>, PreviewError>
where
    Id: Clone + Ord + Display + DeserializeOwned,
    Spec: DeserializeOwned,
    Status: DeserializeOwned,
{
    let listed = store.list(&prefix).await?;
    let mut resources = BTreeMap::new();
    for stored in listed.values {
        let mut resource: Object<Id, Spec, Status> = serde_json::from_slice(&stored.value)
            .map_err(|error| PreviewError::MalformedResource {
                kind,
                key: stored.key.to_string(),
                message: error.to_string(),
            })?;
        resource.meta.revision = stored.version.resource_revision();
        let id = resource.meta.id.clone();
        if resources
            .insert(id.clone(), StoredResource { resource, stored })
            .is_some()
        {
            return Err(PreviewError::DuplicateResource {
                kind,
                resource_id: id.to_string(),
            });
        }
    }
    Ok(resources)
}

fn kind(value: &str) -> Result<kernel_api::ResourceKind, PreviewError> {
    kernel_api::ResourceKind::new(value).map_err(Into::into)
}
