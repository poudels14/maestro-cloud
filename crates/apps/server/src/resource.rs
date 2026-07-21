use std::fmt::Display;

use kernel_api::{BuiltinKind, Object, ResourceName};
use kernel_store::{Keyspace, StoredValue};
use serde::de::DeserializeOwned;

use crate::{ApiError, AppState};

const MAX_RESOURCE_BYTES: usize = 1024 * 1_024;

pub(crate) async fn list<Id, Spec, Status>(
    state: &AppState,
    kind: BuiltinKind,
) -> Result<Vec<Object<Id, Spec, Status>>, ApiError>
where
    Id: Clone + Display + Into<ResourceName> + DeserializeOwned,
    Spec: DeserializeOwned,
    Status: DeserializeOwned,
{
    let keys = Keyspace::new(&state.cluster_id);
    let resource_kind = kernel_api::ResourceKind::new(kind.as_str())
        .map_err(|error| ApiError::internal(error.to_string()))?;
    let listed = state
        .store
        .list(&keys.resource_kind(&resource_kind))
        .await
        .map_err(|error| ApiError::internal(format!("failed to list {kind} resources: {error}")))?;
    decode_list(&listed.values, &keys, &resource_kind, kind)
}

pub(crate) fn decode_list<Id, Spec, Status>(
    values: &[StoredValue],
    keys: &Keyspace,
    resource_kind: &kernel_api::ResourceKind,
    kind: BuiltinKind,
) -> Result<Vec<Object<Id, Spec, Status>>, ApiError>
where
    Id: Clone + Display + Into<ResourceName> + DeserializeOwned,
    Spec: DeserializeOwned,
    Status: DeserializeOwned,
{
    let prefix = keys.resource_kind(resource_kind);
    values
        .iter()
        .filter(|stored| stored.key.as_str().starts_with(prefix.as_str()))
        .map(|stored| decode(stored, keys, resource_kind, kind))
        .collect()
}

pub(crate) async fn get<Id, Spec, Status>(
    state: &AppState,
    kind: BuiltinKind,
    id: Id,
) -> Result<Object<Id, Spec, Status>, ApiError>
where
    Id: Clone + Display + Into<ResourceName> + DeserializeOwned,
    Spec: DeserializeOwned,
    Status: DeserializeOwned,
{
    let keys = Keyspace::new(&state.cluster_id);
    let resource_kind = kernel_api::ResourceKind::new(kind.as_str())
        .map_err(|error| ApiError::internal(error.to_string()))?;
    let key = keys.resource(&resource_kind, &id.clone().into());
    let stored = state
        .store
        .get(&key)
        .await
        .map_err(|error| ApiError::internal(format!("failed to read {kind} `{id}`: {error}")))?
        .ok_or_else(|| ApiError::not_found(format!("{kind} `{id}` does not exist")))?;
    decode(&stored, &keys, &resource_kind, kind)
}

pub(crate) fn decode<Id, Spec, Status>(
    stored: &StoredValue,
    keys: &Keyspace,
    resource_kind: &kernel_api::ResourceKind,
    kind: BuiltinKind,
) -> Result<Object<Id, Spec, Status>, ApiError>
where
    Id: Clone + Display + Into<ResourceName> + DeserializeOwned,
    Spec: DeserializeOwned,
    Status: DeserializeOwned,
{
    if stored.value.len() > MAX_RESOURCE_BYTES {
        return Err(ApiError::internal(format!(
            "stored {kind} document exceeds {MAX_RESOURCE_BYTES} bytes"
        )));
    }
    let mut resource: Object<Id, Spec, Status> = serde_json::from_slice(&stored.value)
        .map_err(|_| ApiError::internal(format!("stored {kind} document is malformed")))?;
    let expected_key = keys.resource(resource_kind, &resource.meta.id.clone().into());
    if expected_key != stored.key {
        return Err(ApiError::internal(format!(
            "stored {kind} identity does not match its canonical key"
        )));
    }
    resource.meta.revision = stored.version.resource_revision();
    Ok(resource)
}
