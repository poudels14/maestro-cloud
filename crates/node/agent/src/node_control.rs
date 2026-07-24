use std::sync::Arc;

use async_trait::async_trait;
use kernel_api::{
    BuiltinResource, ClusterId, RequestId, ResourceKind, ResourceName, ResourceRevision,
};
use kernel_controller::{ControllerError, DedupOutcome, RequestDeduplicator, RequestFingerprint};
use kernel_store::{
    Compare, ExpectedVersion, Keyspace, Mutation, MutationResult as StoreMutationResult, Store,
    StoreError, StoredValue, Transaction,
};
use node_fabric::WorkloadClaims;
use node_fabric::proto::{
    MutationDisposition, MutationOperation, MutationResult, ResourceMutation,
};
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use tonic::Status;

use crate::{NodeControlHandler, StatusClock};

const MAX_RESOURCE_BYTES: usize = 1024 * 1024;

/// Store-backed privileged node API mutations with atomic request deduplication.
pub struct StoreNodeControlHandler {
    store: Arc<dyn Store>,
    requests: RequestDeduplicator,
    keys: Keyspace,
    clock: Arc<dyn StatusClock>,
}

impl StoreNodeControlHandler {
    /// Binds authenticated workload mutations to one cluster keyspace.
    pub fn new(store: Arc<dyn Store>, cluster_id: &ClusterId, clock: Arc<dyn StatusClock>) -> Self {
        Self {
            requests: RequestDeduplicator::new(store.clone()),
            store,
            keys: Keyspace::new(cluster_id),
            clock,
        }
    }
}

#[async_trait]
impl NodeControlHandler for StoreNodeControlHandler {
    async fn mutate(
        &self,
        claims: WorkloadClaims,
        mutation: ResourceMutation,
    ) -> Result<MutationResult, Status> {
        let request_id =
            RequestId::new(&mutation.request_id).map_err(|error| invalid(error.to_string()))?;
        let kind = ResourceKind::new(&mutation.kind).map_err(|error| invalid(error.to_string()))?;
        let resource_name =
            ResourceName::new(&mutation.resource_id).map_err(|error| invalid(error.to_string()))?;
        let operation = MutationOperation::try_from(mutation.operation)
            .map_err(|_| invalid("node control mutation operation is unknown"))?;
        let key = self.keys.resource(&kind, &resource_name);
        let claim_key = self.keys.request_claim(&request_id);
        let fingerprint = fingerprint(&claims, &mutation);
        if self
            .requests
            .replay(&claim_key, fingerprint)
            .await
            .map_err(controller_status)?
            .is_some()
        {
            return Ok(MutationResult {
                disposition: MutationDisposition::Duplicate.into(),
                revision: observed_revision(self.store.as_ref(), &key).await?,
            });
        }
        let current = self.store.get(&key).await.map_err(store_status)?;
        let plan = match operation {
            MutationOperation::Apply => plan_apply(
                current.as_ref(),
                &kind,
                &resource_name,
                &mutation.resource_json,
            )?,
            MutationOperation::Delete => plan_delete(
                current.as_ref(),
                &kind,
                &resource_name,
                &mutation.resource_json,
                self.clock.now(),
            )?,
            MutationOperation::Unspecified => {
                return Err(invalid("node control mutation operation is required"));
            }
        };
        let response_revision = plan.current_revision();
        let outcome = self
            .requests
            .deduplicate(
                claim_key,
                fingerprint,
                Vec::new(),
                plan.transaction(key.clone()),
            )
            .await
            .map_err(controller_status)?;
        match outcome {
            DedupOutcome::Committed { results, .. } => Ok(MutationResult {
                disposition: MutationDisposition::Committed.into(),
                revision: committed_revision(&results, response_revision)?,
            }),
            DedupOutcome::Duplicate { .. } => Ok(MutationResult {
                disposition: MutationDisposition::Duplicate.into(),
                revision: observed_revision(self.store.as_ref(), &key).await?,
            }),
            DedupOutcome::MutationConflict => Err(Status::aborted(
                "resource changed during node control mutation",
            )),
        }
    }
}

struct MutationPlan {
    expected: ExpectedVersion,
    value: Option<Vec<u8>>,
    current_revision: u64,
}

impl MutationPlan {
    fn current_revision(&self) -> u64 {
        self.current_revision
    }

    fn transaction(self, key: kernel_store::StoreKey) -> Transaction {
        let mutations = self.value.map_or_else(Vec::new, |value| {
            vec![Mutation::Put {
                key: key.clone(),
                value,
                session: None,
            }]
        });
        Transaction {
            compares: vec![Compare {
                key,
                expected: self.expected,
            }],
            mutations,
        }
    }
}

fn plan_apply(
    current: Option<&StoredValue>,
    kind: &ResourceKind,
    resource_name: &ResourceName,
    resource_json: &[u8],
) -> Result<MutationPlan, Status> {
    let (value, metadata) = decode_document(kind, resource_name, resource_json)?;
    let expected = match current {
        None if metadata.revision == ResourceRevision::default() => ExpectedVersion::Missing,
        None => {
            return Err(Status::aborted(
                "resource does not exist at the requested revision",
            ));
        }
        Some(stored) if stored.version.resource_revision() == metadata.revision => {
            ExpectedVersion::Exact(stored.version)
        }
        Some(_) => {
            return Err(Status::aborted(
                "resource changed after the requested revision was observed",
            ));
        }
    };
    let current_revision = current
        .map(|stored| stored.version.resource_revision().0)
        .unwrap_or_default();
    Ok(MutationPlan {
        expected,
        value: Some(encode_value(&value)?),
        current_revision,
    })
}

fn plan_delete(
    current: Option<&StoredValue>,
    kind: &ResourceKind,
    resource_name: &ResourceName,
    resource_json: &[u8],
    deleted_at: kernel_api::Timestamp,
) -> Result<MutationPlan, Status> {
    if !resource_json.is_empty() {
        return Err(invalid(
            "delete mutations must not include a resource JSON body",
        ));
    }
    let stored = current.ok_or_else(|| Status::not_found("resource does not exist"))?;
    let (mut value, _metadata) = decode_document(kind, resource_name, &stored.value)
        .map_err(|_| Status::failed_precondition("stored resource is malformed"))?;
    let metadata = value
        .get_mut("meta")
        .and_then(Value::as_object_mut)
        .ok_or_else(|| Status::failed_precondition("stored resource metadata is malformed"))?;
    let value = if metadata
        .get("deletionTimestamp")
        .is_some_and(|timestamp| !timestamp.is_null())
    {
        None
    } else {
        metadata.insert(
            "deletionTimestamp".to_owned(),
            serde_json::to_value(deleted_at)
                .map_err(|error| Status::internal(format!("encode deletion timestamp: {error}")))?,
        );
        Some(encode_value(&value)?)
    };
    Ok(MutationPlan {
        expected: ExpectedVersion::Exact(stored.version),
        value,
        current_revision: stored.version.resource_revision().0,
    })
}

#[derive(Deserialize)]
struct ResourceDocument {
    meta: ResourceMetadata,
}

#[derive(Deserialize)]
struct ResourceMetadata {
    id: ResourceName,
    revision: ResourceRevision,
}

fn decode_document(
    kind: &ResourceKind,
    resource_name: &ResourceName,
    encoded: &[u8],
) -> Result<(Value, ResourceMetadata), Status> {
    if encoded.is_empty() {
        return Err(invalid("apply mutations require a resource JSON body"));
    }
    if encoded.len() > MAX_RESOURCE_BYTES {
        return Err(Status::resource_exhausted(format!(
            "resource JSON exceeds {MAX_RESOURCE_BYTES} bytes"
        )));
    }
    let value: Value = serde_json::from_slice(encoded)
        .map_err(|error| invalid(format!("malformed JSON: {error}")))?;
    let document: ResourceDocument = serde_json::from_value(value.clone())
        .map_err(|error| invalid(format!("resource metadata is malformed: {error}")))?;
    if &document.meta.id != resource_name {
        return Err(invalid(
            "resource JSON identity does not match the mutation envelope",
        ));
    }
    let resource = kernel_api::decode_builtin(kind, value.clone())
        .map_err(|error| invalid(error.to_string()))?;
    validate_resource(&resource)?;
    Ok((value, document.meta))
}

fn validate_resource(resource: &BuiltinResource) -> Result<(), Status> {
    match resource {
        BuiltinResource::Service(service) => service
            .spec
            .validate()
            .map_err(|error| invalid(error.to_string())),
        _ => Ok(()),
    }
}

fn encode_value(value: &Value) -> Result<Vec<u8>, Status> {
    serde_json::to_vec(value)
        .map_err(|error| Status::internal(format!("encode resource JSON: {error}")))
}

fn fingerprint(claims: &WorkloadClaims, mutation: &ResourceMutation) -> RequestFingerprint {
    let operation = mutation.operation.to_be_bytes();
    let mut digest = Sha256::new();
    for value in [
        claims.workload_id.as_str().as_bytes(),
        claims.assignment_id.as_str().as_bytes(),
        claims.node_id.as_str().as_bytes(),
        claims.service_id.as_str().as_bytes(),
        claims.deployment_id.as_str().as_bytes(),
        mutation.kind.as_bytes(),
        mutation.resource_id.as_bytes(),
        operation.as_slice(),
        mutation.resource_json.as_slice(),
    ] {
        digest.update(u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
        digest.update(value);
    }
    RequestFingerprint::new(digest.finalize().into())
}

fn committed_revision(
    results: &[StoreMutationResult],
    current_revision: u64,
) -> Result<u64, Status> {
    match results {
        [] => Ok(current_revision),
        [StoreMutationResult::Put(stored)] => Ok(stored.version.resource_revision().0),
        _ => Err(Status::internal(
            "node control transaction returned unexpected mutation evidence",
        )),
    }
}

async fn observed_revision(store: &dyn Store, key: &kernel_store::StoreKey) -> Result<u64, Status> {
    store.get(key).await.map_err(store_status).map(|stored| {
        stored
            .map(|stored| stored.version.resource_revision().0)
            .unwrap_or_default()
    })
}

fn invalid(message: impl Into<String>) -> Status {
    Status::invalid_argument(message.into())
}

fn store_status(error: StoreError) -> Status {
    Status::unavailable(format!("node control store is unavailable: {error}"))
}

fn controller_status(error: ControllerError) -> Status {
    match error {
        ControllerError::RequestCollision => {
            Status::already_exists("request ID was already used for another mutation")
        }
        error => Status::unavailable(format!("node control request failed: {error}")),
    }
}
