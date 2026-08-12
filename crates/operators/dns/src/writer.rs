use kernel_api::{DnsRecord, DnsRecordId, ResourceKind, ResourceName, ResourceRevision};
use kernel_controller::{ControllerError, FencedStore};
use kernel_store::{
    Compare, ExpectedVersion, Keyspace, Mutation, TRANSACTION_OPERATION_LIMIT, Transaction,
    TransactionOutcome,
};

use crate::DnsPlan;
use crate::snapshot::{ResourceSnapshot, StoredResource};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct DnsWriteReport {
    pub(crate) created_records: usize,
    pub(crate) replaced_records: usize,
    pub(crate) deleted_records: usize,
    pub(crate) conflict: bool,
}

pub(crate) struct DnsWriter {
    keyspace: Keyspace,
    record_kind: ResourceKind,
}

impl DnsWriter {
    pub(crate) fn new(
        cluster_id: &kernel_api::ClusterId,
    ) -> Result<Self, kernel_api::InvalidIdentifier> {
        Ok(Self {
            keyspace: Keyspace::new(cluster_id),
            record_kind: ResourceKind::new("DnsRecord")?,
        })
    }

    /// Commits one all-or-nothing DNS resource generation under the leader fence.
    pub(crate) async fn apply(
        &self,
        store: &FencedStore,
        snapshot: &ResourceSnapshot,
        plan: &DnsPlan,
    ) -> Result<DnsWriteReport, DnsWriteError> {
        if plan.create_records.is_empty()
            && plan.replace_records.is_empty()
            && plan.delete_records.is_empty()
        {
            return Ok(DnsWriteReport::default());
        }
        let mut compares = snapshot.dependency_compares();
        let mut mutations = Vec::new();
        for record in &plan.create_records {
            let key = self.key(&record.meta.id);
            compares.push(Compare {
                key: key.clone(),
                expected: ExpectedVersion::Missing,
            });
            mutations.push(Mutation::Put {
                key,
                value: serialize(record)?,
                session: None,
            });
        }
        for replacement in &plan.replace_records {
            let current = required(&snapshot.records, &replacement.meta.id)?;
            let actual = current.stored.version.resource_revision();
            if replacement.meta.revision != actual {
                return Err(DnsWriteError::ObservedRevisionMismatch {
                    record_id: replacement.meta.id.clone(),
                    planned: replacement.meta.revision,
                    actual,
                });
            }
            mutations.push(Mutation::Put {
                key: current.stored.key.clone(),
                value: serialize(replacement)?,
                session: None,
            });
        }
        for record_id in &plan.delete_records {
            mutations.push(Mutation::Delete {
                key: required(&snapshot.records, record_id)?.stored.key.clone(),
            });
        }
        let operations = compares
            .len()
            .saturating_add(mutations.len())
            .saturating_add(1);
        if operations > TRANSACTION_OPERATION_LIMIT {
            return Err(DnsWriteError::AtomicGroupTooLarge {
                operations,
                limit: TRANSACTION_OPERATION_LIMIT,
            });
        }
        let outcome = store
            .txn(Transaction {
                compares,
                mutations,
            })
            .await?;
        if outcome == TransactionOutcome::Conflict {
            Ok(DnsWriteReport {
                conflict: true,
                ..Default::default()
            })
        } else {
            Ok(DnsWriteReport {
                created_records: plan.create_records.len(),
                replaced_records: plan.replace_records.len(),
                deleted_records: plan.delete_records.len(),
                conflict: false,
            })
        }
    }

    fn key(&self, record_id: &DnsRecordId) -> kernel_store::StoreKey {
        self.keyspace
            .resource(&self.record_kind, &ResourceName::from(record_id.clone()))
    }
}

fn required<'a>(
    records: &'a std::collections::BTreeMap<DnsRecordId, StoredResource<DnsRecord>>,
    id: &DnsRecordId,
) -> Result<&'a StoredResource<DnsRecord>, DnsWriteError> {
    records
        .get(id)
        .ok_or_else(|| DnsWriteError::MissingPlannedRecord {
            record_id: id.clone(),
        })
}

fn serialize(record: &DnsRecord) -> Result<Vec<u8>, DnsWriteError> {
    serde_json::to_vec(record).map_err(|error| DnsWriteError::Serialize {
        record_id: record.meta.id.to_string(),
        message: error.to_string(),
    })
}

/// Atomic DNS mutation validation and persistence failures.
#[derive(Debug, thiserror::Error)]
pub enum DnsWriteError {
    /// The controller kernel rejected the fenced transaction.
    #[error(transparent)]
    Controller(#[from] ControllerError),
    /// A planned update or deletion referenced a record absent from its snapshot.
    #[error("planned DnsRecord `{record_id}` is absent from the resource snapshot")]
    MissingPlannedRecord { record_id: DnsRecordId },
    /// A planned replacement did not retain the exact observed record revision.
    #[error(
        "planned DnsRecord `{record_id}` revision {planned:?} does not match snapshot {actual:?}"
    )]
    ObservedRevisionMismatch {
        record_id: DnsRecordId,
        planned: ResourceRevision,
        actual: ResourceRevision,
    },
    /// A record resource could not be serialized.
    #[error("failed to serialize DnsRecord `{record_id}`: {message}")]
    Serialize { record_id: String, message: String },
    /// One Service's DNS generation cannot fit in one etcd transaction.
    #[error("DNS resource generation requires {operations} operations; limit is {limit}")]
    AtomicGroupTooLarge { operations: usize, limit: usize },
}
