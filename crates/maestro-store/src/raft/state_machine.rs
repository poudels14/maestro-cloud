use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use openraft::storage::{RaftStateMachine, Snapshot};
use openraft::{
    BasicNode, Entry, EntryPayload, LogId, OptionalSend, RaftSnapshotBuilder, SnapshotMeta,
    StorageError, StorageIOError, StoredMembership,
};
use redb::WriteTransaction;
use serde::{Deserialize, Serialize};

use super::{Response, TypeConfig};
use crate::error::StoreError;
use crate::state::{BatchWriter, StateExport, StateMachine, write_raft_meta};

const LAST_APPLIED_KEY: &str = "last_applied";
const MEMBERSHIP_KEY: &str = "membership";

type RaftResult<T> = Result<T, StorageError<u64>>;
type Membership = StoredMembership<u64, BasicNode>;

#[derive(Clone)]
pub(crate) struct StateMachineStore {
    state: Arc<StateMachine>,
    snapshot_path: PathBuf,
}

#[derive(Serialize, Deserialize)]
struct StoredSnapshot {
    meta: SnapshotMeta<u64, BasicNode>,
    data: Vec<u8>,
}

impl StateMachineStore {
    pub(crate) fn new(state: Arc<StateMachine>, snapshot_path: PathBuf) -> Self {
        Self {
            state,
            snapshot_path,
        }
    }

    fn apply_alone(&self, entry: Entry<TypeConfig>) -> Result<Response, StoreError> {
        let log_id = entry.log_id;
        match self.state.apply_batch(|batch| apply_entry(batch, entry)) {
            Err(error) if error.is_rejection() => self.state.apply_batch(|batch| {
                record_applied(batch.transaction(), log_id, None)?;
                Ok(Response::Rejected {
                    reason: error.to_string(),
                })
            }),
            other => other,
        }
    }

    fn build(&self) -> Result<Snapshot<TypeConfig>, StoreError> {
        let last_log_id: Option<LogId<u64>> = self.state.raft_meta(LAST_APPLIED_KEY)?;
        let membership: Membership = self.state.raft_meta(MEMBERSHIP_KEY)?.unwrap_or_default();
        let data = serde_json::to_vec(&self.state.export()?)?;
        let snapshot_id = last_log_id.map_or_else(
            || "empty".to_string(),
            |log_id| format!("{}-{}", log_id.leader_id, log_id.index),
        );
        let meta = SnapshotMeta {
            last_log_id,
            last_membership: membership,
            snapshot_id,
        };
        persist_snapshot(&self.snapshot_path, &meta, &data)?;
        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }

    fn install(
        &self,
        meta: &SnapshotMeta<u64, BasicNode>,
        data: Vec<u8>,
    ) -> Result<(), StoreError> {
        let export: StateExport = serde_json::from_slice(&data)?;
        self.state.import(&export, |transaction| {
            write_raft_meta(transaction, LAST_APPLIED_KEY, &meta.last_log_id)?;
            write_raft_meta(transaction, MEMBERSHIP_KEY, &meta.last_membership)
        })?;
        persist_snapshot(&self.snapshot_path, meta, &data)
    }

    fn current(&self) -> Result<Option<Snapshot<TypeConfig>>, StoreError> {
        match std::fs::read(&self.snapshot_path) {
            Ok(bytes) => {
                let stored: StoredSnapshot = serde_json::from_slice(&bytes)?;
                Ok(Some(Snapshot {
                    meta: stored.meta,
                    snapshot: Box::new(Cursor::new(stored.data)),
                }))
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(StoreError::Io(error)),
        }
    }
}

impl RaftSnapshotBuilder<TypeConfig> for StateMachineStore {
    async fn build_snapshot(&mut self) -> RaftResult<Snapshot<TypeConfig>> {
        self.build()
            .map_err(|error| StorageIOError::read_state_machine(&error).into())
    }
}

impl RaftStateMachine<TypeConfig> for StateMachineStore {
    type SnapshotBuilder = Self;

    async fn applied_state(&mut self) -> RaftResult<(Option<LogId<u64>>, Membership)> {
        let last_applied = self
            .state
            .raft_meta(LAST_APPLIED_KEY)
            .map_err(|error| StorageIOError::read_state_machine(&error))?;
        let membership = self
            .state
            .raft_meta(MEMBERSHIP_KEY)
            .map_err(|error| StorageIOError::read_state_machine(&error))?;
        Ok((last_applied, membership.unwrap_or_default()))
    }

    async fn apply<I>(&mut self, entries: I) -> RaftResult<Vec<Response>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let entries = entries.into_iter().collect::<Vec<_>>();
        let together = self.state.apply_batch(|batch| {
            entries
                .iter()
                .map(|entry| apply_entry(batch, entry.clone()))
                .collect::<Result<Vec<_>, _>>()
        });
        match together {
            Err(error) if error.is_rejection() => entries
                .into_iter()
                .map(|entry| {
                    let log_id = entry.log_id;
                    self.apply_alone(entry).map_err(|error| (log_id, error))
                })
                .collect::<Result<Vec<_>, _>>()
                .map_err(|(log_id, error)| StorageIOError::apply(log_id, &error).into()),
            other => other.map_err(|error| StorageIOError::write_state_machine(&error).into()),
        }
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(&mut self) -> RaftResult<Box<Cursor<Vec<u8>>>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> RaftResult<()> {
        self.install(meta, snapshot.into_inner())
            .map_err(|error| StorageIOError::write_snapshot(Some(meta.signature()), &error).into())
    }

    async fn get_current_snapshot(&mut self) -> RaftResult<Option<Snapshot<TypeConfig>>> {
        self.current()
            .map_err(|error| StorageIOError::read_snapshot(None, &error).into())
    }
}

fn apply_entry(batch: &mut BatchWriter, entry: Entry<TypeConfig>) -> Result<Response, StoreError> {
    let log_id = entry.log_id;
    let (response, membership) = match entry.payload {
        EntryPayload::Blank => (Response::Empty, None),
        EntryPayload::Membership(membership) => (
            Response::Empty,
            Some(StoredMembership::new(Some(log_id), membership)),
        ),
        EntryPayload::Normal(request) => (
            Response::Applied {
                version: batch.apply(&request.mutations)?,
            },
            None,
        ),
    };
    record_applied(batch.transaction(), log_id, membership.as_ref())?;
    Ok(response)
}

fn record_applied(
    transaction: &WriteTransaction,
    log_id: LogId<u64>,
    membership: Option<&Membership>,
) -> Result<(), StoreError> {
    write_raft_meta(transaction, LAST_APPLIED_KEY, &Some(log_id))?;
    match membership {
        Some(membership) => write_raft_meta(transaction, MEMBERSHIP_KEY, membership),
        None => Ok(()),
    }
}

fn persist_snapshot(
    path: &Path,
    meta: &SnapshotMeta<u64, BasicNode>,
    data: &[u8],
) -> Result<(), StoreError> {
    let encoded = serde_json::to_vec(&StoredSnapshot {
        meta: meta.clone(),
        data: data.to_vec(),
    })?;
    let temporary_path = path.with_extension("tmp");
    std::fs::write(&temporary_path, encoded)?;
    std::fs::rename(&temporary_path, path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use maestro_core::{DeploySpec, ServiceId, ServiceSpec};

    use super::*;
    use crate::mutation::Mutation;

    fn open_store(directory: &Path) -> StateMachineStore {
        let state = StateMachine::open(directory.join("state.redb")).unwrap();
        StateMachineStore::new(Arc::new(state), directory.join("snapshot.json"))
    }

    fn service(id: &str) -> Mutation {
        Mutation::PutService {
            id: ServiceId::from(id),
            spec: ServiceSpec {
                name: id.to_string(),
                image: Some("nginx".to_string()),
                build: None,
                deploy: DeploySpec::default(),
                ingress: None,
                preview: None,
            },
        }
    }

    fn entry(index: u64, mutations: Vec<Mutation>) -> Entry<TypeConfig> {
        Entry {
            log_id: LogId::new(openraft::CommittedLeaderId::new(1, 1), index),
            payload: EntryPayload::Normal(super::super::Request { mutations }),
        }
    }

    #[tokio::test]
    async fn applies_a_batch_in_one_transaction_and_isolates_rejected_entries() {
        let directory = tempfile::tempdir().unwrap();
        let mut store = open_store(directory.path());
        let rejected = Mutation::SetDeploymentStatus {
            service_id: ServiceId::from("b"),
            deployment_id: maestro_core::DeploymentId::from("missing"),
            status: maestro_core::DeploymentStatus::Ready,
            image: None,
            failure: None,
            at_ms: 1,
        };

        let responses = store
            .apply(vec![
                entry(1, vec![service("a")]),
                entry(2, vec![service("b"), rejected]),
                entry(3, vec![service("c")]),
            ])
            .await
            .unwrap();

        assert!(matches!(responses[0], Response::Applied { version: 1 }));
        assert!(matches!(responses[1], Response::Rejected { .. }));
        assert!(matches!(responses[2], Response::Applied { version: 2 }));
        let services = store
            .state
            .services()
            .unwrap()
            .into_iter()
            .map(|(id, _)| id.to_string())
            .collect::<Vec<_>>();
        assert_eq!(services, ["a", "c"]);
        let (last_applied, _) = store.applied_state().await.unwrap();
        assert_eq!(last_applied.map(|log_id| log_id.index), Some(3));
    }

    #[test]
    fn installs_snapshot_into_empty_store() {
        let source_directory = tempfile::tempdir().unwrap();
        let target_directory = tempfile::tempdir().unwrap();
        let source = open_store(source_directory.path());
        let target = open_store(target_directory.path());
        source
            .state
            .apply(&[Mutation::PutService {
                id: ServiceId::from("api"),
                spec: ServiceSpec {
                    name: "API".to_string(),
                    image: Some("nginx".to_string()),
                    build: None,
                    deploy: DeploySpec::default(),
                    ingress: None,
                    preview: None,
                },
            }])
            .unwrap();
        target
            .state
            .apply(&[Mutation::DeleteService {
                id: ServiceId::from("stale"),
            }])
            .unwrap();

        let snapshot = source.build().unwrap();
        target
            .install(&snapshot.meta, snapshot.snapshot.into_inner())
            .unwrap();

        assert_eq!(target.state.services().unwrap().len(), 1);
        assert_eq!(target.state.version().unwrap(), 1);
        assert_eq!(
            target.current().unwrap().unwrap().meta.snapshot_id,
            snapshot.meta.snapshot_id
        );
    }
}
