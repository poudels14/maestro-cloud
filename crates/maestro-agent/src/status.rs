use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use maestro_core::{ReplicaId, ReplicaStatus};
use tokio::sync::watch;

#[derive(Clone)]
pub struct StatusBoard {
    replicas: Arc<RwLock<BTreeMap<ReplicaId, ReplicaStatus>>>,
    version: Arc<watch::Sender<u64>>,
}

impl StatusBoard {
    pub fn new() -> Self {
        let (version, _) = watch::channel(0);
        Self {
            replicas: Arc::default(),
            version: Arc::new(version),
        }
    }

    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.version.subscribe()
    }

    pub fn snapshot(&self) -> Vec<ReplicaStatus> {
        self.read().values().cloned().collect()
    }

    pub fn get(&self, replica_id: &ReplicaId) -> Option<ReplicaStatus> {
        self.read().get(replica_id).cloned()
    }

    pub fn upsert(&self, status: ReplicaStatus) {
        let changed = self
            .write()
            .insert(status.replica_id.clone(), status.clone())
            .is_none_or(|previous| previous != status);
        if changed {
            self.bump();
        }
    }

    pub fn update(&self, replica_id: &ReplicaId, change: impl FnOnce(&mut ReplicaStatus)) {
        let changed = {
            let mut replicas = self.write();
            replicas.get_mut(replica_id).is_some_and(|status| {
                let before = status.clone();
                change(status);
                *status != before
            })
        };
        if changed {
            self.bump();
        }
    }

    pub fn remove(&self, replica_id: &ReplicaId) {
        if self.write().remove(replica_id).is_some() {
            self.bump();
        }
    }

    fn bump(&self) {
        self.version.send_modify(|version| *version += 1);
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, BTreeMap<ReplicaId, ReplicaStatus>> {
        self.replicas
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, BTreeMap<ReplicaId, ReplicaStatus>> {
        self.replicas
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl Default for StatusBoard {
    fn default() -> Self {
        Self::new()
    }
}
