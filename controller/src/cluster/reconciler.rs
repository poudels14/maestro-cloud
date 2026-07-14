use std::{collections::BTreeMap, sync::Arc, time::Duration};

use anyhow::{Result, bail};
use tokio::sync::{Mutex, broadcast};

use crate::{
    cluster::{
        Assignment, AssignmentManifest,
        assignment_store::AssignmentStore,
        executor::{EngineReplicaExecutor, RunningReplica},
    },
    logs::Logger,
    signal::ShutdownEvent,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReconcileAction {
    Stop(String),
    Start(Assignment),
}

pub fn diff_assignments(
    actual: &BTreeMap<String, RunningReplica>,
    desired: &AssignmentManifest,
) -> Result<Vec<ReconcileAction>> {
    let desired_by_id: BTreeMap<&str, &Assignment> = desired
        .assignments
        .iter()
        .map(|assignment| (assignment.assignment_id.as_str(), assignment))
        .collect();
    for (id, running) in actual {
        if let Some(desired) = desired_by_id.get(id.as_str())
            && running.assignment != **desired
        {
            bail!("assignment id `{id}` was mutated in place");
        }
    }
    let mut actions = actual
        .keys()
        .filter(|id| !desired_by_id.contains_key(id.as_str()))
        .cloned()
        .map(ReconcileAction::Stop)
        .collect::<Vec<_>>();
    actions.extend(
        desired
            .assignments
            .iter()
            .filter(|assignment| !actual.contains_key(&assignment.assignment_id))
            .cloned()
            .map(ReconcileAction::Start),
    );
    Ok(actions)
}

pub struct AssignmentReconciler {
    node_id: String,
    store: Arc<dyn AssignmentStore>,
    executor: Mutex<EngineReplicaExecutor>,
    logger: Logger,
}

impl AssignmentReconciler {
    pub fn new(
        node_id: String,
        store: Arc<dyn AssignmentStore>,
        executor: EngineReplicaExecutor,
        logger: Logger,
    ) -> Self {
        Self {
            node_id,
            store,
            executor: Mutex::new(executor),
            logger,
        }
    }

    pub async fn run(self, mut shutdown: broadcast::Receiver<ShutdownEvent>) {
        let mut watcher = match self.store.watch_node(&self.node_id).await {
            Ok(watcher) => watcher,
            Err(error) => {
                self.logger.emit(
                    "error",
                    &format!("failed to start assignment watcher: {error}"),
                );
                return;
            }
        };
        let mut interval = tokio::time::interval(Duration::from_secs(5));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut discovered = false;
        let mut last_generation = 0;
        loop {
            tokio::select! {
                _ = shutdown.recv() => break,
                changed = watcher.changed() => {
                    if changed.is_err() {
                        // Polling remains the level-triggered safety net.
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
                _ = interval.tick() => {}
            }
            let manifest = match self.store.get_for_node(&self.node_id).await {
                Ok(Some(manifest)) => manifest,
                Ok(None) => AssignmentManifest {
                    node_id: self.node_id.clone(),
                    generation: last_generation,
                    assignments: Vec::new(),
                },
                Err(error) => {
                    self.logger
                        .emit("warn", &format!("assignment resync failed: {error}"));
                    continue;
                }
            };
            if manifest.generation < last_generation {
                continue;
            }
            let mut executor = self.executor.lock().await;
            if !discovered {
                if let Err(error) = executor.discover(&manifest).await {
                    self.logger.emit(
                        "warn",
                        &format!("assignment container discovery failed: {error}"),
                    );
                    continue;
                }
                discovered = true;
            }
            if let Err(error) = executor.reap_finished().await {
                self.logger
                    .emit("warn", &format!("replica reap failed: {error}"));
            }
            let actions = match diff_assignments(executor.actual(), &manifest) {
                Ok(actions) => actions,
                Err(error) => {
                    self.logger
                        .emit("error", &format!("corrupt assignment manifest: {error}"));
                    continue;
                }
            };
            for action in actions {
                let result = match action {
                    ReconcileAction::Stop(assignment_id) => executor.stop(&assignment_id).await,
                    ReconcileAction::Start(assignment) => {
                        let result = executor.start(&assignment).await;
                        if let Err(error) = &result {
                            let _ = executor.record_start_failure(&assignment, error).await;
                        }
                        result
                    }
                };
                if let Err(error) = result {
                    self.logger
                        .emit("error", &format!("assignment action failed: {error}"));
                }
            }
            last_generation = manifest.generation;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::executor::RunningReplica;

    fn assignment(id: &str, replica: u32) -> Assignment {
        Assignment {
            assignment_id: id.to_string(),
            placement_epoch: 1,
            service_id: "web".to_string(),
            deployment_id: "dep".to_string(),
            replica_index: replica,
            node_id: "node".to_string(),
            replaces_assignment_id: None,
            created_at_ms: 1,
        }
    }

    #[test]
    fn diff_stops_before_starting() {
        let old = assignment("old", 0);
        let new = assignment("new", 0);
        let actual = BTreeMap::from([(
            old.assignment_id.clone(),
            RunningReplica {
                assignment: old,
                endpoint: None,
                container_hostname: "web".to_string(),
                handle: None,
            },
        )]);
        let desired = AssignmentManifest {
            node_id: "node".to_string(),
            generation: 2,
            assignments: vec![new.clone()],
        };
        assert_eq!(
            diff_assignments(&actual, &desired).unwrap(),
            vec![
                ReconcileAction::Stop("old".to_string()),
                ReconcileAction::Start(new)
            ]
        );
    }

    #[test]
    fn diff_rejects_identity_mutation() {
        let current = assignment("same", 0);
        let mut changed = current.clone();
        changed.replica_index = 1;
        let actual = BTreeMap::from([(
            current.assignment_id.clone(),
            RunningReplica {
                assignment: current,
                endpoint: None,
                container_hostname: "web".to_string(),
                handle: None,
            },
        )]);
        let desired = AssignmentManifest {
            node_id: "node".to_string(),
            generation: 2,
            assignments: vec![changed],
        };
        assert!(diff_assignments(&actual, &desired).is_err());
    }
}
