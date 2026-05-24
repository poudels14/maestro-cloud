//! Tests for [`diff_assignments`] and [`AssignmentReconciler`] using an
//! in-memory executor that records calls.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;

use crate::cluster::assignment_store::{
    AssignmentReconciler, AssignmentStore, InMemoryAssignmentStore, ReconcileAction,
    ReplicaExecutor, diff_assignments,
};
use crate::cluster::scheduling::Assignment;

fn make_assignment(
    service_id: &str,
    replica_index: u32,
    deployment_id: &str,
    port: u16,
    node_id: &str,
) -> Assignment {
    Assignment {
        service_id: service_id.to_string(),
        deployment_id: deployment_id.to_string(),
        replica_index,
        node_id: node_id.to_string(),
        port,
        created_at_ms: 0,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ExecutorCall {
    Start(String, u32, String),
    Stop(String, u32),
}

#[derive(Default)]
struct RecordingExecutor {
    calls: Mutex<Vec<ExecutorCall>>,
    fail_starts: Mutex<Vec<String>>,
}

#[async_trait]
impl ReplicaExecutor for RecordingExecutor {
    async fn start(&self, assignment: &Assignment) -> Result<()> {
        let fail = self
            .fail_starts
            .lock()
            .unwrap()
            .contains(&assignment.service_id);
        self.calls.lock().unwrap().push(ExecutorCall::Start(
            assignment.service_id.clone(),
            assignment.replica_index,
            assignment.deployment_id.clone(),
        ));
        if fail {
            Err(anyhow::anyhow!("simulated start failure"))
        } else {
            Ok(())
        }
    }

    async fn stop(&self, service_id: &str, replica_index: u32) -> Result<()> {
        self.calls
            .lock()
            .unwrap()
            .push(ExecutorCall::Stop(service_id.to_string(), replica_index));
        Ok(())
    }
}

#[test]
fn diff_yields_starts_for_new_assignments() {
    let prev: Vec<Assignment> = Vec::new();
    let desired = vec![
        make_assignment("a", 0, "d1", 8080, "node-1"),
        make_assignment("b", 0, "d1", 8081, "node-1"),
    ];
    let actions = diff_assignments(&prev, &desired);
    assert_eq!(actions.len(), 2);
    for action in &actions {
        assert!(matches!(action, ReconcileAction::Start(_)));
    }
}

#[test]
fn diff_yields_stops_for_removed_assignments() {
    let prev = vec![make_assignment("a", 0, "d1", 8080, "node-1")];
    let actions = diff_assignments(&prev, &[]);
    assert_eq!(actions.len(), 1);
    match &actions[0] {
        ReconcileAction::Stop {
            service_id,
            replica_index,
        } => {
            assert_eq!(service_id, "a");
            assert_eq!(*replica_index, 0);
        }
        other => panic!("expected Stop, got {other:?}"),
    }
}

#[test]
fn diff_emits_no_actions_when_unchanged() {
    let same = vec![make_assignment("a", 0, "d1", 8080, "node-1")];
    let actions = diff_assignments(&same, &same);
    assert!(actions.is_empty());
}

#[test]
fn diff_emits_restart_when_deployment_changes() {
    let prev = vec![make_assignment("a", 0, "d1", 8080, "node-1")];
    let desired = vec![make_assignment("a", 0, "d2", 8080, "node-1")];
    let actions = diff_assignments(&prev, &desired);
    assert_eq!(actions.len(), 1);
    assert!(matches!(&actions[0], ReconcileAction::Start(_)));
}

#[test]
fn diff_orders_stops_before_starts() {
    let prev = vec![make_assignment("removed", 0, "d1", 8080, "node-1")];
    let desired = vec![make_assignment("added", 0, "d1", 8081, "node-1")];
    let actions = diff_assignments(&prev, &desired);
    assert!(matches!(&actions[0], ReconcileAction::Stop { .. }));
    assert!(matches!(&actions[1], ReconcileAction::Start(_)));
}

#[tokio::test]
async fn reconciler_drives_executor_to_desired_state() {
    let store = Arc::new(InMemoryAssignmentStore::new());
    let executor = Arc::new(RecordingExecutor::default());
    let reconciler = AssignmentReconciler {
        node_id: "node-1".to_string(),
        store: store.clone() as Arc<dyn AssignmentStore>,
        executor: executor.clone(),
        poll_interval: Duration::from_millis(10),
    };

    let initial = vec![
        make_assignment("web", 0, "d1", 8080, "node-1"),
        make_assignment("api", 0, "d1", 8081, "node-1"),
    ];
    store
        .replace_for_node(&"node-1".to_string(), &initial)
        .await
        .unwrap();

    let mut last_seen: Vec<Assignment> = Vec::new();
    reconciler.reconcile_once(&mut last_seen).await.unwrap();

    let calls = executor.calls.lock().unwrap().clone();
    assert_eq!(calls.len(), 2);
    for call in &calls {
        assert!(matches!(call, ExecutorCall::Start(_, _, _)));
    }
    assert_eq!(last_seen.len(), 2);
}

#[tokio::test]
async fn reconciler_stops_removed_replicas_on_next_tick() {
    let store = Arc::new(InMemoryAssignmentStore::new());
    let executor = Arc::new(RecordingExecutor::default());
    let reconciler = AssignmentReconciler {
        node_id: "node-1".to_string(),
        store: store.clone() as Arc<dyn AssignmentStore>,
        executor: executor.clone(),
        poll_interval: Duration::from_millis(10),
    };

    let initial = vec![make_assignment("web", 0, "d1", 8080, "node-1")];
    store
        .replace_for_node(&"node-1".to_string(), &initial)
        .await
        .unwrap();
    let mut last_seen: Vec<Assignment> = Vec::new();
    reconciler.reconcile_once(&mut last_seen).await.unwrap();
    executor.calls.lock().unwrap().clear();

    store
        .replace_for_node(&"node-1".to_string(), &[])
        .await
        .unwrap();
    reconciler.reconcile_once(&mut last_seen).await.unwrap();

    let calls = executor.calls.lock().unwrap().clone();
    assert_eq!(calls.len(), 1);
    assert!(matches!(&calls[0], ExecutorCall::Stop(service, 0) if service == "web"));
}

#[tokio::test]
async fn reconciler_continues_when_executor_returns_err_on_one_replica() {
    let store = Arc::new(InMemoryAssignmentStore::new());
    let executor = Arc::new(RecordingExecutor::default());
    executor.fail_starts.lock().unwrap().push("api".to_string());
    let reconciler = AssignmentReconciler {
        node_id: "node-1".to_string(),
        store: store.clone() as Arc<dyn AssignmentStore>,
        executor: executor.clone(),
        poll_interval: Duration::from_millis(10),
    };
    let desired = vec![
        make_assignment("web", 0, "d1", 8080, "node-1"),
        make_assignment("api", 0, "d1", 8081, "node-1"),
        make_assignment("worker", 0, "d1", 8082, "node-1"),
    ];
    store
        .replace_for_node(&"node-1".to_string(), &desired)
        .await
        .unwrap();
    let mut last_seen: Vec<Assignment> = Vec::new();
    reconciler.reconcile_once(&mut last_seen).await.unwrap();
    let calls = executor.calls.lock().unwrap().clone();
    let started_services: Vec<&str> = calls
        .iter()
        .filter_map(|call| match call {
            ExecutorCall::Start(service, _, _) => Some(service.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(
        started_services.len(),
        3,
        "all three starts attempted even when api fails"
    );
    assert!(started_services.contains(&"web"));
    assert!(started_services.contains(&"api"));
    assert!(started_services.contains(&"worker"));
}

#[tokio::test]
async fn diff_handles_concurrent_start_then_stop_correctly() {
    let prev = vec![
        make_assignment("a", 0, "d1", 8080, "node-1"),
        make_assignment("a", 1, "d1", 8080, "node-1"),
        make_assignment("b", 0, "d1", 8081, "node-1"),
    ];
    let desired = vec![
        make_assignment("a", 0, "d1", 8080, "node-1"),
        make_assignment("c", 0, "d1", 8082, "node-1"),
    ];
    let actions = diff_assignments(&prev, &desired);
    let starts: Vec<_> = actions
        .iter()
        .filter(|action| matches!(action, ReconcileAction::Start(_)))
        .collect();
    let stops: Vec<_> = actions
        .iter()
        .filter(|action| matches!(action, ReconcileAction::Stop { .. }))
        .collect();
    assert_eq!(starts.len(), 1, "only `c/0` is new");
    assert_eq!(stops.len(), 2, "`a/1` and `b/0` removed");
    let stop_positions: Vec<usize> = actions
        .iter()
        .enumerate()
        .filter_map(|(idx, action)| match action {
            ReconcileAction::Stop { .. } => Some(idx),
            _ => None,
        })
        .collect();
    assert!(
        stop_positions.iter().all(|pos| *pos == 0 || *pos == 1),
        "stops must come before starts to free names: positions = {stop_positions:?}"
    );
}

#[tokio::test]
async fn assignment_store_subscribe_pushes_updates() {
    let store = InMemoryAssignmentStore::new();
    let mut receiver = store.subscribe(&"node-1".to_string());
    let initial = receiver.borrow().clone();
    assert!(initial.is_empty());

    let assignments = vec![make_assignment("web", 0, "d1", 8080, "node-1")];
    store
        .replace_for_node(&"node-1".to_string(), &assignments)
        .await
        .unwrap();

    receiver.changed().await.expect("watcher should fire");
    let observed = receiver.borrow().clone();
    assert_eq!(observed.len(), 1);
    assert_eq!(observed[0].service_id, "web");
}
