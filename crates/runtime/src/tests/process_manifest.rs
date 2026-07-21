use std::collections::BTreeMap;

use kernel_api::{AssignmentId, ClusterId, NodeId, WorkloadId};
use supervisor::ProcessHandle;

use crate::WorkloadMetadata;
use crate::process_manifest::{
    ManifestState, ProcessManifest, commit_process_start, remove_manifest, write_manifest,
};

#[test]
fn superseded_start_commit_cannot_recreate_a_removed_manifest() {
    let root = tempfile::tempdir().unwrap();
    let workload_id = WorkloadId::new("workload-1").unwrap();
    let mut manifest = ProcessManifest::created(
        WorkloadMetadata {
            cluster_id: ClusterId::new("cluster-1").unwrap(),
            node_id: NodeId::new("node-1").unwrap(),
            service_id: kernel_api::ServiceId::new("api").unwrap(),
            deployment_id: kernel_api::DeploymentId::new("deployment-1").unwrap(),
            assignment_id: AssignmentId::new("assignment-1").unwrap(),
            workload_id: workload_id.clone(),
            labels: BTreeMap::new(),
        },
        "fingerprint".to_owned(),
        root.path().join("stdout.log"),
        root.path().join("stderr.log"),
    );
    manifest.state = ManifestState::Starting;
    manifest.operation_generation = 1;
    write_manifest(root.path(), &manifest).unwrap();
    remove_manifest(root.path(), &workload_id).unwrap();

    assert!(
        commit_process_start(
            root.path(),
            &workload_id,
            1,
            ProcessHandle::from_parts(42, 99).unwrap(),
        )
        .is_err()
    );
    assert!(!root.path().join("workloads/workload-1").exists());
}
