use std::collections::BTreeMap;

use kernel_api::{AssignmentId, ClusterId, NodeId, WorkloadId};

use crate::{
    CgroupPath, ProcessWorkload, RuntimeError, WorkloadConfiguration, WorkloadHandle,
    WorkloadMetadata, WorkloadSpec,
};

#[test]
fn workload_kinds_share_metadata_without_fake_container_fields() {
    let workload_id = WorkloadId::new("workload-1").unwrap();
    let spec = WorkloadSpec::Process(ProcessWorkload {
        configuration: WorkloadConfiguration {
            metadata: WorkloadMetadata {
                cluster_id: ClusterId::new("cluster-1").unwrap(),
                node_id: NodeId::new("node-1").unwrap(),
                service_id: kernel_api::ServiceId::new("api").unwrap(),
                deployment_id: kernel_api::DeploymentId::new("deployment-1").unwrap(),
                assignment_id: AssignmentId::new("assignment-1").unwrap(),
                workload_id: workload_id.clone(),
                labels: BTreeMap::new(),
            },
            hostname: "process-1".to_owned(),
            environment: BTreeMap::new(),
            mounts: Vec::new(),
            workload_address: None,
            dns_server: None,
            user: None,
        },
        command: kernel_api::CommandSpec {
            executable: "/bin/true".to_owned(),
            arguments: Vec::new(),
        },
    });

    assert_eq!(spec.configuration().metadata.workload_id, workload_id);
    let wire = serde_json::to_value(&spec).unwrap();
    assert_eq!(wire.get("type"), Some(&serde_json::json!("process")));
}

#[test]
fn workload_handles_and_cgroup_paths_reject_ambiguous_backend_values() {
    let workload_id = WorkloadId::new("workload-1").unwrap();
    assert!(matches!(
        WorkloadHandle::new(workload_id, ""),
        Err(RuntimeError::InvalidSpec { .. })
    ));
    assert!(CgroupPath::new("relative/path".into()).is_err());
    assert_eq!(
        CgroupPath::new("/sys/fs/cgroup/maestro/workload-1".into())
            .unwrap()
            .as_path(),
        std::path::Path::new("/sys/fs/cgroup/maestro/workload-1")
    );
}
