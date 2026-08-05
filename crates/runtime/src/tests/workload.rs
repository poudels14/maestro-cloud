use std::collections::BTreeMap;

use kernel_api::{AssignmentId, ClusterId, NodeId, WorkloadId};

use crate::{
    CgroupPath, ProcessWorkload, RuntimeError, WorkloadConfiguration, WorkloadHandle,
    WorkloadIdMapping, WorkloadMetadata, WorkloadSpec, WorkloadUser, WorkloadUserNamespace,
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
            user_namespace: None,
            capabilities: Default::default(),
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
fn workload_user_namespaces_translate_only_ids_inside_the_mapping() {
    let namespace = WorkloadUserNamespace {
        uid: WorkloadIdMapping {
            container_id: 0,
            host_id: 1_048_576,
            size: 65_536,
        },
        gid: WorkloadIdMapping {
            container_id: 0,
            host_id: 2_097_152,
            size: 65_536,
        },
    };
    assert_eq!(
        namespace
            .host_user(WorkloadUser {
                user_id: 0,
                group_id: 65_535,
            })
            .unwrap(),
        WorkloadUser {
            user_id: 1_048_576,
            group_id: 2_162_687,
        }
    );
    assert!(matches!(
        namespace.host_user(WorkloadUser {
            user_id: 65_536,
            group_id: 0,
        }),
        Err(RuntimeError::InvalidSpec { .. })
    ));
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
