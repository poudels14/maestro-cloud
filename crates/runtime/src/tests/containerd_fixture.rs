use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr};
use std::path::PathBuf;

use kernel_api::{AssignmentId, ClusterId, CommandSpec, NodeId, WorkloadId};

use crate::{
    ArtifactReference, ContainerWorkload, MountAccess, MountSource, WorkloadConfiguration,
    WorkloadMetadata, WorkloadMount, WorkloadSpec, WorkloadUser,
};

pub(crate) fn container_spec() -> WorkloadSpec {
    WorkloadSpec::Container(ContainerWorkload {
        configuration: WorkloadConfiguration {
            metadata: metadata(),
            hostname: "workload-1".to_owned(),
            environment: BTreeMap::from([(
                "PLAIN".to_owned(),
                kernel_api::SecretValue::new("visible"),
            )]),
            mounts: vec![WorkloadMount {
                source: MountSource::HostPath(PathBuf::from("/run/maestro/workload-1")),
                target: PathBuf::from("/run/maestro"),
                access: MountAccess::ReadOnly,
            }],
            workload_address: Some(IpAddr::V4(Ipv4Addr::new(10, 42, 0, 8))),
            dns_server: Some(IpAddr::V4(Ipv4Addr::new(10, 42, 0, 1))),
            user: Some(WorkloadUser {
                user_id: 1000,
                group_id: 1001,
            }),
            capabilities: Default::default(),
        },
        image: ArtifactReference::new(
            "maestro.test/runtime@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        )
        .unwrap(),
        command: Some(CommandSpec {
            executable: "/bin/service".to_owned(),
            arguments: vec!["--foreground".to_owned()],
        }),
        published_ports: Vec::new(),
    })
}

pub(crate) fn metadata() -> WorkloadMetadata {
    WorkloadMetadata {
        cluster_id: ClusterId::new("cluster-1").unwrap(),
        node_id: NodeId::new("node-1").unwrap(),
        service_id: kernel_api::ServiceId::new("api").unwrap(),
        deployment_id: kernel_api::DeploymentId::new("deployment-1").unwrap(),
        assignment_id: AssignmentId::new("assignment-1").unwrap(),
        workload_id: WorkloadId::new("workload-1").unwrap(),
        labels: BTreeMap::from([("service".to_owned(), "api".to_owned())]),
    }
}
