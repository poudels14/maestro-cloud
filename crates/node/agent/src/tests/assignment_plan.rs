use std::net::{IpAddr, Ipv4Addr};

use kernel_api::{HealthCheckSpec, HealthProbe, VolumeSource, WorkloadUserSpec};
use runtime::{
    HEALTHCHECK_PATH_LABEL, HostPortPublication, MountAccess, MountSource, PortProtocol,
    WorkloadSpec, WorkloadUser,
};

use crate::assignment_plan::{WorkloadPlanError, node_api_user, workload_spec};

use super::assignment::{assignment, cluster_id, deployment, node_id};

#[test]
fn assignment_plan_preserves_identity_artifact_configuration_and_address()
-> Result<(), Box<dyn std::error::Error>> {
    let assignment = assignment();
    let deployment = deployment();
    let WorkloadSpec::Container(workload) = workload_spec(
        &cluster_id(),
        &assignment,
        &deployment,
        Some(dns_server()),
        Vec::new(),
        Vec::new(),
    )?
    else {
        return Err("assignment did not produce a container workload".into());
    };
    assert_eq!(
        workload.configuration.metadata.workload_id.as_str(),
        "assignment-1"
    );
    assert_eq!(
        workload.configuration.metadata.assignment_id,
        assignment.meta.id
    );
    assert_eq!(
        workload.configuration.workload_address,
        assignment.spec.workload_address
    );
    assert_eq!(workload.configuration.dns_server, Some(dns_server()));
    assert_eq!(workload.image.as_str(), "registry.test/api@sha256:abc");
    assert_eq!(workload.configuration.hostname, "api-0");
    assert_eq!(
        workload
            .configuration
            .environment
            .get("MODE")
            .map(String::as_str),
        Some("production")
    );
    assert_eq!(workload.configuration.mounts.len(), 1);
    assert_eq!(
        workload.configuration.mounts.first().unwrap().access,
        MountAccess::ReadOnly
    );
    assert!(matches!(
        workload.configuration.mounts.first().unwrap().source,
        MountSource::HostPath(_)
    ));
    assert_eq!(workload.configuration.user, None);
    Ok(())
}

#[test]
fn assignment_plan_preserves_an_explicit_numeric_workload_user()
-> Result<(), Box<dyn std::error::Error>> {
    let assignment = assignment();
    let mut deployment = deployment();
    deployment.spec.service.user = Some(WorkloadUserSpec {
        user_id: 1_000,
        group_id: 1_001,
    });

    let WorkloadSpec::Container(workload) = workload_spec(
        &cluster_id(),
        &assignment,
        &deployment,
        Some(dns_server()),
        Vec::new(),
        Vec::new(),
    )?
    else {
        return Err("assignment did not produce a container workload".into());
    };
    assert_eq!(
        workload.configuration.user,
        Some(WorkloadUser {
            user_id: 1_000,
            group_id: 1_001,
        })
    );
    Ok(())
}

#[test]
fn assignment_plan_carries_daemon_granted_host_ports() -> Result<(), Box<dyn std::error::Error>> {
    let assignment = assignment();
    let deployment = deployment();
    let publication = HostPortPublication {
        container_port: 80,
        host_address: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
        host_port: 80,
        protocol: PortProtocol::Tcp,
    };
    let WorkloadSpec::Container(workload) = workload_spec(
        &cluster_id(),
        &assignment,
        &deployment,
        Some(dns_server()),
        Vec::new(),
        vec![publication.clone()],
    )?
    else {
        return Err("assignment did not produce a container workload".into());
    };
    assert_eq!(workload.published_ports, vec![publication]);
    Ok(())
}

#[test]
fn assignment_plan_carries_the_http_healthcheck_path_as_observability_metadata()
-> Result<(), Box<dyn std::error::Error>> {
    let assignment = assignment();
    let mut deployment = deployment();
    deployment.spec.service.health_check = Some(HealthCheckSpec {
        probe: HealthProbe::Http {
            port: 8080,
            path: "/health".to_owned(),
        },
        interval_secs: 10,
        unhealthy_threshold: 3,
    });

    let WorkloadSpec::Container(workload) = workload_spec(
        &cluster_id(),
        &assignment,
        &deployment,
        Some(dns_server()),
        Vec::new(),
        Vec::new(),
    )?
    else {
        return Err("assignment did not produce a container workload".into());
    };
    assert_eq!(
        workload
            .configuration
            .metadata
            .labels
            .get(HEALTHCHECK_PATH_LABEL)
            .map(String::as_str),
        Some("/health")
    );
    Ok(())
}

#[test]
fn assignment_plan_rejects_mounts_over_the_private_node_api_directory() {
    let mut deployment = deployment();
    deployment.spec.service.node_api = kernel_api::NodeApiAccess::IdentityAndTelemetry;
    deployment.spec.service.user = Some(WorkloadUserSpec {
        user_id: 1_000,
        group_id: 1_001,
    });
    deployment.spec.service.volumes.first_mut().unwrap().target =
        "/run/maestro/node.sock".to_owned();

    assert!(matches!(
        node_api_user(&deployment),
        Err(WorkloadPlanError::ReservedNodeApiMount { .. })
    ));
}

#[test]
fn assignment_plan_rejects_a_host_volume_owned_by_another_node() {
    let assignment = assignment();
    let mut deployment = deployment();
    deployment.spec.service.volumes.first_mut().unwrap().source = VolumeSource::HostPath {
        path: "/srv/api".to_owned(),
        node_id: kernel_api::NodeId::new("node-2").unwrap(),
    };
    assert!(matches!(
        workload_spec(
            &cluster_id(),
            &assignment,
            &deployment,
            Some(dns_server()),
            Vec::new(),
            Vec::new(),
        ),
        Err(WorkloadPlanError::HostVolumeNodeMismatch { .. })
    ));
    assert_ne!(assignment.spec.node_id, node_id("node-2"));
}

#[test]
fn assignment_plan_scopes_replica_managed_volumes_to_the_active_rollout_slot()
-> Result<(), Box<dyn std::error::Error>> {
    let mut first_assignment = assignment();
    let mut deployment = deployment();
    deployment.spec.service.volumes.first_mut().unwrap().source = VolumeSource::ReplicaManaged {
        name: "identity".to_owned(),
    };

    let WorkloadSpec::Container(first) = workload_spec(
        &cluster_id(),
        &first_assignment,
        &deployment,
        Some(dns_server()),
        Vec::new(),
        Vec::new(),
    )?
    else {
        return Err("assignment did not produce a container workload".into());
    };
    first_assignment.spec.replica_index = 1;
    let WorkloadSpec::Container(second) = workload_spec(
        &cluster_id(),
        &first_assignment,
        &deployment,
        Some(dns_server()),
        Vec::new(),
        Vec::new(),
    )?
    else {
        return Err("assignment did not produce a container workload".into());
    };
    first_assignment.spec.restart_generation = kernel_api::Generation(2);
    let WorkloadSpec::Container(restarted) = workload_spec(
        &cluster_id(),
        &first_assignment,
        &deployment,
        Some(dns_server()),
        Vec::new(),
        Vec::new(),
    )?
    else {
        return Err("assignment did not produce a container workload".into());
    };

    assert_eq!(
        first
            .configuration
            .mounts
            .first()
            .map(|mount| &mount.source),
        Some(&MountSource::ManagedVolume(
            "replica:api:deployment-1:1:0:identity".to_owned()
        ))
    );
    assert_eq!(
        second
            .configuration
            .mounts
            .first()
            .map(|mount| &mount.source),
        Some(&MountSource::ManagedVolume(
            "replica:api:deployment-1:1:1:identity".to_owned()
        ))
    );
    assert_eq!(
        restarted
            .configuration
            .mounts
            .first()
            .map(|mount| &mount.source),
        Some(&MountSource::ManagedVolume(
            "replica:api:deployment-1:2:1:identity".to_owned()
        ))
    );
    Ok(())
}

fn dns_server() -> IpAddr {
    IpAddr::V4(Ipv4Addr::new(10, 42, 1, 1))
}
