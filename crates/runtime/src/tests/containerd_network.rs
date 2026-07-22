use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use containerd::types::v1::{Process, Status};
use kernel_api::WorkloadId;

use crate::containerd_network::{
    TaskAttachment, attachment_plan, host_interface_name, peer_interface_name, task_attachment,
};
use crate::{AddressLease, NetworkCidr, NetworkProviderError, NetworkSpec};

#[test]
fn native_attachment_plan_is_stable_bounded_and_mesh_compatible() {
    let workload_id = WorkloadId::new("assignment-generation-with-a-long-identity").unwrap();
    let spec = network_spec();
    let lease = AddressLease {
        workload_id: workload_id.clone(),
        address: address(11),
    };

    let first = attachment_plan(&spec, &workload_id, &lease).unwrap();
    let second = attachment_plan(&spec, &workload_id, &lease).unwrap();

    assert_eq!(first.bridge_name, second.bridge_name);
    assert_eq!(first.host_name, second.host_name);
    assert_eq!(first.peer_name, second.peer_name);
    assert_eq!(first.bridge_name, "maestro0");
    assert_eq!(first.container_name, "eth0");
    assert_eq!(first.host_name, host_interface_name(&workload_id));
    assert_eq!(first.peer_name, peer_interface_name(&workload_id));
    assert!(first.host_name.len() <= 15);
    assert!(first.peer_name.len() <= 15);
    assert_ne!(first.host_name, first.peer_name);
    assert_eq!(first.address, Ipv4Addr::new(10, 42, 1, 11));
    assert_eq!(first.gateway, Ipv4Addr::new(10, 42, 1, 1));
    assert_eq!(first.prefix_length, 24);
    assert_eq!(first.mtu_bytes, 1_420);
}

#[test]
fn native_attachment_plan_rejects_wrong_owners_and_unsupported_addresses() {
    let workload_id = WorkloadId::new("workload-1").unwrap();
    let spec = network_spec();
    let wrong_owner = AddressLease {
        workload_id: WorkloadId::new("workload-2").unwrap(),
        address: address(11),
    };
    assert!(matches!(
        attachment_plan(&spec, &workload_id, &wrong_owner),
        Err(NetworkProviderError::AddressConflict { .. })
    ));

    for unusable in [address(0), address(1), address(255)] {
        let lease = AddressLease {
            workload_id: workload_id.clone(),
            address: unusable,
        };
        assert!(matches!(
            attachment_plan(&spec, &workload_id, &lease),
            Err(NetworkProviderError::InvalidRange { .. })
        ));
    }

    let ipv6 = NetworkSpec {
        name: "maestro0".to_owned(),
        range: NetworkCidr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 128).unwrap(),
        gateway: IpAddr::V6(Ipv6Addr::LOCALHOST),
        mtu_bytes: 1_420,
    };
    let lease = AddressLease {
        workload_id: workload_id.clone(),
        address: IpAddr::V6(Ipv6Addr::LOCALHOST),
    };
    assert!(matches!(
        attachment_plan(&ipv6, &workload_id, &lease),
        Err(NetworkProviderError::Rejected { .. })
    ));
}

#[test]
fn native_attachment_recreates_exited_tasks_before_entering_their_namespace() {
    assert_eq!(task_attachment(None).unwrap(), TaskAttachment::Recreate);
    let running = Process {
        pid: 42,
        status: Status::Running as i32,
        ..Default::default()
    };
    assert_eq!(
        task_attachment(Some(&running)).unwrap(),
        TaskAttachment::Reuse(42)
    );
    let stopped = Process {
        pid: 42,
        status: Status::Stopped as i32,
        ..Default::default()
    };
    assert_eq!(
        task_attachment(Some(&stopped)).unwrap(),
        TaskAttachment::Recreate
    );
    let missing_pid = Process {
        status: Status::Created as i32,
        ..Default::default()
    };
    assert!(task_attachment(Some(&missing_pid)).is_err());
}

fn network_spec() -> NetworkSpec {
    NetworkSpec {
        name: "maestro0".to_owned(),
        range: NetworkCidr::new(address(0), 24).unwrap(),
        gateway: address(1),
        mtu_bytes: 1_420,
    }
}

fn address(last_octet: u8) -> IpAddr {
    IpAddr::V4(Ipv4Addr::new(10, 42, 1, last_octet))
}
