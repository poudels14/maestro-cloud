use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr};

use docker::models::{EndpointResource, NetworkInspect};
use kernel_api::WorkloadId;

use crate::docker_network_ipam::{
    DockerNetworkState, lease_is_reserved, reconcile_network, release_address, reserve_address,
};
use crate::{AddressLease, AddressRequest, NetworkCidr, NetworkProviderError, NetworkSpec};

#[test]
fn docker_ipam_rebuilds_attachments_and_allocates_stably() {
    let spec = network_spec();
    let mut state = DockerNetworkState::default();
    reconcile_network(&mut state, &spec, &network_inspect()).unwrap();
    let first_id = WorkloadId::new("first").unwrap();
    let first = reserve_address(&mut state, &spec.name, &first_id, AddressRequest::Any).unwrap();
    assert_eq!(first.address, address(2));
    assert_eq!(
        reserve_address(&mut state, &spec.name, &first_id, AddressRequest::Any).unwrap(),
        first
    );
    assert!(lease_is_reserved(&state, &spec.name, &first));

    let second_id = WorkloadId::new("second").unwrap();
    let second = reserve_address(&mut state, &spec.name, &second_id, AddressRequest::Any).unwrap();
    assert_eq!(second.address, address(5));
    assert!(matches!(
        reserve_address(
            &mut state,
            &spec.name,
            &WorkloadId::new("conflict").unwrap(),
            AddressRequest::Exact(address(3)),
        ),
        Err(NetworkProviderError::AddressConflict { .. })
    ));

    release_address(&mut state, &spec.name, &first).unwrap();
    assert!(!lease_is_reserved(&state, &spec.name, &first));
}

#[test]
fn docker_ipam_rejects_gateway_broadcast_and_wrong_owner_release() {
    let spec = network_spec();
    let mut state = DockerNetworkState::default();
    reconcile_network(&mut state, &spec, &NetworkInspect::default()).unwrap();
    let owner = WorkloadId::new("owner").unwrap();
    for unusable in [address(1), address(255)] {
        assert!(matches!(
            reserve_address(
                &mut state,
                &spec.name,
                &owner,
                AddressRequest::Exact(unusable),
            ),
            Err(NetworkProviderError::InvalidRange { .. })
        ));
    }
    let lease = reserve_address(
        &mut state,
        &spec.name,
        &owner,
        AddressRequest::Exact(address(20)),
    )
    .unwrap();
    let wrong_owner = AddressLease {
        workload_id: WorkloadId::new("other").unwrap(),
        address: lease.address,
    };
    assert!(matches!(
        release_address(&mut state, &spec.name, &wrong_owner),
        Err(NetworkProviderError::AddressConflict { .. })
    ));
}

fn network_spec() -> NetworkSpec {
    NetworkSpec {
        name: "maestro-node-1".to_owned(),
        range: NetworkCidr::new(address(0), 24).unwrap(),
        gateway: address(1),
    }
}

fn network_inspect() -> NetworkInspect {
    NetworkInspect {
        containers: Some(HashMap::from([
            (
                "existing-id".to_owned(),
                EndpointResource {
                    name: Some("maestro-existing".to_owned()),
                    ipv4_address: Some("10.42.0.3/24".to_owned()),
                    ..Default::default()
                },
            ),
            (
                "external-id".to_owned(),
                EndpointResource {
                    name: Some("external".to_owned()),
                    ipv4_address: Some("10.42.0.4/24".to_owned()),
                    ..Default::default()
                },
            ),
        ])),
        ..Default::default()
    }
}

fn address(last_octet: u8) -> IpAddr {
    IpAddr::V4(Ipv4Addr::new(10, 42, 0, last_octet))
}
