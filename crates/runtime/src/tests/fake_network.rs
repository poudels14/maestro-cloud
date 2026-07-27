use std::collections::BTreeSet;
use std::net::{IpAddr, Ipv4Addr};

use kernel_api::WorkloadId;

use crate::{
    AddressRequest, FakeNetworkProvider, NetworkAddressing, NetworkCidr, NetworkProvider,
    NetworkProviderError, NetworkSpec, WorkloadHandle,
};

#[tokio::test]
async fn fake_network_allocates_any_and_exact_addresses_with_stable_ownership() {
    let provider = FakeNetworkProvider::default();
    let spec = NetworkSpec {
        name: "maestro0".to_owned(),
        addressing: NetworkAddressing::Managed {
            range: NetworkCidr::new(address(0), 24).unwrap(),
            gateway: address(1),
        },
        mtu_bytes: 1_420,
    };
    let network = provider.ensure_network(&spec).await.unwrap();
    let first_owner = WorkloadId::new("workload-1").unwrap();
    let second_owner = WorkloadId::new("workload-2").unwrap();

    let first = provider
        .allocate_address(&network, &first_owner, AddressRequest::Any)
        .await
        .unwrap();
    assert_eq!(first.address(), Some(address(2)));
    assert_eq!(
        provider
            .allocate_address(&network, &first_owner, AddressRequest::Any)
            .await
            .unwrap(),
        first
    );
    let _second = provider
        .allocate_address(&network, &second_owner, AddressRequest::Exact(address(3)))
        .await
        .unwrap();
    assert_eq!(provider.lease_count(), 2);
    assert!(matches!(
        provider
            .allocate_address(
                &network,
                &second_owner,
                AddressRequest::Exact(first.address().unwrap())
            )
            .await,
        Err(NetworkProviderError::Rejected { .. })
    ));
    provider
        .release_address(&network, &first_owner)
        .await
        .unwrap();
    provider
        .release_address(&network, &second_owner)
        .await
        .unwrap();
    provider
        .release_address(&network, &second_owner)
        .await
        .unwrap();
    assert_eq!(provider.lease_count(), 0);
}

#[tokio::test]
async fn fake_network_reports_addresses_selected_during_delegated_attachment() {
    let provider = FakeNetworkProvider::default();
    let spec = NetworkSpec {
        name: "maestro-dev".to_owned(),
        addressing: NetworkAddressing::Delegated,
        mtu_bytes: 1_500,
    };
    let network = provider.ensure_network(&spec).await.unwrap();
    let workload_id = WorkloadId::new("workload-dev").unwrap();
    let workload = WorkloadHandle::new(workload_id.clone(), "fake/workload-dev").unwrap();
    let reservation = provider
        .allocate_address(&network, &workload_id, AddressRequest::Any)
        .await
        .unwrap();

    assert_eq!(reservation.address(), None);
    let first = provider
        .attach(&workload, &network, &reservation)
        .await
        .unwrap();
    let second = provider
        .attach(&workload, &network, &reservation)
        .await
        .unwrap();
    assert_eq!(first, second);
    assert_eq!(first.address, IpAddr::V4(Ipv4Addr::new(192, 0, 2, 2)));
}

#[tokio::test]
async fn fake_network_reclaims_only_reservations_absent_from_the_desired_snapshot() {
    let provider = FakeNetworkProvider::default();
    let spec = NetworkSpec {
        name: "maestro0".to_owned(),
        addressing: NetworkAddressing::Managed {
            range: NetworkCidr::new(address(0), 24).unwrap(),
            gateway: address(1),
        },
        mtu_bytes: 1_420,
    };
    let network = provider.ensure_network(&spec).await.unwrap();
    let active = WorkloadId::new("workload-active").unwrap();
    let orphaned = WorkloadId::new("workload-orphaned").unwrap();
    provider
        .allocate_address(&network, &active, AddressRequest::Exact(address(2)))
        .await
        .unwrap();
    provider
        .allocate_address(&network, &orphaned, AddressRequest::Exact(address(3)))
        .await
        .unwrap();

    let reclaimed = provider
        .reconcile_address_owners(&network, &BTreeSet::from([active]))
        .await
        .unwrap();

    assert_eq!(reclaimed, 1);
    assert_eq!(provider.lease_count(), 1);
    provider
        .allocate_address(
            &network,
            &WorkloadId::new("workload-replacement").unwrap(),
            AddressRequest::Exact(address(3)),
        )
        .await
        .unwrap();
}

fn address(last_octet: u8) -> IpAddr {
    IpAddr::V4(Ipv4Addr::new(10, 42, 1, last_octet))
}
