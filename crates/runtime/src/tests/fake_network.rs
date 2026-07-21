use std::net::{IpAddr, Ipv4Addr};

use kernel_api::WorkloadId;

use crate::{
    AddressLease, AddressRequest, FakeNetworkProvider, NetworkCidr, NetworkProvider,
    NetworkProviderError, NetworkSpec,
};

#[tokio::test]
async fn fake_network_allocates_any_and_exact_addresses_with_stable_ownership() {
    let provider = FakeNetworkProvider::default();
    let spec = NetworkSpec {
        name: "maestro0".to_owned(),
        range: NetworkCidr::new(address(0), 24).unwrap(),
        gateway: address(1),
        mtu_bytes: 1_420,
    };
    let network = provider.ensure_network(&spec).await.unwrap();
    let first_owner = WorkloadId::new("workload-1").unwrap();
    let second_owner = WorkloadId::new("workload-2").unwrap();

    let first = provider
        .allocate_address(&network, &first_owner, AddressRequest::Any)
        .await
        .unwrap();
    assert_eq!(first.address, address(2));
    assert_eq!(
        provider
            .allocate_address(&network, &first_owner, AddressRequest::Any)
            .await
            .unwrap(),
        first
    );
    let second = provider
        .allocate_address(&network, &second_owner, AddressRequest::Exact(address(3)))
        .await
        .unwrap();
    assert_eq!(provider.lease_count(), 2);
    assert!(matches!(
        provider
            .allocate_address(
                &network,
                &second_owner,
                AddressRequest::Exact(first.address)
            )
            .await,
        Err(NetworkProviderError::Rejected { .. })
    ));
    let wrong_owner = AddressLease {
        workload_id: first_owner.clone(),
        address: second.address,
    };
    assert!(matches!(
        provider.release_address(&network, &wrong_owner).await,
        Err(NetworkProviderError::AddressConflict { .. })
    ));

    provider.release_address(&network, &first).await.unwrap();
    provider.release_address(&network, &second).await.unwrap();
    provider.release_address(&network, &second).await.unwrap();
    assert_eq!(provider.lease_count(), 0);
}

fn address(last_octet: u8) -> IpAddr {
    IpAddr::V4(Ipv4Addr::new(10, 42, 1, last_octet))
}
