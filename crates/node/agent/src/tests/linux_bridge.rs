use std::net::{Ipv4Addr, Ipv6Addr};

use rtnetlink::AddressMessageBuilder;

use crate::linux_bridge::address_delta;

#[test]
fn bridge_address_delta_adds_missing_and_removes_only_stale_ipv4() {
    let gateway = Ipv4Addr::new(10, 42, 1, 1);
    let admin = Ipv4Addr::new(10, 42, 1, 250);
    let desired_addresses = [(gateway, 24), (admin, 24)];
    let stale = AddressMessageBuilder::<Ipv4Addr>::new()
        .index(12)
        .address(Ipv4Addr::new(10, 42, 9, 1), 24)
        .build();
    let ipv6 = AddressMessageBuilder::<Ipv6Addr>::new()
        .index(12)
        .address(Ipv6Addr::LOCALHOST, 128)
        .build();

    let missing = address_delta(vec![stale.clone(), ipv6.clone()], &desired_addresses);
    assert_eq!(missing.missing, desired_addresses);
    assert_eq!(missing.stale, vec![stale]);

    let desired_gateway = AddressMessageBuilder::<Ipv4Addr>::new()
        .index(12)
        .address(gateway, 24)
        .build();
    let desired_admin = AddressMessageBuilder::<Ipv4Addr>::new()
        .index(12)
        .address(admin, 24)
        .build();
    let current = address_delta(
        vec![desired_gateway, desired_admin, ipv6],
        &desired_addresses,
    );
    assert!(current.missing.is_empty());
    assert!(current.stale.is_empty());
}

#[test]
fn bridge_address_delta_removes_duplicates_and_wrong_prefixes() {
    let gateway = Ipv4Addr::new(10, 42, 1, 1);
    let desired = AddressMessageBuilder::<Ipv4Addr>::new()
        .index(12)
        .address(gateway, 24)
        .build();
    let wrong_prefix = AddressMessageBuilder::<Ipv4Addr>::new()
        .index(12)
        .address(gateway, 16)
        .build();
    let delta = address_delta(
        vec![desired.clone(), desired, wrong_prefix],
        &[(gateway, 24)],
    );

    assert!(delta.missing.is_empty());
    assert_eq!(delta.stale.len(), 2);
}
