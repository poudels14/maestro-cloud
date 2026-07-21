use std::net::{Ipv4Addr, Ipv6Addr};

use rtnetlink::AddressMessageBuilder;

use crate::linux_bridge::address_delta;

#[test]
fn bridge_address_delta_adds_missing_and_removes_only_stale_ipv4() {
    let gateway = Ipv4Addr::new(10, 42, 1, 1);
    let stale = AddressMessageBuilder::<Ipv4Addr>::new()
        .index(12)
        .address(Ipv4Addr::new(10, 42, 9, 1), 24)
        .build();
    let ipv6 = AddressMessageBuilder::<Ipv6Addr>::new()
        .index(12)
        .address(Ipv6Addr::LOCALHOST, 128)
        .build();

    let missing = address_delta(vec![stale.clone(), ipv6.clone()], gateway, 24);
    assert!(!missing.present);
    assert_eq!(missing.stale, vec![stale]);

    let desired = AddressMessageBuilder::<Ipv4Addr>::new()
        .index(12)
        .address(gateway, 24)
        .build();
    let current = address_delta(vec![desired, ipv6], gateway, 24);
    assert!(current.present);
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
    let delta = address_delta(vec![desired.clone(), desired, wrong_prefix], gateway, 24);

    assert!(delta.present);
    assert_eq!(delta.stale.len(), 2);
}
