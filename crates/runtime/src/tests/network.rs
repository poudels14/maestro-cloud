use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use crate::{NetworkCidr, NetworkProviderError};

#[test]
fn network_ranges_reject_host_bits_and_invalid_prefixes() {
    assert!(matches!(
        NetworkCidr::new(Ipv4Addr::new(10, 42, 0, 1).into(), 24),
        Err(NetworkProviderError::InvalidRange { .. })
    ));
    assert!(matches!(
        NetworkCidr::new(Ipv4Addr::UNSPECIFIED.into(), 33),
        Err(NetworkProviderError::InvalidRange { .. })
    ));
    assert!(matches!(
        NetworkCidr::new(Ipv6Addr::UNSPECIFIED.into(), 129),
        Err(NetworkProviderError::InvalidRange { .. })
    ));
}

#[test]
fn network_ranges_contain_only_matching_address_families_and_prefixes() {
    let ipv4 = NetworkCidr::new(Ipv4Addr::new(10, 42, 0, 0).into(), 24).unwrap();
    assert!(ipv4.contains(Ipv4Addr::new(10, 42, 0, 99).into()));
    assert!(!ipv4.contains(Ipv4Addr::new(10, 42, 1, 1).into()));
    assert!(!ipv4.contains(IpAddr::V6(Ipv6Addr::LOCALHOST)));

    let ipv6 = NetworkCidr::new("fd00::".parse().unwrap(), 64).unwrap();
    assert!(ipv6.contains("fd00::1234".parse().unwrap()));
    assert!(!ipv6.contains("fd01::1".parse().unwrap()));
}
