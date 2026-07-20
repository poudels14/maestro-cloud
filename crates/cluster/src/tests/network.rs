use std::net::Ipv4Addr;

use proptest::prelude::*;

use crate::Ipv4Cidr;

#[test]
fn cidr_requires_a_canonical_network_address() {
    let error = "10.20.30.7/24".parse::<Ipv4Cidr>();
    assert!(error.is_err());
}

#[test]
fn workload_range_preserves_gateway_and_system_addresses() -> Result<(), Box<dyn std::error::Error>>
{
    let network = "172.22.4.0/24".parse::<Ipv4Cidr>()?;
    let addresses = network.workload_addresses().collect::<Vec<_>>();

    assert_eq!(
        network.gateway_address(),
        Some(Ipv4Addr::new(172, 22, 4, 1))
    );
    assert_eq!(addresses.first(), Some(&Ipv4Addr::new(172, 22, 4, 2)));
    assert_eq!(addresses.last(), Some(&Ipv4Addr::new(172, 22, 4, 199)));
    assert_eq!(
        network.host_address_from_end(1),
        Some(Ipv4Addr::new(172, 22, 4, 254))
    );
    assert!(network.is_workload_address(Ipv4Addr::new(172, 22, 4, 100)));
    assert!(!network.is_workload_address(Ipv4Addr::new(172, 22, 4, 200)));
    Ok(())
}

proptest! {
    #[test]
    fn distinct_private_slash_24_networks_do_not_overlap(
        second_octet in any::<u8>(),
        first_third in any::<u8>(),
        second_third in any::<u8>(),
    ) {
        prop_assume!(first_third != second_third);
        let first = Ipv4Cidr::new(Ipv4Addr::new(10, second_octet, first_third, 0), 24)?;
        let second = Ipv4Cidr::new(Ipv4Addr::new(10, second_octet, second_third, 0), 24)?;

        prop_assert!(!first.overlaps(second));
        prop_assert!(!second.overlaps(first));
    }

    #[test]
    fn generated_workload_addresses_stay_inside_their_network(
        second_octet in any::<u8>(),
        third_octet in any::<u8>(),
    ) {
        let network = Ipv4Cidr::new(Ipv4Addr::new(10, second_octet, third_octet, 0), 24)?;

        for address in network.workload_addresses() {
            prop_assert!(network.contains(address));
            prop_assert!(network.is_workload_address(address));
            prop_assert_ne!(address, network.network_address());
            prop_assert_ne!(address, network.broadcast_address());
        }
    }
}
