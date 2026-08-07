use std::net::Ipv4Addr;

use proptest::prelude::*;

use crate::Ipv4Cidr;

#[test]
fn cidr_requires_a_canonical_network_address() {
    let error = "10.20.30.7/24".parse::<Ipv4Cidr>();
    assert!(error.is_err());
}

#[test]
fn reports_network_containment_and_address_capacity() -> Result<(), Box<dyn std::error::Error>> {
    let supernet = "10.42.0.0/16".parse::<Ipv4Cidr>()?;

    assert!(supernet.contains_network("10.42.9.0/24".parse()?));
    assert!(!supernet.contains_network("10.43.0.0/24".parse()?));
    assert_eq!(supernet.address_count(), 65_536);
    Ok(())
}

#[test]
fn address_ranges_separate_gateway_admin_system_and_user_workloads()
-> Result<(), Box<dyn std::error::Error>> {
    let network = "172.22.4.0/24".parse::<Ipv4Cidr>()?;
    let system = network.system_service_addresses().collect::<Vec<_>>();
    let users = network.user_workload_addresses().collect::<Vec<_>>();

    assert_eq!(
        network.gateway_address(),
        Some(Ipv4Addr::new(172, 22, 4, 1))
    );
    assert_eq!(system.len(), 29);
    assert_eq!(system.first(), Some(&Ipv4Addr::new(172, 22, 4, 2)));
    assert_eq!(system.last(), Some(&Ipv4Addr::new(172, 22, 4, 31)));
    assert!(!system.contains(&Ipv4Addr::new(172, 22, 4, 5)));
    assert_eq!(network.admin_address(), Some(Ipv4Addr::new(172, 22, 4, 5)));
    assert_eq!(users.first(), Some(&Ipv4Addr::new(172, 22, 4, 32)));
    assert_eq!(users.last(), Some(&Ipv4Addr::new(172, 22, 4, 254)));
    assert!(network.is_system_service_address(Ipv4Addr::new(172, 22, 4, 2)));
    assert!(!network.is_system_service_address(Ipv4Addr::new(172, 22, 4, 5)));
    assert!(network.is_user_workload_address(Ipv4Addr::new(172, 22, 4, 200)));
    Ok(())
}

#[test]
fn larger_standalone_range_uses_low_system_addresses_and_remaining_user_hosts()
-> Result<(), Box<dyn std::error::Error>> {
    let network = "10.202.0.0/16".parse::<Ipv4Cidr>()?;
    let addresses = network.user_workload_addresses().collect::<Vec<_>>();

    assert!(!addresses.contains(&Ipv4Addr::new(10, 202, 0, 31)));
    assert!(addresses.contains(&Ipv4Addr::new(10, 202, 0, 32)));
    assert!(addresses.contains(&Ipv4Addr::new(10, 202, 0, 199)));
    assert!(addresses.contains(&Ipv4Addr::new(10, 202, 0, 200)));
    assert!(addresses.contains(&Ipv4Addr::new(10, 202, 0, 250)));
    assert!(addresses.contains(&Ipv4Addr::new(10, 202, 0, 255)));
    assert!(addresses.contains(&Ipv4Addr::new(10, 202, 255, 254)));
    assert_eq!(network.admin_address(), Some(Ipv4Addr::new(10, 202, 0, 5)));
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
    fn generated_user_workload_addresses_stay_inside_their_network(
        second_octet in any::<u8>(),
        third_octet in any::<u8>(),
    ) {
        let network = Ipv4Cidr::new(Ipv4Addr::new(10, second_octet, third_octet, 0), 24)?;

        for address in network.user_workload_addresses() {
            prop_assert!(network.contains(address));
            prop_assert!(network.is_user_workload_address(address));
            prop_assert_ne!(address, network.network_address());
            prop_assert_ne!(address, network.broadcast_address());
        }
    }
}
