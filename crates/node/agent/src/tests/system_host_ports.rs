use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr};

use kernel_api::ServiceId;
use runtime::{
    Capabilities, FakeRuntime, HostPortPublication, NetworkAddressing, NetworkCidr, PortProtocol,
    RuntimeCapability, RuntimeError,
};

use crate::system_host_ports::{runtime_host_ports, validate_system_host_ports};

#[test]
fn host_port_grants_require_backend_support() -> Result<(), Box<dyn std::error::Error>> {
    let Err(error) = validate_system_host_ports(
        &FakeRuntime::new(),
        NetworkAddressing::Delegated,
        &grants("maestro-system-ingress")?,
    ) else {
        return Err("unsupported runtime accepted host ports".into());
    };
    assert_eq!(
        error,
        RuntimeError::Unsupported {
            capability: RuntimeCapability::HostPortPublishing
        }
    );
    Ok(())
}

#[test]
fn host_port_grants_accept_reserved_services_on_a_capable_backend()
-> Result<(), Box<dyn std::error::Error>> {
    let runtime =
        FakeRuntime::with_capabilities(Capabilities::new([RuntimeCapability::HostPortPublishing]));
    validate_system_host_ports(
        &runtime,
        NetworkAddressing::Delegated,
        &grants("maestro-system-ingress")?,
    )?;
    Ok(())
}

#[test]
fn managed_network_host_ports_are_routed_outside_the_runtime()
-> Result<(), Box<dyn std::error::Error>> {
    let addressing = NetworkAddressing::Managed {
        range: NetworkCidr::new("10.42.1.0".parse()?, 24)?,
        gateway: "10.42.1.1".parse()?,
    };
    let grants = grants("maestro-system-ingress")?;
    validate_system_host_ports(&FakeRuntime::new(), addressing, &grants)?;
    assert!(runtime_host_ports(addressing, grants.into_values().flatten().collect()).is_empty());
    Ok(())
}

#[test]
fn host_port_grants_reject_user_services() -> Result<(), Box<dyn std::error::Error>> {
    let runtime =
        FakeRuntime::with_capabilities(Capabilities::new([RuntimeCapability::HostPortPublishing]));
    let Err(error) = validate_system_host_ports(
        &runtime,
        NetworkAddressing::Delegated,
        &grants("user-ingress")?,
    ) else {
        return Err("user service accepted a host port grant".into());
    };
    assert!(matches!(error, RuntimeError::InvalidSpec { .. }));
    Ok(())
}

fn grants(
    service_id: &str,
) -> Result<BTreeMap<ServiceId, Vec<HostPortPublication>>, kernel_api::InvalidIdentifier> {
    Ok(BTreeMap::from([(
        ServiceId::new(service_id)?,
        vec![HostPortPublication {
            container_port: 80,
            host_address: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            host_port: 80,
            protocol: PortProtocol::Tcp,
        }],
    )]))
}
