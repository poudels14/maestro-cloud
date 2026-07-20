use std::net::{IpAddr, Ipv4Addr};

use docker::models::RestartPolicyNameEnum;

use crate::docker_config::container_config;
use crate::docker_network::network_create_request;
use crate::{NetworkCidr, NetworkSpec, RuntimeCapability};

use super::docker_fixture::container_spec;

#[test]
fn docker_network_request_uses_exact_host_owned_ipam() {
    let spec = NetworkSpec {
        name: "maestro-node-1".to_owned(),
        range: NetworkCidr::new(IpAddr::V4(Ipv4Addr::new(10, 42, 0, 0)), 24).unwrap(),
        gateway: IpAddr::V4(Ipv4Addr::new(10, 42, 0, 1)),
    };
    let request = network_create_request(&spec);
    assert_eq!(request.name, spec.name);
    assert_eq!(request.driver.as_deref(), Some("bridge"));
    assert_eq!(request.scope.as_deref(), Some("local"));
    assert_eq!(request.enable_ipv4, Some(true));
    assert_eq!(request.enable_ipv6, Some(false));
    let ipam = request.ipam.unwrap();
    let config = ipam.config.unwrap();
    assert_eq!(
        config.first().unwrap().subnet.as_deref(),
        Some("10.42.0.0/24")
    );
    assert_eq!(
        config.first().unwrap().gateway.as_deref(),
        Some("10.42.0.1")
    );
}

#[test]
fn docker_container_starts_detached_from_default_networking() {
    let config = container_config(&container_spec()).unwrap();
    let host = config.body.host_config.unwrap();
    assert_eq!(host.network_mode.as_deref(), Some("none"));
    assert_eq!(
        host.restart_policy.unwrap().name,
        Some(RestartPolicyNameEnum::NO)
    );
}

#[test]
fn docker_runtime_advertises_dynamic_networking_only_with_the_provider() {
    let client = docker::Docker::connect_with_http_defaults().unwrap();
    let runtime = crate::DockerRuntime::new(client);
    assert!(
        crate::WorkloadRuntime::capabilities(&runtime).supports(RuntimeCapability::DynamicNetwork)
    );
}
