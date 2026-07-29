use std::collections::HashMap;

use docker::models::NetworkInspect;
use docker::models::RestartPolicyNameEnum;

use crate::docker_config::container_config;
use crate::docker_network::{network_create_request, validate_existing_network};
use crate::{NetworkAddressing, NetworkSpec, RuntimeCapability};

use super::docker_fixture::container_spec;

#[test]
fn docker_network_request_delegates_ipam_to_the_engine() {
    let spec = NetworkSpec {
        name: "maestro-node-1".to_owned(),
        addressing: NetworkAddressing::Delegated,
        mtu_bytes: 1_420,
    };
    let request = network_create_request(&spec);
    assert_eq!(request.name, spec.name);
    assert_eq!(request.driver.as_deref(), Some("bridge"));
    assert_eq!(request.scope.as_deref(), Some("local"));
    assert_eq!(request.enable_ipv4, None);
    assert_eq!(request.enable_ipv6, None);
    assert_eq!(
        request
            .options
            .as_ref()
            .and_then(|options| options.get("com.docker.network.driver.mtu"))
            .map(String::as_str),
        Some("1420")
    );
    assert_eq!(request.ipam, None);
}

#[test]
fn docker_network_rejects_legacy_maestro_owned_ipam() {
    let spec = NetworkSpec {
        name: "maestro-node-1".to_owned(),
        addressing: NetworkAddressing::Delegated,
        mtu_bytes: 1_420,
    };
    let mut inspect = NetworkInspect {
        name: Some(spec.name.clone()),
        driver: Some("bridge".to_owned()),
        labels: Some(HashMap::from([
            ("com.maestro.network".to_owned(), "true".to_owned()),
            ("com.maestro.network-mtu".to_owned(), "1420".to_owned()),
        ])),
        ..Default::default()
    };
    assert!(validate_existing_network(&inspect, &spec).is_ok());
    inspect.labels.as_mut().unwrap().insert(
        "com.maestro.network-subnet".to_owned(),
        "10.42.0.0/24".to_owned(),
    );
    assert!(validate_existing_network(&inspect, &spec).is_err());
}

#[test]
fn docker_container_uses_an_attachable_bootstrap_network() {
    let config = container_config(&container_spec()).unwrap();
    let host = config.body.host_config.unwrap();
    assert_eq!(host.network_mode, None);
    assert_eq!(
        host.restart_policy.unwrap().name,
        Some(RestartPolicyNameEnum::NO)
    );
}

#[test]
fn docker_runtime_advertises_dynamic_networking_and_host_port_publication() {
    let client = docker::Docker::connect_with_http_defaults().unwrap();
    let runtime = crate::DockerRuntime::new(client);
    let capabilities = crate::WorkloadRuntime::capabilities(&runtime);
    assert!(capabilities.supports(RuntimeCapability::DynamicNetwork));
    assert!(capabilities.supports(RuntimeCapability::HostPortPublishing));
}
