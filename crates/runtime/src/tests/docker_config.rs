use std::net::{IpAddr, Ipv4Addr};

use docker::models::{MountType, RestartPolicyNameEnum};

use crate::docker_config::{METADATA_LABEL, SPEC_LABEL, container_config};
use crate::{HostPortPublication, MountSource, PortProtocol, RuntimeError, WorkloadSpec};

use super::docker_fixture::container_spec;

#[test]
fn docker_config_preserves_identity_and_disables_runtime_restarts() {
    let spec = container_spec();
    let config = container_config(&spec).unwrap();
    assert_eq!(config.name, "maestro-workload-1");
    assert_eq!(config.body.hostname.as_deref(), Some("workload-1"));
    assert_eq!(config.body.user.as_deref(), Some("1000:1001"));
    assert_eq!(
        config.body.entrypoint.as_deref(),
        Some(["/bin/service".to_owned()].as_slice())
    );
    assert_eq!(
        config.body.cmd.as_deref(),
        Some(["--foreground".to_owned()].as_slice())
    );
    let environment = config.body.env.unwrap();
    assert!(environment.contains(&"PLAIN=visible".to_owned()));
    assert!(!environment.iter().any(|value| value.starts_with("TOKEN=")));
    assert!(environment.contains(&"MAESTRO_WORKLOAD_ADDRESS=10.42.0.8".to_owned()));
    let labels = config.body.labels.unwrap();
    assert_eq!(labels.get(SPEC_LABEL), Some(&config.fingerprint));
    assert!(!labels.get(METADATA_LABEL).unwrap().contains("TOKEN"));

    let host = config.body.host_config.unwrap();
    assert_eq!(host.network_mode, None);
    assert_eq!(host.dns, Some(vec!["10.42.0.1".to_owned()]));
    assert_eq!(
        host.restart_policy.unwrap().name,
        Some(RestartPolicyNameEnum::NO)
    );
    let mounts = host.mounts.unwrap();
    assert_eq!(mounts.len(), 2);
    assert_eq!(mounts.first().unwrap().typ, Some(MountType::BIND));
    assert_eq!(mounts.first().unwrap().read_only, Some(true));
    assert_eq!(mounts.get(1).unwrap().typ, Some(MountType::VOLUME));
    assert_eq!(
        mounts.get(1).unwrap().source.as_deref(),
        Some("maestro-sha256-f36cb2e71951fe9ac6bb55e9874b077a78c2172270b79b410ea0330e7c03a053")
    );
}

#[test]
fn docker_managed_volumes_are_isolated_between_clusters() {
    let first = container_config(&container_spec()).unwrap();
    let mut second_spec = container_spec();
    let WorkloadSpec::Container(second) = &mut second_spec else {
        unreachable!();
    };
    second.configuration.metadata.cluster_id = kernel_api::ClusterId::new("cluster-2").unwrap();
    let second = container_config(&second_spec).unwrap();

    let first_mounts = first.body.host_config.unwrap().mounts.unwrap();
    let second_mounts = second.body.host_config.unwrap().mounts.unwrap();
    let first_source = first_mounts.get(1).unwrap().source.clone();
    let second_source = second_mounts.get(1).unwrap().source.clone();
    assert_ne!(first_source, second_source);
    assert_ne!(first_source.as_deref(), Some("workload-data"));
}

#[test]
fn docker_config_publishes_explicit_host_ports() {
    let mut spec = container_spec();
    let WorkloadSpec::Container(container) = &mut spec else {
        unreachable!();
    };
    container.published_ports = vec![
        HostPortPublication {
            container_port: 80,
            host_address: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            host_port: 80,
            protocol: PortProtocol::Tcp,
        },
        HostPortPublication {
            container_port: 53,
            host_address: IpAddr::V4(Ipv4Addr::LOCALHOST),
            host_port: 5353,
            protocol: PortProtocol::Udp,
        },
    ];

    let body = container_config(&spec).unwrap().body;
    assert_eq!(
        body.exposed_ports,
        Some(vec!["53/udp".to_owned(), "80/tcp".to_owned()])
    );
    let bindings = body.host_config.unwrap().port_bindings.unwrap();
    let http = bindings.get("80/tcp").unwrap().as_ref().unwrap();
    let http = http.first().unwrap();
    assert_eq!(http.host_ip.as_deref(), Some("0.0.0.0"));
    assert_eq!(http.host_port.as_deref(), Some("80"));
    let dns = bindings.get("53/udp").unwrap().as_ref().unwrap();
    let dns = dns.first().unwrap();
    assert_eq!(dns.host_ip.as_deref(), Some("127.0.0.1"));
    assert_eq!(dns.host_port.as_deref(), Some("5353"));
}

#[test]
fn docker_config_rejects_invalid_or_conflicting_host_ports() {
    let mut spec = container_spec();
    let WorkloadSpec::Container(container) = &mut spec else {
        unreachable!();
    };
    container.published_ports.push(HostPortPublication {
        container_port: 0,
        host_address: IpAddr::V4(Ipv4Addr::LOCALHOST),
        host_port: 80,
        protocol: PortProtocol::Tcp,
    });
    assert!(matches!(
        container_config(&spec),
        Err(RuntimeError::InvalidSpec { .. })
    ));

    let WorkloadSpec::Container(container) = &mut spec else {
        unreachable!();
    };
    container.published_ports = vec![
        HostPortPublication {
            container_port: 80,
            host_address: IpAddr::V4(Ipv4Addr::LOCALHOST),
            host_port: 8080,
            protocol: PortProtocol::Tcp,
        },
        HostPortPublication {
            container_port: 8080,
            host_address: IpAddr::V4(Ipv4Addr::LOCALHOST),
            host_port: 8080,
            protocol: PortProtocol::Tcp,
        },
    ];
    assert!(matches!(
        container_config(&spec),
        Err(RuntimeError::InvalidSpec { .. })
    ));
}

#[test]
fn docker_config_rejects_other_workload_kinds_and_relative_mounts() {
    let process = WorkloadSpec::Process(crate::ProcessWorkload {
        configuration: container_spec().configuration().clone(),
        command: kernel_api::CommandSpec {
            executable: "/bin/true".to_owned(),
            arguments: Vec::new(),
        },
    });
    assert!(matches!(
        container_config(&process),
        Err(RuntimeError::InvalidSpec { .. })
    ));

    let mut spec = container_spec();
    let WorkloadSpec::Container(container) = &mut spec else {
        unreachable!();
    };
    container.configuration.mounts.first_mut().unwrap().source =
        MountSource::HostPath("relative".into());
    assert!(matches!(
        container_config(&spec),
        Err(RuntimeError::InvalidSpec { .. })
    ));

    let WorkloadSpec::Container(container) = &mut spec else {
        unreachable!();
    };
    container.configuration.mounts.first_mut().unwrap().source =
        MountSource::HostPath("/tmp/resolv.conf".into());
    container.configuration.mounts.first_mut().unwrap().target = "/etc/resolv.conf".into();
    assert!(matches!(
        container_config(&spec),
        Err(RuntimeError::InvalidSpec { .. })
    ));
}
