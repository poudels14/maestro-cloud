use std::collections::{BTreeMap, BTreeSet, HashMap};

use docker::models::{
    ContainerCreateBody, HostConfig, Mount, MountType, PortBinding, PortMap, RestartPolicy,
    RestartPolicyNameEnum,
};
use sha2::{Digest, Sha256};

use crate::managed_volume::managed_volume_key;
use crate::{
    ContainerWorkload, HostPortPublication, MountAccess, MountSource, PortProtocol, RuntimeError,
    WorkloadCapability, WorkloadConfiguration, WorkloadMount, WorkloadSpec,
};

pub(crate) const MANAGED_LABEL: &str = "com.maestro.managed";
pub(crate) const CLUSTER_LABEL: &str = "com.maestro.cluster-id";
pub(crate) const NODE_LABEL: &str = "com.maestro.node-id";
pub(crate) const WORKLOAD_LABEL: &str = "com.maestro.workload-id";
pub(crate) const METADATA_LABEL: &str = "com.maestro.metadata";
pub(crate) const SPEC_LABEL: &str = "com.maestro.spec-sha256";

pub(crate) struct DockerContainerConfig {
    pub(crate) name: String,
    pub(crate) body: ContainerCreateBody,
    pub(crate) fingerprint: String,
}

pub(crate) fn container_config(spec: &WorkloadSpec) -> Result<DockerContainerConfig, RuntimeError> {
    let WorkloadSpec::Container(container) = spec else {
        return Err(RuntimeError::InvalidSpec {
            message: "docker runtime accepts only container workloads".to_owned(),
        });
    };
    validate_configuration(&container.configuration)?;
    let fingerprint = fingerprint(spec)?;
    let metadata = &container.configuration.metadata;
    let labels = HashMap::from([
        (MANAGED_LABEL.to_owned(), "true".to_owned()),
        (CLUSTER_LABEL.to_owned(), metadata.cluster_id.to_string()),
        (NODE_LABEL.to_owned(), metadata.node_id.to_string()),
        (WORKLOAD_LABEL.to_owned(), metadata.workload_id.to_string()),
        (
            METADATA_LABEL.to_owned(),
            serde_json::to_string(metadata).map_err(|error| RuntimeError::InvalidSpec {
                message: format!("failed to encode docker ownership metadata: {error}"),
            })?,
        ),
        (SPEC_LABEL.to_owned(), fingerprint.clone()),
    ]);
    let (exposed_ports, port_bindings) = published_ports(&container.published_ports)?;
    let body = ContainerCreateBody {
        hostname: Some(container.configuration.hostname.clone()),
        user: container
            .configuration
            .user
            .map(|user| format!("{}:{}", user.user_id, user.group_id)),
        env: Some(environment(&container.configuration)),
        image: Some(container.image.as_str().to_owned()),
        labels: Some(labels),
        exposed_ports,
        host_config: Some(host_config(container, port_bindings)?),
        ..Default::default()
    };
    let body = apply_command(body, container);
    Ok(DockerContainerConfig {
        name: format!("maestro-{}", metadata.workload_id.as_str()),
        body,
        fingerprint,
    })
}

fn validate_configuration(configuration: &WorkloadConfiguration) -> Result<(), RuntimeError> {
    if configuration.user_namespace.is_some() {
        return Err(RuntimeError::InvalidSpec {
            message: "Docker development workloads cannot request production user namespaces"
                .to_owned(),
        });
    }
    if configuration.hostname.is_empty() {
        return Err(RuntimeError::InvalidSpec {
            message: "container hostname cannot be empty".to_owned(),
        });
    }
    for mount in &configuration.mounts {
        validate_mount(mount)?;
    }
    if configuration.dns_server.is_some()
        && configuration
            .mounts
            .iter()
            .any(|mount| mount.target == std::path::Path::new("/etc/resolv.conf"))
    {
        return Err(RuntimeError::InvalidSpec {
            message: "docker workload DNS owns `/etc/resolv.conf`".to_owned(),
        });
    }
    Ok(())
}

fn validate_mount(mount: &WorkloadMount) -> Result<(), RuntimeError> {
    if !mount.target.is_absolute() {
        return Err(RuntimeError::InvalidSpec {
            message: format!(
                "container mount target `{}` is not absolute",
                mount.target.display()
            ),
        });
    }
    if let MountSource::HostPath(source) = &mount.source
        && !source.is_absolute()
    {
        return Err(RuntimeError::InvalidSpec {
            message: format!(
                "container host mount `{}` is not absolute",
                source.display()
            ),
        });
    }
    Ok(())
}

fn environment(configuration: &WorkloadConfiguration) -> Vec<String> {
    let mut environment = configuration
        .environment
        .iter()
        .map(|(key, value)| (key.clone(), value.expose().to_owned()))
        .collect::<BTreeMap<_, _>>();
    let metadata = &configuration.metadata;
    environment.insert(
        "MAESTRO_CLUSTER_ID".to_owned(),
        metadata.cluster_id.to_string(),
    );
    environment.insert("MAESTRO_NODE_ID".to_owned(), metadata.node_id.to_string());
    environment.insert(
        "MAESTRO_ASSIGNMENT_ID".to_owned(),
        metadata.assignment_id.to_string(),
    );
    environment.insert(
        "MAESTRO_WORKLOAD_ID".to_owned(),
        metadata.workload_id.to_string(),
    );
    if let Some(address) = configuration.workload_address {
        environment.insert("MAESTRO_WORKLOAD_ADDRESS".to_owned(), address.to_string());
    }
    environment
        .into_iter()
        .map(|(name, value)| format!("{name}={value}"))
        .collect()
}

fn host_config(
    container: &ContainerWorkload,
    port_bindings: Option<PortMap>,
) -> Result<HostConfig, RuntimeError> {
    let cluster_id = &container.configuration.metadata.cluster_id;
    let mounts = container
        .configuration
        .mounts
        .iter()
        .map(|mount| docker_mount(mount, cluster_id))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(HostConfig {
        cap_add: capabilities(&container.configuration),
        dns: container
            .configuration
            .dns_server
            .map(|server| vec![server.to_string()]),
        dns_search: container.configuration.dns_server.map(|_| {
            vec![format!(
                "{}.maestro.internal",
                container.configuration.metadata.cluster_id
            )]
        }),
        port_bindings,
        restart_policy: Some(RestartPolicy {
            name: Some(RestartPolicyNameEnum::NO),
            maximum_retry_count: Some(0),
        }),
        mounts: Some(mounts),
        ..Default::default()
    })
}

fn capabilities(configuration: &WorkloadConfiguration) -> Option<Vec<String>> {
    let capabilities = configuration
        .capabilities
        .iter()
        .map(|capability| match capability {
            WorkloadCapability::NetBindService => "NET_BIND_SERVICE".to_owned(),
        })
        .collect::<Vec<_>>();
    (!capabilities.is_empty()).then_some(capabilities)
}

fn published_ports(
    publications: &[HostPortPublication],
) -> Result<(Option<Vec<String>>, Option<PortMap>), RuntimeError> {
    let mut exposed_ports = BTreeSet::new();
    let mut host_endpoints = BTreeSet::new();
    let mut bindings = PortMap::new();
    for publication in publications {
        if publication.container_port == 0 || publication.host_port == 0 {
            return Err(RuntimeError::InvalidSpec {
                message: "published container and host ports must be nonzero".to_owned(),
            });
        }
        let endpoint = (
            publication.host_address,
            publication.host_port,
            publication.protocol,
        );
        if !host_endpoints.insert(endpoint) {
            return Err(RuntimeError::InvalidSpec {
                message: format!(
                    "host endpoint `{}:{}/{}` is published more than once",
                    publication.host_address,
                    publication.host_port,
                    protocol_name(publication.protocol)
                ),
            });
        }
        let container_endpoint = format!(
            "{}/{}",
            publication.container_port,
            protocol_name(publication.protocol)
        );
        exposed_ports.insert(container_endpoint.clone());
        bindings
            .entry(container_endpoint)
            .or_insert_with(|| Some(Vec::new()))
            .get_or_insert_with(Vec::new)
            .push(PortBinding {
                host_ip: Some(publication.host_address.to_string()),
                host_port: Some(publication.host_port.to_string()),
            });
    }
    if bindings.is_empty() {
        Ok((None, None))
    } else {
        Ok((Some(exposed_ports.into_iter().collect()), Some(bindings)))
    }
}

fn protocol_name(protocol: PortProtocol) -> &'static str {
    match protocol {
        PortProtocol::Tcp => "tcp",
        PortProtocol::Udp => "udp",
    }
}

fn docker_mount(
    mount: &WorkloadMount,
    cluster_id: &kernel_api::ClusterId,
) -> Result<Mount, RuntimeError> {
    let target = path_text(&mount.target, "container mount target")?;
    let (source, mount_type) = match &mount.source {
        MountSource::HostPath(source) => {
            (path_text(source, "container host mount")?, MountType::BIND)
        }
        MountSource::ManagedVolume(name) => (
            format!("maestro-{}", managed_volume_key(cluster_id, name)?),
            MountType::VOLUME,
        ),
    };
    Ok(Mount {
        target: Some(target),
        source: Some(source),
        typ: Some(mount_type),
        read_only: Some(mount.access == MountAccess::ReadOnly),
        ..Default::default()
    })
}

fn path_text(path: &std::path::Path, purpose: &str) -> Result<String, RuntimeError> {
    path.to_str()
        .map(ToOwned::to_owned)
        .ok_or_else(|| RuntimeError::InvalidSpec {
            message: format!("{purpose} `{}` is not valid UTF-8", path.display()),
        })
}

fn apply_command(
    mut body: ContainerCreateBody,
    container: &ContainerWorkload,
) -> ContainerCreateBody {
    if let Some(command) = &container.command {
        body.entrypoint = Some(vec![command.executable.clone()]);
        body.cmd = Some(command.arguments.clone());
    }
    body
}

fn fingerprint(spec: &WorkloadSpec) -> Result<String, RuntimeError> {
    let bytes = serde_json::to_vec(spec).map_err(|error| RuntimeError::InvalidSpec {
        message: format!("failed to fingerprint docker workload: {error}"),
    })?;
    Ok(hex::encode(Sha256::digest(bytes)))
}
