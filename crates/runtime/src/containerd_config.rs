use std::collections::BTreeMap;

use containerd::services::v1::{Container, container::Runtime};
use prost_types::Any;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::containerd_image::ImageDefaults;
use crate::containerd_resolver::resolver_path;
use crate::containerd_settings::ContainerdRuntimeSettings;
use crate::containerd_support::{container_name, metadata_labels};
use crate::containerd_volume::managed_volume_path;
use crate::{
    ContainerWorkload, MountAccess, MountSource, RuntimeError, WorkloadConfiguration,
    WorkloadMount, WorkloadSpec,
};

const OCI_SPEC_TYPE: &str = "types.containerd.io/opencontainers/runtime-spec/1/Spec";

pub(crate) fn fingerprint(spec: &WorkloadSpec) -> Result<String, RuntimeError> {
    let bytes = serde_json::to_vec(spec).map_err(|error| RuntimeError::InvalidSpec {
        message: format!("failed to fingerprint containerd workload: {error}"),
    })?;
    Ok(hex::encode(Sha256::digest(bytes)))
}

pub(crate) fn container_record(
    spec: &WorkloadSpec,
    image: &ImageDefaults,
    settings: &ContainerdRuntimeSettings,
    snapshot_key: String,
    fingerprint: String,
) -> Result<Container, RuntimeError> {
    let WorkloadSpec::Container(workload) = spec else {
        return Err(RuntimeError::InvalidSpec {
            message: "containerd runtime accepts only container workloads".to_owned(),
        });
    };
    validate_runtime_features(workload)?;
    let metadata = &workload.configuration.metadata;
    Ok(Container {
        id: container_name(&metadata.workload_id),
        labels: metadata_labels(metadata, fingerprint)?,
        image: workload.image.as_str().to_owned(),
        runtime: Some(Runtime {
            name: settings.runtime_name.clone(),
            options: None,
        }),
        spec: Some(Any {
            type_url: OCI_SPEC_TYPE.to_owned(),
            value: serde_json::to_vec(&oci_spec(workload, image, settings)?).map_err(|error| {
                RuntimeError::InvalidSpec {
                    message: format!("failed to encode containerd OCI specification: {error}"),
                }
            })?,
        }),
        snapshotter: settings.snapshotter.clone(),
        snapshot_key,
        ..Default::default()
    })
}

pub(crate) fn validate_runtime_features(workload: &ContainerWorkload) -> Result<(), RuntimeError> {
    if !workload.published_ports.is_empty() {
        return Err(RuntimeError::Unsupported {
            capability: crate::RuntimeCapability::HostPortPublishing,
        });
    }
    Ok(())
}

fn oci_spec(
    workload: &ContainerWorkload,
    image: &ImageDefaults,
    settings: &ContainerdRuntimeSettings,
) -> Result<Value, RuntimeError> {
    let command = workload
        .command
        .as_ref()
        .map_or_else(|| image.command(), |command| Ok(command.clone()))?;
    let mut arguments = Vec::with_capacity(command.arguments.len() + 1);
    arguments.push(command.executable);
    arguments.extend(command.arguments);
    let environment = environment(&workload.configuration, image);
    let (user_id, group_id) = workload.configuration.user.map_or_else(
        || parse_image_user(&image.user),
        |user| Ok((user.user_id, user.group_id)),
    )?;
    let mut mounts = base_mounts();
    mounts.extend(
        workload
            .configuration
            .mounts
            .iter()
            .map(|mount| {
                oci_mount(
                    mount,
                    &workload.configuration.metadata.cluster_id,
                    &settings.state_root,
                )
            })
            .collect::<Result<Vec<_>, _>>()?,
    );
    if workload.configuration.dns_server.is_some() {
        if workload
            .configuration
            .mounts
            .iter()
            .any(|mount| mount.target == std::path::Path::new("/etc/resolv.conf"))
        {
            return Err(RuntimeError::InvalidSpec {
                message: "containerd workload DNS owns `/etc/resolv.conf`".to_owned(),
            });
        }
        mounts.push(oci_mount(
            &WorkloadMount {
                source: MountSource::HostPath(resolver_path(
                    &settings.state_root,
                    &workload.configuration.metadata.workload_id,
                )),
                target: "/etc/resolv.conf".into(),
                access: MountAccess::ReadOnly,
            },
            &workload.configuration.metadata.cluster_id,
            &settings.state_root,
        )?);
    }
    Ok(json!({
        "ociVersion": "1.0.2",
        "process": {
            "terminal": false,
            "user": { "uid": user_id, "gid": group_id },
            "args": arguments,
            "env": environment,
            "cwd": image.working_directory.as_deref().unwrap_or("/"),
            "noNewPrivileges": true
        },
        "root": { "path": "rootfs", "readonly": false },
        "hostname": workload.configuration.hostname,
        "mounts": mounts,
        "linux": {
            "cgroupsPath": format!("/{}/{}", settings.namespace, workload.configuration.metadata.workload_id),
            "resources": { "devices": [{ "allow": false, "access": "rwm" }] },
            "namespaces": [
                { "type": "pid" },
                { "type": "network" },
                { "type": "ipc" },
                { "type": "uts" },
                { "type": "mount" }
            ],
            "maskedPaths": [
                "/proc/acpi", "/proc/asound", "/proc/kcore", "/proc/keys",
                "/proc/latency_stats", "/proc/timer_list", "/proc/timer_stats",
                "/proc/sched_debug", "/sys/firmware"
            ],
            "readonlyPaths": [
                "/proc/bus", "/proc/fs", "/proc/irq", "/proc/sys", "/proc/sysrq-trigger"
            ]
        }
    }))
}

fn environment(configuration: &WorkloadConfiguration, image: &ImageDefaults) -> Vec<String> {
    let mut variables = image
        .environment
        .iter()
        .filter_map(|entry| entry.split_once('='))
        .map(|(name, value)| (name.to_owned(), value.to_owned()))
        .collect::<BTreeMap<_, _>>();
    variables.extend(
        configuration
            .environment
            .iter()
            .map(|(key, value)| (key.clone(), value.expose().to_owned())),
    );
    let metadata = &configuration.metadata;
    variables.insert(
        "MAESTRO_CLUSTER_ID".to_owned(),
        metadata.cluster_id.to_string(),
    );
    variables.insert("MAESTRO_NODE_ID".to_owned(), metadata.node_id.to_string());
    variables.insert(
        "MAESTRO_ASSIGNMENT_ID".to_owned(),
        metadata.assignment_id.to_string(),
    );
    variables.insert(
        "MAESTRO_WORKLOAD_ID".to_owned(),
        metadata.workload_id.to_string(),
    );
    if let Some(address) = configuration.workload_address {
        variables.insert("MAESTRO_WORKLOAD_ADDRESS".to_owned(), address.to_string());
    }
    variables
        .into_iter()
        .map(|(name, value)| format!("{name}={value}"))
        .collect()
}

fn parse_image_user(value: &str) -> Result<(u32, u32), RuntimeError> {
    if value.is_empty() {
        return Ok((0, 0));
    }
    let (user, group) = value.split_once(':').map_or((value, "0"), |parts| parts);
    let user_id = user.parse().map_err(|_| RuntimeError::InvalidSpec {
        message: format!("containerd cannot resolve named image user `{value}` without NSS"),
    })?;
    let group_id = group.parse().map_err(|_| RuntimeError::InvalidSpec {
        message: format!("containerd cannot resolve named image group `{value}` without NSS"),
    })?;
    Ok((user_id, group_id))
}

fn oci_mount(
    mount: &WorkloadMount,
    cluster_id: &kernel_api::ClusterId,
    state_root: &std::path::Path,
) -> Result<Value, RuntimeError> {
    let source = match &mount.source {
        MountSource::HostPath(source) => source.clone(),
        MountSource::ManagedVolume(name) => managed_volume_path(state_root, cluster_id, name)?,
    };
    if !source.is_absolute() || !mount.target.is_absolute() {
        return Err(RuntimeError::InvalidSpec {
            message: "containerd bind mount source and destination must be absolute".to_owned(),
        });
    }
    let source = path_text(&source)?;
    let destination = path_text(&mount.target)?;
    let access = match mount.access {
        MountAccess::ReadOnly => "ro",
        MountAccess::ReadWrite => "rw",
    };
    Ok(json!({
        "destination": destination,
        "type": "bind",
        "source": source,
        "options": ["rbind", "rprivate", access]
    }))
}

fn base_mounts() -> Vec<Value> {
    vec![
        json!({ "destination": "/proc", "type": "proc", "source": "proc" }),
        json!({
            "destination": "/dev", "type": "tmpfs", "source": "tmpfs",
            "options": ["nosuid", "strictatime", "mode=755", "size=65536k"]
        }),
        json!({
            "destination": "/dev/pts", "type": "devpts", "source": "devpts",
            "options": ["nosuid", "noexec", "newinstance", "ptmxmode=0666", "mode=0620", "gid=5"]
        }),
        json!({
            "destination": "/dev/shm", "type": "tmpfs", "source": "shm",
            "options": ["nosuid", "noexec", "nodev", "mode=1777", "size=65536k"]
        }),
        json!({
            "destination": "/dev/mqueue", "type": "mqueue", "source": "mqueue",
            "options": ["nosuid", "noexec", "nodev"]
        }),
        json!({
            "destination": "/sys", "type": "sysfs", "source": "sysfs",
            "options": ["nosuid", "noexec", "nodev", "ro"]
        }),
        json!({
            "destination": "/sys/fs/cgroup", "type": "cgroup", "source": "cgroup",
            "options": ["nosuid", "noexec", "nodev", "relatime", "ro"]
        }),
    ]
}

fn path_text(path: &std::path::Path) -> Result<String, RuntimeError> {
    path.to_str()
        .map(ToOwned::to_owned)
        .ok_or_else(|| RuntimeError::InvalidSpec {
            message: format!("containerd mount path `{}` is not UTF-8", path.display()),
        })
}
