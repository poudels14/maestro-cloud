use std::collections::BTreeMap;

use containerd::services::v1::{Container, container::Runtime};
use prost_types::Any;
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::containerd_image::ImageDefaults;
use crate::containerd_resolver::resolver_path;
use crate::containerd_settings::ContainerdRuntimeSettings;
use crate::containerd_support::{container_name, metadata_labels};
use crate::containerd_volume::managed_volume_path;
use crate::{
    ContainerWorkload, MountAccess, MountSource, RuntimeError, WorkloadCapability,
    WorkloadConfiguration, WorkloadIdMapping, WorkloadMount, WorkloadSpec, WorkloadUser,
    WorkloadUserNamespace,
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
    resolved_image_user: Option<WorkloadUser>,
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
    let oci = oci_spec(workload, image, resolved_image_user, settings)?;
    let encoded_oci = serde_json::to_vec(&oci).map_err(|error| RuntimeError::InvalidSpec {
        message: format!("failed to encode containerd OCI specification: {error}"),
    })?;
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
            value: encoded_oci,
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
    validate_user_namespace(&workload.configuration)?;
    Ok(())
}

fn oci_spec(
    workload: &ContainerWorkload,
    image: &ImageDefaults,
    resolved_image_user: Option<WorkloadUser>,
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
    let user = workload
        .configuration
        .user
        .or(resolved_image_user)
        .ok_or_else(|| RuntimeError::InvalidSpec {
            message: "containerd image user was not resolved".to_owned(),
        })?;
    let (user_id, group_id) = (user.user_id, user.group_id);
    let user_namespace =
        workload
            .configuration
            .user_namespace
            .ok_or_else(|| RuntimeError::InvalidSpec {
                message: "containerd workloads require an allocated user namespace".to_owned(),
            })?;
    user_namespace.host_user(WorkloadUser { user_id, group_id })?;
    let capabilities = process_capabilities(&workload.configuration);
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
                    user_namespace,
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
            user_namespace,
        )?);
    }
    Ok(json!({
        "ociVersion": "1.2.0",
        "process": {
            "terminal": false,
            "user": { "uid": user_id, "gid": group_id },
            "args": arguments,
            "env": environment,
            "cwd": image.working_directory.as_deref().unwrap_or("/"),
            "noNewPrivileges": true,
            "capabilities": capabilities
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
                { "type": "mount" },
                { "type": "user" }
            ],
            "uidMappings": [id_mapping(user_namespace.uid)],
            "gidMappings": [id_mapping(user_namespace.gid)],
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

fn process_capabilities(configuration: &WorkloadConfiguration) -> Value {
    let values = configuration
        .capabilities
        .iter()
        .map(|capability| match capability {
            WorkloadCapability::NetBindService => "CAP_NET_BIND_SERVICE",
        })
        .collect::<Vec<_>>();
    json!({
        "bounding": values,
        "effective": values,
        "inheritable": values,
        "permitted": values,
        "ambient": values
    })
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

fn oci_mount(
    mount: &WorkloadMount,
    cluster_id: &kernel_api::ClusterId,
    state_root: &std::path::Path,
    user_namespace: WorkloadUserNamespace,
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
        "options": ["rbind", "rprivate", access],
        "uidMappings": [id_mapping(user_namespace.uid)],
        "gidMappings": [id_mapping(user_namespace.gid)]
    }))
}

fn id_mapping(mapping: WorkloadIdMapping) -> Value {
    json!({
        "containerID": mapping.container_id,
        "hostID": mapping.host_id,
        "size": mapping.size
    })
}

fn validate_user_namespace(configuration: &WorkloadConfiguration) -> Result<(), RuntimeError> {
    let namespace = configuration
        .user_namespace
        .ok_or_else(|| RuntimeError::InvalidSpec {
            message: "containerd workloads require an allocated user namespace".to_owned(),
        })?;
    for (kind, mapping) in [("UID", namespace.uid), ("GID", namespace.gid)] {
        if mapping.container_id != 0 || mapping.size == 0 {
            return Err(RuntimeError::InvalidSpec {
                message: format!(
                    "containerd workload {kind} mapping must start at container identity 0 and contain at least one identity"
                ),
            });
        }
        mapping
            .host_id
            .checked_add(mapping.size - 1)
            .ok_or_else(|| RuntimeError::InvalidSpec {
                message: format!("containerd workload {kind} mapping overflows u32"),
            })?;
    }
    Ok(())
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TaskIdentitySpec {
    process: TaskProcess,
    linux: TaskLinux,
}

#[derive(Deserialize)]
struct TaskProcess {
    user: TaskUser,
}

#[derive(Deserialize)]
struct TaskUser {
    uid: u32,
    gid: u32,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TaskLinux {
    uid_mappings: Vec<TaskIdMapping>,
    gid_mappings: Vec<TaskIdMapping>,
}

#[derive(Clone, Copy, Deserialize)]
struct TaskIdMapping {
    #[serde(rename = "containerID")]
    container_id: u32,
    #[serde(rename = "hostID")]
    host_id: u32,
    size: u32,
}

impl From<TaskIdMapping> for WorkloadIdMapping {
    fn from(mapping: TaskIdMapping) -> Self {
        Self {
            container_id: mapping.container_id,
            host_id: mapping.host_id,
            size: mapping.size,
        }
    }
}

pub(crate) fn task_host_user(container: &Container) -> Result<WorkloadUser, RuntimeError> {
    let spec = container
        .spec
        .as_ref()
        .ok_or_else(|| RuntimeError::Unavailable {
            message: format!(
                "containerd container `{}` omitted its OCI specification",
                container.id
            ),
        })?;
    if spec.type_url != OCI_SPEC_TYPE {
        return Err(RuntimeError::Rejected {
            message: format!(
                "containerd container `{}` has unexpected specification type `{}`",
                container.id, spec.type_url
            ),
        });
    }
    let spec: TaskIdentitySpec =
        serde_json::from_slice(&spec.value).map_err(|error| RuntimeError::Rejected {
            message: format!(
                "containerd container `{}` has invalid OCI identity configuration: {error}",
                container.id
            ),
        })?;
    let uid = exactly_one_mapping(&container.id, "UID", spec.linux.uid_mappings)?;
    let gid = exactly_one_mapping(&container.id, "GID", spec.linux.gid_mappings)?;
    WorkloadUserNamespace { uid, gid }.host_user(WorkloadUser {
        user_id: spec.process.user.uid,
        group_id: spec.process.user.gid,
    })
}

fn exactly_one_mapping(
    container_id: &str,
    kind: &str,
    mappings: Vec<TaskIdMapping>,
) -> Result<WorkloadIdMapping, RuntimeError> {
    let [mapping] = mappings.as_slice() else {
        return Err(RuntimeError::Rejected {
            message: format!(
                "containerd container `{container_id}` must have exactly one {kind} mapping"
            ),
        });
    };
    Ok((*mapping).into())
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
