use serde_json::{Value, json};

use crate::spec::{ContainerFiles, ContainerSpec, ImageConfig, NetworkMode, RuntimeError};

const CPU_PERIOD_MICROS: u64 = 100_000;
const DEFAULT_PATH: &str = "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin";
const MAX_HOSTNAME_LENGTH: usize = 63;
const OPEN_FILES_LIMIT: u64 = 1_048_576;

const DEFAULT_CAPABILITIES: [&str; 14] = [
    "CAP_CHOWN",
    "CAP_DAC_OVERRIDE",
    "CAP_FSETID",
    "CAP_FOWNER",
    "CAP_MKNOD",
    "CAP_NET_RAW",
    "CAP_SETGID",
    "CAP_SETUID",
    "CAP_SETFCAP",
    "CAP_SETPCAP",
    "CAP_NET_BIND_SERVICE",
    "CAP_SYS_CHROOT",
    "CAP_KILL",
    "CAP_AUDIT_WRITE",
];

const MASKED_PATHS: [&str; 10] = [
    "/proc/acpi",
    "/proc/asound",
    "/proc/kcore",
    "/proc/keys",
    "/proc/latency_stats",
    "/proc/timer_list",
    "/proc/timer_stats",
    "/proc/sched_debug",
    "/proc/scsi",
    "/sys/firmware",
];

const READONLY_PATHS: [&str; 5] = [
    "/proc/bus",
    "/proc/fs",
    "/proc/irq",
    "/proc/sys",
    "/proc/sysrq-trigger",
];

pub fn build_spec(
    container: &ContainerSpec,
    image: &ImageConfig,
    files: &ContainerFiles,
) -> Result<Value, RuntimeError> {
    let (uid, gid) = parse_user(image.user.as_deref())?;
    let limits = container.limits;
    let cpu_quota = u64::from(limits.cpu.get()) * CPU_PERIOD_MICROS / 1000;
    let memory_limit = limits.memory.bytes();
    let capabilities = DEFAULT_CAPABILITIES.to_vec();
    Ok(json!({
        "ociVersion": "1.1.0",
        "process": {
            "terminal": false,
            "user": { "uid": uid, "gid": gid },
            "args": process_args(container, image),
            "env": process_env(container, image),
            "cwd": image.working_dir.as_deref().filter(|dir| !dir.is_empty()).unwrap_or("/"),
            "capabilities": {
                "bounding": capabilities,
                "effective": capabilities,
                "permitted": capabilities,
            },
            "rlimits": [{ "type": "RLIMIT_NOFILE", "hard": OPEN_FILES_LIMIT, "soft": OPEN_FILES_LIMIT }],
            "noNewPrivileges": true,
            "oomScoreAdj": limits.oom_score_adj,
        },
        "root": { "path": "rootfs", "readonly": false },
        "hostname": hostname(&container.id),
        "mounts": mounts(container, files),
        "linux": {
            "resources": {
                "devices": [{ "allow": false, "access": "rwm" }],
                "memory": { "limit": memory_limit, "swap": memory_limit },
                "cpu": { "quota": cpu_quota, "period": CPU_PERIOD_MICROS },
                "pids": { "limit": limits.pids },
            },
            "cgroupsPath": format!("{}:maestro:{}", container.cgroup_parent, container.id),
            "namespaces": namespaces(container.network),
            "maskedPaths": MASKED_PATHS,
            "readonlyPaths": READONLY_PATHS,
        },
    }))
}

fn namespaces(network: NetworkMode) -> Vec<Value> {
    let shared = ["pid", "ipc", "uts", "mount", "cgroup"];
    let network_namespace = match network {
        NetworkMode::Bridge => Some("network"),
        NetworkMode::Host => None,
    };
    shared
        .into_iter()
        .chain(network_namespace)
        .map(|kind| json!({ "type": kind }))
        .collect()
}

fn process_args(container: &ContainerSpec, image: &ImageConfig) -> Vec<String> {
    let entrypoint = container
        .entrypoint
        .clone()
        .or_else(|| image.entrypoint.clone())
        .unwrap_or_default();
    let args = match (&container.entrypoint, &container.args) {
        (_, Some(args)) => args.clone(),
        (Some(_), None) => Vec::new(),
        (None, None) => image.cmd.clone().unwrap_or_default(),
    };
    [entrypoint, args].concat()
}

fn process_env(container: &ContainerSpec, image: &ImageConfig) -> Vec<String> {
    let image_env = image.env.clone().unwrap_or_default();
    let has_path = image_env
        .iter()
        .chain(container.env.keys())
        .any(|entry| entry == "PATH" || entry.starts_with("PATH="));
    let overridden = |entry: &String| {
        let name = entry.split('=').next().unwrap_or_default();
        container.env.contains_key(name)
    };
    let defaults = (!has_path).then(|| DEFAULT_PATH.to_string());
    defaults
        .into_iter()
        .chain(image_env.into_iter().filter(|entry| !overridden(entry)))
        .chain(
            container
                .env
                .iter()
                .map(|(name, value)| format!("{name}={value}")),
        )
        .collect()
}

fn mounts(container: &ContainerSpec, files: &ContainerFiles) -> Vec<Value> {
    let system = vec![
        json!({ "destination": "/proc", "type": "proc", "source": "proc", "options": ["nosuid", "noexec", "nodev"] }),
        json!({ "destination": "/dev", "type": "tmpfs", "source": "tmpfs", "options": ["nosuid", "strictatime", "mode=755", "size=65536k"] }),
        json!({ "destination": "/dev/pts", "type": "devpts", "source": "devpts", "options": ["nosuid", "noexec", "newinstance", "ptmxmode=0666", "mode=0620", "gid=5"] }),
        json!({ "destination": "/dev/shm", "type": "tmpfs", "source": "shm", "options": ["nosuid", "noexec", "nodev", "mode=1777", "size=65536k"] }),
        json!({ "destination": "/dev/mqueue", "type": "mqueue", "source": "mqueue", "options": ["nosuid", "noexec", "nodev"] }),
        json!({ "destination": "/sys", "type": "sysfs", "source": "sysfs", "options": ["nosuid", "noexec", "nodev", "ro"] }),
        json!({ "destination": "/sys/fs/cgroup", "type": "cgroup", "source": "cgroup", "options": ["nosuid", "noexec", "nodev", "relatime", "ro"] }),
        bind_mount(&files.hosts.to_string_lossy(), "/etc/hosts", false),
        bind_mount(&files.hostname.to_string_lossy(), "/etc/hostname", false),
        bind_mount(
            &files.resolv_conf.to_string_lossy(),
            "/etc/resolv.conf",
            false,
        ),
    ];
    let user = container.mounts.iter().map(|mount| {
        bind_mount(
            &mount.source.to_string_lossy(),
            &mount.destination,
            mount.read_only,
        )
    });
    system.into_iter().chain(user).collect()
}

fn bind_mount(source: &str, destination: &str, read_only: bool) -> Value {
    let access = match read_only {
        true => "ro",
        false => "rw",
    };
    json!({
        "destination": destination,
        "type": "bind",
        "source": source,
        "options": ["rbind", "rprivate", access],
    })
}

fn hostname(container_id: &str) -> String {
    container_id.chars().take(MAX_HOSTNAME_LENGTH).collect()
}

fn parse_user(user: Option<&str>) -> Result<(u32, u32), RuntimeError> {
    let trimmed = user.unwrap_or_default().trim();
    let (uid_text, gid_text) = trimmed.split_once(':').unwrap_or((trimmed, ""));
    let uid = match uid_text {
        "" | "root" => Some(0),
        numeric => numeric.parse::<u32>().ok(),
    };
    let gid = match gid_text {
        "" => uid,
        "root" => Some(0),
        numeric => numeric.parse::<u32>().ok(),
    };
    uid.zip(gid)
        .ok_or_else(|| RuntimeError::UnsupportedUser(trimmed.to_string()))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    use maestro_core::{ByteSize, Millicores};

    use super::*;
    use crate::spec::{BindMount, ContainerLimits};

    fn container() -> ContainerSpec {
        ContainerSpec {
            id: "api-d1-0".to_string(),
            image: "nginx:1.27".to_string(),
            entrypoint: None,
            args: None,
            env: BTreeMap::from([("RUST_LOG".to_string(), "info".to_string())]),
            labels: BTreeMap::new(),
            limits: ContainerLimits {
                cpu: Millicores::new(2500),
                memory: ByteSize::new(4 << 30),
                pids: 4096,
                oom_score_adj: 500,
            },
            cgroup_parent: "maestro-workloads.slice".to_string(),
            mounts: vec![BindMount {
                source: PathBuf::from("/data/api"),
                destination: "/var/lib/api".to_string(),
                read_only: true,
            }],
            network: NetworkMode::Bridge,
        }
    }

    fn files() -> ContainerFiles {
        ContainerFiles {
            hosts: PathBuf::from("/run/maestro/c/hosts"),
            hostname: PathBuf::from("/run/maestro/c/hostname"),
            resolv_conf: PathBuf::from("/run/maestro/c/resolv.conf"),
        }
    }

    fn image() -> ImageConfig {
        ImageConfig {
            env: Some(vec![
                "PATH=/usr/bin".to_string(),
                "RUST_LOG=debug".to_string(),
            ]),
            entrypoint: Some(vec!["/docker-entrypoint.sh".to_string()]),
            cmd: Some(vec!["nginx".to_string(), "-g".to_string()]),
            working_dir: Some("/srv".to_string()),
            user: Some("101:101".to_string()),
        }
    }

    #[test]
    fn applies_resource_limits_and_cgroup_parent() {
        let spec = build_spec(&container(), &image(), &files()).unwrap();
        let resources = &spec["linux"]["resources"];
        assert_eq!(resources["cpu"]["quota"], 250_000);
        assert_eq!(resources["cpu"]["period"], 100_000);
        assert_eq!(resources["memory"]["limit"], 4u64 << 30);
        assert_eq!(resources["memory"]["swap"], 4u64 << 30);
        assert_eq!(resources["pids"]["limit"], 4096);
        assert_eq!(spec["process"]["oomScoreAdj"], 500);
        assert_eq!(
            spec["linux"]["cgroupsPath"],
            "maestro-workloads.slice:maestro:api-d1-0"
        );
    }

    #[test]
    fn merges_process_from_image_and_container() {
        let spec = build_spec(&container(), &image(), &files()).unwrap();
        assert_eq!(
            spec["process"]["args"],
            json!(["/docker-entrypoint.sh", "nginx", "-g"])
        );
        assert_eq!(
            spec["process"]["env"],
            json!(["PATH=/usr/bin", "RUST_LOG=info"])
        );
        assert_eq!(spec["process"]["cwd"], "/srv");
        assert_eq!(spec["process"]["user"], json!({ "uid": 101, "gid": 101 }));
    }

    #[test]
    fn container_entrypoint_drops_image_cmd() {
        let mut overridden = container();
        overridden.entrypoint = Some(vec!["/bin/server".to_string()]);
        let spec = build_spec(&overridden, &image(), &files()).unwrap();
        assert_eq!(spec["process"]["args"], json!(["/bin/server"]));

        overridden.args = Some(vec!["--port".to_string(), "80".to_string()]);
        let spec = build_spec(&overridden, &image(), &files()).unwrap();
        assert_eq!(
            spec["process"]["args"],
            json!(["/bin/server", "--port", "80"])
        );
    }

    #[test]
    fn adds_default_path_when_missing() {
        let spec = build_spec(&container(), &ImageConfig::default(), &files()).unwrap();
        assert_eq!(spec["process"]["env"][0], DEFAULT_PATH);
        assert_eq!(spec["process"]["user"], json!({ "uid": 0, "gid": 0 }));
        assert_eq!(spec["process"]["cwd"], "/");
    }

    #[test]
    fn mounts_container_files_and_user_binds() {
        let spec = build_spec(&container(), &image(), &files()).unwrap();
        let mounts = spec["mounts"].as_array().unwrap();
        let resolv = mounts
            .iter()
            .find(|mount| mount["destination"] == "/etc/resolv.conf")
            .unwrap();
        assert_eq!(resolv["source"], "/run/maestro/c/resolv.conf");
        let user = mounts
            .iter()
            .find(|mount| mount["destination"] == "/var/lib/api")
            .unwrap();
        assert_eq!(user["options"], json!(["rbind", "rprivate", "ro"]));
    }

    #[test]
    fn host_network_shares_the_host_namespace() {
        let mut host = container();
        let bridged = build_spec(&host, &image(), &files()).unwrap();
        host.network = NetworkMode::Host;
        let shared = build_spec(&host, &image(), &files()).unwrap();
        let has_network = |spec: &Value| {
            spec["linux"]["namespaces"]
                .as_array()
                .unwrap()
                .iter()
                .any(|namespace| namespace["type"] == "network")
        };
        assert!(has_network(&bridged));
        assert!(!has_network(&shared));
    }

    #[test]
    fn rejects_named_users() {
        let mut named = image();
        named.user = Some("node".to_string());
        assert!(matches!(
            build_spec(&container(), &named, &files()),
            Err(RuntimeError::UnsupportedUser(_))
        ));
    }
}
