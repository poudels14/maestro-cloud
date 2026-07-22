use std::ffi::OsStr;
use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use clustertest::FixtureNodeName;
use kernel_api::NodeId;

use super::{RETRY_DELAY, RealClusterError, RealProcessCluster, SETUP_TIMEOUT, kill_process_group};

pub(super) struct RealNode {
    pub(super) fixture: FixtureNodeName,
    pub(super) node_id: NodeId,
    pub(super) namespace: String,
    pub(super) workload_namespace: String,
    pub(super) host_veth: String,
    pub(super) namespace_veth: String,
    pub(super) workload_host_veth: String,
    pub(super) workload_peer_veth: String,
    pub(super) host_address: Ipv4Addr,
    pub(super) workload_gateway: Ipv4Addr,
    pub(super) workload_address: Ipv4Addr,
    pub(super) data_directory: PathBuf,
    pub(super) config_path: PathBuf,
    pub(super) log_path: PathBuf,
    pub(super) child: Option<Child>,
    pub(super) launch_sequence: u32,
}

impl RealProcessCluster {
    pub(super) fn create_network(&mut self, segment: u8) -> Result<(), Box<dyn std::error::Error>> {
        run_checked("ip", ["link", "add", &self.bridge, "type", "bridge"])?;
        run_checked(
            "ip",
            [
                "addr",
                "add",
                &format!("10.203.{segment}.1/24"),
                "dev",
                &self.bridge,
            ],
        )?;
        run_checked("ip", ["link", "set", &self.bridge, "up"])?;
        for node in &self.nodes {
            run_checked("ip", ["netns", "add", &node.namespace])?;
            run_checked("ip", ["netns", "add", &node.workload_namespace])?;
            run_checked(
                "ip",
                [
                    "link",
                    "add",
                    &node.host_veth,
                    "type",
                    "veth",
                    "peer",
                    "name",
                    &node.namespace_veth,
                ],
            )?;
            run_checked(
                "ip",
                [
                    "link",
                    "set",
                    &node.namespace_veth,
                    "netns",
                    &node.namespace,
                ],
            )?;
            run_checked(
                "ip",
                ["link", "set", &node.host_veth, "master", &self.bridge],
            )?;
            run_checked("ip", ["link", "set", &node.host_veth, "up"])?;
            run_checked("ip", ["-n", &node.namespace, "link", "set", "lo", "up"])?;
            run_checked(
                "ip",
                ["-n", &node.workload_namespace, "link", "set", "lo", "up"],
            )?;
            run_checked(
                "ip",
                [
                    "netns",
                    "exec",
                    &node.namespace,
                    "sysctl",
                    "-q",
                    "-w",
                    "net.ipv4.ip_forward=1",
                ],
            )?;
            run_checked(
                "ip",
                [
                    "-n",
                    &node.namespace,
                    "addr",
                    "add",
                    &format!("{}/24", node.host_address),
                    "dev",
                    &node.namespace_veth,
                ],
            )?;
            run_checked(
                "ip",
                [
                    "-n",
                    &node.namespace,
                    "link",
                    "set",
                    &node.namespace_veth,
                    "up",
                ],
            )?;
        }
        Ok(())
    }

    pub(super) async fn ensure_workload_ready(
        &mut self,
        index: usize,
    ) -> Result<(), RealClusterError> {
        self.wait_workload_bridge(index).await?;
        self.ensure_workload_endpoint(index)
    }

    async fn wait_workload_bridge(&mut self, index: usize) -> Result<(), RealClusterError> {
        let namespace = self.node(index)?.namespace.clone();
        let deadline = tokio::time::Instant::now() + SETUP_TIMEOUT;
        loop {
            self.ensure_children_running()?;
            let status = Command::new("ip")
                .args(["-n", &namespace, "link", "show", "maestro0"])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .map_err(RealClusterError::from_display)?;
            if status.success() {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(RealClusterError::new(format!(
                    "workload bridge did not appear in namespace `{namespace}`"
                )));
            }
            tokio::time::sleep(RETRY_DELAY).await;
        }
    }

    fn ensure_workload_endpoint(&self, index: usize) -> Result<(), RealClusterError> {
        let node = self.node(index)?;
        let existing = Command::new("ip")
            .args(["-n", &node.workload_namespace, "link", "show", "eth0"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map_err(RealClusterError::from_display)?;
        if existing.success() {
            return Ok(());
        }
        let commands = [
            vec![
                "link",
                "add",
                &node.workload_host_veth,
                "type",
                "veth",
                "peer",
                "name",
                &node.workload_peer_veth,
            ],
            vec![
                "link",
                "set",
                &node.workload_host_veth,
                "netns",
                &node.namespace,
            ],
            vec![
                "link",
                "set",
                &node.workload_peer_veth,
                "netns",
                &node.workload_namespace,
            ],
            vec![
                "-n",
                &node.namespace,
                "link",
                "set",
                &node.workload_host_veth,
                "master",
                "maestro0",
            ],
            vec![
                "-n",
                &node.namespace,
                "link",
                "set",
                &node.workload_host_veth,
                "mtu",
                "1420",
                "up",
            ],
            vec![
                "-n",
                &node.workload_namespace,
                "link",
                "set",
                &node.workload_peer_veth,
                "name",
                "eth0",
            ],
            vec![
                "-n",
                &node.workload_namespace,
                "link",
                "set",
                "eth0",
                "mtu",
                "1420",
                "up",
            ],
        ];
        for arguments in commands {
            run_checked("ip", arguments).map_err(RealClusterError::from_display)?;
        }
        run_checked(
            "ip",
            [
                "-n",
                &node.workload_namespace,
                "address",
                "add",
                &format!("{}/24", node.workload_address),
                "dev",
                "eth0",
            ],
        )
        .map_err(RealClusterError::from_display)?;
        run_checked(
            "ip",
            [
                "-n",
                &node.workload_namespace,
                "route",
                "add",
                "default",
                "via",
                &node.workload_gateway.to_string(),
            ],
        )
        .map_err(RealClusterError::from_display)
    }
}

impl Drop for RealProcessCluster {
    fn drop(&mut self) {
        for node in &mut self.nodes {
            if let Some(mut child) = node.child.take() {
                let _ = Command::new("kill")
                    .args(["-TERM", &child.id().to_string()])
                    .status();
                for _attempt in 0..50 {
                    if child.try_wait().ok().flatten().is_some() {
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(100));
                }
                if child.try_wait().ok().flatten().is_none() {
                    let _ = child.kill();
                    let _ = child.wait();
                }
                kill_process_group(child.id());
            }
            kill_namespace_processes(&node.workload_namespace);
            let _ = Command::new("ip")
                .args(["netns", "delete", &node.workload_namespace])
                .status();
            kill_namespace_processes(&node.namespace);
            let _ = Command::new("ip")
                .args(["netns", "delete", &node.namespace])
                .status();
        }
        let volatile_root = PathBuf::from("/run/maestro").join(self.cluster.cluster_id.as_str());
        let _ = std::fs::remove_dir_all(volatile_root);
        let _ = Command::new("ctr")
            .args([
                "--address",
                self.containerd_socket.to_string_lossy().as_ref(),
                "namespaces",
                "remove",
                &format!("maestro-{}", self.cluster.cluster_id),
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        let _ = Command::new("ip")
            .args(["link", "delete", &self.bridge])
            .status();
        let _root_path = self.root.path();
    }
}

fn run_checked<I, S>(program: &str, arguments: I) -> Result<(), Box<dyn std::error::Error>>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let output = Command::new(program).args(arguments).output()?;
    if output.status.success() {
        Ok(())
    } else {
        Err(format!(
            "`{program}` failed with {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        )
        .into())
    }
}

pub(super) fn shortened_interface_name(prefix: &str, token: &str, index: usize) -> String {
    let available = 15_usize.saturating_sub(prefix.len() + index.to_string().len());
    let shortened = token.chars().take(available).collect::<String>();
    format!("{prefix}{shortened}{index}")
}

pub(super) fn kill_namespace_processes(namespace: &str) {
    let Ok(output) = Command::new("ip")
        .args(["netns", "pids", namespace])
        .output()
    else {
        return;
    };
    for pid in String::from_utf8_lossy(&output.stdout).split_whitespace() {
        let _ = Command::new("kill").args(["-KILL", pid]).status();
    }
}

pub(super) fn node_namespace_diagnostics(namespace: &str) -> String {
    diagnostics([
        ("addresses", vec!["-n", namespace, "address", "show"]),
        ("routes", vec!["-n", namespace, "route", "show"]),
        ("wireguard", vec!["netns", "exec", namespace, "wg", "show"]),
        (
            "firewall",
            vec!["netns", "exec", namespace, "nft", "list", "ruleset"],
        ),
    ])
}

pub(super) fn workload_namespace_diagnostics(namespace: &str) -> String {
    diagnostics([
        ("addresses", vec!["-n", namespace, "address", "show"]),
        ("routes", vec!["-n", namespace, "route", "show"]),
    ])
}

fn diagnostics<const COUNT: usize>(commands: [(&str, Vec<&str>); COUNT]) -> String {
    commands
        .into_iter()
        .map(|(label, arguments)| {
            let output = Command::new("ip").args(arguments).output();
            match output {
                Ok(output) => format!(
                    "{label}=[status {}; stdout={}; stderr={}]",
                    output.status,
                    String::from_utf8_lossy(&output.stdout).trim(),
                    String::from_utf8_lossy(&output.stderr).trim(),
                ),
                Err(error) => format!("{label}=[unavailable: {error}]"),
            }
        })
        .collect::<Vec<_>>()
        .join("; ")
}
