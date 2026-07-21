use std::collections::BTreeSet;
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use runtime::{NetworkProvider, NetworkProviderError, WorkloadHandle};

const MAX_COUNTER_BYTES: u64 = 64;

/// Cumulative network counters for all host interfaces owned by one workload.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WorkloadNetworkStats {
    /// Bytes received by the workload since interface creation.
    pub receive_bytes: u64,
    /// Bytes transmitted by the workload since interface creation.
    pub transmit_bytes: u64,
}

/// Optional workload network counter boundary used by the stats agent.
#[async_trait]
pub trait WorkloadNetworkStatsReader: Send + Sync + 'static {
    /// Reads cumulative counters, returning `None` when the backend exposes no host interface.
    async fn read(
        &self,
        workload: &WorkloadHandle,
    ) -> Result<Option<WorkloadNetworkStats>, WorkloadNetworkStatsError>;
}

/// Runtime-attachment-aware Linux sysfs network counter reader.
pub struct HostNetworkStatsReader {
    network: Arc<dyn NetworkProvider>,
    sys_class_net: PathBuf,
}

impl HostNetworkStatsReader {
    /// Uses the production Linux network-class path.
    pub fn production(network: Arc<dyn NetworkProvider>) -> Self {
        Self {
            network,
            sys_class_net: PathBuf::from("/sys/class/net"),
        }
    }

    /// Uses an explicit absolute sysfs-compatible root for deterministic composition tests.
    pub fn new(
        network: Arc<dyn NetworkProvider>,
        sys_class_net: PathBuf,
    ) -> Result<Self, WorkloadNetworkStatsError> {
        if !sys_class_net.is_absolute() {
            return Err(WorkloadNetworkStatsError::InvalidRoot {
                path: sys_class_net,
            });
        }
        Ok(Self {
            network,
            sys_class_net,
        })
    }
}

#[async_trait]
impl WorkloadNetworkStatsReader for HostNetworkStatsReader {
    async fn read(
        &self,
        workload: &WorkloadHandle,
    ) -> Result<Option<WorkloadNetworkStats>, WorkloadNetworkStatsError> {
        let interfaces = self
            .network
            .inspect(workload)
            .await?
            .attachments
            .into_iter()
            .filter_map(|attachment| attachment.interface_name)
            .collect::<BTreeSet<_>>();
        if interfaces.is_empty() {
            return Ok(None);
        }
        let root = self.sys_class_net.clone();
        tokio::task::spawn_blocking(move || read_interfaces(&root, &interfaces))
            .await
            .map_err(|error| WorkloadNetworkStatsError::Task {
                message: error.to_string(),
            })?
            .map(Some)
    }
}

fn read_interfaces(
    root: &Path,
    interfaces: &BTreeSet<String>,
) -> Result<WorkloadNetworkStats, WorkloadNetworkStatsError> {
    let mut stats = WorkloadNetworkStats::default();
    for interface in interfaces {
        validate_interface(interface)?;
        let statistics = root.join(interface).join("statistics");
        // Runtime attachments expose the host end of each veth pair. Traffic sent by
        // that host interface is received by the workload, while host RX is workload TX.
        stats.receive_bytes = stats
            .receive_bytes
            .checked_add(read_counter(&statistics.join("tx_bytes"))?)
            .ok_or(WorkloadNetworkStatsError::CounterOverflow)?;
        stats.transmit_bytes = stats
            .transmit_bytes
            .checked_add(read_counter(&statistics.join("rx_bytes"))?)
            .ok_or(WorkloadNetworkStatsError::CounterOverflow)?;
    }
    Ok(stats)
}

fn validate_interface(interface: &str) -> Result<(), WorkloadNetworkStatsError> {
    if interface.is_empty()
        || interface.len() > 15
        || matches!(interface, "." | "..")
        || !interface
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        Err(WorkloadNetworkStatsError::InvalidInterface {
            name: interface.to_owned(),
        })
    } else {
        Ok(())
    }
}

fn read_counter(path: &Path) -> Result<u64, WorkloadNetworkStatsError> {
    let file = File::open(path).map_err(|source| WorkloadNetworkStatsError::Io {
        action: "open",
        path: path.to_path_buf(),
        source,
    })?;
    let mut contents = String::new();
    file.take(MAX_COUNTER_BYTES.saturating_add(1))
        .read_to_string(&mut contents)
        .map_err(|source| WorkloadNetworkStatsError::Io {
            action: "read",
            path: path.to_path_buf(),
            source,
        })?;
    if contents.len() as u64 > MAX_COUNTER_BYTES {
        return Err(WorkloadNetworkStatsError::CounterTooLarge {
            path: path.to_path_buf(),
        });
    }
    contents
        .trim()
        .parse::<u64>()
        .map_err(|_| WorkloadNetworkStatsError::InvalidCounter {
            path: path.to_path_buf(),
        })
}

/// A runtime attachment or Linux interface counter could not be sampled safely.
#[derive(Debug, thiserror::Error)]
pub enum WorkloadNetworkStatsError {
    /// A configurable sysfs root was not absolute.
    #[error("network stats root must be absolute: `{}`", path.display())]
    InvalidRoot { path: PathBuf },
    /// A backend interface name could escape or ambiguously address sysfs.
    #[error("network stats interface name is unsafe: `{name}`")]
    InvalidInterface { name: String },
    /// Runtime network inspection was unavailable or rejected.
    #[error(transparent)]
    Network(#[from] NetworkProviderError),
    /// The bounded filesystem reader task could not be joined.
    #[error("network stats reader task failed: {message}")]
    Task { message: String },
    /// A sysfs counter could not be opened or read.
    #[error("failed to {action} network counter `{}`: {source}", path.display())]
    Io {
        action: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    /// A counter exceeded its strict text bound.
    #[error("network counter is too large: `{}`", path.display())]
    CounterTooLarge { path: PathBuf },
    /// A counter was not an unsigned decimal integer.
    #[error("network counter is invalid: `{}`", path.display())]
    InvalidCounter { path: PathBuf },
    /// Summing multiple owned interfaces exceeded the counter type.
    #[error("workload network counters overflowed")]
    CounterOverflow,
}

#[cfg(test)]
#[path = "tests/network_stats.rs"]
mod tests;
