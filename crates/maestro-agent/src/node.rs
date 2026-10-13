use maestro_core::{ByteSize, Millicores, NodeCapacity, ReserveSettings};
use tokio::process::Command;

use crate::error::AgentError;

const MIN_MEMORY_RESERVE: u64 = 2 << 30;
const MEMORY_RESERVE_PERCENT: u64 = 10;
const DEFAULT_CPU_RESERVE: Millicores = Millicores::new(1000);
const MEMORY_HIGH_PERCENT: u64 = 90;
const WORKLOADS_SLICE: &str = "maestro-workloads.slice";

pub fn detect_capacity() -> Result<NodeCapacity, AgentError> {
    let cores = std::thread::available_parallelism().map_or(1, |cores| cores.get()) as u32;
    let meminfo = std::fs::read_to_string("/proc/meminfo")
        .map_err(|error| AgentError::Host(format!("failed to read /proc/meminfo: {error}")))?;
    let memory = parse_mem_total(&meminfo)
        .ok_or_else(|| AgentError::Host("MemTotal missing from /proc/meminfo".to_string()))?;
    Ok(NodeCapacity {
        cpu: Millicores::new(cores * 1000),
        memory,
    })
}

pub fn system_reserve(capacity: &NodeCapacity, settings: &ReserveSettings) -> NodeCapacity {
    let default_memory =
        (capacity.memory.bytes() * MEMORY_RESERVE_PERCENT / 100).max(MIN_MEMORY_RESERVE);
    NodeCapacity {
        cpu: settings
            .cpu
            .unwrap_or(DEFAULT_CPU_RESERVE)
            .min(capacity.cpu),
        memory: settings
            .memory
            .unwrap_or(ByteSize::new(default_memory))
            .min(capacity.memory),
    }
}

pub async fn apply_workload_slice_limits(
    capacity: &NodeCapacity,
    reserve: &NodeCapacity,
) -> Result<(), AgentError> {
    let memory_max = capacity.memory.saturating_sub(reserve.memory).bytes();
    let memory_high = memory_max * MEMORY_HIGH_PERCENT / 100;
    let output = Command::new("systemctl")
        .args([
            "set-property",
            "--runtime",
            WORKLOADS_SLICE,
            &format!("MemoryMax={memory_max}"),
            &format!("MemoryHigh={memory_high}"),
            "MemorySwapMax=0",
        ])
        .output()
        .await
        .map_err(|error| AgentError::Host(format!("failed to run systemctl: {error}")))?;
    match output.status.success() {
        true => Ok(()),
        false => Err(AgentError::Host(format!(
            "failed to set {WORKLOADS_SLICE} limits: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ))),
    }
}

fn parse_mem_total(meminfo: &str) -> Option<ByteSize> {
    meminfo
        .lines()
        .find_map(|line| line.strip_prefix("MemTotal:"))
        .and_then(|value| {
            value
                .trim()
                .trim_end_matches("kB")
                .trim()
                .parse::<u64>()
                .ok()
        })
        .map(|kibibytes| ByteSize::new(kibibytes * 1024))
}

#[cfg(test)]
mod tests {
    use super::*;

    const GIB: u64 = 1 << 30;

    #[test]
    fn parses_mem_total() {
        let meminfo = "MemTotal:        8131512 kB\nMemFree:          123 kB\n";
        assert_eq!(
            parse_mem_total(meminfo),
            Some(ByteSize::new(8_131_512 * 1024))
        );
        assert_eq!(parse_mem_total("MemFree: 1 kB"), None);
    }

    #[test]
    fn reserves_ten_percent_with_two_gib_floor() {
        let large = NodeCapacity {
            cpu: Millicores::new(16_000),
            memory: ByteSize::new(64 * GIB),
        };
        let small = NodeCapacity {
            cpu: Millicores::new(2_000),
            memory: ByteSize::new(8 * GIB),
        };
        let defaults = ReserveSettings::default();
        assert_eq!(
            system_reserve(&large, &defaults).memory,
            ByteSize::new(64 * GIB / 10)
        );
        assert_eq!(
            system_reserve(&small, &defaults).memory,
            ByteSize::new(2 * GIB)
        );
        assert_eq!(system_reserve(&small, &defaults).cpu, Millicores::new(1000));

        let explicit = ReserveSettings {
            cpu: Some(Millicores::new(500)),
            memory: Some(ByteSize::new(4 * GIB)),
        };
        assert_eq!(
            system_reserve(&large, &explicit),
            NodeCapacity {
                cpu: Millicores::new(500),
                memory: ByteSize::new(4 * GIB)
            }
        );
    }
}
