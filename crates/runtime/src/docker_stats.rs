use std::collections::HashMap;

use docker::models::{ContainerBlkioStatEntry, ContainerNetworkStats, ContainerStatsResponse};

use crate::{
    RuntimeError, WorkloadCpuStats, WorkloadIoStats, WorkloadMemoryEvents, WorkloadMemoryStats,
    WorkloadNetworkStats, WorkloadProcessStats, WorkloadResourceStats, WorkloadStatsSnapshot,
};

const NANOSECONDS_PER_MICROSECOND: u64 = 1_000;

pub(crate) fn normalize_stats(
    response: &ContainerStatsResponse,
) -> Result<WorkloadStatsSnapshot, RuntimeError> {
    let cpu = response
        .cpu_stats
        .as_ref()
        .ok_or_else(|| missing("cpu_stats"))?;
    let usage = cpu
        .cpu_usage
        .as_ref()
        .ok_or_else(|| missing("cpu_stats.cpu_usage"))?;
    let memory = response
        .memory_stats
        .as_ref()
        .ok_or_else(|| missing("memory_stats"))?;
    let pids = response
        .pids_stats
        .as_ref()
        .ok_or_else(|| missing("pids_stats"))?;
    let memory_values = memory.stats.as_ref();
    let throttling = cpu.throttling_data.as_ref();
    Ok(WorkloadStatsSnapshot {
        resources: WorkloadResourceStats {
            cpu: WorkloadCpuStats {
                usage_usec: nanoseconds_to_microseconds(required(
                    usage.total_usage,
                    "cpu_stats.cpu_usage.total_usage",
                )?),
                user_usec: nanoseconds_to_microseconds(usage.usage_in_usermode.unwrap_or_default()),
                system_usec: nanoseconds_to_microseconds(
                    usage.usage_in_kernelmode.unwrap_or_default(),
                ),
                periods: throttling
                    .and_then(|stats| stats.periods)
                    .unwrap_or_default(),
                throttled_periods: throttling
                    .and_then(|stats| stats.throttled_periods)
                    .unwrap_or_default(),
                throttled_usec: nanoseconds_to_microseconds(
                    throttling
                        .and_then(|stats| stats.throttled_time)
                        .unwrap_or_default(),
                ),
            },
            memory: WorkloadMemoryStats {
                current_bytes: required(memory.usage, "memory_stats.usage")?,
                maximum_bytes: nonzero(memory.limit),
                events: WorkloadMemoryEvents {
                    low: memory_value(memory_values, "low"),
                    high: memory_value(memory_values, "high"),
                    maximum: memory_value(memory_values, "max")
                        .max(memory.failcnt.unwrap_or_default()),
                    out_of_memory: memory_value(memory_values, "oom"),
                    out_of_memory_kills: memory_value(memory_values, "oom_kill"),
                    out_of_memory_group_kills: memory_value(memory_values, "oom_group_kill"),
                },
            },
            io: normalize_io(response)?,
            processes: WorkloadProcessStats {
                current: required(pids.current, "pids_stats.current")?,
                maximum: nonzero(pids.limit),
            },
        },
        network: normalize_network(response.networks.as_ref())?,
    })
}

fn normalize_io(response: &ContainerStatsResponse) -> Result<WorkloadIoStats, RuntimeError> {
    let block = response.blkio_stats.as_ref();
    let bytes = block.and_then(|stats| stats.io_service_bytes_recursive.as_deref());
    let operations = block.and_then(|stats| stats.io_serviced_recursive.as_deref());
    Ok(WorkloadIoStats {
        read_bytes: sum_io(bytes, "read", "io read bytes")?,
        write_bytes: sum_io(bytes, "write", "io write bytes")?,
        read_operations: sum_io(operations, "read", "io read operations")?,
        write_operations: sum_io(operations, "write", "io write operations")?,
        discarded_bytes: sum_io(bytes, "discard", "io discarded bytes")?,
        discard_operations: sum_io(operations, "discard", "io discard operations")?,
    })
}

fn sum_io(
    entries: Option<&[ContainerBlkioStatEntry]>,
    operation: &str,
    counter: &'static str,
) -> Result<u64, RuntimeError> {
    entries
        .unwrap_or_default()
        .iter()
        .filter(|entry| {
            entry
                .op
                .as_deref()
                .is_some_and(|value| value.eq_ignore_ascii_case(operation))
        })
        .try_fold(0_u64, |total, entry| {
            total
                .checked_add(entry.value.unwrap_or_default())
                .ok_or_else(|| overflow(counter))
        })
}

fn normalize_network(
    networks: Option<&HashMap<String, ContainerNetworkStats>>,
) -> Result<Option<WorkloadNetworkStats>, RuntimeError> {
    let Some(networks) = networks.filter(|networks| !networks.is_empty()) else {
        return Ok(None);
    };
    let (receive_bytes, transmit_bytes) =
        networks
            .values()
            .try_fold((0_u64, 0_u64), |(receive, transmit), network| {
                let receive = receive
                    .checked_add(network.rx_bytes.unwrap_or_default())
                    .ok_or_else(|| overflow("network receive bytes"))?;
                let transmit = transmit
                    .checked_add(network.tx_bytes.unwrap_or_default())
                    .ok_or_else(|| overflow("network transmit bytes"))?;
                Ok::<_, RuntimeError>((receive, transmit))
            })?;
    Ok(Some(WorkloadNetworkStats {
        receive_bytes,
        transmit_bytes,
    }))
}

fn memory_value(values: Option<&HashMap<String, u64>>, key: &str) -> u64 {
    values
        .and_then(|values| values.get(key))
        .copied()
        .unwrap_or_default()
}

fn required(value: Option<u64>, field: &'static str) -> Result<u64, RuntimeError> {
    value.ok_or_else(|| missing(field))
}

fn nonzero(value: Option<u64>) -> Option<u64> {
    value.filter(|value| *value != 0)
}

fn nanoseconds_to_microseconds(value: u64) -> u64 {
    value / NANOSECONDS_PER_MICROSECOND
}

fn missing(field: &'static str) -> RuntimeError {
    RuntimeError::Unavailable {
        message: format!("Docker stats response omitted `{field}`"),
    }
}

fn overflow(counter: &'static str) -> RuntimeError {
    RuntimeError::Rejected {
        message: format!("Docker stats `{counter}` counter overflowed"),
    }
}
