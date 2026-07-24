use std::collections::HashMap;

use docker::models::{
    ContainerBlkioStatEntry, ContainerBlkioStats, ContainerCpuStats, ContainerCpuUsage,
    ContainerMemoryStats, ContainerNetworkStats, ContainerPidsStats, ContainerStatsResponse,
    ContainerThrottlingData,
};

use crate::docker_stats::normalize_stats;
use crate::{RuntimeError, WorkloadNetworkStats};

#[test]
fn docker_stats_are_normalized_without_host_cgroup_access() {
    let response = ContainerStatsResponse {
        cpu_stats: Some(ContainerCpuStats {
            cpu_usage: Some(ContainerCpuUsage {
                total_usage: Some(9_000_000),
                usage_in_usermode: Some(6_000_000),
                usage_in_kernelmode: Some(3_000_000),
                ..Default::default()
            }),
            throttling_data: Some(ContainerThrottlingData {
                periods: Some(12),
                throttled_periods: Some(2),
                throttled_time: Some(500_000),
            }),
            ..Default::default()
        }),
        memory_stats: Some(ContainerMemoryStats {
            usage: Some(4_096),
            limit: Some(8_192),
            stats: Some(HashMap::from([
                ("low".to_owned(), 1),
                ("high".to_owned(), 2),
                ("max".to_owned(), 3),
                ("oom".to_owned(), 4),
                ("oom_kill".to_owned(), 5),
                ("oom_group_kill".to_owned(), 6),
            ])),
            ..Default::default()
        }),
        pids_stats: Some(ContainerPidsStats {
            current: Some(7),
            limit: Some(64),
        }),
        blkio_stats: Some(ContainerBlkioStats {
            io_service_bytes_recursive: Some(vec![
                io_entry("Read", 10),
                io_entry("read", 20),
                io_entry("Write", 30),
                io_entry("Discard", 40),
            ]),
            io_serviced_recursive: Some(vec![
                io_entry("Read", 1),
                io_entry("Write", 2),
                io_entry("Discard", 3),
            ]),
            ..Default::default()
        }),
        networks: Some(HashMap::from([
            (
                "eth0".to_owned(),
                ContainerNetworkStats {
                    rx_bytes: Some(100),
                    tx_bytes: Some(200),
                    ..Default::default()
                },
            ),
            (
                "eth1".to_owned(),
                ContainerNetworkStats {
                    rx_bytes: Some(300),
                    tx_bytes: Some(400),
                    ..Default::default()
                },
            ),
        ])),
        ..Default::default()
    };

    let sample = normalize_stats(&response).unwrap();

    assert_eq!(sample.resources.cpu.usage_usec, 9_000);
    assert_eq!(sample.resources.cpu.user_usec, 6_000);
    assert_eq!(sample.resources.cpu.system_usec, 3_000);
    assert_eq!(sample.resources.cpu.periods, 12);
    assert_eq!(sample.resources.cpu.throttled_periods, 2);
    assert_eq!(sample.resources.cpu.throttled_usec, 500);
    assert_eq!(sample.resources.memory.current_bytes, 4_096);
    assert_eq!(sample.resources.memory.maximum_bytes, Some(8_192));
    assert_eq!(sample.resources.memory.events.out_of_memory_kills, 5);
    assert_eq!(sample.resources.io.read_bytes, 30);
    assert_eq!(sample.resources.io.write_bytes, 30);
    assert_eq!(sample.resources.io.discarded_bytes, 40);
    assert_eq!(sample.resources.io.read_operations, 1);
    assert_eq!(sample.resources.io.write_operations, 2);
    assert_eq!(sample.resources.io.discard_operations, 3);
    assert_eq!(sample.resources.processes.current, 7);
    assert_eq!(sample.resources.processes.maximum, Some(64));
    assert_eq!(
        sample.network,
        Some(WorkloadNetworkStats {
            receive_bytes: 400,
            transmit_bytes: 600,
        })
    );
}

#[test]
fn docker_stats_reject_missing_primary_counters() {
    assert!(matches!(
        normalize_stats(&ContainerStatsResponse::default()),
        Err(RuntimeError::Unavailable { .. })
    ));
}

fn io_entry(operation: &str, value: u64) -> ContainerBlkioStatEntry {
    ContainerBlkioStatEntry {
        op: Some(operation.to_owned()),
        value: Some(value),
        ..Default::default()
    }
}
