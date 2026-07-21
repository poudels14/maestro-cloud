use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use kernel_api::{ClusterId, NodeId, Timestamp};
use kernel_store::{Clock, MonotonicTime};
use tokio::sync::{Mutex, Notify, watch};

use crate::{
    HostCpuStats, HostDiskError, HostDiskFailure, HostDiskReader, HostDiskReport, HostDiskStats,
    HostMemoryStats, HostNetworkStats, HostResourceStats, HostStatsError, HostStatsReader,
    HostTelemetryAgent, HostTelemetryFailureStage, HostTelemetrySample, HostTelemetrySettings,
    HostTelemetrySink, HostTelemetrySinkError, StatusClock,
};

#[tokio::test]
async fn host_agent_collects_and_delivers_one_complete_snapshot()
-> Result<(), Box<dyn std::error::Error>> {
    let sink = Arc::new(RecordingSink::default());
    let agent = agent(
        Arc::new(FixedResourceReader { fail: false }),
        Arc::new(FixedDiskReader {
            fail: false,
            capacity_failure: true,
        }),
        sink.clone(),
        Arc::new(ManualClock::default()),
    )?;

    let report = agent.collect().await;

    assert!(report.delivered);
    assert!(report.delivery_failure.is_none());
    assert_eq!(report.failures.len(), 1);
    let failure = report.failures.first().expect("capacity failure");
    assert_eq!(failure.stage, HostTelemetryFailureStage::ReadDiskCapacity);
    assert_eq!(failure.mount_point.as_deref(), Some("/gone"));
    let sample = report.sample.as_ref().expect("host sample");
    assert_eq!(sample.cluster_id, cluster_id());
    assert_eq!(sample.node_id, node_id());
    assert_eq!(sample.collected_at, Timestamp(1_750_000_000_000));
    assert_eq!(sample.resources, Some(resources()));
    assert_eq!(sample.disks.as_deref(), Some(disks().as_slice()));
    let delivered = sink.samples.lock().await;
    assert_eq!(delivered.len(), 1);
    assert_eq!(delivered.first(), Some(sample));
    Ok(())
}

#[tokio::test]
async fn host_agent_delivers_healthy_parts_and_skips_only_when_both_readers_fail()
-> Result<(), Box<dyn std::error::Error>> {
    let sink = Arc::new(RecordingSink::default());
    let resource_failure = agent(
        Arc::new(FixedResourceReader { fail: true }),
        Arc::new(FixedDiskReader {
            fail: false,
            capacity_failure: false,
        }),
        sink.clone(),
        Arc::new(ManualClock::default()),
    )?
    .collect()
    .await;
    assert!(resource_failure.delivered);
    assert_eq!(resource_failure.failures.len(), 1);
    assert_eq!(
        resource_failure
            .failures
            .first()
            .expect("resource failure")
            .stage,
        HostTelemetryFailureStage::ReadResources
    );
    let partial = resource_failure.sample.expect("partial sample");
    assert!(partial.resources.is_none());
    assert!(partial.disks.is_some());

    let disk_failure = agent(
        Arc::new(FixedResourceReader { fail: false }),
        Arc::new(FixedDiskReader {
            fail: true,
            capacity_failure: false,
        }),
        sink.clone(),
        Arc::new(ManualClock::default()),
    )?
    .collect()
    .await;
    assert!(disk_failure.delivered);
    assert!(
        disk_failure
            .sample
            .as_ref()
            .is_some_and(|sample| sample.resources.is_some() && sample.disks.is_none())
    );
    assert_eq!(
        disk_failure.failures.first().expect("disk failure").stage,
        HostTelemetryFailureStage::ReadDisks
    );

    let both_failed = agent(
        Arc::new(FixedResourceReader { fail: true }),
        Arc::new(FixedDiskReader {
            fail: true,
            capacity_failure: false,
        }),
        sink.clone(),
        Arc::new(ManualClock::default()),
    )?
    .collect()
    .await;
    assert!(!both_failed.delivered);
    assert!(both_failed.sample.is_none());
    assert_eq!(both_failed.failures.len(), 2);
    assert_eq!(sink.samples.lock().await.len(), 2);
    Ok(())
}

#[tokio::test]
async fn host_agent_isolates_sink_failure_and_retries_on_the_next_snapshot()
-> Result<(), Box<dyn std::error::Error>> {
    let sink = Arc::new(RecordingSink::default());
    sink.fail_next.store(true, Ordering::Release);
    let agent = agent(
        Arc::new(FixedResourceReader { fail: false }),
        Arc::new(FixedDiskReader {
            fail: false,
            capacity_failure: false,
        }),
        sink.clone(),
        Arc::new(ManualClock::default()),
    )?;

    let failed = agent.collect().await;
    assert!(!failed.delivered);
    assert!(matches!(
        failed.delivery_failure,
        Some(HostTelemetrySinkError::Unavailable { .. })
    ));
    assert!(sink.samples.lock().await.is_empty());

    let recovered = agent.collect().await;
    assert!(recovered.delivered);
    assert_eq!(sink.samples.lock().await.len(), 1);
    Ok(())
}

#[tokio::test]
async fn host_agent_runs_immediately_then_on_each_injected_deadline()
-> Result<(), Box<dyn std::error::Error>> {
    let sink = Arc::new(RecordingSink::default());
    let monotonic_clock = Arc::new(ManualClock::default());
    let agent = agent(
        Arc::new(FixedResourceReader { fail: false }),
        Arc::new(FixedDiskReader {
            fail: false,
            capacity_failure: false,
        }),
        sink.clone(),
        monotonic_clock.clone(),
    )?;
    let (shutdown, shutdown_receiver) = watch::channel(false);
    let first_delivery = sink.delivered.notified();
    let task = tokio::spawn(async move { agent.run(shutdown_receiver).await });

    first_delivery.await;
    assert_eq!(sink.samples.lock().await.len(), 1);
    let second_delivery = sink.delivered.notified();
    monotonic_clock.advance(Duration::from_secs(5));
    second_delivery.await;
    assert_eq!(sink.samples.lock().await.len(), 2);

    shutdown.send(true)?;
    task.await??;
    Ok(())
}

#[test]
fn host_agent_rejects_a_zero_poll_interval() {
    let mut settings = settings();
    settings.poll_interval = Duration::ZERO;
    assert!(
        HostTelemetryAgent::new(
            Arc::new(FixedResourceReader { fail: false }),
            Arc::new(FixedDiskReader {
                fail: false,
                capacity_failure: false,
            }),
            Arc::new(RecordingSink::default()),
            settings,
            Arc::new(FixedClock(Timestamp(1))),
            Arc::new(ManualClock::default()),
        )
        .is_err()
    );
}

fn agent(
    resource_reader: Arc<dyn HostStatsReader>,
    disk_reader: Arc<dyn HostDiskReader>,
    sink: Arc<dyn HostTelemetrySink>,
    monotonic_clock: Arc<dyn Clock>,
) -> Result<HostTelemetryAgent, crate::HostTelemetryAgentError> {
    HostTelemetryAgent::new(
        resource_reader,
        disk_reader,
        sink,
        settings(),
        Arc::new(FixedClock(Timestamp(1_750_000_000_000))),
        monotonic_clock,
    )
}

struct FixedResourceReader {
    fail: bool,
}

#[async_trait]
impl HostStatsReader for FixedResourceReader {
    async fn read(&self) -> Result<HostResourceStats, HostStatsError> {
        if self.fail {
            Err(HostStatsError::InvalidValue {
                file: "stat",
                value: "injected".to_owned(),
            })
        } else {
            Ok(resources())
        }
    }
}

struct FixedDiskReader {
    fail: bool,
    capacity_failure: bool,
}

#[async_trait]
impl HostDiskReader for FixedDiskReader {
    async fn read(&self) -> Result<HostDiskReport, HostDiskError> {
        if self.fail {
            return Err(HostDiskError::InvalidMount {
                line: "injected".to_owned(),
            });
        }
        Ok(HostDiskReport {
            disks: disks(),
            failures: self
                .capacity_failure
                .then(|| HostDiskFailure {
                    mount_point: "/gone".to_owned(),
                    message: "injected".to_owned(),
                })
                .into_iter()
                .collect(),
        })
    }
}

#[derive(Default)]
struct RecordingSink {
    samples: Mutex<Vec<HostTelemetrySample>>,
    delivered: Notify,
    fail_next: AtomicBool,
}

#[async_trait]
impl HostTelemetrySink for RecordingSink {
    async fn ingest(&self, sample: &HostTelemetrySample) -> Result<(), HostTelemetrySinkError> {
        if self.fail_next.swap(false, Ordering::AcqRel) {
            return Err(HostTelemetrySinkError::Unavailable {
                message: "injected sink outage".to_owned(),
            });
        }
        self.samples.lock().await.push(sample.clone());
        self.delivered.notify_one();
        Ok(())
    }
}

#[derive(Default)]
struct ManualClock {
    now_millis: AtomicU64,
    advanced: Notify,
}

impl ManualClock {
    fn advance(&self, duration: Duration) {
        let millis = u64::try_from(duration.as_millis()).unwrap_or(u64::MAX);
        self.now_millis.fetch_add(millis, Ordering::AcqRel);
        self.advanced.notify_waiters();
    }
}

#[async_trait]
impl Clock for ManualClock {
    fn now(&self) -> MonotonicTime {
        MonotonicTime::from_duration(Duration::from_millis(
            self.now_millis.load(Ordering::Acquire),
        ))
    }

    async fn sleep_until(&self, deadline: MonotonicTime) {
        loop {
            let advanced = self.advanced.notified();
            if self.now() >= deadline {
                return;
            }
            advanced.await;
        }
    }
}

struct FixedClock(Timestamp);

impl StatusClock for FixedClock {
    fn now(&self) -> Timestamp {
        self.0
    }
}

fn settings() -> HostTelemetrySettings {
    HostTelemetrySettings {
        cluster_id: cluster_id(),
        node_id: node_id(),
        poll_interval: Duration::from_secs(5),
    }
}

fn resources() -> HostResourceStats {
    HostResourceStats {
        cpu: HostCpuStats {
            total_ticks: 1_000,
            idle_ticks: 400,
        },
        memory: HostMemoryStats {
            used_bytes: 2_048,
            total_bytes: 4_096,
        },
        network: HostNetworkStats {
            receive_bytes: 100,
            transmit_bytes: 200,
        },
    }
}

fn disks() -> Vec<HostDiskStats> {
    vec![HostDiskStats {
        name: "/dev/vda".to_owned(),
        mount_point: "/".to_owned(),
        total_bytes: 10_000,
        available_bytes: 4_000,
        file_system: "ext4".to_owned(),
    }]
}

fn cluster_id() -> ClusterId {
    ClusterId::new("cluster-1").expect("cluster id")
}

fn node_id() -> NodeId {
    NodeId::new("node-1").expect("node id")
}
