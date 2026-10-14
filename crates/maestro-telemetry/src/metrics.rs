use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use serde::{Deserialize, Serialize};

use crate::resolver::ReplicaResolver;

const SCOPE_PREFIX: &str = "maestro-";
const SCOPE_SUFFIX: &str = ".scope";
const NODE_SOURCE: &str = "node";
const CLUSTER_SOURCE: &str = "cluster";
const CONTAINER_SOURCE_PREFIX: &str = "container:";
const SERVICE_SOURCE_PREFIX: &str = "service:";

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MetricPoint {
    pub ts: i64,
    pub source: String,
    pub cpu_percent: f64,
    pub memory_bytes: u64,
    pub memory_limit_bytes: u64,
    pub net_rx_bytes: u64,
    pub net_tx_bytes: u64,
}

pub struct MetricsCollector {
    cgroup_root: PathBuf,
    proc_root: PathBuf,
    resolver: Arc<dyn ReplicaResolver>,
    previous_node_cpu: Option<CpuTimes>,
    previous_containers: HashMap<String, (u64, Instant)>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct CpuTimes {
    busy: u64,
    total: u64,
}

impl MetricsCollector {
    pub fn new(
        cgroup_root: PathBuf,
        proc_root: PathBuf,
        resolver: Arc<dyn ReplicaResolver>,
    ) -> Self {
        Self {
            cgroup_root,
            proc_root,
            resolver,
            previous_node_cpu: None,
            previous_containers: HashMap::new(),
        }
    }

    pub fn collect(&mut self, ts: i64) -> Vec<MetricPoint> {
        let node = self.node_point(ts);
        let containers = self.container_points(ts, node.memory_limit_bytes);
        let mut services = BTreeMap::<String, MetricPoint>::new();
        containers.iter().for_each(|(replica_id, point)| {
            if let Some(service_id) = self.resolver.resolve(replica_id).service_id {
                let total = services
                    .entry(service_id.clone())
                    .or_insert_with(|| MetricPoint {
                        ts,
                        source: format!("{SERVICE_SOURCE_PREFIX}{service_id}"),
                        ..MetricPoint::default()
                    });
                add_into(total, point);
            }
        });
        let mut cluster = MetricPoint {
            ts,
            source: CLUSTER_SOURCE.to_string(),
            ..MetricPoint::default()
        };
        containers
            .iter()
            .for_each(|(_, point)| add_into(&mut cluster, point));
        [node, cluster]
            .into_iter()
            .chain(services.into_values())
            .chain(containers.into_iter().map(|(_, point)| point))
            .collect()
    }

    fn node_point(&mut self, ts: i64) -> MetricPoint {
        let cpu = read(&self.proc_root.join("stat")).and_then(|stat| parse_cpu_times(&stat));
        let cpu_percent = match (self.previous_node_cpu, cpu) {
            (Some(previous), Some(current)) if current.total > previous.total => {
                (current.busy.saturating_sub(previous.busy)) as f64 * 100.0
                    / (current.total - previous.total) as f64
            }
            _ => 0.0,
        };
        self.previous_node_cpu = cpu;
        let (memory_bytes, memory_limit_bytes) = read(&self.proc_root.join("meminfo"))
            .map(|meminfo| parse_meminfo(&meminfo))
            .unwrap_or_default();
        let (net_rx_bytes, net_tx_bytes) = read(&self.proc_root.join("net/dev"))
            .map(|dev| parse_net_dev(&dev))
            .unwrap_or_default();
        MetricPoint {
            ts,
            source: NODE_SOURCE.to_string(),
            cpu_percent,
            memory_bytes,
            memory_limit_bytes,
            net_rx_bytes,
            net_tx_bytes,
        }
    }

    fn container_points(&mut self, ts: i64, node_memory: u64) -> Vec<(String, MetricPoint)> {
        let now = Instant::now();
        let scopes = std::fs::read_dir(&self.cgroup_root)
            .into_iter()
            .flatten()
            .flatten()
            .filter_map(|entry| {
                let name = entry.file_name().to_string_lossy().to_string();
                let replica_id = name
                    .strip_prefix(SCOPE_PREFIX)?
                    .strip_suffix(SCOPE_SUFFIX)?
                    .to_string();
                Some((replica_id, entry.path()))
            })
            .collect::<Vec<_>>();
        let points = scopes
            .iter()
            .map(|(replica_id, scope)| {
                let usage =
                    read(&scope.join("cpu.stat")).and_then(|stat| parse_cpu_usage_usec(&stat));
                let cpu_percent = match (self.previous_containers.get(replica_id), usage) {
                    (Some((previous, at)), Some(current)) => {
                        let elapsed = now.duration_since(*at).as_micros().max(1) as f64;
                        current.saturating_sub(*previous) as f64 * 100.0 / elapsed
                    }
                    _ => 0.0,
                };
                if let Some(usage) = usage {
                    self.previous_containers
                        .insert(replica_id.clone(), (usage, now));
                }
                let memory_bytes = read(&scope.join("memory.current"))
                    .and_then(|value| value.trim().parse().ok())
                    .unwrap_or_default();
                let memory_limit_bytes = read(&scope.join("memory.max"))
                    .and_then(|value| value.trim().parse().ok())
                    .unwrap_or(node_memory);
                let (net_rx_bytes, net_tx_bytes) = read(&scope.join("cgroup.procs"))
                    .and_then(|procs| procs.lines().next().map(str::to_string))
                    .and_then(|pid| read(&self.proc_root.join(pid.trim()).join("net/dev")))
                    .map(|dev| parse_net_dev(&dev))
                    .unwrap_or_default();
                (
                    replica_id.clone(),
                    MetricPoint {
                        ts,
                        source: format!("{CONTAINER_SOURCE_PREFIX}{replica_id}"),
                        cpu_percent,
                        memory_bytes,
                        memory_limit_bytes,
                        net_rx_bytes,
                        net_tx_bytes,
                    },
                )
            })
            .collect::<Vec<_>>();
        self.previous_containers
            .retain(|replica_id, _| scopes.iter().any(|(live, _)| live == replica_id));
        points
    }
}

fn add_into(total: &mut MetricPoint, point: &MetricPoint) {
    total.cpu_percent += point.cpu_percent;
    total.memory_bytes += point.memory_bytes;
    total.memory_limit_bytes += point.memory_limit_bytes;
    total.net_rx_bytes += point.net_rx_bytes;
    total.net_tx_bytes += point.net_tx_bytes;
}

fn read(path: &Path) -> Option<String> {
    std::fs::read_to_string(path).ok()
}

fn parse_cpu_times(stat: &str) -> Option<CpuTimes> {
    let values = stat
        .lines()
        .find(|line| line.starts_with("cpu "))?
        .split_whitespace()
        .skip(1)
        .filter_map(|value| value.parse::<u64>().ok())
        .collect::<Vec<_>>();
    let total = values.iter().take(8).sum::<u64>();
    let idle =
        values.get(3).copied().unwrap_or_default() + values.get(4).copied().unwrap_or_default();
    Some(CpuTimes {
        busy: total.saturating_sub(idle),
        total,
    })
}

fn parse_meminfo(meminfo: &str) -> (u64, u64) {
    let field = |name: &str| {
        meminfo
            .lines()
            .find(|line| line.starts_with(name))
            .and_then(|line| line.split_whitespace().nth(1))
            .and_then(|value| value.parse::<u64>().ok())
            .map(|kib| kib * 1024)
            .unwrap_or_default()
    };
    let total = field("MemTotal:");
    (total.saturating_sub(field("MemAvailable:")), total)
}

fn parse_net_dev(dev: &str) -> (u64, u64) {
    dev.lines()
        .skip(2)
        .filter_map(|line| line.split_once(':'))
        .filter(|(interface, _)| interface.trim() != "lo")
        .map(|(_, counters)| {
            let values = counters
                .split_whitespace()
                .filter_map(|value| value.parse::<u64>().ok())
                .collect::<Vec<_>>();
            (
                values.first().copied().unwrap_or_default(),
                values.get(8).copied().unwrap_or_default(),
            )
        })
        .fold((0, 0), |(rx, tx), (line_rx, line_tx)| {
            (rx + line_rx, tx + line_tx)
        })
}

fn parse_cpu_usage_usec(stat: &str) -> Option<u64> {
    stat.lines()
        .find_map(|line| line.strip_prefix("usage_usec "))
        .and_then(|value| value.trim().parse().ok())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::resolver::ReplicaIdentity;

    #[test]
    fn parses_proc_files() {
        let stat = "cpu  100 0 50 800 50 0 0 0 0 0\ncpu0 1 2 3 4\n";
        assert_eq!(
            parse_cpu_times(stat),
            Some(CpuTimes {
                busy: 150,
                total: 1000
            })
        );
        assert_eq!(
            parse_meminfo("MemTotal:  1000 kB\nMemFree: 10 kB\nMemAvailable:   400 kB\n"),
            (600 * 1024, 1000 * 1024)
        );
        let dev = "Inter-|   Receive\n face |bytes packets\n    lo: 999 1 0 0 0 0 0 0 999 1 0 0 0 0 0 0\n  eth0: 100 2 0 0 0 0 0 0 40 3 0 0 0 0 0 0\n  wg0: 5 1 0 0 0 0 0 0 6 1 0 0 0 0 0 0\n";
        assert_eq!(parse_net_dev(dev), (105, 46));
        assert_eq!(
            parse_cpu_usage_usec("usage_usec 12345\nuser_usec 1\n"),
            Some(12345)
        );
    }

    #[test]
    fn collects_replica_service_and_cluster_points_from_cgroups() {
        let root = tempfile::tempdir().unwrap();
        let cgroups = root.path().join("cgroups");
        let proc_root = root.path().join("proc");
        let scope = cgroups.join("maestro-d1-0.scope");
        std::fs::create_dir_all(&scope).unwrap();
        std::fs::create_dir_all(proc_root.join("42/net")).unwrap();
        std::fs::write(scope.join("cpu.stat"), "usage_usec 1000\n").unwrap();
        std::fs::write(scope.join("memory.current"), "2048\n").unwrap();
        std::fs::write(scope.join("memory.max"), "max\n").unwrap();
        std::fs::write(scope.join("cgroup.procs"), "42\n").unwrap();
        std::fs::write(
            proc_root.join("42/net/dev"),
            "h\nh\n  eth0: 7 0 0 0 0 0 0 0 9 0 0 0 0 0 0 0\n",
        )
        .unwrap();
        std::fs::write(
            proc_root.join("meminfo"),
            "MemTotal: 4 kB\nMemAvailable: 1 kB\n",
        )
        .unwrap();
        let resolver: HashMap<String, ReplicaIdentity> = HashMap::from([(
            "d1-0".to_string(),
            ReplicaIdentity {
                service_id: Some("web".to_string()),
                deployment_id: Some("d1".to_string()),
                healthcheck_path: None,
            },
        )]);
        let mut collector = MetricsCollector::new(cgroups, proc_root, Arc::new(resolver));

        let points = collector.collect(1);
        let sources = points
            .iter()
            .map(|point| point.source.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            sources,
            ["node", "cluster", "service:web", "container:d1-0"]
        );
        let container = &points[3];
        assert_eq!(container.memory_bytes, 2048);
        assert_eq!(container.memory_limit_bytes, 4096);
        assert_eq!((container.net_rx_bytes, container.net_tx_bytes), (7, 9));
        assert_eq!(points[2].memory_bytes, 2048);
        assert_eq!(points[0].memory_bytes, 3072);
    }
}
