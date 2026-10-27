use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use maestro_core::{NodeId, NodeState, SlackCategory};
use maestro_store::Consensus;
use maestro_telemetry::Disk;

use crate::node_service::{NodeClients, NodeRequest, call_nodes};
use crate::slack::{SlackState, escape, notify};

const CHECK_INTERVAL: Duration = Duration::from_secs(5 * 60);
const REPEAT_AFTER: Duration = Duration::from_secs(24 * 60 * 60);
const THRESHOLDS: [u8; 4] = [80, 85, 90, 95];
const GIB: f64 = (1u64 << 30) as f64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Alerted {
    threshold: u8,
    sent_at: Instant,
}

pub async fn run_disk_alerts(state: SlackState, consensus: Arc<Consensus>, clients: NodeClients) {
    let mut ticker = tokio::time::interval(CHECK_INTERVAL);
    let mut alerted = HashMap::<(NodeId, String), Alerted>::new();
    loop {
        ticker.tick().await;
        match consensus.is_leader() {
            false => alerted.clear(),
            true => {
                let nodes = consensus
                    .state()
                    .nodes()
                    .unwrap_or_default()
                    .into_iter()
                    .filter(|node| node.state != NodeState::Removed)
                    .collect();
                for (node_id, node_name, reply) in
                    call_nodes(&clients, nodes, NodeRequest::Disks).await
                {
                    let disks = reply
                        .ok()
                        .and_then(|disks| serde_json::from_value::<Vec<Disk>>(disks).ok())
                        .unwrap_or_default();
                    for disk in disks.iter().filter(|disk| disk.total_bytes > 0) {
                        let key = (node_id, disk.mount_point.clone());
                        let (next, send) = decide(
                            alerted.get(&key).copied(),
                            used_percent(disk),
                            Instant::now(),
                        );
                        if let (true, Some(next)) = (send, next) {
                            notify(
                                &state,
                                SlackCategory::Error,
                                &message(&state.cluster, &node_name, disk, next.threshold),
                            )
                            .await;
                        }
                        match next {
                            Some(next) => alerted.insert(key, next),
                            None => alerted.remove(&key),
                        };
                    }
                }
            }
        }
    }
}

fn decide(previous: Option<Alerted>, percent: f64, now: Instant) -> (Option<Alerted>, bool) {
    let level = THRESHOLDS
        .iter()
        .rev()
        .find(|threshold| percent >= f64::from(**threshold))
        .copied();
    match (level, previous) {
        (None, _) => (None, false),
        (Some(threshold), Some(previous))
            if threshold <= previous.threshold
                && now.duration_since(previous.sent_at) < REPEAT_AFTER =>
        {
            (
                Some(Alerted {
                    threshold,
                    sent_at: previous.sent_at,
                }),
                false,
            )
        }
        (Some(threshold), _) => (
            Some(Alerted {
                threshold,
                sent_at: now,
            }),
            true,
        ),
    }
}

fn used_percent(disk: &Disk) -> f64 {
    let used = disk.total_bytes.saturating_sub(disk.available_bytes);
    used as f64 * 100.0 / disk.total_bytes as f64
}

fn message(cluster: &str, node: &str, disk: &Disk, threshold: u8) -> String {
    format!(
        ":warning: *Disk {:.0}% full* — `{}` on `{}` (above {threshold}%)\n> cluster: `{cluster}` · free: `{:.1} GiB` of `{:.1} GiB`",
        used_percent(disk),
        escape(&disk.mount_point),
        escape(node),
        disk.available_bytes as f64 / GIB,
        disk.total_bytes as f64 / GIB,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn alerted(threshold: u8, sent_at: Instant) -> Option<Alerted> {
        Some(Alerted { threshold, sent_at })
    }

    #[test]
    fn alerts_when_a_threshold_is_first_crossed() {
        let now = Instant::now();
        assert_eq!(decide(None, 79.9, now), (None, false));
        assert_eq!(decide(None, 81.0, now), (alerted(80, now), true));
        assert_eq!(
            decide(alerted(80, now), 86.0, now + Duration::from_secs(60)),
            (alerted(85, now + Duration::from_secs(60)), true)
        );
        assert_eq!(decide(None, 97.0, now), (alerted(95, now), true));
    }

    #[test]
    fn repeats_only_after_a_day_at_the_same_level() {
        let now = Instant::now();
        let hour_later = now + Duration::from_secs(60 * 60);
        assert_eq!(
            decide(alerted(85, now), 87.0, hour_later),
            (alerted(85, now), false)
        );
        let day_later = now + REPEAT_AFTER;
        assert_eq!(
            decide(alerted(85, now), 87.0, day_later),
            (alerted(85, day_later), true)
        );
    }

    #[test]
    fn dropping_below_resets_so_the_next_crossing_alerts() {
        let now = Instant::now();
        let later = now + Duration::from_secs(60);
        assert_eq!(
            decide(alerted(90, now), 86.0, later),
            (alerted(85, now), false)
        );
        assert_eq!(decide(alerted(85, now), 70.0, later), (None, false));
        assert_eq!(
            decide(alerted(85, now), 91.0, later),
            (alerted(90, later), true)
        );
    }
}
