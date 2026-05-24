//! Keeps the effective worker pool an odd size by deterministically excluding
//! the lexicographically-last node when the live workload-eligible count is
//! even. Prevents tied splits during scheduling and matches Raft's "odd is
//! better than even" guidance for the workload plane.
//!
//! Note: this does NOT affect etcd cluster membership — etcd's own Raft
//! quorum is governed by initial-cluster + member add/remove RPCs, which
//! are operator concerns. This filter is purely about scheduler input.

use super::types::NodeInfo;

/// Returns a copy of `nodes` with at most one entry removed so the count of
/// workload-eligible nodes is odd (or zero). When the input is even, the
/// node with the largest `node_id` (lexicographic) is the one dropped — a
/// stable choice that's reversible the moment a new node joins.
///
/// Nodes that are already `unschedulable` or whose role can't run workloads
/// don't count toward the parity check; they pass through unchanged.
pub fn balance_for_odd_workload_count(nodes: &[NodeInfo]) -> Vec<NodeInfo> {
    let mut eligible: Vec<&NodeInfo> = nodes
        .iter()
        .filter(|node| !node.unschedulable && node.role.can_run_workloads())
        .collect();
    let drop_node_id: Option<String> = if eligible.len() >= 2 && eligible.len().is_multiple_of(2) {
        eligible.sort_by(|left, right| left.node_id.cmp(&right.node_id));
        eligible.last().map(|node| node.node_id.clone())
    } else {
        None
    };
    nodes
        .iter()
        .cloned()
        .map(|mut node| {
            if Some(&node.node_id) == drop_node_id.as_ref() {
                node.unschedulable = true;
            }
            node
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::types::NodeRole;
    use std::collections::BTreeMap;

    fn node(node_id: &str, role: NodeRole, unschedulable: bool) -> NodeInfo {
        NodeInfo {
            node_id: node_id.to_string(),
            hostname: format!("{node_id}.local"),
            role,
            tailscale_ip: None,
            api_port: 3001,
            version: "test".to_string(),
            started_at_ms: 0,
            labels: BTreeMap::new(),
            unschedulable,
        }
    }

    #[test]
    fn three_nodes_passes_through_unchanged() {
        let nodes = vec![
            node("a", NodeRole::Both, false),
            node("b", NodeRole::Both, false),
            node("c", NodeRole::Both, false),
        ];
        let balanced = balance_for_odd_workload_count(&nodes);
        let eligible: Vec<&str> = balanced
            .iter()
            .filter(|node| !node.unschedulable)
            .map(|node| node.node_id.as_str())
            .collect();
        assert_eq!(eligible, vec!["a", "b", "c"]);
    }

    #[test]
    fn four_nodes_drops_the_lexicographically_last() {
        let nodes = vec![
            node("a", NodeRole::Both, false),
            node("b", NodeRole::Both, false),
            node("c", NodeRole::Both, false),
            node("d", NodeRole::Both, false),
        ];
        let balanced = balance_for_odd_workload_count(&nodes);
        let eligible: Vec<&str> = balanced
            .iter()
            .filter(|node| !node.unschedulable)
            .map(|node| node.node_id.as_str())
            .collect();
        assert_eq!(eligible, vec!["a", "b", "c"]);
        let dropped = balanced
            .iter()
            .find(|node| node.unschedulable)
            .expect("one node should be marked unschedulable");
        assert_eq!(dropped.node_id, "d");
    }

    #[test]
    fn two_nodes_drops_one_for_odd_count() {
        let nodes = vec![
            node("alpha", NodeRole::Both, false),
            node("beta", NodeRole::Both, false),
        ];
        let balanced = balance_for_odd_workload_count(&nodes);
        let eligible: Vec<&str> = balanced
            .iter()
            .filter(|node| !node.unschedulable)
            .map(|node| node.node_id.as_str())
            .collect();
        assert_eq!(eligible, vec!["alpha"]);
    }

    #[test]
    fn single_node_passes_through() {
        let nodes = vec![node("solo", NodeRole::Both, false)];
        let balanced = balance_for_odd_workload_count(&nodes);
        assert!(!balanced[0].unschedulable);
    }

    #[test]
    fn already_drained_nodes_dont_count_toward_parity() {
        let nodes = vec![
            node("a", NodeRole::Both, false),
            node("b", NodeRole::Both, false),
            node("c", NodeRole::Both, true), // already drained
            node("d", NodeRole::Both, false),
        ];
        // 3 eligible (a, b, d) → odd → no further change
        let balanced = balance_for_odd_workload_count(&nodes);
        let still_eligible: Vec<&str> = balanced
            .iter()
            .filter(|node| !node.unschedulable)
            .map(|node| node.node_id.as_str())
            .collect();
        assert_eq!(still_eligible, vec!["a", "b", "d"]);
    }

    #[test]
    fn controller_only_nodes_dont_count_toward_parity() {
        let nodes = vec![
            node("worker-a", NodeRole::Both, false),
            node("worker-b", NodeRole::Both, false),
            node("ctrl", NodeRole::Controller, false), // not workload-eligible
        ];
        // 2 eligible workers → even → drop the lex-last (worker-b)
        let balanced = balance_for_odd_workload_count(&nodes);
        let dropped: Vec<&str> = balanced
            .iter()
            .filter(|node| node.unschedulable)
            .map(|node| node.node_id.as_str())
            .collect();
        assert_eq!(dropped, vec!["worker-b"]);
    }
}
