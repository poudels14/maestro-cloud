use ipnet::Ipv4Net;
use kernel_api::{BuiltinResource, NodeId, NodeInstanceId};
use std::collections::{BTreeMap, BTreeSet};
use std::net::Ipv4Addr;

use crate::LegacyEntry;
use crate::legacy_convert::LegacyPlanError;
use crate::legacy_node_resources::convert_nodes;
use crate::legacy_node_schema::{
    LIVE_PREFIX, LegacyControlReservation, LegacyNodeInfo, LegacyNodeRecord, LegacyNodeRole,
    LegacyNodeState, LegacySubnetReservation, NodeKey, RECORD_PREFIX, classify_key, is_dns_label,
    is_hostname,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LegacyNodeCatalog {
    nodes: BTreeMap<NodeId, NodeBundle>,
    pub(crate) unclaimed: Vec<LegacyEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NodeBundle {
    pub(crate) record: LegacyNodeRecord,
    pub(crate) state: Option<LegacyNodeState>,
    pub(crate) subnet: LegacySubnetReservation,
    pub(crate) control: LegacyControlReservation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LegacyPorts {
    gateway: u16,
    store_client: u16,
    store_peer: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LegacyControlEndpoint {
    pub(crate) host_ip: Ipv4Addr,
    pub(crate) api_port: u16,
    pub(crate) gateway_port: u16,
    pub(crate) etcd_client_port: u16,
    pub(crate) etcd_peer_port: u16,
}

impl LegacyNodeCatalog {
    pub(crate) fn decode(entries: &[LegacyEntry]) -> Result<Self, LegacyNodeError> {
        let mut records = BTreeMap::<String, LegacyNodeRecord>::new();
        let mut live = BTreeMap::<String, LegacyNodeInfo>::new();
        let mut states = BTreeMap::<String, LegacyNodeState>::new();
        let mut subnets = BTreeMap::<String, LegacySubnetReservation>::new();
        let mut controls = BTreeMap::<String, LegacyControlReservation>::new();
        let mut unclaimed = Vec::new();

        for entry in entries {
            match classify_key(entry.key())? {
                Some(NodeKey::Record(node_id)) => {
                    insert(entry, &mut records, node_id, decode_json(entry)?)?;
                }
                Some(NodeKey::Live(node_id)) => {
                    insert(entry, &mut live, node_id, decode_json(entry)?)?;
                }
                Some(NodeKey::State(node_id)) => {
                    insert(entry, &mut states, node_id, decode_json(entry)?)?;
                }
                Some(NodeKey::Subnet(node_id)) => {
                    insert(entry, &mut subnets, node_id, decode_json(entry)?)?;
                }
                Some(NodeKey::Control(suffix)) => {
                    let reservation: LegacyControlReservation = decode_json(entry)?;
                    let node_id = reservation.node_id.clone().ok_or_else(|| {
                        invalid(entry.key(), "unclaimed control reservation blocks cutover")
                    })?;
                    validate_control_key(entry.key(), &suffix, &reservation)?;
                    insert(entry, &mut controls, node_id, reservation)?;
                }
                None => unclaimed.push(entry.clone()),
            }
        }

        validate_orphans(&records, &live, &states, &subnets, &controls)?;
        let mut nodes = BTreeMap::new();
        for (key_node_id, record) in records {
            let node_id = validate_record_identity(&key_node_id, &record)?;
            if let Some(live) = live.get(&key_node_id)
                && live != &record.last_info
            {
                return Err(invalid(
                    format!("{LIVE_PREFIX}{key_node_id}"),
                    "live registration disagrees with its durable node record",
                ));
            }
            let state = states.get(&key_node_id).cloned();
            validate_state(&key_node_id, state.as_ref())?;
            let subnet = subnets.get(&key_node_id).cloned().ok_or_else(|| {
                invalid(
                    format!("{RECORD_PREFIX}{key_node_id}"),
                    "durable node has no workload-subnet reservation",
                )
            })?;
            let control = controls.get(&key_node_id).cloned().ok_or_else(|| {
                invalid(
                    format!("{RECORD_PREFIX}{key_node_id}"),
                    "durable node has no control-address reservation",
                )
            })?;
            validate_bundle(&key_node_id, &record, &subnet, &control)?;
            nodes.insert(
                node_id,
                NodeBundle {
                    record,
                    state,
                    subnet,
                    control,
                },
            );
        }
        validate_topology(&nodes)?;
        Ok(Self { nodes, unclaimed })
    }

    pub(crate) fn contains(&self, node_id: &str) -> bool {
        self.nodes
            .keys()
            .any(|candidate| candidate.as_str() == node_id)
    }

    pub(crate) fn convert(&self) -> Result<Vec<BuiltinResource>, LegacyPlanError> {
        convert_nodes(&self.nodes)
    }

    pub(crate) fn master_host(&self) -> Option<Ipv4Addr> {
        self.nodes.values().find_map(|node| {
            (node.record.last_info.role == LegacyNodeRole::Master)
                .then_some(node.record.last_info.cluster_host_ip)
        })
    }

    pub(crate) fn control_endpoint(&self, host_ip: Ipv4Addr) -> Option<LegacyControlEndpoint> {
        self.nodes.values().find_map(|node| {
            (node.control.host_ip == host_ip).then_some(LegacyControlEndpoint {
                host_ip,
                api_port: node.control.api_port,
                gateway_port: node.control.gateway_port,
                etcd_client_port: node.control.etcd_client_port,
                etcd_peer_port: node.control.etcd_peer_port,
            })
        })
    }

    pub(crate) fn control_plane_endpoints(&self) -> BTreeMap<NodeId, LegacyControlEndpoint> {
        self.nodes
            .iter()
            .filter_map(|(node_id, node)| {
                node.record.last_info.role.is_control_plane().then_some((
                    node_id.clone(),
                    LegacyControlEndpoint {
                        host_ip: node.control.host_ip,
                        api_port: node.control.api_port,
                        gateway_port: node.control.gateway_port,
                        etcd_client_port: node.control.etcd_client_port,
                        etcd_peer_port: node.control.etcd_peer_port,
                    },
                ))
            })
            .collect()
    }

    pub(crate) fn maintenance_drain(&self) -> Option<&NodeId> {
        self.nodes.iter().find_map(|(node_id, node)| {
            node.state.as_ref().and_then(|state| {
                (state.unschedulable
                    && matches!(state.reason.as_deref(), Some("upgrade" | "restart")))
                .then_some(node_id)
            })
        })
    }
}

fn validate_orphans(
    records: &BTreeMap<String, LegacyNodeRecord>,
    live: &BTreeMap<String, LegacyNodeInfo>,
    states: &BTreeMap<String, LegacyNodeState>,
    subnets: &BTreeMap<String, LegacySubnetReservation>,
    controls: &BTreeMap<String, LegacyControlReservation>,
) -> Result<(), LegacyNodeError> {
    for (family, keys) in [
        ("live registration", live.keys().collect::<Vec<_>>()),
        ("node state", states.keys().collect()),
        ("subnet reservation", subnets.keys().collect()),
        ("control reservation", controls.keys().collect()),
    ] {
        if let Some(node_id) = keys
            .into_iter()
            .find(|node_id| !records.contains_key(*node_id))
        {
            return Err(invalid(
                node_id,
                format!("orphan {family} has no durable node record"),
            ));
        }
    }
    Ok(())
}

fn validate_record_identity(
    key_node_id: &str,
    record: &LegacyNodeRecord,
) -> Result<NodeId, LegacyNodeError> {
    if record.last_info.node_id != key_node_id {
        return Err(invalid(
            format!("{RECORD_PREFIX}{key_node_id}"),
            "node record key and payload identities disagree",
        ));
    }
    let node_id = NodeId::new(key_node_id)
        .map_err(|error| invalid(key_node_id, format!("node id is invalid: {error}")))?;
    if !is_dns_label(key_node_id) {
        return Err(invalid(
            key_node_id,
            "node id is not a lowercase DNS label accepted by the new topology",
        ));
    }
    NodeInstanceId::new(&record.last_info.instance_id)
        .map_err(|error| invalid(key_node_id, format!("instance id is invalid: {error}")))?;
    if !is_hostname(&record.last_info.hostname) {
        return Err(invalid(
            key_node_id,
            "node hostname is not a lowercase DNS name",
        ));
    }
    validate_timestamps(key_node_id, record)?;
    Ok(node_id)
}

fn validate_timestamps(node_id: &str, record: &LegacyNodeRecord) -> Result<(), LegacyNodeError> {
    let info = &record.last_info;
    let values = [
        ("startedAtMs", Some(info.started_at_ms)),
        ("dataPlaneCheckedAtMs", Some(info.data_plane_checked_at_ms)),
        ("lastSeenAtMs", Some(record.last_seen_at_ms)),
        ("lostAtMs", record.lost_at_ms),
        ("dataPlaneLostAtMs", record.data_plane_lost_at_ms),
        (
            "controlPlaneAlertedAtMs",
            record.control_plane_alerted_at_ms,
        ),
        ("dataPlaneAlertedAtMs", record.data_plane_alerted_at_ms),
    ];
    if let Some((field, _)) = values
        .into_iter()
        .find(|(_, value)| value.is_some_and(|value| value < 0))
    {
        return Err(invalid(node_id, format!("{field} cannot be negative")));
    }
    if record.last_seen_at_ms < info.started_at_ms {
        return Err(invalid(node_id, "last-seen time predates daemon start"));
    }
    Ok(())
}

fn validate_state(node_id: &str, state: Option<&LegacyNodeState>) -> Result<(), LegacyNodeError> {
    let Some(state) = state else {
        return Ok(());
    };
    if state.drained_at_ms.is_some_and(|value| value < 0) {
        return Err(invalid(node_id, "drainedAtMs cannot be negative"));
    }
    if !state.unschedulable && (state.drained_at_ms.is_some() || state.reason.is_some()) {
        return Err(invalid(
            node_id,
            "schedulable node retains contradictory drain metadata",
        ));
    }
    Ok(())
}

fn validate_bundle(
    node_id: &str,
    record: &LegacyNodeRecord,
    subnet: &LegacySubnetReservation,
    control: &LegacyControlReservation,
) -> Result<(), LegacyNodeError> {
    let info = &record.last_info;
    if subnet.node_id != node_id || subnet.state != "active" || subnet.cidr != info.subnet {
        return Err(invalid(
            node_id,
            "workload-subnet reservation is not the node's active declared subnet",
        ));
    }
    if control.node_id.as_deref() != Some(node_id)
        || control.state != "active"
        || control.host_ip != info.cluster_host_ip
        || control.api_port != info.cluster_api_port
        || control.gateway_port != info.cluster_gateway_port
    {
        return Err(invalid(
            node_id,
            "control reservation is not the node's active declared endpoint",
        ));
    }
    let ports = [
        control.api_port,
        control.gateway_port,
        control.etcd_client_port,
        control.etcd_peer_port,
    ];
    if ports.contains(&0) || ports.into_iter().collect::<BTreeSet<_>>().len() != ports.len() {
        return Err(invalid(
            node_id,
            "node control ports must be non-zero and distinct",
        ));
    }
    let network = parse_workload_subnet(node_id, &info.subnet)?;
    if network.contains(&info.cluster_host_ip) {
        return Err(invalid(
            node_id,
            "node endpoint is inside its workload subnet",
        ));
    }
    Ok(())
}

fn validate_topology(nodes: &BTreeMap<NodeId, NodeBundle>) -> Result<(), LegacyNodeError> {
    if nodes.is_empty() {
        return Ok(());
    }
    let masters = nodes
        .values()
        .filter(|node| node.record.last_info.role == LegacyNodeRole::Master)
        .count();
    let control_planes = nodes
        .values()
        .filter(|node| node.record.last_info.role.is_control_plane())
        .count();
    if masters != 1 || !matches!(control_planes, 1 | 3) {
        return Err(invalid(
            RECORD_PREFIX,
            format!(
                "new topology requires one master and one or three control-plane nodes; found {masters} masters and {control_planes} control-plane nodes"
            ),
        ));
    }
    let Some(first) = nodes.values().next() else {
        return Ok(());
    };
    let expected_ports = LegacyPorts {
        gateway: first.control.gateway_port,
        store_client: first.control.etcd_client_port,
        store_peer: first.control.etcd_peer_port,
    };
    let mut endpoints = BTreeSet::new();
    let mut subnets = Vec::<(&NodeId, Ipv4Net)>::new();
    for (node_id, node) in nodes {
        let info = &node.record.last_info;
        if !info.cluster_host_ip.is_private()
            || info.cluster_host_ip.is_loopback()
            || info.cluster_host_ip.is_unspecified()
        {
            return Err(invalid(
                node_id.to_string(),
                "node endpoint must be private and routable",
            ));
        }
        if !endpoints.insert((info.cluster_host_ip, info.cluster_api_port)) {
            return Err(invalid(
                node_id.to_string(),
                "node control endpoint occurs more than once",
            ));
        }
        let ports = LegacyPorts {
            gateway: node.control.gateway_port,
            store_client: node.control.etcd_client_port,
            store_peer: node.control.etcd_peer_port,
        };
        if ports != expected_ports {
            return Err(invalid(
                node_id.to_string(),
                "per-node gateway or etcd ports cannot map to cluster-wide ports",
            ));
        }
        let subnet = parse_workload_subnet(node_id.as_str(), &info.subnet)?;
        for (other_id, other_subnet) in &subnets {
            if subnet.contains(&other_subnet.network()) || other_subnet.contains(&subnet.network())
            {
                return Err(invalid(
                    node_id.to_string(),
                    format!("workload subnet overlaps node `{other_id}`"),
                ));
            }
        }
        subnets.push((node_id, subnet));
    }
    for (subnet_node, subnet) in &subnets {
        for (endpoint_node, node) in nodes {
            if subnet.contains(&node.record.last_info.cluster_host_ip) {
                return Err(invalid(
                    endpoint_node.to_string(),
                    format!("endpoint is inside node `{subnet_node}` workload subnet"),
                ));
            }
        }
    }
    Ok(())
}

fn parse_workload_subnet(node_id: &str, value: &str) -> Result<Ipv4Net, LegacyNodeError> {
    let network = value
        .parse::<Ipv4Net>()
        .map_err(|error| invalid(node_id, format!("workload subnet is invalid: {error}")))?;
    if network.to_string() != value
        || network.addr() != network.network()
        || network.prefix_len() != 24
        || !network.network().is_private()
        || !network.broadcast().is_private()
    {
        return Err(invalid(
            node_id,
            "workload subnet must be a canonical private IPv4 /24",
        ));
    }
    Ok(network)
}

fn validate_control_key(
    key: &str,
    suffix: &str,
    control: &LegacyControlReservation,
) -> Result<(), LegacyNodeError> {
    let expected = format!(
        "{:08x}-{:04x}",
        u32::from(control.host_ip),
        control.api_port
    );
    if suffix != expected {
        return Err(invalid(
            key,
            "control reservation key does not match its endpoint",
        ));
    }
    Ok(())
}

fn insert<Value>(
    entry: &LegacyEntry,
    values: &mut BTreeMap<String, Value>,
    node_id: String,
    value: Value,
) -> Result<(), LegacyNodeError> {
    if values.insert(node_id.clone(), value).is_some() {
        Err(invalid(
            entry.key(),
            format!("node `{node_id}` occurs more than once in this key family"),
        ))
    } else {
        Ok(())
    }
}

fn decode_json<Value: serde::de::DeserializeOwned>(
    entry: &LegacyEntry,
) -> Result<Value, LegacyNodeError> {
    serde_json::from_slice(entry.value())
        .map_err(|error| invalid(entry.key(), format!("invalid JSON: {error}")))
}

fn invalid(key: impl Into<String>, message: impl Into<String>) -> LegacyNodeError {
    LegacyNodeError::InvalidState {
        key: key.into(),
        message: message.into(),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum LegacyNodeError {
    #[error("legacy node state at `{key}` is invalid: {message}")]
    InvalidState { key: String, message: String },
}
