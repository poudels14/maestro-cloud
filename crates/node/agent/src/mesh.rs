use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::str::FromStr;

use async_trait::async_trait;
use kernel_api::{NodeId, NodeNetworkSpec};

use crate::{MeshIdentity, MeshIdentityError, WireGuardPrivateKey, WireGuardPublicKey};

/// Fixed kernel interface used for Maestro workload traffic.
pub const MESH_INTERFACE_NAME: &str = "wg0";
/// WireGuard and workload-network MTU accounting for encapsulation overhead.
pub const MESH_MTU_BYTES: u16 = 1_420;

/// A canonical private IPv4 `/24` allocated to one node's workloads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MeshSubnet {
    network: Ipv4Addr,
}

impl MeshSubnet {
    /// Returns the canonical network address.
    pub fn network_address(self) -> Ipv4Addr {
        self.network
    }

    /// Returns whether this allocation contains an address.
    pub fn contains(self, address: Ipv4Addr) -> bool {
        let network = u32::from(self.network);
        let address = u32::from(address);
        address >= network && address <= network.saturating_add(255)
    }
}

impl Display for MeshSubnet {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}/24", self.network)
    }
}

impl FromStr for MeshSubnet {
    type Err = MeshError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (address, prefix) = value
            .split_once('/')
            .ok_or_else(|| MeshError::InvalidSubnet {
                subnet: value.to_owned(),
            })?;
        let network = address
            .parse::<Ipv4Addr>()
            .map_err(|_| MeshError::InvalidSubnet {
                subnet: value.to_owned(),
            })?;
        let prefix = prefix.parse::<u8>().map_err(|_| MeshError::InvalidSubnet {
            subnet: value.to_owned(),
        })?;
        let canonical = Ipv4Addr::from(u32::from(network) & 0xffff_ff00);
        if prefix != 24 || canonical != network || !network.is_private() {
            return Err(MeshError::InvalidSubnet {
                subnet: value.to_owned(),
            });
        }
        Ok(Self { network })
    }
}

/// Complete desired settings for the local WireGuard interface.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MeshInterface {
    /// Stable kernel interface name.
    pub name: String,
    /// Node-local private key, never published in cluster state.
    pub private_key: WireGuardPrivateKey,
    /// Persisted cluster-wide UDP port.
    pub listen_port: u16,
    /// Encapsulation-safe interface MTU.
    pub mtu_bytes: u16,
    /// Workload subnet routed from this node.
    pub local_subnet: MeshSubnet,
}

/// One exact WireGuard peer admitted to the workload mesh.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MeshPeer {
    /// Stable cluster node identity.
    pub node_id: NodeId,
    /// Public key authenticated through the `NodeNetwork` resource.
    pub public_key: WireGuardPublicKey,
    /// Direct private-network UDP endpoint.
    pub endpoint: SocketAddrV4,
    /// Only source and destination range this peer may carry.
    pub allowed_subnet: MeshSubnet,
}

/// One host route that sends a remote workload subnet through WireGuard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MeshRoute {
    /// Remote workload subnet.
    pub destination: MeshSubnet,
    /// Kernel interface receiving the route.
    pub interface: String,
}

/// Complete replacement artifact handed to a mesh backend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MeshConfiguration {
    /// Local interface and secret identity.
    pub interface: MeshInterface,
    /// Stable peer set sorted by node identity.
    pub peers: Vec<MeshPeer>,
    /// Stable remote routes matching the peer set one-for-one.
    pub routes: Vec<MeshRoute>,
}

/// Idempotent side-effect boundary for programming a host workload mesh.
#[async_trait]
pub trait MeshBackend: Send + Sync {
    /// Converges the host to the complete desired artifact, removing stale
    /// peers and routes. Retrying after cancellation or failure must converge.
    async fn apply(&self, desired: &MeshConfiguration) -> Result<(), MeshBackendError>;
}

/// Backend detail suitable for a node-network readiness condition.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("mesh backend failed: {detail}")]
pub struct MeshBackendError {
    detail: String,
}

impl MeshBackendError {
    /// Creates an adapter error without exposing backend-specific types.
    pub fn new(detail: impl Into<String>) -> Self {
        Self {
            detail: detail.into(),
        }
    }

    /// Returns operator-facing backend detail.
    pub fn detail(&self) -> &str {
        &self.detail
    }
}

/// Pure desired-state builder for one node's view of every mesh publication.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MeshPlanner {
    local_node_id: NodeId,
    identity: MeshIdentity,
    listen_port: u16,
}

impl MeshPlanner {
    /// Creates a planner bound to a stable node identity and cluster port.
    pub fn new(
        local_node_id: NodeId,
        identity: MeshIdentity,
        listen_port: u16,
    ) -> Result<Self, MeshError> {
        if listen_port == 0 {
            return Err(MeshError::InvalidListenPort);
        }
        Ok(Self {
            local_node_id,
            identity,
            listen_port,
        })
    }

    /// Builds the local `NodeNetwork` publication from validated topology.
    pub fn publication(
        &self,
        host_address: Ipv4Addr,
        workload_subnet: MeshSubnet,
    ) -> Result<NodeNetworkSpec, MeshError> {
        if !valid_host_address(host_address) {
            return Err(MeshError::InvalidEndpoint {
                node_id: self.local_node_id.clone(),
                endpoint: SocketAddr::V4(SocketAddrV4::new(host_address, self.listen_port)),
            });
        }
        Ok(NodeNetworkSpec {
            node_id: self.local_node_id.clone(),
            public_key: self.identity.public_key().to_string(),
            endpoint: SocketAddr::V4(SocketAddrV4::new(host_address, self.listen_port)),
            workload_subnet: workload_subnet.to_string(),
            mtu_bytes: MESH_MTU_BYTES,
        })
    }

    /// Validates all publications and derives the complete local peer state.
    pub fn plan(&self, publications: &[NodeNetworkSpec]) -> Result<MeshConfiguration, MeshError> {
        let networks = validate_publications(publications, self.listen_port)?;
        let local = networks.get(&self.local_node_id).ok_or_else(|| {
            MeshError::MissingLocalPublication {
                node_id: self.local_node_id.clone(),
            }
        })?;
        let expected_public_key = self.identity.public_key();
        if local.public_key != expected_public_key {
            return Err(MeshError::LocalKeyMismatch {
                node_id: self.local_node_id.clone(),
            });
        }

        let peers = networks
            .iter()
            .filter(|(node_id, _)| *node_id != &self.local_node_id)
            .map(|(node_id, network)| MeshPeer {
                node_id: node_id.clone(),
                public_key: network.public_key.clone(),
                endpoint: network.endpoint,
                allowed_subnet: network.subnet,
            })
            .collect::<Vec<_>>();
        let routes = peers
            .iter()
            .map(|peer| MeshRoute {
                destination: peer.allowed_subnet,
                interface: MESH_INTERFACE_NAME.to_owned(),
            })
            .collect();
        Ok(MeshConfiguration {
            interface: MeshInterface {
                name: MESH_INTERFACE_NAME.to_owned(),
                private_key: self.identity.private_key().clone(),
                listen_port: self.listen_port,
                mtu_bytes: MESH_MTU_BYTES,
                local_subnet: local.subnet,
            },
            peers,
            routes,
        })
    }
}

/// Agent reconciliation wrapper that plans before crossing the backend seam.
pub struct MeshReconciler<Backend> {
    planner: MeshPlanner,
    backend: Backend,
}

impl<Backend> MeshReconciler<Backend>
where
    Backend: MeshBackend,
{
    /// Binds a pure planner to one side-effecting backend.
    pub fn new(planner: MeshPlanner, backend: Backend) -> Self {
        Self { planner, backend }
    }

    /// Returns the backend for diagnostics and exact-artifact test assertions.
    pub fn backend(&self) -> &Backend {
        &self.backend
    }

    /// Applies one complete resource snapshot and returns the exact artifact.
    pub async fn reconcile(
        &self,
        publications: &[NodeNetworkSpec],
    ) -> Result<MeshConfiguration, MeshError> {
        let desired = self.planner.plan(publications)?;
        self.backend.apply(&desired).await?;
        Ok(desired)
    }
}

/// Why a mesh identity, publication snapshot, or backend apply was rejected.
#[derive(Debug, thiserror::Error)]
pub enum MeshError {
    /// The persisted cluster port was invalid.
    #[error("WireGuard listen port must be non-zero")]
    InvalidListenPort,
    /// A publication did not contain a canonical private `/24`.
    #[error("invalid private workload subnet `{subnet}`; expected a canonical IPv4 /24")]
    InvalidSubnet { subnet: String },
    /// A node endpoint was not private IPv4 or did not use the cluster port.
    #[error("node `{node_id}` has invalid WireGuard endpoint `{endpoint}`")]
    InvalidEndpoint {
        node_id: NodeId,
        endpoint: SocketAddr,
    },
    /// A node publication did not encode a valid WireGuard public key.
    #[error("node `{node_id}` has an invalid WireGuard public key: {source}")]
    InvalidPublicKey {
        node_id: NodeId,
        #[source]
        source: MeshIdentityError,
    },
    /// A publication attempted to change the encoded mesh MTU.
    #[error("node `{node_id}` published mesh MTU {observed}; expected {MESH_MTU_BYTES}")]
    InvalidMtu { node_id: NodeId, observed: u16 },
    /// More than one publication claimed the same node identity.
    #[error("node `{node_id}` has duplicate network publications")]
    DuplicateNode { node_id: NodeId },
    /// Multiple nodes claimed one cryptographic identity.
    #[error("nodes `{first}` and `{second}` published the same WireGuard key")]
    DuplicatePublicKey { first: NodeId, second: NodeId },
    /// Multiple nodes claimed one workload subnet.
    #[error("nodes `{first}` and `{second}` published the same workload subnet")]
    DuplicateSubnet { first: NodeId, second: NodeId },
    /// Multiple nodes claimed one host endpoint.
    #[error("nodes `{first}` and `{second}` published the same WireGuard endpoint")]
    DuplicateEndpoint { first: NodeId, second: NodeId },
    /// The local node had no publication in the current complete snapshot.
    #[error("local node `{node_id}` has no network publication")]
    MissingLocalPublication { node_id: NodeId },
    /// Store state attempted to replace the node-local public key.
    #[error("local node `{node_id}` publication differs from its persisted private key")]
    LocalKeyMismatch { node_id: NodeId },
    /// Host programming failed after desired-state validation.
    #[error(transparent)]
    Backend(#[from] MeshBackendError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ValidatedNetwork {
    public_key: WireGuardPublicKey,
    endpoint: SocketAddrV4,
    subnet: MeshSubnet,
}

fn validate_publications(
    publications: &[NodeNetworkSpec],
    listen_port: u16,
) -> Result<BTreeMap<NodeId, ValidatedNetwork>, MeshError> {
    let mut networks = BTreeMap::new();
    let mut public_keys = BTreeMap::new();
    let mut subnets = BTreeMap::new();
    let mut endpoints = BTreeMap::new();
    for publication in publications {
        if networks.contains_key(&publication.node_id) {
            return Err(MeshError::DuplicateNode {
                node_id: publication.node_id.clone(),
            });
        }
        let validated = validate_publication(publication, listen_port)?;
        reject_duplicate(
            &mut public_keys,
            validated.public_key.clone(),
            &publication.node_id,
            DuplicateField::PublicKey,
        )?;
        reject_duplicate(
            &mut subnets,
            validated.subnet,
            &publication.node_id,
            DuplicateField::Subnet,
        )?;
        reject_duplicate(
            &mut endpoints,
            validated.endpoint,
            &publication.node_id,
            DuplicateField::Endpoint,
        )?;
        networks.insert(publication.node_id.clone(), validated);
    }
    Ok(networks)
}

fn validate_publication(
    publication: &NodeNetworkSpec,
    listen_port: u16,
) -> Result<ValidatedNetwork, MeshError> {
    let endpoint = match publication.endpoint {
        SocketAddr::V4(endpoint)
            if valid_host_address(*endpoint.ip()) && endpoint.port() == listen_port =>
        {
            endpoint
        }
        endpoint => {
            return Err(MeshError::InvalidEndpoint {
                node_id: publication.node_id.clone(),
                endpoint,
            });
        }
    };
    if publication.mtu_bytes != MESH_MTU_BYTES {
        return Err(MeshError::InvalidMtu {
            node_id: publication.node_id.clone(),
            observed: publication.mtu_bytes,
        });
    }
    let public_key =
        publication
            .public_key
            .parse()
            .map_err(|source| MeshError::InvalidPublicKey {
                node_id: publication.node_id.clone(),
                source,
            })?;
    let subnet = publication.workload_subnet.parse()?;
    Ok(ValidatedNetwork {
        public_key,
        endpoint,
        subnet,
    })
}

fn valid_host_address(address: Ipv4Addr) -> bool {
    address.is_private() && !address.is_loopback() && !address.is_unspecified()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DuplicateField {
    PublicKey,
    Subnet,
    Endpoint,
}

fn reject_duplicate<Value>(
    values: &mut BTreeMap<Value, NodeId>,
    value: Value,
    node_id: &NodeId,
    field: DuplicateField,
) -> Result<(), MeshError>
where
    Value: Ord,
{
    if let Some(first) = values.insert(value, node_id.clone()) {
        return Err(match field {
            DuplicateField::PublicKey => MeshError::DuplicatePublicKey {
                first,
                second: node_id.clone(),
            },
            DuplicateField::Subnet => MeshError::DuplicateSubnet {
                first,
                second: node_id.clone(),
            },
            DuplicateField::Endpoint => MeshError::DuplicateEndpoint {
                first,
                second: node_id.clone(),
            },
        });
    }
    Ok(())
}
