//! Cluster coordination: node identity, registration, and leader election.
//!
//! The cluster module exposes two abstractions, [`NodeRegistry`] and
//! [`LeaderElector`], with two implementations each:
//!   * etcd-backed for production
//!   * in-memory for tests (multiple "nodes" share a single [`InMemoryCluster`])
//!
//! [`ClusterService`] glues them together and exposes cluster topology +
//! leadership state for the rest of the controller (API server, CLI,
//! deployment scheduler).

#![allow(dead_code)]

pub mod adapters;
pub mod assignment_store;
pub mod disk_snapshot;
pub mod elector;
pub mod engine_executor;
pub mod etcd_assignment_store;
pub mod etcd_elector;
pub mod etcd_registry;
pub mod http_upgrader;
#[cfg(test)]
pub mod in_memory;
pub mod leader_loop;
pub mod metrics;
pub mod node_id;
pub mod port_allocator;
pub mod quorum;
pub mod registry;
pub mod scheduler;
pub mod scheduling;
pub mod service;
pub mod traefik_aggregator;
pub mod types;
pub mod upgrade;

pub use elector::LeaderElector;
pub use etcd_elector::EtcdLeaderElector;
pub use etcd_registry::EtcdNodeRegistry;
pub use metrics::ClusterMetrics;
pub use registry::NodeRegistry;
pub use service::ClusterService;
pub use types::NodeRole;

#[cfg(test)]
#[path = "../tests/assignment_reconciler.rs"]
mod assignment_reconciler_tests;
#[cfg(test)]
#[path = "../tests/cluster_adapters.rs"]
mod cluster_adapters_tests;
#[cfg(test)]
#[path = "../tests/cluster_upgrade.rs"]
mod cluster_upgrade_tests;
#[cfg(test)]
#[path = "../tests/etcd_chaos.rs"]
mod etcd_chaos_tests;
#[cfg(test)]
#[path = "../tests/leader_loop.rs"]
mod leader_loop_tests;
#[cfg(test)]
#[path = "../tests/scheduling.rs"]
mod scheduling_tests;
#[cfg(test)]
#[path = "../tests/traefik_aggregator.rs"]
mod traefik_aggregator_tests;

#[cfg(test)]
#[path = "../tests/cluster_lifecycle.rs"]
mod cluster_lifecycle_tests;
