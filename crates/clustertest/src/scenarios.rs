//! Shared acceptance scenarios that every Maestro implementation must pass.

mod election;
mod lifecycle;
mod quorum;
mod restart;
mod routing;
mod scheduling;

pub use election::leader_failover_fences_stale_writes;
pub use lifecycle::{
    build_failure_marks_deployment_crashed, crashed_replica_restarts_in_place,
    drained_deployment_finalizes, prepare_failure_marks_deployment_crashed,
    queued_deployment_can_be_canceled, redeploy_drains_previous, replica_override_round_trips,
    rollout_reaches_ready,
};
pub use quorum::all_voter_restart_waits_for_quorum_and_preserves_state;
pub use restart::serial_node_restarts_preserve_quorum_and_routing;
pub use routing::routing_survives_workload_and_gateway_failures;
pub use scheduling::scheduler_scales_replicas_across_nodes;
