//! Shared acceptance scenarios that every Maestro implementation must pass.

mod affinity;
mod election;
mod formation;
mod lifecycle;
mod lifecycle_faults;
mod quorum;
mod restart;
mod rollout;
mod routing;
mod scheduling;
mod upgrade;

pub use affinity::affinity_is_opaque_sticky_and_overridable;
pub use election::leader_failover_fences_stale_writes;
pub use formation::designated_seed_and_learners_form_registered_cluster;
pub use lifecycle::{
    build_failure_marks_deployment_crashed, crashed_replica_restarts_in_place,
    drained_deployment_finalizes, prepare_failure_marks_deployment_crashed,
    queued_deployment_can_be_canceled, redeploy_drains_previous, replica_override_round_trips,
    rollout_reaches_ready,
};
pub use lifecycle_faults::{
    all_exhausted_replicas_crash_deployment, exhausted_replica_stays_down_while_peers_run,
    initial_replica_crash_preserves_pending_peers, missing_workload_record_is_recovered,
    old_workload_crash_does_not_break_redeployment, rollout_failure_is_isolated_between_services,
};
pub use quorum::all_voter_restart_waits_for_quorum_and_preserves_state;
pub use restart::serial_node_restarts_preserve_quorum_and_routing;
pub use rollout::readiness_gated_cutover_preserves_traffic_and_inflight_requests;
pub use routing::routing_survives_workload_and_gateway_failures;
pub use scheduling::scheduler_scales_replicas_across_nodes;
pub use upgrade::rolling_upgrade_retries_and_restores_nodes_serially;
