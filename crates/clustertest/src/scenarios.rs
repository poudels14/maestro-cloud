//! Shared acceptance scenarios that every Maestro implementation must pass.

mod affinity;
mod cluster_setup;
mod election;
mod formation;
mod lifecycle;
mod lifecycle_control;
mod lifecycle_faults;
mod lifecycle_sequence;
mod lifecycle_stress;
mod quorum;
mod restart;
mod rollout;
mod routing;
mod scheduling;
mod service_lifecycle;
mod smoke;
mod upgrade;

pub use affinity::affinity_is_opaque_sticky_and_overridable;
pub use cluster_setup::cluster_bootstraps_joins_meshes_and_recovers;
pub use election::leader_failover_fences_stale_writes;
pub use formation::designated_seed_and_learners_form_registered_cluster;
pub use lifecycle::{
    build_failure_marks_deployment_crashed, crashed_replica_restarts_in_place,
    drained_deployment_finalizes, prepare_failure_marks_deployment_crashed,
    queued_deployment_can_be_canceled, redeploy_drains_previous, replica_override_round_trips,
    rollout_reaches_ready,
};
pub use lifecycle_control::{
    artifact_preparation_precedes_build, built_artifact_is_persisted,
    hanging_build_crashes_after_timeout, health_monitor_readies_deployment,
    healthy_report_resets_failure_count, healthy_report_updates_pending_replica,
    in_progress_build_can_be_canceled, queued_rollout_ignores_later_freeze,
    repeated_healthy_reports_are_write_free, unhealthy_report_increments_and_persists,
    unhealthy_threshold_restarts_replica,
};
pub use lifecycle_faults::{
    all_exhausted_replicas_crash_deployment, exhausted_replica_stays_down_while_peers_run,
    initial_replica_crash_preserves_pending_peers, missing_workload_record_is_recovered,
    old_workload_crash_does_not_break_redeployment, rollout_failure_is_isolated_between_services,
};
pub use lifecycle_sequence::{
    assert_lifecycle_invariants, lifecycle_operation_sequence_preserves_invariants,
};
pub use lifecycle_stress::{
    back_to_back_redeploys_keep_only_latest_ready, many_rapid_redeploys_settle_to_one_ready,
    many_services_roll_out_independently, multi_replica_rollout_starts_all_replicas,
    replica_override_respects_configured_floor, replica_override_scales_up,
    sequential_redeploys_supersede_history,
};
pub use quorum::all_voter_restart_waits_for_quorum_and_preserves_state;
pub use restart::serial_node_restarts_preserve_quorum_and_routing;
pub use rollout::readiness_gated_cutover_preserves_traffic_and_inflight_requests;
pub use routing::routing_survives_workload_and_gateway_failures;
pub use scheduling::scheduler_scales_replicas_across_nodes;
pub use service_lifecycle::{
    delete_service_collects_owned_state, drain_and_restore_move_placement,
    freeze_and_unfreeze_gate_rollout, remove_deployment_retains_history,
    restart_recycles_workloads_in_place,
};
pub use smoke::{
    isolated_seed_security_restart_is_idempotent,
    production_ingress_access_log_configuration_starts,
    single_node_store_endpoint_is_peer_reachable,
};
pub use upgrade::rolling_upgrade_retries_and_restores_nodes_serially;
