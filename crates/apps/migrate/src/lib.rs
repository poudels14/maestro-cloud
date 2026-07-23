//! One-shot, resumable migration from legacy Maestro state into typed resources.

mod artifact;
mod etcd_source;
mod legacy_cluster;
mod legacy_config;
mod legacy_convert;
mod legacy_crypto;
mod legacy_derived;
mod legacy_derived_schema;
mod legacy_identity;
mod legacy_maintenance;
mod legacy_maintenance_schema;
mod legacy_membership;
mod legacy_network;
mod legacy_node_lifecycle;
mod legacy_node_lifecycle_schema;
mod legacy_node_resources;
mod legacy_node_schema;
mod legacy_nodes;
mod legacy_placement_schema;
mod legacy_placements;
mod legacy_request_schema;
mod legacy_requests;
mod legacy_resources;
mod legacy_schema;
mod legacy_services;
mod legacy_webhooks;
mod plan;
mod report;
mod runner;
mod snapshot;

pub use artifact::SnapshotArtifactError;
pub use etcd_source::{
    CapturedLegacySnapshot, CutoverEtcdConnection, CutoverEtcdError, LegacyEtcdSource,
};
pub use legacy_convert::{LegacyPlanError, plan_legacy_snapshot};
pub use plan::{MigrationPlan, MigrationRequestClaim, MigrationWrite, PlanError};
pub use report::{MigrationPlanReport, ReportError};
pub use runner::{CutoverMigration, MigrationError, MigrationOutcome, MigrationVerification};
pub use snapshot::{LegacyEntry, LegacySnapshot, SnapshotError};

#[cfg(test)]
mod legacy_cluster_tests;
#[cfg(test)]
mod legacy_derived_tests;
#[cfg(test)]
mod legacy_fixtures;
#[cfg(test)]
mod legacy_maintenance_tests;
#[cfg(test)]
mod legacy_membership_tests;
#[cfg(test)]
mod legacy_network_tests;
#[cfg(test)]
mod legacy_node_lifecycle_tests;
#[cfg(test)]
mod legacy_node_tests;
#[cfg(test)]
mod legacy_placement_tests;
#[cfg(test)]
mod legacy_request_tests;
#[cfg(test)]
mod legacy_tests;
#[cfg(test)]
mod legacy_upload_tests;
#[cfg(test)]
mod legacy_webhook_tests;
#[cfg(test)]
mod real_etcd_tests;
#[cfg(test)]
mod tests;
