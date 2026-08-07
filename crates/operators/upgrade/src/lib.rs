//! Leader-owned planning and execution of cluster upgrade runs.

mod agent;
mod backend;
mod conditions;
mod dispatch;
mod error;
mod model;
mod nixos;
mod plan;
mod plan_phases;
mod plan_support;
mod reboot;
mod reconciler;
mod recovery;
mod snapshot;
mod writer;

pub use agent::{
    NodeUpgradeAgent, NodeUpgradeAgentAction, NodeUpgradeAgentError, NodeUpgradeAgentSettings,
    NodeUpgradeAgentSettingsError,
};
pub use backend::{NodeUpgradeBackend, NodeUpgradeBackendError};
pub use dispatch::{
    NodeUpgradeCommand, NodeUpgradeCommandFailure, NodeUpgradeCommandState,
    StoreNodeUpgradeBackend, StoreNodeUpgradeBackendSettings, StoreNodeUpgradeBackendSettingsError,
};
pub use error::UpgradePlanError;
pub use model::{
    NodeUpgradeRequest, NodeUpgradeTarget, PlannedStoreRecovery, UpgradeDispatchOutcome,
    UpgradeInput, UpgradePlan, UpgradePlanAction, UpgradeSettings, UpgradeSettingsError,
};
pub use nixos::{
    NixosUpgradeSource, NixosUpgradeStager, NixosUpgradeStagerSettings, NixosUpgradeStagingError,
    ProcessNixosUpgradeStager,
};
pub use plan::{plan_upgrade, record_dispatch_outcome};
pub use reboot::{NodeRebootError, NodeRebooter, ProcessNodeRebooter};
pub use reconciler::{UpgradeError, UpgradeReconciler};
pub use recovery::{ActivatedStoreRecovery, FileStoreRecoveryMarker, StoreRecoveryMarkerError};

#[cfg(test)]
mod tests;
