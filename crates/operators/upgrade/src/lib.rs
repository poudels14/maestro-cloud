//! Leader-owned planning and execution of cluster upgrade runs.

mod backend;
mod conditions;
mod dispatch;
mod error;
mod model;
mod nixos;
mod plan;
mod reconciler;
mod snapshot;
mod writer;

pub use backend::{NodeUpgradeBackend, NodeUpgradeBackendError};
pub use dispatch::{
    NodeUpgradeCommand, NodeUpgradeCommandFailure, NodeUpgradeCommandState,
    StoreNodeUpgradeBackend, StoreNodeUpgradeBackendSettings, StoreNodeUpgradeBackendSettingsError,
};
pub use error::UpgradePlanError;
pub use model::{
    NodeUpgradeRequest, NodeUpgradeTarget, UpgradeDispatchOutcome, UpgradeInput, UpgradePlan,
    UpgradePlanAction, UpgradeSettings, UpgradeSettingsError,
};
pub use nixos::{
    NixosUpgradeSource, NixosUpgradeStager, NixosUpgradeStagerSettings, NixosUpgradeStagingError,
    ProcessNixosUpgradeStager,
};
pub use plan::{plan_upgrade, record_dispatch_outcome};
pub use reconciler::{UpgradeError, UpgradeReconciler};

#[cfg(test)]
mod tests;
