//! Leader-owned planning and execution of cluster upgrade runs.

mod conditions;
mod error;
mod model;
mod plan;

pub use error::UpgradePlanError;
pub use model::{
    NodeUpgradeRequest, NodeUpgradeTarget, UpgradeDispatchOutcome, UpgradeInput, UpgradePlan,
    UpgradePlanAction, UpgradeSettings, UpgradeSettingsError,
};
pub use plan::{plan_upgrade, record_dispatch_outcome};

#[cfg(test)]
mod tests;
