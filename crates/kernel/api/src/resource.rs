use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::ObjectMeta;

/// A typed Maestro resource with desired and observed state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Object<Id, Spec, Status> {
    /// Identity, concurrency, ownership, and deletion metadata.
    pub meta: ObjectMeta<Id>,
    /// Desired state written by users or another controller.
    pub spec: Spec,
    /// Observed state written by the resource's owning controller.
    pub status: Status,
}
