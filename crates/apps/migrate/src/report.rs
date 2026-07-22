use std::collections::BTreeMap;

use kernel_api::ClusterId;
use serde::Serialize;

use crate::MigrationPlan;

const REPORT_SCHEMA_VERSION: u32 = 1;

impl MigrationPlan {
    /// Builds a stable, secret-free review report for this exact plan.
    pub fn report(&self) -> MigrationPlanReport {
        let mut resources = BTreeMap::new();
        for write in self.writes() {
            *resources.entry(write.kind().to_string()).or_insert(0) += 1;
        }
        MigrationPlanReport {
            schema_version: REPORT_SCHEMA_VERSION,
            cluster_id: self.cluster_id().clone(),
            source_sha256: hex::encode(self.source_digest()),
            total_resources: self.writes().len(),
            resources,
            request_claims: self.request_claims().len(),
        }
    }
}

/// Secret-free summary of one snapshot-bound migration plan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MigrationPlanReport {
    schema_version: u32,
    cluster_id: ClusterId,
    source_sha256: String,
    total_resources: usize,
    resources: BTreeMap<String, usize>,
    request_claims: usize,
}

impl MigrationPlanReport {
    /// Encodes stable pretty JSON suitable for a reviewed cutover artifact.
    pub fn encode(&self) -> Result<Vec<u8>, ReportError> {
        serde_json::to_vec_pretty(self).map_err(|error| ReportError {
            message: error.to_string(),
        })
    }
}

/// A secret-free plan report could not be encoded.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("could not encode migration plan report: {message}")]
pub struct ReportError {
    message: String,
}
