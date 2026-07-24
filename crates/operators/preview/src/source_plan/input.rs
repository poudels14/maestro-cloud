use std::collections::BTreeMap;

use kernel_api::{Preview, PreviewPolicy, Service, ServiceId};

use super::{PreviewSourceDiagnostic, PreviewSourcePlanError, RepositoryPullRequests};
use crate::repository::service_repository;

#[derive(Clone)]
pub(super) struct PreviewBase<'a> {
    pub(super) service: &'a Service,
    pub(super) policy: &'a PreviewPolicy,
    pub(super) repository: String,
}

pub(super) fn preview_bases(
    services: &[Service],
) -> (
    BTreeMap<ServiceId, PreviewBase<'_>>,
    Vec<PreviewSourceDiagnostic>,
) {
    let mut bases = BTreeMap::new();
    let mut diagnostics = Vec::new();
    for service in services {
        let Some(policy) = service.spec.preview.as_ref() else {
            continue;
        };
        if service.meta.deletion_timestamp.is_some() {
            continue;
        }
        match service_repository(service) {
            Ok(repository) => {
                bases.insert(
                    service.meta.id.clone(),
                    PreviewBase {
                        service,
                        policy,
                        repository: repository.full_name,
                    },
                );
            }
            Err(message) => diagnostics.push(PreviewSourceDiagnostic {
                service_id: service.meta.id.clone(),
                message,
            }),
        }
    }
    (bases, diagnostics)
}

pub(super) fn repository_snapshots(
    repositories: &[RepositoryPullRequests],
) -> Result<BTreeMap<String, &RepositoryPullRequests>, PreviewSourcePlanError> {
    let mut snapshots = BTreeMap::new();
    for snapshot in repositories {
        let repository = snapshot.repository.to_ascii_lowercase();
        if snapshots.insert(repository.clone(), snapshot).is_some() {
            return Err(PreviewSourcePlanError::DuplicateRepository { repository });
        }
    }
    Ok(snapshots)
}

pub(super) fn existing_previews(
    previews: &[Preview],
) -> Result<BTreeMap<(ServiceId, u64), &Preview>, PreviewSourcePlanError> {
    let mut existing = BTreeMap::new();
    for preview in previews {
        let key = (
            preview.spec.base_service_id.clone(),
            preview.spec.pull_request_number,
        );
        if existing.insert(key.clone(), preview).is_some() {
            return Err(PreviewSourcePlanError::DuplicatePreview {
                service_id: key.0,
                pull_request_number: key.1,
            });
        }
    }
    Ok(existing)
}
