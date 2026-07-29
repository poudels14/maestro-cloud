use std::collections::BTreeMap;

use kernel_api::{
    Service, Timestamp, TrafficGeneration, TrafficGenerationId, TrafficGenerationPhase,
    TrafficGenerationSpec, TrafficGenerationStatus,
};

use crate::resource::new_generation;
use crate::{IngressPlan, IngressPlanError, PublishedTraffic};

#[allow(clippy::too_many_arguments)]
pub(crate) fn converge_desired(
    cluster_id: &kernel_api::ClusterId,
    now: Timestamp,
    service: &Service,
    mut spec: TrafficGenerationSpec,
    related: &[&TrafficGeneration],
    statuses: &mut BTreeMap<TrafficGenerationId, TrafficGenerationStatus>,
    output: &mut IngressPlan,
    remove: &mut Vec<TrafficGenerationId>,
) -> Result<Option<PublishedTraffic>, IngressPlanError> {
    let matching_active = related.iter().copied().find(|generation| {
        statuses
            .get(&generation.meta.id)
            .is_some_and(|status| status.phase == TrafficGenerationPhase::Active)
            && same_configuration(&generation.spec, &spec)
    });
    let matching_staged = related.iter().copied().find(|generation| {
        statuses
            .get(&generation.meta.id)
            .is_some_and(|status| status.phase == TrafficGenerationPhase::Staged)
            && same_configuration(&generation.spec, &spec)
    });

    let active = if let Some(active) = matching_active {
        retire_other_active(related, Some(&active.meta.id), statuses, now);
        delete_other_staged(
            related,
            None,
            statuses,
            &mut output.delete_generations,
            remove,
        );
        Some(published(active))
    } else if let Some(staged) = matching_staged {
        let status = statuses.get_mut(&staged.meta.id).ok_or_else(|| {
            IngressPlanError::MissingIndexedGeneration {
                generation_id: staged.meta.id.clone(),
            }
        })?;
        status.phase = TrafficGenerationPhase::Active;
        status.activated_at.get_or_insert(now);
        status.retired_at = None;
        retire_other_active(related, Some(&staged.meta.id), statuses, now);
        delete_other_staged(
            related,
            Some(&staged.meta.id),
            statuses,
            &mut output.delete_generations,
            remove,
        );
        Some(published(staged))
    } else {
        spec.epoch = related
            .iter()
            .map(|generation| generation.spec.epoch)
            .max()
            .unwrap_or(0)
            .saturating_add(1);
        output
            .create_generations
            .push(new_generation(cluster_id, service, spec, now)?);
        delete_other_staged(
            related,
            None,
            statuses,
            &mut output.delete_generations,
            remove,
        );
        let selected = select_active(related, statuses);
        retire_other_active(
            related,
            selected.map(|generation| &generation.meta.id),
            statuses,
            now,
        );
        selected.map(published)
    };
    Ok(active)
}

fn same_configuration(left: &TrafficGenerationSpec, right: &TrafficGenerationSpec) -> bool {
    left.service_id == right.service_id
        && left.deployment_id == right.deployment_id
        && left.routes == right.routes
        && left.targets == right.targets
}

pub(crate) fn published(generation: &TrafficGeneration) -> PublishedTraffic {
    PublishedTraffic {
        generation_id: generation.meta.id.clone(),
        spec: generation.spec.clone(),
    }
}

pub(crate) fn select_active<'a>(
    related: &[&'a TrafficGeneration],
    statuses: &BTreeMap<TrafficGenerationId, TrafficGenerationStatus>,
) -> Option<&'a TrafficGeneration> {
    related
        .iter()
        .copied()
        .filter(|generation| {
            statuses
                .get(&generation.meta.id)
                .is_some_and(|status| status.phase == TrafficGenerationPhase::Active)
        })
        .max_by(|left, right| {
            left.spec
                .epoch
                .cmp(&right.spec.epoch)
                .then_with(|| left.meta.id.cmp(&right.meta.id))
        })
}

fn retire_other_active(
    related: &[&TrafficGeneration],
    keep: Option<&TrafficGenerationId>,
    statuses: &mut BTreeMap<TrafficGenerationId, TrafficGenerationStatus>,
    now: Timestamp,
) {
    for generation in related {
        if keep == Some(&generation.meta.id) {
            continue;
        }
        if let Some(status) = statuses.get_mut(&generation.meta.id)
            && status.phase == TrafficGenerationPhase::Active
        {
            status.phase = TrafficGenerationPhase::Retired;
            status.retired_at.get_or_insert(now);
        }
    }
}

fn delete_other_staged(
    related: &[&TrafficGeneration],
    keep: Option<&TrafficGenerationId>,
    statuses: &mut BTreeMap<TrafficGenerationId, TrafficGenerationStatus>,
    delete: &mut Vec<TrafficGenerationId>,
    remove: &mut Vec<TrafficGenerationId>,
) {
    for generation in related {
        if keep == Some(&generation.meta.id) {
            continue;
        }
        if statuses
            .get(&generation.meta.id)
            .is_some_and(|status| status.phase == TrafficGenerationPhase::Staged)
        {
            statuses.remove(&generation.meta.id);
            delete.push(generation.meta.id.clone());
            remove.push(generation.meta.id.clone());
        }
    }
}

pub(crate) fn retire_all(
    related: &[&TrafficGeneration],
    statuses: &mut BTreeMap<TrafficGenerationId, TrafficGenerationStatus>,
    delete: &mut Vec<TrafficGenerationId>,
    remove: &mut Vec<TrafficGenerationId>,
    now: Timestamp,
) {
    for generation in related {
        let Some(status) = statuses.get_mut(&generation.meta.id) else {
            continue;
        };
        match status.phase {
            TrafficGenerationPhase::Staged => {
                statuses.remove(&generation.meta.id);
                delete.push(generation.meta.id.clone());
                remove.push(generation.meta.id.clone());
            }
            TrafficGenerationPhase::Active => {
                status.phase = TrafficGenerationPhase::Retired;
                status.retired_at.get_or_insert(now);
            }
            TrafficGenerationPhase::Retired => {}
        }
    }
}

pub(crate) fn collect_expired(
    related: &[&TrafficGeneration],
    statuses: &mut BTreeMap<TrafficGenerationId, TrafficGenerationStatus>,
    delete: &mut Vec<TrafficGenerationId>,
    now: Timestamp,
    grace: std::time::Duration,
) -> Vec<TrafficGenerationId> {
    let grace = i64::try_from(grace.as_millis()).unwrap_or(i64::MAX);
    let mut remove = Vec::new();
    for generation in related {
        let Some(status) = statuses.get(&generation.meta.id) else {
            continue;
        };
        if status.phase == TrafficGenerationPhase::Retired
            && status
                .retired_at
                .is_some_and(|retired| now.0 >= retired.0.saturating_add(grace))
        {
            statuses.remove(&generation.meta.id);
            delete.push(generation.meta.id.clone());
            remove.push(generation.meta.id.clone());
        }
    }
    remove
}
