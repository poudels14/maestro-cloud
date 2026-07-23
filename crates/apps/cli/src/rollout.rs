use std::io::{BufRead, Write};

use kernel_api::{
    ServiceDiffStatus, ServiceId, ServiceRolloutDiffRequest, ServiceRolloutDiffResponse,
    ServiceRolloutRequest,
};

use crate::CliError;
use crate::api_client::request_id;
use crate::config_source::ConfigSourceReader;
use crate::service_config::{DesiredService, load_services};
use crate::services::ServiceApi;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RolloutMode {
    Preview,
    Apply,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FrozenServicePolicy {
    HonorFreeze,
    BypassFreeze,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ConfirmationMode {
    Prompt,
    AssumeYes,
}

pub(crate) struct RolloutOptions<'a> {
    pub(crate) config_source: &'a str,
    pub(crate) filters: &'a [String],
    pub(crate) mode: RolloutMode,
    pub(crate) frozen_service_policy: FrozenServicePolicy,
    pub(crate) confirmation: ConfirmationMode,
    pub(crate) idempotency_key: Option<String>,
}

pub(crate) async fn run(
    client: &impl ServiceApi,
    options: RolloutOptions<'_>,
    input: &mut dyn BufRead,
    output: &mut dyn Write,
    reader: &impl ConfigSourceReader,
) -> Result<(), CliError> {
    let loaded = load_services(options.config_source, reader).await?;
    write_ignored(&loaded.ignored_fields, output)?;
    let selected = select(&loaded.services, options.filters, options.config_source)?;
    if options.idempotency_key.is_some()
        && (options.mode != RolloutMode::Apply || selected.len() != 1)
    {
        return Err(CliError::invalid_input(
            "--idempotency-key requires --apply with exactly one selected service",
        ));
    }

    let mut plans = Vec::with_capacity(selected.len());
    for (service_id, desired) in selected {
        let desired = desired.rollout_spec(service_id)?;
        let diff = client
            .diff_rollout(
                service_id,
                ServiceRolloutDiffRequest {
                    desired: desired.clone(),
                },
            )
            .await?;
        write_diff(&diff, output)?;
        plans.push((service_id.clone(), desired, diff));
    }
    let change_count = plans
        .iter()
        .filter(|(_, _, diff)| diff.status != ServiceDiffStatus::Unchanged)
        .count();
    if options.mode == RolloutMode::Preview {
        if change_count > 0 {
            writeln!(output, "\nrun with --apply to deploy these changes").map_err(output_error)?;
        }
        return Ok(());
    }
    if change_count == 0 {
        writeln!(output, "[maestro]: all selected services are unchanged").map_err(output_error)?;
        return Ok(());
    }
    if options.confirmation == ConfirmationMode::Prompt && !confirm(change_count, input, output)? {
        writeln!(output, "[maestro]: aborted").map_err(output_error)?;
        return Ok(());
    }

    let mut explicit_key = options.idempotency_key;
    for (service_id, spec, diff) in plans {
        if diff.status == ServiceDiffStatus::Unchanged {
            continue;
        }
        let response = client
            .apply_rollout(
                &service_id,
                &request_id(explicit_key.take())?,
                ServiceRolloutRequest {
                    expected_revisions: diff.expected_revisions,
                    force: options.frozen_service_policy == FrozenServicePolicy::BypassFreeze,
                    desired: spec,
                },
            )
            .await?;
        writeln!(
            output,
            "[maestro]: rollout accepted for `{}` at generation {}",
            response.service_id, response.service_generation.0
        )
        .map_err(output_error)?;
    }
    Ok(())
}

fn select<'a>(
    services: &'a std::collections::BTreeMap<ServiceId, DesiredService>,
    filters: &[String],
    source: &str,
) -> Result<Vec<(&'a ServiceId, &'a DesiredService)>, CliError> {
    if filters.is_empty() {
        return Ok(services.iter().collect());
    }
    let mut selected = Vec::new();
    for filter in filters {
        let id = ServiceId::new(filter.clone())
            .map_err(|error| CliError::invalid_input(error.to_string()))?;
        let service = services.get_key_value(&id).ok_or_else(|| {
            CliError::invalid_input(format!("service `{id}` is not configured in `{source}`"))
        })?;
        if !selected
            .iter()
            .any(|(selected_id, _)| *selected_id == service.0)
        {
            selected.push(service);
        }
    }
    Ok(selected)
}

pub(crate) fn write_ignored(fields: &[String], output: &mut dyn Write) -> Result<(), CliError> {
    for field in fields {
        writeln!(output, "[maestro]: warning: ignored field `{field}`").map_err(output_error)?;
    }
    Ok(())
}

pub(crate) fn write_diff(
    diff: &ServiceRolloutDiffResponse,
    output: &mut dyn Write,
) -> Result<(), CliError> {
    match diff.status {
        ServiceDiffStatus::New => {
            writeln!(output, "\n  + {} (new service)", diff.service_id).map_err(output_error)?;
        }
        ServiceDiffStatus::Unchanged => {
            writeln!(output, "\n  = {} (no changes)", diff.service_id).map_err(output_error)?;
        }
        ServiceDiffStatus::Changed => {
            writeln!(output, "\n  ~ {} (changed)", diff.service_id).map_err(output_error)?;
            for change in &diff.changes {
                match (&change.from, &change.to) {
                    (None, Some(to)) => writeln!(output, "    + {}: {to}", change.field),
                    (Some(from), None) => writeln!(output, "    - {}: {from}", change.field),
                    (Some(from), Some(to)) => {
                        writeln!(output, "    ~ {}: {from} -> {to}", change.field)
                    }
                    (None, None) => Ok(()),
                }
                .map_err(output_error)?;
            }
        }
    }
    Ok(())
}

fn confirm(
    change_count: usize,
    input: &mut dyn BufRead,
    output: &mut dyn Write,
) -> Result<bool, CliError> {
    write!(output, "Apply {change_count} service change(s)? [y/N]: ").map_err(output_error)?;
    output
        .flush()
        .map_err(|source| CliError::io("failed to flush confirmation prompt", source))?;
    let mut answer = String::new();
    input
        .read_line(&mut answer)
        .map_err(|source| CliError::io("failed to read confirmation", source))?;
    Ok(matches!(
        answer.trim().to_ascii_lowercase().as_str(),
        "y" | "yes"
    ))
}

fn output_error(source: std::io::Error) -> CliError {
    CliError::io("failed to write command output", source)
}
