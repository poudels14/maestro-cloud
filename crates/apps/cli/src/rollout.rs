use std::io::{BufRead, Write};

use kernel_api::{
    ServiceDiffRequest, ServiceDiffResponse, ServiceDiffStatus, ServiceId, ServiceWriteRequest,
};

use crate::CliError;
use crate::api_client::request_id;
use crate::config_source::ConfigSourceReader;
use crate::service_config::{DesiredService, load_services};
use crate::services::ServiceApi;

#[allow(clippy::too_many_arguments)]
pub(crate) async fn run(
    client: &impl ServiceApi,
    config_source: &str,
    filters: &[String],
    apply: bool,
    yes: bool,
    idempotency_key: Option<String>,
    input: &mut dyn BufRead,
    output: &mut dyn Write,
    reader: &impl ConfigSourceReader,
) -> Result<(), CliError> {
    let loaded = load_services(config_source, reader).await?;
    write_ignored(&loaded.ignored_fields, output)?;
    let selected = select(&loaded.services, filters, config_source)?;
    if idempotency_key.is_some() && (!apply || selected.len() != 1) {
        return Err(CliError::invalid_input(
            "--idempotency-key requires --apply with exactly one selected service",
        ));
    }

    let mut plans = Vec::with_capacity(selected.len());
    for (service_id, desired) in selected {
        reject_pending_auxiliary(service_id, desired)?;
        let diff = client
            .diff_service(
                service_id,
                ServiceDiffRequest {
                    spec: desired.spec.clone(),
                },
            )
            .await?;
        write_diff(&diff, output)?;
        plans.push((service_id.clone(), desired.spec.clone(), diff));
    }
    let change_count = plans
        .iter()
        .filter(|(_, _, diff)| diff.status != ServiceDiffStatus::Unchanged)
        .count();
    if !apply {
        if change_count > 0 {
            writeln!(output, "\nrun with --apply to deploy these changes").map_err(output_error)?;
        }
        return Ok(());
    }
    if change_count == 0 {
        writeln!(output, "[maestro]: all selected services are unchanged").map_err(output_error)?;
        return Ok(());
    }
    if !yes && !confirm(change_count, input, output)? {
        writeln!(output, "[maestro]: aborted").map_err(output_error)?;
        return Ok(());
    }

    let mut explicit_key = idempotency_key;
    for (service_id, spec, diff) in plans {
        if diff.status == ServiceDiffStatus::Unchanged {
            continue;
        }
        let response = client
            .put_service(
                &service_id,
                &request_id(explicit_key.take())?,
                ServiceWriteRequest {
                    expected_revision: diff.expected_revision,
                    spec,
                },
            )
            .await?;
        writeln!(
            output,
            "[maestro]: rollout accepted for `{}` at generation {}",
            response.service_id, response.generation.0
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

fn reject_pending_auxiliary(
    service_id: &ServiceId,
    desired: &DesiredService,
) -> Result<(), CliError> {
    if desired.ingress.is_some() {
        return Err(CliError::invalid_input(format!(
            "services.{service_id}.ingress: ingress desired-state writes are not available yet"
        )));
    }
    if !desired.egress.is_empty() {
        return Err(CliError::invalid_input(format!(
            "services.{service_id}.deploy.egress: egress desired-state writes are not available yet"
        )));
    }
    Ok(())
}

fn write_ignored(fields: &[String], output: &mut dyn Write) -> Result<(), CliError> {
    for field in fields {
        writeln!(output, "[maestro]: warning: ignored field `{field}`").map_err(output_error)?;
    }
    Ok(())
}

fn write_diff(diff: &ServiceDiffResponse, output: &mut dyn Write) -> Result<(), CliError> {
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
