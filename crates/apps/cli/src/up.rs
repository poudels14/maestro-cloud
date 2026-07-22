use std::io::Write;
use std::path::Path;

use kernel_api::{
    ArtifactArchiveId, RequestId, ServiceDiffStatus, ServiceRolloutDiffRequest,
    ServiceRolloutRequest,
};
use sha2::{Digest, Sha256};

use crate::CliError;
use crate::config_source::ConfigSourceReader;
use crate::rollout::{write_diff, write_ignored};
use crate::service_config::load_uploaded_service;
use crate::services::ServiceApi;

pub(crate) async fn run(
    client: &impl ServiceApi,
    config_source: &str,
    service_id: String,
    context: &Path,
    request_id: RequestId,
    output: &mut dyn Write,
    reader: &impl ConfigSourceReader,
) -> Result<(), CliError> {
    let context_path = context.to_path_buf();
    let archive = tokio::task::spawn_blocking(move || crate::archive::pack_context(&context_path))
        .await
        .map_err(|error| {
            CliError::invalid_input(format!("build context packer did not complete: {error}"))
        })??;
    let archive_id = ArtifactArchiveId::from_sha256(Sha256::digest(&archive).into());
    let archive_size = u64::try_from(archive.len()).unwrap_or(u64::MAX);
    let service_id = kernel_api::ServiceId::new(service_id)
        .map_err(|error| CliError::invalid_input(error.to_string()))?;
    let loaded =
        load_uploaded_service(config_source, service_id, archive_id.clone(), reader).await?;
    write_ignored(&loaded.ignored_fields, output)?;
    writeln!(
        output,
        "[maestro]: uploading {} bytes from `{}` as `{archive_id}`",
        archive_size,
        context.display()
    )
    .map_err(output_error)?;
    let uploaded = client.upload_artifact_archive(&archive_id, archive).await?;
    if uploaded.archive_id != archive_id || uploaded.size_bytes != archive_size {
        return Err(CliError::invalid_api_response(
            "artifact upload receipt does not match the submitted content",
        ));
    }
    writeln!(
        output,
        "[maestro]: archive `{}` is available ({} bytes)",
        uploaded.archive_id, uploaded.size_bytes
    )
    .map_err(output_error)?;

    let desired = loaded.desired.rollout_spec(&loaded.service_id)?;
    let diff = client
        .diff_rollout(
            &loaded.service_id,
            ServiceRolloutDiffRequest {
                desired: desired.clone(),
            },
        )
        .await?;
    write_diff(&diff, output)?;
    if diff.status == ServiceDiffStatus::Unchanged {
        writeln!(
            output,
            "[maestro]: service `{}` is unchanged",
            loaded.service_id
        )
        .map_err(output_error)?;
        return Ok(());
    }
    let response = client
        .apply_rollout(
            &loaded.service_id,
            &request_id,
            ServiceRolloutRequest {
                expected_revisions: diff.expected_revisions,
                desired,
            },
        )
        .await?;
    writeln!(
        output,
        "[maestro]: local rollout accepted for `{}` at generation {}",
        response.service_id, response.service_generation.0
    )
    .map_err(output_error)
}

fn output_error(source: std::io::Error) -> CliError {
    CliError::io("failed to write command output", source)
}
