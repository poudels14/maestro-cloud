use std::collections::{HashMap, HashSet};
use std::path::{Component, Path};

use docker::errors::Error as DockerError;
use docker::models::{BuildInfo, ImageInspect, ImageSummary};
use docker::query_parameters::{BuildImageOptions, BuildImageOptionsBuilder};

use crate::artifact::parse_oci_reference;
use crate::{ArtifactBuildRequest, ArtifactDigest, ArtifactStoreError};

pub(crate) const MANAGED_IMAGE_LABEL: &str = "maestro.managed";
pub(crate) const MANAGED_IMAGE_VALUE: &str = "true";
pub(crate) const TEMPORARY_TAG_PREFIX: &str = "maestro-build-local:";

pub(crate) fn build_options(
    request: &ArtifactBuildRequest,
    temporary_tag: &str,
) -> Result<BuildImageOptions, ArtifactStoreError> {
    if !request.secrets.is_empty() {
        return Err(rejected(
            "Docker Engine builds cannot transmit protected build secrets; configure a BuildKit or Depot builder",
        ));
    }
    for key in request.arguments.keys() {
        validate_build_key(key)?;
    }
    for reference in &request.tags {
        split_tag(reference.as_str())?;
    }
    let definition = definition_text(match &request.source {
        crate::ArtifactSource::Directory { definition, .. }
        | crate::ArtifactSource::Archive { definition, .. } => definition,
    })?;
    let arguments = request
        .arguments
        .iter()
        .map(|(key, value)| (key.clone(), value.expose().to_owned()))
        .collect::<HashMap<_, _>>();
    let labels = HashMap::from([(
        MANAGED_IMAGE_LABEL.to_owned(),
        MANAGED_IMAGE_VALUE.to_owned(),
    )]);
    Ok(BuildImageOptionsBuilder::default()
        .dockerfile(&definition)
        .t(temporary_tag)
        .rm(true)
        .forcerm(true)
        .buildargs(&arguments)
        .labels(&labels)
        .build())
}

pub(crate) fn definition_text(definition: &Path) -> Result<String, ArtifactStoreError> {
    if definition.as_os_str().is_empty() {
        return Ok("Dockerfile".to_owned());
    }
    if definition.is_absolute()
        || !definition
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
        || definition
            .components()
            .any(|component| component.as_os_str() == ".git")
    {
        return Err(rejected(
            "Docker build definition must be a normalized relative path",
        ));
    }
    definition
        .to_str()
        .map(ToOwned::to_owned)
        .ok_or_else(|| rejected("Docker build definition must be valid UTF-8"))
}

pub(crate) fn split_tag(reference: &str) -> Result<(String, String), ArtifactStoreError> {
    let parsed = parse_oci_reference(reference).map_err(|_| {
        rejected(format!(
            "Docker destination `{reference}` is not a valid OCI image reference"
        ))
    })?;
    if parsed.digest().is_some() {
        return Err(rejected(format!(
            "Docker destination `{reference}` must be a repository tag, not a digest"
        )));
    }
    let tag = parsed.tag().ok_or_else(|| {
        rejected(format!(
            "Docker destination `{reference}` must include a repository tag"
        ))
    })?;
    Ok((
        format!("{}/{}", parsed.registry(), parsed.repository()),
        tag.to_owned(),
    ))
}

pub(crate) fn inspect_digest(
    inspect: &ImageInspect,
    reference: &str,
) -> Result<ArtifactDigest, ArtifactStoreError> {
    let id = inspect
        .id
        .as_deref()
        .filter(|id| !id.trim().is_empty())
        .ok_or_else(|| ArtifactStoreError::Unavailable {
            message: format!("Docker inspection for `{reference}` omitted the image ID"),
        })?;
    ArtifactDigest::new(id)
}

pub(crate) fn imported_candidates(info: &BuildInfo) -> Vec<String> {
    let mut candidates = Vec::new();
    if let Some(id) = info.id.as_deref().filter(|id| !id.trim().is_empty()) {
        candidates.push(id.to_owned());
    }
    for line in info.stream.as_deref().unwrap_or_default().lines() {
        for prefix in ["Loaded image ID: ", "Loaded image: "] {
            if let Some(reference) = line.trim().strip_prefix(prefix)
                && !reference.trim().is_empty()
            {
                candidates.push(reference.trim().to_owned());
            }
        }
    }
    candidates
}

pub(crate) fn prunable_image_ids(
    images: &[ImageSummary],
    preserved: &[ArtifactDigest],
) -> Vec<String> {
    let preserved = preserved
        .iter()
        .map(ArtifactDigest::as_str)
        .collect::<HashSet<_>>();
    let mut ids = images
        .iter()
        .filter(|image| {
            image.labels.get(MANAGED_IMAGE_LABEL).map(String::as_str) == Some(MANAGED_IMAGE_VALUE)
        })
        .filter(|image| image.containers <= 0)
        .filter(|image| !preserved.contains(image.id.as_str()))
        .filter(|image| {
            image.repo_tags.is_empty()
                || image
                    .repo_tags
                    .iter()
                    .all(|tag| tag.starts_with(TEMPORARY_TAG_PREFIX))
        })
        .map(|image| image.id.clone())
        .collect::<Vec<_>>();
    ids.sort();
    ids.dedup();
    ids
}

pub(crate) fn operation_error(
    operation: &str,
    reference: Option<&str>,
    error: DockerError,
) -> ArtifactStoreError {
    match (error, reference) {
        (
            DockerError::DockerResponseServerError {
                status_code: 404, ..
            },
            Some(reference),
        ) => ArtifactStoreError::NotFound {
            reference: reference.to_owned(),
        },
        (
            DockerError::DockerResponseServerError {
                status_code: 400..=499,
                message,
            }
            | DockerError::DockerStreamError { error: message },
            _,
        ) => rejected(format!("Docker {operation}: {message}")),
        (error, _) => ArtifactStoreError::Unavailable {
            message: format!("Docker {operation}: {error}"),
        },
    }
}

pub(crate) fn stream_error(operation: &str, error: DockerError) -> ArtifactStoreError {
    ArtifactStoreError::Stream {
        message: format!("Docker {operation}: {error}"),
    }
}

fn validate_build_key(key: &str) -> Result<(), ArtifactStoreError> {
    if key.is_empty() || key.contains('=') || key.chars().any(char::is_control) {
        Err(rejected(format!(
            "Docker build argument name `{key}` is invalid"
        )))
    } else {
        Ok(())
    }
}

fn rejected(message: impl Into<String>) -> ArtifactStoreError {
    ArtifactStoreError::Rejected {
        message: message.into(),
    }
}
