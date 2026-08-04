use std::collections::{BTreeSet, HashSet};

use containerd::services::v1::Image;
use containerd::tonic::{Code, Request, Status};
use containerd::types::Platform;

use crate::artifact::parse_oci_reference;
use crate::{ArtifactDigest, ArtifactStoreError};

pub(crate) const MANAGED_ARTIFACT_LABEL: &str = "com.maestro.managed-artifact";
pub(crate) const MANAGED_ARTIFACT_VALUE: &str = "true";

pub(crate) fn host_platform() -> Platform {
    Platform {
        os: "linux".to_owned(),
        architecture: match std::env::consts::ARCH {
            "x86_64" => "amd64",
            "aarch64" => "arm64",
            architecture => architecture,
        }
        .to_owned(),
        variant: String::new(),
        os_version: String::new(),
    }
}

pub(crate) fn namespaced_artifact<T>(
    message: T,
    namespace: &str,
) -> Result<Request<T>, ArtifactStoreError> {
    artifact_request(message, namespace, None)
}

pub(crate) fn artifact_request<T>(
    message: T,
    namespace: &str,
    lease_id: Option<&str>,
) -> Result<Request<T>, ArtifactStoreError> {
    let value = namespace
        .parse()
        .map_err(|error| ArtifactStoreError::Rejected {
            message: format!("containerd namespace is invalid gRPC metadata: {error}"),
        })?;
    let mut request = Request::new(message);
    request.metadata_mut().insert("containerd-namespace", value);
    if let Some(lease_id) = lease_id {
        let value = lease_id
            .parse()
            .map_err(|error| ArtifactStoreError::Rejected {
                message: format!("containerd lease is invalid gRPC metadata: {error}"),
            })?;
        request.metadata_mut().insert("containerd-lease", value);
    }
    Ok(request)
}

pub(crate) fn image_digest(
    image: &Image,
    reference: &str,
) -> Result<ArtifactDigest, ArtifactStoreError> {
    let digest = image
        .target
        .as_ref()
        .map(|target| target.digest.as_str())
        .filter(|digest| !digest.trim().is_empty())
        .ok_or_else(|| ArtifactStoreError::Unavailable {
            message: format!("containerd image `{reference}` omitted its target digest"),
        })?;
    let prefix = reference_prefix(reference)?;
    ArtifactDigest::new(format!("{prefix}@{digest}"))
}

pub(crate) fn reference_prefix(reference: &str) -> Result<String, ArtifactStoreError> {
    let reference = parse_oci_reference(reference)?;
    Ok(format!(
        "{}/{}",
        reference.registry(),
        reference.repository()
    ))
}

pub(crate) fn registry_reference(reference: &str) -> Result<String, ArtifactStoreError> {
    Ok(parse_oci_reference(reference)?.whole())
}

pub(crate) fn select_image(
    images: &[Image],
    digest: &ArtifactDigest,
) -> Result<Image, ArtifactStoreError> {
    let mut matches = images
        .iter()
        .filter(|image| target_digest(image) == Some(content_digest(digest)))
        .cloned()
        .collect::<Vec<_>>();
    matches.sort_by(|left, right| {
        let left_managed = is_managed(left);
        let right_managed = is_managed(right);
        right_managed
            .cmp(&left_managed)
            .then_with(|| left.name.cmp(&right.name))
    });
    matches
        .into_iter()
        .next()
        .ok_or_else(|| ArtifactStoreError::NotFound {
            reference: digest.as_str().to_owned(),
        })
}

pub(crate) fn prune_candidates(images: &[Image], preserved: &[ArtifactDigest]) -> Vec<Image> {
    let preserved = preserved.iter().map(content_digest).collect::<HashSet<_>>();
    let mut candidates = images
        .iter()
        .filter(|image| is_managed(image))
        .filter(|image| target_digest(image).is_some_and(|digest| !preserved.contains(digest)))
        .cloned()
        .collect::<Vec<_>>();
    candidates.sort_by(|left, right| left.name.cmp(&right.name));
    candidates
}

pub(crate) fn removed_digests(
    candidates: &[Image],
    remaining: &[Image],
) -> Result<Vec<ArtifactDigest>, ArtifactStoreError> {
    let still_referenced = remaining
        .iter()
        .filter_map(target_digest)
        .collect::<HashSet<_>>();
    let removed = candidates
        .iter()
        .filter_map(target_digest)
        .filter(|digest| !still_referenced.contains(digest))
        .collect::<BTreeSet<_>>();
    removed.into_iter().map(ArtifactDigest::new).collect()
}

pub(crate) fn operation_error(
    operation: &str,
    reference: Option<&str>,
    error: Status,
) -> ArtifactStoreError {
    let message = error.message().to_owned();
    match error.code() {
        Code::NotFound => ArtifactStoreError::NotFound {
            reference: reference.unwrap_or("containerd artifact").to_owned(),
        },
        Code::InvalidArgument
        | Code::AlreadyExists
        | Code::FailedPrecondition
        | Code::OutOfRange
        | Code::PermissionDenied
        | Code::Unauthenticated => ArtifactStoreError::Rejected {
            message: format!("containerd {operation}: {message}"),
        },
        _ => ArtifactStoreError::Unavailable {
            message: format!("containerd {operation}: {message}"),
        },
    }
}

pub(crate) fn stream_error(operation: &str, error: impl std::fmt::Display) -> ArtifactStoreError {
    ArtifactStoreError::Stream {
        message: format!("containerd {operation}: {error}"),
    }
}

pub(crate) fn is_not_found(error: &Status) -> bool {
    error.code() == Code::NotFound
}

pub(crate) fn target_digest(image: &Image) -> Option<&str> {
    image
        .target
        .as_ref()
        .map(|target| target.digest.as_str())
        .filter(|digest| !digest.is_empty())
}

fn is_managed(image: &Image) -> bool {
    image.labels.get(MANAGED_ARTIFACT_LABEL).map(String::as_str) == Some(MANAGED_ARTIFACT_VALUE)
}

fn content_digest(digest: &ArtifactDigest) -> &str {
    digest.content_digest()
}
