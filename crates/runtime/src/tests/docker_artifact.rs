use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};

use docker::errors::Error as DockerError;
use docker::models::{BuildInfo, ImageSummary};
use kernel_api::SecretValue;

use crate::docker_artifact::bounded_chunk;
use crate::docker_artifact_context::write_directory_archive;
use crate::docker_artifact_support::{
    MANAGED_IMAGE_LABEL, MANAGED_IMAGE_VALUE, TEMPORARY_TAG_PREFIX, build_options, definition_text,
    imported_candidates, operation_error, prunable_image_ids, split_tag,
};
use crate::{
    ArtifactBuildRequest, ArtifactDigest, ArtifactReference, ArtifactSource, ArtifactStoreError,
    RuntimeCapability, WorkloadRuntime,
};

#[test]
fn docker_build_options_preserve_public_arguments_and_managed_ownership() {
    let request = ArtifactBuildRequest {
        source: ArtifactSource::Directory {
            root: PathBuf::from("/tmp/maestro-build"),
            definition: PathBuf::from("containers/App.Dockerfile"),
        },
        arguments: BTreeMap::from([
            ("MODE".to_owned(), "release".to_owned()),
            ("REVISION".to_owned(), "abc123".to_owned()),
        ]),
        secrets: BTreeMap::new(),
        tags: vec![ArtifactReference::new("registry.example/app:v1").unwrap()],
    };

    let options = build_options(&request, "maestro-build-local:1-1").unwrap();

    assert_eq!(options.dockerfile, "containers/App.Dockerfile");
    assert_eq!(options.t.as_deref(), Some("maestro-build-local:1-1"));
    assert!(options.rm);
    assert!(options.forcerm);
    assert_eq!(
        options.buildargs,
        Some(HashMap::from([
            ("MODE".to_owned(), "release".to_owned()),
            ("REVISION".to_owned(), "abc123".to_owned()),
        ]))
    );
    assert_eq!(
        options.labels,
        Some(HashMap::from([(
            MANAGED_IMAGE_LABEL.to_owned(),
            MANAGED_IMAGE_VALUE.to_owned(),
        )]))
    );
}

#[test]
fn docker_build_options_reject_secrets_before_contacting_the_daemon() {
    let mut request = build_request();
    request
        .secrets
        .insert("TOKEN".to_owned(), SecretValue::new("do-not-log"));

    let error = build_options(&request, "maestro-build-local:1-1").unwrap_err();

    assert!(matches!(error, ArtifactStoreError::Rejected { .. }));
    assert!(!error.to_string().contains("do-not-log"));
    assert!(error.to_string().contains("BuildKit or Depot"));
}

#[test]
fn docker_paths_and_tags_reject_ambiguous_shapes() {
    assert_eq!(definition_text(Path::new("")).unwrap(), "Dockerfile");
    assert!(definition_text(Path::new("../Dockerfile")).is_err());
    assert!(definition_text(Path::new(".git/Dockerfile")).is_err());
    assert_eq!(
        split_tag("registry.example:5000/team/app:v2").unwrap(),
        ("registry.example:5000/team/app", "v2")
    );
    assert_eq!(split_tag("team/app").unwrap(), ("team/app", "latest"));
    assert!(split_tag("team/app@sha256:abc").is_err());
    assert!(split_tag("team/app:").is_err());
}

#[test]
fn import_output_recovers_ids_and_named_references() {
    let info = BuildInfo {
        id: Some("sha256:direct".to_owned()),
        stream: Some(
            "Loaded image: registry.example/app:v1\nLoaded image ID: sha256:loaded\n".to_owned(),
        ),
        ..Default::default()
    };

    assert_eq!(
        imported_candidates(&info),
        vec!["sha256:direct", "registry.example/app:v1", "sha256:loaded"]
    );
}

#[test]
fn exported_transport_chunks_are_bounded_without_losing_bytes() {
    let original = vec![7_u8; 160 * 1_024];
    let mut pending = Some(bytes::Bytes::from(original.clone()));
    let mut rebuilt = Vec::new();
    let mut lengths = Vec::new();

    while let Some(chunk) = bounded_chunk(&mut pending) {
        lengths.push(chunk.len());
        rebuilt.extend(chunk);
    }

    assert_eq!(lengths, vec![64 * 1_024, 64 * 1_024, 32 * 1_024]);
    assert_eq!(rebuilt, original);
}

#[test]
fn prune_selects_only_unused_internal_images_not_preserved_digests() {
    let images = vec![
        image("sha256:old", 0, &["maestro-build-local:1-1"], true),
        image("sha256:kept", 0, &["maestro-build-local:1-2"], true),
        image("sha256:running", 1, &["maestro-build-local:1-3"], true),
        image("sha256:tagged", 0, &["registry.example/app:v1"], true),
        image("sha256:foreign", 0, &[], false),
        image("sha256:unknown", -1, &["maestro-build-local:1-4"], true),
    ];

    assert_eq!(
        prunable_image_ids(&images, &[ArtifactDigest::new("sha256:kept").unwrap()]),
        vec!["sha256:old", "sha256:unknown"]
    );
}

#[test]
fn docker_runtime_advertises_every_artifact_operation_it_implements() {
    let client = docker::Docker::connect_with_http_defaults().unwrap();
    let runtime = crate::DockerRuntime::new(client);
    let capabilities = runtime.capabilities();

    assert!(capabilities.supports(RuntimeCapability::BuildArtifact));
    assert!(capabilities.supports(RuntimeCapability::PushArtifact));
    assert!(capabilities.supports(RuntimeCapability::TransferArtifact));
}

#[test]
fn docker_errors_keep_missing_rejected_and_retryable_failures_distinct() {
    let missing = operation_error(
        "inspect",
        Some("app:v1"),
        DockerError::DockerResponseServerError {
            status_code: 404,
            message: "not found".to_owned(),
        },
    );
    let rejected = operation_error(
        "build",
        None,
        DockerError::DockerStreamError {
            error: "Dockerfile failed".to_owned(),
        },
    );
    let unavailable = operation_error(
        "list",
        None,
        DockerError::DockerResponseServerError {
            status_code: 503,
            message: "starting".to_owned(),
        },
    );

    assert!(matches!(missing, ArtifactStoreError::NotFound { .. }));
    assert!(matches!(rejected, ArtifactStoreError::Rejected { .. }));
    assert!(matches!(
        unavailable,
        ArtifactStoreError::Unavailable { .. }
    ));
}

#[test]
fn directory_context_honors_ignores_but_always_carries_the_definition() {
    let temporary = tempfile::tempdir().unwrap();
    std::fs::write(
        temporary.path().join(".dockerignore"),
        "ignored\nDockerfile\n",
    )
    .unwrap();
    std::fs::write(temporary.path().join("Dockerfile"), "FROM scratch\n").unwrap();
    std::fs::write(temporary.path().join("app.txt"), "included").unwrap();
    std::fs::write(temporary.path().join("ignored"), "excluded").unwrap();
    std::fs::create_dir(temporary.path().join(".git")).unwrap();
    std::fs::write(temporary.path().join(".git/HEAD"), "secret history").unwrap();
    let mut bytes = Vec::new();

    write_directory_archive(temporary.path(), Path::new("Dockerfile"), &mut bytes).unwrap();

    let mut archive = tar::Archive::new(bytes.as_slice());
    let mut paths = archive
        .entries()
        .unwrap()
        .map(|entry| entry.unwrap().path().unwrap().into_owned())
        .collect::<Vec<_>>();
    paths.sort();
    assert!(paths.contains(&PathBuf::from("Dockerfile")));
    assert!(paths.contains(&PathBuf::from("app.txt")));
    assert!(!paths.contains(&PathBuf::from("ignored")));
    assert!(!paths.iter().any(|path| path.starts_with(".git")));
}

#[cfg(unix)]
#[test]
fn directory_context_rejects_links_that_escape_the_source() {
    use std::os::unix::fs::symlink;

    let temporary = tempfile::tempdir().unwrap();
    std::fs::write(temporary.path().join("Dockerfile"), "FROM scratch\n").unwrap();
    symlink("../outside", temporary.path().join("escape")).unwrap();
    let mut bytes = Vec::new();

    let error =
        write_directory_archive(temporary.path(), Path::new("Dockerfile"), &mut bytes).unwrap_err();

    assert!(matches!(error, ArtifactStoreError::Rejected { .. }));
    assert!(error.to_string().contains("escapes the context"));
}

fn build_request() -> ArtifactBuildRequest {
    ArtifactBuildRequest {
        source: ArtifactSource::Directory {
            root: PathBuf::from("/tmp/maestro-build"),
            definition: PathBuf::from("Dockerfile"),
        },
        arguments: BTreeMap::new(),
        secrets: BTreeMap::new(),
        tags: Vec::new(),
    }
}

fn image(id: &str, containers: i64, tags: &[&str], managed: bool) -> ImageSummary {
    let labels = if managed {
        HashMap::from([(
            MANAGED_IMAGE_LABEL.to_owned(),
            MANAGED_IMAGE_VALUE.to_owned(),
        )])
    } else {
        HashMap::new()
    };
    ImageSummary {
        id: id.to_owned(),
        repo_tags: tags.iter().map(|tag| (*tag).to_owned()).collect(),
        labels,
        containers,
        ..Default::default()
    }
}

#[test]
fn temporary_tag_prefix_stays_private_to_the_adapter() {
    assert_eq!(TEMPORARY_TAG_PREFIX, "maestro-build-local:");
}
