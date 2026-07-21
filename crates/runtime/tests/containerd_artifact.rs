#![allow(clippy::expect_used, clippy::unwrap_used)]

#[cfg(all(feature = "containerd", target_os = "linux"))]
use std::collections::BTreeMap;
#[cfg(all(feature = "containerd", target_os = "linux"))]
use std::path::PathBuf;
#[cfg(all(feature = "containerd", target_os = "linux"))]
use std::sync::Arc;
#[cfg(all(feature = "containerd", target_os = "linux"))]
use std::time::Duration;

#[cfg(all(feature = "containerd", target_os = "linux"))]
use kernel_api::SecretValue;
#[cfg(all(feature = "containerd", target_os = "linux"))]
use runtime::{
    ArtifactBuildRequest, ArtifactDigest, ArtifactPrunePolicy, ArtifactReference, ArtifactSource,
    ArtifactStore, ContainerdRuntime, ContainerdRuntimeSettings, TokioRuntimeClock,
};

#[cfg(all(feature = "containerd", target_os = "linux"))]
#[tokio::test]
#[ignore = "requires isolated containerd storage and registry access"]
async fn containerd_artifact_pull_export_import_and_prune_round_trip() {
    let reference = ArtifactReference::new(
        std::env::var("MAESTRO_CONTAINERD_ARTIFACT_TEST_IMAGE")
            .expect("MAESTRO_CONTAINERD_ARTIFACT_TEST_IMAGE must name a remote image"),
    )
    .unwrap();
    let temporary_root = tempfile::tempdir().unwrap();
    let settings = ContainerdRuntimeSettings {
        socket: std::env::var_os("MAESTRO_CONTAINERD_SOCKET").map_or_else(
            || PathBuf::from("/run/containerd/containerd.sock"),
            PathBuf::from,
        ),
        namespace: std::env::var("MAESTRO_CONTAINERD_NAMESPACE")
            .unwrap_or_else(|_| "maestro-artifact-test".to_owned()),
        snapshotter: std::env::var("MAESTRO_CONTAINERD_SNAPSHOTTER")
            .unwrap_or_else(|_| "overlayfs".to_owned()),
        state_root: temporary_root.path().to_path_buf(),
        ..ContainerdRuntimeSettings::default()
    };
    let runtime = ContainerdRuntime::connect(settings, Arc::new(TokioRuntimeClock::new()))
        .await
        .unwrap();

    let pulled = tokio::time::timeout(Duration::from_secs(30), runtime.pull(&reference))
        .await
        .expect("containerd pull timed out")
        .unwrap();
    assert!(pulled.as_str().contains("@sha256:"));
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(30), runtime.resolve_digest(&reference))
            .await
            .expect("containerd resolve timed out")
            .unwrap(),
        pulled
    );
    let archive = tokio::time::timeout(Duration::from_secs(30), runtime.export(&pulled))
        .await
        .expect("containerd export setup timed out")
        .unwrap();
    let imported = tokio::time::timeout(Duration::from_secs(30), runtime.import(archive))
        .await
        .expect("containerd import timed out")
        .unwrap();
    assert!(imported.as_str().contains("@sha256:"));
    let imported_reference = ArtifactReference::new(imported.as_str()).unwrap();
    assert_eq!(
        tokio::time::timeout(
            Duration::from_secs(30),
            runtime.resolve_digest(&imported_reference),
        )
        .await
        .expect("containerd imported resolve timed out")
        .unwrap(),
        imported
    );

    let preserved = tokio::time::timeout(
        Duration::from_secs(30),
        runtime.prune(&ArtifactPrunePolicy::Preserve(vec![
            pulled.clone(),
            imported.clone(),
        ])),
    )
    .await
    .expect("containerd preserving prune timed out")
    .unwrap();
    assert!(preserved.removed.is_empty());
    let removed = tokio::time::timeout(
        Duration::from_secs(30),
        runtime.prune(&ArtifactPrunePolicy::Preserve(Vec::<ArtifactDigest>::new())),
    )
    .await
    .expect("containerd removal prune timed out")
    .unwrap();
    assert!(!removed.removed.is_empty());
}

#[cfg(all(feature = "containerd", target_os = "linux"))]
#[tokio::test]
#[ignore = "requires containerd, buildkitd, and registry access"]
async fn containerd_buildkit_build_import_tag_and_prune_round_trip() {
    let temporary_root = tempfile::tempdir().unwrap();
    let context = temporary_root.path().join("context");
    std::fs::create_dir(&context).unwrap();
    std::fs::write(
        context.join("Dockerfile"),
        "FROM busybox:1.36.1\n\
         RUN --mount=type=secret,id=TOKEN test -s /run/secrets/TOKEN\n\
         COPY payload /payload\n",
    )
    .unwrap();
    std::fs::write(context.join("payload"), "maestro-buildkit-acceptance\n").unwrap();
    let process_id = std::process::id();
    let namespace = format!("maestro-build-test-{process_id}");
    let tag =
        ArtifactReference::new(format!("maestro.local/build-acceptance:{process_id}")).unwrap();
    let settings = ContainerdRuntimeSettings {
        socket: std::env::var_os("MAESTRO_CONTAINERD_SOCKET").map_or_else(
            || PathBuf::from("/run/containerd/containerd.sock"),
            PathBuf::from,
        ),
        namespace,
        snapshotter: std::env::var("MAESTRO_CONTAINERD_SNAPSHOTTER")
            .unwrap_or_else(|_| "overlayfs".to_owned()),
        state_root: temporary_root.path().join("state"),
        buildctl: std::env::var_os("MAESTRO_BUILDCTL")
            .map_or_else(|| PathBuf::from("buildctl"), PathBuf::from),
        buildkit_address: std::env::var("MAESTRO_BUILDKIT_ADDRESS")
            .unwrap_or_else(|_| "unix:///run/buildkit/buildkitd.sock".to_owned()),
        build_timeout: Duration::from_secs(120),
        ..ContainerdRuntimeSettings::default()
    };
    let runtime = ContainerdRuntime::connect(settings, Arc::new(TokioRuntimeClock::new()))
        .await
        .unwrap();
    let request = ArtifactBuildRequest {
        source: ArtifactSource::Directory {
            root: context,
            definition: PathBuf::from("Dockerfile"),
        },
        arguments: BTreeMap::new(),
        secrets: BTreeMap::from([(
            "TOKEN".to_owned(),
            SecretValue::new("acceptance-secret-never-logged"),
        )]),
        tags: vec![tag.clone()],
    };

    let built = tokio::time::timeout(Duration::from_secs(150), runtime.build(&request))
        .await
        .expect("BuildKit build timed out")
        .unwrap();
    assert!(built.as_str().contains("@sha256:"));
    let tagged = runtime.resolve_digest(&tag).await.unwrap();
    assert_eq!(content_digest(&tagged), content_digest(&built));
    let removed = runtime
        .prune(&ArtifactPrunePolicy::Preserve(Vec::new()))
        .await
        .unwrap();
    assert!(
        removed
            .removed
            .iter()
            .any(|digest| content_digest(digest) == content_digest(&built))
    );
}

#[cfg(all(feature = "containerd", target_os = "linux"))]
fn content_digest(digest: &ArtifactDigest) -> &str {
    digest
        .as_str()
        .rsplit_once('@')
        .map_or_else(|| digest.as_str(), |(_, digest)| digest)
}
