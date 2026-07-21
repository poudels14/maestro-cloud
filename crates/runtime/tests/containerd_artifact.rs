#![allow(clippy::expect_used, clippy::unwrap_used)]

#[cfg(all(feature = "containerd", target_os = "linux"))]
use std::path::PathBuf;
#[cfg(all(feature = "containerd", target_os = "linux"))]
use std::sync::Arc;
#[cfg(all(feature = "containerd", target_os = "linux"))]
use std::time::Duration;

#[cfg(all(feature = "containerd", target_os = "linux"))]
use runtime::{
    ArtifactDigest, ArtifactPrunePolicy, ArtifactReference, ArtifactStore, ContainerdRuntime,
    ContainerdRuntimeSettings, TokioRuntimeClock,
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
