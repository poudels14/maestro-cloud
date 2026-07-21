#![allow(clippy::expect_used, clippy::unwrap_used)]

#[cfg(all(feature = "docker", target_os = "linux"))]
use std::collections::BTreeMap;

#[cfg(all(feature = "docker", target_os = "linux"))]
use docker::query_parameters::RemoveImageOptionsBuilder;
#[cfg(all(feature = "docker", target_os = "linux"))]
use runtime::{
    ArtifactBuildRequest, ArtifactReference, ArtifactSource, ArtifactStore, DockerRuntime,
};

#[cfg(all(feature = "docker", target_os = "linux"))]
#[tokio::test]
#[ignore = "requires an isolated Docker daemon for artifact build and transfer acceptance"]
async fn docker_artifact_build_resolve_export_and_import_round_trip() {
    let temporary = tempfile::tempdir().unwrap();
    std::fs::write(
        temporary.path().join("Dockerfile"),
        "FROM scratch\nCOPY artifact.txt /artifact.txt\n",
    )
    .unwrap();
    std::fs::write(
        temporary.path().join("artifact.txt"),
        format!("maestro-artifact-test-{}", std::process::id()),
    )
    .unwrap();
    let tag = ArtifactReference::new(format!(
        "maestro-artifact-conformance:{}",
        std::process::id()
    ))
    .unwrap();
    let request = ArtifactBuildRequest {
        source: ArtifactSource::Directory {
            root: temporary.path().to_path_buf(),
            definition: "Dockerfile".into(),
        },
        arguments: BTreeMap::new(),
        secrets: BTreeMap::new(),
        tags: vec![tag.clone()],
    };
    let client = docker::Docker::connect_with_defaults().unwrap();
    let runtime = DockerRuntime::new(client.clone());

    let digest = runtime.build(&request).await.unwrap();
    assert_eq!(runtime.resolve_digest(&tag).await.unwrap(), digest);
    let archive = runtime.export(&digest).await.unwrap();
    assert_eq!(runtime.import(archive).await.unwrap(), digest);

    let cleanup = RemoveImageOptionsBuilder::default().noprune(true).build();
    client
        .remove_image(tag.as_str(), Some(cleanup), None)
        .await
        .unwrap();
}
