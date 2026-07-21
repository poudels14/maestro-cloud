use std::collections::HashMap;
use std::sync::Arc;

use containerd::services::v1::Image;
use containerd::tonic::transport::Endpoint;
use containerd::tonic::{Code, Status};
use containerd::types::Descriptor;

use crate::containerd_artifact_support::{
    MANAGED_ARTIFACT_LABEL, MANAGED_ARTIFACT_VALUE, artifact_request, image_digest,
    operation_error, prune_candidates, reference_prefix, removed_digests, select_image,
};
use crate::{
    ArtifactDigest, ArtifactStoreError, ContainerdRuntime, ContainerdRuntimeSettings,
    RuntimeCapability, TokioRuntimeClock, WorkloadRuntime,
};

#[test]
fn containerd_digest_references_preserve_repository_and_registry_ports() {
    assert_eq!(
        reference_prefix("registry.test:5000/team/api:preview").unwrap(),
        "registry.test:5000/team/api"
    );
    assert_eq!(
        reference_prefix("registry.test:5000/team/api@sha256:abc").unwrap(),
        "registry.test:5000/team/api"
    );
    assert_eq!(reference_prefix("alpine").unwrap(), "alpine");

    let image = image("registry.test/team/api:preview", "sha256:abc", true);
    assert_eq!(
        image_digest(&image, &image.name).unwrap().as_str(),
        "registry.test/team/api@sha256:abc"
    );
}

#[test]
fn containerd_image_selection_uses_content_digest_and_prefers_managed_names() {
    let images = vec![
        image("z.example/api:latest", "sha256:abc", false),
        image("b.example/api:latest", "sha256:abc", true),
        image("a.example/other:latest", "sha256:def", true),
        image("a.example/api:latest", "sha256:abc", true),
    ];
    let digest = ArtifactDigest::new("registry.test/api@sha256:abc").unwrap();

    assert_eq!(
        select_image(&images, &digest).unwrap().name,
        "a.example/api:latest"
    );
    assert!(matches!(
        select_image(&images, &ArtifactDigest::new("sha256:missing").unwrap()),
        Err(ArtifactStoreError::NotFound { .. })
    ));
}

#[test]
fn containerd_prune_only_selects_unpreserved_managed_references() {
    let images = vec![
        image("managed-a", "sha256:keep", true),
        image("managed-b", "sha256:remove", true),
        image("managed-c", "sha256:remove", true),
        image("external", "sha256:external", false),
    ];
    let preserved = vec![ArtifactDigest::new("repo/api@sha256:keep").unwrap()];
    let candidates = prune_candidates(&images, &preserved);

    assert_eq!(
        candidates
            .iter()
            .map(|image| image.name.as_str())
            .collect::<Vec<_>>(),
        vec!["managed-b", "managed-c"]
    );
    assert_eq!(
        removed_digests(&candidates, &[]).unwrap(),
        vec![ArtifactDigest::new("sha256:remove").unwrap()]
    );
    assert!(
        removed_digests(
            &candidates,
            &[image("external-alias", "sha256:remove", false)]
        )
        .unwrap()
        .is_empty()
    );
}

#[test]
fn containerd_statuses_remain_matchable_at_the_artifact_boundary() {
    assert!(matches!(
        operation_error("pull", Some("missing"), Status::new(Code::NotFound, "gone")),
        ArtifactStoreError::NotFound { reference } if reference == "missing"
    ));
    assert!(matches!(
        operation_error(
            "push",
            Some("private"),
            Status::new(Code::Unauthenticated, "credentials required")
        ),
        ArtifactStoreError::Rejected { .. }
    ));
    assert!(matches!(
        operation_error("pull", None, Status::new(Code::Unavailable, "offline")),
        ArtifactStoreError::Unavailable { .. }
    ));
}

#[test]
fn containerd_transfer_requests_carry_namespace_and_lease_ownership() {
    let request = artifact_request((), "maestro-test", Some("lease-1")).unwrap();
    assert_eq!(
        request
            .metadata()
            .get("containerd-namespace")
            .unwrap()
            .to_str()
            .unwrap(),
        "maestro-test"
    );
    assert_eq!(
        request
            .metadata()
            .get("containerd-lease")
            .unwrap()
            .to_str()
            .unwrap(),
        "lease-1"
    );
}

#[tokio::test]
async fn containerd_advertises_native_transport_and_buildkit() {
    let channel = Endpoint::from_static("http://[::]:50051").connect_lazy();
    let runtime = ContainerdRuntime::new(
        channel,
        ContainerdRuntimeSettings::default(),
        Arc::new(TokioRuntimeClock::new()),
    )
    .unwrap();
    let capabilities = runtime.capabilities();

    assert!(capabilities.supports(RuntimeCapability::PushArtifact));
    assert!(capabilities.supports(RuntimeCapability::TransferArtifact));
    assert!(capabilities.supports(RuntimeCapability::BuildArtifact));
}

fn image(name: &str, digest: &str, managed: bool) -> Image {
    let labels = managed.then(|| {
        HashMap::from([(
            MANAGED_ARTIFACT_LABEL.to_owned(),
            MANAGED_ARTIFACT_VALUE.to_owned(),
        )])
    });
    Image {
        name: name.to_owned(),
        labels: labels.unwrap_or_default(),
        target: Some(Descriptor {
            media_type: "application/vnd.oci.image.manifest.v1+json".to_owned(),
            digest: digest.to_owned(),
            size: 123,
            annotations: HashMap::new(),
        }),
        created_at: None,
        updated_at: None,
    }
}
