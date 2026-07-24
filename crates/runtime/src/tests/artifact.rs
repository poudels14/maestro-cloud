use crate::{ArtifactDigest, ArtifactReference};

#[test]
fn immutable_reference_rebinds_only_the_repository() {
    let digest = ArtifactDigest::new("maestro.local/builds/output@sha256:abc123").expect("digest");
    let destination =
        ArtifactReference::new("registry.example:5000/team/api:deployment-1").expect("reference");

    assert_eq!(
        digest
            .for_reference(&destination)
            .expect("immutable reference")
            .as_str(),
        "registry.example:5000/team/api@sha256:abc123"
    );
}
