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

#[test]
fn artifact_references_use_the_oci_distribution_grammar() {
    assert!(ArtifactReference::new("alpine:3.22").is_ok());
    assert!(ArtifactReference::new("registry.example:5000/team/api:v1").is_ok());
    assert!(ArtifactReference::new("Team/API:v1").is_err());
    assert!(ArtifactReference::new("team/api:").is_err());
    assert!(ArtifactReference::new("team/api@sha256:abc").is_err());
}
