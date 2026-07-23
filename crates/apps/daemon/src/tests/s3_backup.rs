use base64::Engine;
use logstore::{BackupObjectBody, BackupObjectUpload};
use sha2::{Digest, Sha256};

use crate::s3_backup::digest::{
    DEFAULT_MULTIPART_PART_BYTES, MAX_MULTIPART_PART_BYTES, MAX_MULTIPART_PARTS,
    MULTIPART_THRESHOLD_BYTES, composite_sha256, digest_bytes_with_part_size, multipart_part_size,
};
use crate::s3_backup::{S3BackupObjectStoreError, kms_key_matches, validate_upload};

#[test]
fn multipart_digest_uses_s3_composite_checksum_contract() -> Result<(), Box<dyn std::error::Error>>
{
    let bytes = vec![7_u8; 10];
    let digest = digest_bytes_with_part_size(&bytes, Some(6))?;
    assert_eq!(digest.parts.len(), 2);
    assert_eq!(digest.parts.first().ok_or("first part missing")?.length, 6);
    assert_eq!(digest.parts.get(1).ok_or("second part missing")?.offset, 6);
    assert_eq!(digest.whole_hex, format!("{:x}", Sha256::digest(&bytes)));
    let checksums = digest
        .parts
        .iter()
        .map(|part| part.checksum_base64.clone())
        .collect::<Vec<_>>();
    let expected = composite_sha256(&checksums)?;
    let decoded = checksums
        .iter()
        .map(|checksum| base64::engine::general_purpose::STANDARD.decode(checksum))
        .collect::<Result<Vec<_>, _>>()?;
    let combined = decoded.concat();
    assert_eq!(
        expected,
        format!(
            "{}-2",
            base64::engine::general_purpose::STANDARD.encode(Sha256::digest(combined))
        )
    );
    Ok(())
}

#[test]
fn multipart_plan_stays_within_s3_limits() -> Result<(), Box<dyn std::error::Error>> {
    assert_eq!(
        multipart_part_size(MULTIPART_THRESHOLD_BYTES)?,
        DEFAULT_MULTIPART_PART_BYTES
    );
    let five_tebibytes = 5 * 1024 * 1024 * 1024 * 1024;
    let part_size = multipart_part_size(five_tebibytes)?;
    assert!(part_size <= MAX_MULTIPART_PART_BYTES);
    assert!(five_tebibytes.div_ceil(part_size) <= MAX_MULTIPART_PARTS);
    Ok(())
}

#[test]
fn upload_validation_rejects_ambiguous_keys_and_checksums() {
    let object = BackupObjectUpload {
        key: "/absolute".to_owned(),
        body: BackupObjectBody::Bytes(Vec::new()),
        size_bytes: 0,
        sha256: "not-a-digest".to_owned(),
        kms_key_id: "kms".to_owned(),
        commit_marker: false,
    };
    assert!(matches!(
        validate_upload(&object),
        Err(S3BackupObjectStoreError::Rejected { .. })
    ));
}

#[test]
fn kms_receipt_accepts_only_exact_or_minio_canonical_identity() {
    assert!(kms_key_matches(
        "arn:aws:kms:west:key/123",
        "arn:aws:kms:west:key/123"
    ));
    assert!(kms_key_matches(
        "maestro-backup",
        "arn:aws:kms:maestro-backup"
    ));
    assert!(!kms_key_matches(
        "key/123",
        "arn:aws:kms:west:account:key/123"
    ));
    assert!(!kms_key_matches("maestro-backup", "arn:aws:kms:other-key"));
}
