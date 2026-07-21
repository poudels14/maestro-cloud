use async_trait::async_trait;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{ChecksumMode, ServerSideEncryption};
use logstore::{
    BackupObjectBody, BackupObjectReceipt, BackupObjectStore, BackupObjectStoreError,
    BackupObjectUpload,
};

mod digest;
mod multipart;

use digest::{ObjectDigest, digest_bytes, digest_file};
use multipart::upload_multipart;

/// Production S3 object-store adapter with whole-object verification and SSE-KMS.
pub struct S3BackupObjectStore {
    client: aws_sdk_s3::Client,
    bucket: String,
}

impl S3BackupObjectStore {
    /// Binds an AWS SDK client to one non-empty destination bucket.
    pub fn new(
        client: aws_sdk_s3::Client,
        bucket: impl Into<String>,
    ) -> Result<Self, S3BackupObjectStoreError> {
        let bucket = bucket.into();
        if bucket.trim() != bucket || bucket.is_empty() || bucket.chars().any(char::is_control) {
            return Err(rejected("S3 backup bucket is invalid"));
        }
        Ok(Self { client, bucket })
    }
}

#[async_trait]
impl BackupObjectStore for S3BackupObjectStore {
    async fn upload(
        &self,
        object: &BackupObjectUpload,
    ) -> Result<BackupObjectReceipt, BackupObjectStoreError> {
        self.upload_verified(object).await.map_err(Into::into)
    }
}

impl S3BackupObjectStore {
    async fn upload_verified(
        &self,
        object: &BackupObjectUpload,
    ) -> Result<BackupObjectReceipt, S3BackupObjectStoreError> {
        validate_upload(object)?;
        let digest = digest_object(&object.body).await?;
        if digest.size != object.size_bytes || digest.whole_hex != object.sha256 {
            return Err(rejected("backup object changed before S3 upload"));
        }
        let stored_checksum = if digest.parts.is_empty() {
            self.put_object(object, &digest).await?
        } else {
            upload_multipart(self, object, &digest).await?
        };
        let head = self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(&object.key)
            .checksum_mode(ChecksumMode::Enabled)
            .send()
            .await
            .map_err(|error| unavailable("read uploaded object metadata", error))?;
        let expected_size = i64::try_from(digest.size)
            .map_err(|error| unavailable("verify uploaded object length", error))?;
        let metadata_sha = head
            .metadata()
            .and_then(|metadata| metadata.get("sha256"))
            .map(String::as_str);
        if head.content_length() != Some(expected_size)
            || head.checksum_sha256() != Some(stored_checksum.as_str())
            || head.server_side_encryption() != Some(&ServerSideEncryption::AwsKms)
            || head.ssekms_key_id() != Some(object.kms_key_id.as_str())
            || metadata_sha != Some(digest.whole_hex.as_str())
        {
            return Err(unavailable(
                "verify uploaded object metadata",
                "S3 metadata did not match the upload contract",
            ));
        }
        Ok(BackupObjectReceipt {
            size_bytes: digest.size,
            sha256: digest.whole_hex,
            kms_key_id: object.kms_key_id.clone(),
        })
    }

    async fn put_object(
        &self,
        object: &BackupObjectUpload,
        digest: &ObjectDigest,
    ) -> Result<String, S3BackupObjectStoreError> {
        let body = match &object.body {
            BackupObjectBody::File(path) => ByteStream::from_path(path)
                .await
                .map_err(|error| unavailable("open backup object", error))?,
            BackupObjectBody::Bytes(bytes) => ByteStream::from(bytes.clone()),
        };
        let uploaded = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(&object.key)
            .body(body)
            .content_length(
                i64::try_from(digest.size)
                    .map_err(|error| unavailable("set backup object length", error))?,
            )
            .checksum_sha256(&digest.whole_base64)
            .metadata("sha256", &digest.whole_hex)
            .server_side_encryption(ServerSideEncryption::AwsKms)
            .ssekms_key_id(&object.kms_key_id)
            .send()
            .await
            .map_err(|error| unavailable("upload backup object", error))?;
        if uploaded.checksum_sha256() != Some(digest.whole_base64.as_str()) {
            return Err(unavailable(
                "verify backup upload response",
                "S3 checksum mismatch",
            ));
        }
        Ok(digest.whole_base64.clone())
    }
}

async fn digest_object(body: &BackupObjectBody) -> Result<ObjectDigest, S3BackupObjectStoreError> {
    match body {
        BackupObjectBody::File(path) => {
            let path = path.clone();
            tokio::task::spawn_blocking(move || digest_file(&path))
                .await
                .map_err(|error| unavailable("join object digest task", error))?
        }
        BackupObjectBody::Bytes(bytes) => digest_bytes(bytes),
    }
}

fn validate_upload(object: &BackupObjectUpload) -> Result<(), S3BackupObjectStoreError> {
    if object.key.is_empty()
        || object.key.starts_with('/')
        || object.key.split('/').any(|part| part.is_empty())
        || object.key.chars().any(char::is_control)
    {
        return Err(rejected("S3 backup object key is invalid"));
    }
    if object.kms_key_id.is_empty()
        || object.kms_key_id.trim() != object.kms_key_id
        || object.kms_key_id.chars().any(char::is_control)
    {
        return Err(rejected("S3 backup KMS key identity is invalid"));
    }
    if object.sha256.len() != 64
        || !object
            .sha256
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(rejected("S3 backup SHA-256 is invalid"));
    }
    Ok(())
}

/// Construction, validation, or S3 delivery failure from the production adapter.
#[derive(Debug, thiserror::Error)]
pub enum S3BackupObjectStoreError {
    /// The upload contract cannot be represented safely in S3.
    #[error("S3 backup rejected upload: {message}")]
    Rejected { message: String },
    /// Local I/O or remote S3 delivery could not complete.
    #[error("S3 backup is unavailable: {message}")]
    Unavailable { message: String },
}

impl From<S3BackupObjectStoreError> for BackupObjectStoreError {
    fn from(error: S3BackupObjectStoreError) -> Self {
        match error {
            S3BackupObjectStoreError::Rejected { message } => Self::Rejected { message },
            S3BackupObjectStoreError::Unavailable { message } => Self::Unavailable { message },
        }
    }
}

fn rejected(message: impl Into<String>) -> S3BackupObjectStoreError {
    S3BackupObjectStoreError::Rejected {
        message: message.into(),
    }
}

fn unavailable(action: &str, error: impl std::fmt::Display) -> S3BackupObjectStoreError {
    S3BackupObjectStoreError::Unavailable {
        message: format!("failed to {action}: {error}"),
    }
}

#[cfg(test)]
mod tests {
    use base64::Engine;
    use sha2::{Digest, Sha256};

    use super::digest::{
        DEFAULT_MULTIPART_PART_BYTES, MAX_MULTIPART_PART_BYTES, MAX_MULTIPART_PARTS,
        MULTIPART_THRESHOLD_BYTES, composite_sha256, digest_bytes_with_part_size,
        multipart_part_size,
    };
    use super::*;

    #[test]
    fn multipart_digest_uses_s3_composite_checksum_contract()
    -> Result<(), Box<dyn std::error::Error>> {
        let bytes = vec![7_u8; 10];
        let digest = digest_bytes_with_part_size(&bytes, 6)?;
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
}
