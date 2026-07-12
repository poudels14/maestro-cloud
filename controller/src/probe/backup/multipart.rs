use std::path::Path;

use anyhow::{Context, Result, anyhow, bail};
use aws_sdk_s3::primitives::{ByteStream, Length};
use aws_sdk_s3::types::{
    ChecksumAlgorithm, ChecksumType, CompletedMultipartUpload, CompletedPart, ServerSideEncryption,
};

use super::BackupConfig;
use super::digest::{FileDigest, composite_sha256};

struct MultipartUploadSession<'a> {
    client: &'a aws_sdk_s3::Client,
    config: &'a BackupConfig,
    key: &'a str,
    upload_id: String,
}
pub(super) async fn upload_file_multipart(
    client: &aws_sdk_s3::Client,
    config: &BackupConfig,
    path: &Path,
    key: &str,
    digest: &FileDigest,
) -> Result<String> {
    let session = MultipartUploadSession::start(client, config, key, &digest.whole_hex).await?;
    session.upload(path, digest).await
}

impl<'a> MultipartUploadSession<'a> {
    async fn start(
        client: &'a aws_sdk_s3::Client,
        config: &'a BackupConfig,
        key: &'a str,
        checksum_hex: &str,
    ) -> Result<Self> {
        let created = client
            .create_multipart_upload()
            .bucket(&config.bucket)
            .key(key)
            .checksum_algorithm(ChecksumAlgorithm::Sha256)
            .checksum_type(ChecksumType::Composite)
            .metadata("sha256", checksum_hex)
            .server_side_encryption(ServerSideEncryption::AwsKms)
            .ssekms_key_id(&config.kms_key_id)
            .send()
            .await
            .with_context(|| format!("start multipart upload s3://{}/{key}", config.bucket))?;
        let upload_id = created
            .upload_id()
            .ok_or_else(|| anyhow!("S3 multipart upload did not return an upload ID for {key}"))?
            .to_string();
        Ok(Self {
            client,
            config,
            key,
            upload_id,
        })
    }

    async fn upload(self, path: &Path, digest: &FileDigest) -> Result<String> {
        let result = self.upload_and_complete(path, digest).await;
        match result {
            Ok(checksum) => Ok(checksum),
            Err(err) => {
                if let Err(abort_err) = self.abort().await {
                    return Err(err.context(format!(
                        "also failed to abort multipart upload {}: {abort_err:#}",
                        self.upload_id
                    )));
                }
                Err(err)
            }
        }
    }

    async fn upload_and_complete(&self, path: &Path, digest: &FileDigest) -> Result<String> {
        let part_checksums = digest
            .parts
            .iter()
            .map(|part| part.checksum_base64.clone())
            .collect::<Vec<_>>();
        let composite_checksum = composite_sha256(&part_checksums)?;
        let mut completed_parts = Vec::with_capacity(digest.parts.len());
        for part in &digest.parts {
            let body = ByteStream::read_from()
                .path(path)
                .offset(part.offset)
                .length(Length::Exact(part.length))
                .build()
                .await
                .with_context(|| {
                    format!(
                        "read multipart part {} from {}",
                        part.number,
                        path.display()
                    )
                })?;
            let uploaded = self
                .client
                .upload_part()
                .bucket(&self.config.bucket)
                .key(self.key)
                .upload_id(&self.upload_id)
                .part_number(part.number)
                .checksum_sha256(&part.checksum_base64)
                .body(body)
                .send()
                .await
                .with_context(|| {
                    format!(
                        "upload multipart part {} to s3://{}/{}",
                        part.number, self.config.bucket, self.key
                    )
                })?;
            if uploaded.checksum_sha256() != Some(part.checksum_base64.as_str()) {
                bail!(
                    "S3 multipart checksum mismatch for {} part {}",
                    self.key,
                    part.number
                );
            }
            let e_tag = uploaded.e_tag().ok_or_else(|| {
                anyhow!(
                    "S3 multipart part {} has no ETag for {}",
                    part.number,
                    self.key
                )
            })?;
            completed_parts.push(
                CompletedPart::builder()
                    .part_number(part.number)
                    .e_tag(e_tag)
                    .checksum_sha256(&part.checksum_base64)
                    .build(),
            );
        }

        let completed = self
            .client
            .complete_multipart_upload()
            .bucket(&self.config.bucket)
            .key(self.key)
            .upload_id(&self.upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .set_parts(Some(completed_parts))
                    .build(),
            )
            .checksum_type(ChecksumType::Composite)
            .checksum_sha256(&composite_checksum)
            .mpu_object_size(i64::try_from(digest.size)?)
            .send()
            .await
            .with_context(|| {
                format!(
                    "complete multipart upload s3://{}/{}",
                    self.config.bucket, self.key
                )
            })?;
        if completed.checksum_sha256() != Some(composite_checksum.as_str()) {
            bail!("S3 completed multipart checksum mismatch for {}", self.key);
        }
        Ok(composite_checksum)
    }

    async fn abort(&self) -> Result<()> {
        self.client
            .abort_multipart_upload()
            .bucket(&self.config.bucket)
            .key(self.key)
            .upload_id(&self.upload_id)
            .send()
            .await
            .with_context(|| format!("abort multipart upload {}", self.upload_id))?;
        Ok(())
    }
}
