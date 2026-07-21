use aws_sdk_s3::primitives::{ByteStream, Length};
use aws_sdk_s3::types::{
    ChecksumAlgorithm, ChecksumType, CompletedMultipartUpload, CompletedPart, ServerSideEncryption,
};
use logstore::{BackupObjectBody, BackupObjectUpload};

use super::digest::{ObjectDigest, PartDigest, composite_sha256};
use super::{S3BackupObjectStore, S3BackupObjectStoreError, unavailable};

pub(super) async fn upload_multipart(
    store: &S3BackupObjectStore,
    object: &BackupObjectUpload,
    digest: &ObjectDigest,
) -> Result<String, S3BackupObjectStoreError> {
    let created = store
        .client
        .create_multipart_upload()
        .bucket(&store.bucket)
        .key(&object.key)
        .checksum_algorithm(ChecksumAlgorithm::Sha256)
        .checksum_type(ChecksumType::Composite)
        .metadata("sha256", &digest.whole_hex)
        .server_side_encryption(ServerSideEncryption::AwsKms)
        .ssekms_key_id(&object.kms_key_id)
        .send()
        .await
        .map_err(|error| unavailable("start multipart upload", error))?;
    let upload_id = created
        .upload_id()
        .ok_or_else(|| unavailable("start multipart upload", "S3 returned no upload ID"))?;
    let result = upload_and_complete(store, object, digest, upload_id).await;
    if let Err(upload_error) = &result
        && let Err(abort_error) = store
            .client
            .abort_multipart_upload()
            .bucket(&store.bucket)
            .key(&object.key)
            .upload_id(upload_id)
            .send()
            .await
    {
        return Err(unavailable(
            "finish multipart upload",
            format!("{upload_error}; abort also failed: {abort_error}"),
        ));
    }
    result
}

async fn upload_and_complete(
    store: &S3BackupObjectStore,
    object: &BackupObjectUpload,
    digest: &ObjectDigest,
    upload_id: &str,
) -> Result<String, S3BackupObjectStoreError> {
    let mut completed_parts = Vec::with_capacity(digest.parts.len());
    for part in &digest.parts {
        let body = part_body(&object.body, part).await?;
        let uploaded = store
            .client
            .upload_part()
            .bucket(&store.bucket)
            .key(&object.key)
            .upload_id(upload_id)
            .part_number(part.number)
            .checksum_sha256(&part.checksum_base64)
            .body(body)
            .send()
            .await
            .map_err(|error| unavailable("upload multipart part", error))?;
        if uploaded.checksum_sha256() != Some(part.checksum_base64.as_str()) {
            return Err(unavailable(
                "verify multipart part",
                format!("S3 checksum mismatch for part {}", part.number),
            ));
        }
        let e_tag = uploaded
            .e_tag()
            .ok_or_else(|| unavailable("verify multipart part", "S3 returned no ETag"))?;
        completed_parts.push(
            CompletedPart::builder()
                .part_number(part.number)
                .e_tag(e_tag)
                .checksum_sha256(&part.checksum_base64)
                .build(),
        );
    }
    let checksums = digest
        .parts
        .iter()
        .map(|part| part.checksum_base64.clone())
        .collect::<Vec<_>>();
    let composite = composite_sha256(&checksums)?;
    let completed = store
        .client
        .complete_multipart_upload()
        .bucket(&store.bucket)
        .key(&object.key)
        .upload_id(upload_id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .set_parts(Some(completed_parts))
                .build(),
        )
        .checksum_type(ChecksumType::Composite)
        .checksum_sha256(&composite)
        .mpu_object_size(
            i64::try_from(digest.size)
                .map_err(|error| unavailable("complete multipart upload", error))?,
        )
        .send()
        .await
        .map_err(|error| unavailable("complete multipart upload", error))?;
    if completed.checksum_sha256() != Some(composite.as_str()) {
        return Err(unavailable(
            "verify completed multipart upload",
            "S3 checksum mismatch",
        ));
    }
    Ok(composite)
}

async fn part_body(
    body: &BackupObjectBody,
    part: &PartDigest,
) -> Result<ByteStream, S3BackupObjectStoreError> {
    match body {
        BackupObjectBody::File(path) => ByteStream::read_from()
            .path(path)
            .offset(part.offset)
            .length(Length::Exact(part.length))
            .build()
            .await
            .map_err(|error| unavailable("open multipart object range", error)),
        BackupObjectBody::Bytes(bytes) => {
            let start = usize::try_from(part.offset)
                .map_err(|error| unavailable("slice multipart object", error))?;
            let end = usize::try_from(part.offset.saturating_add(part.length))
                .map_err(|error| unavailable("slice multipart object", error))?;
            let range = bytes
                .get(start..end)
                .ok_or_else(|| unavailable("slice multipart object", "range is outside body"))?;
            Ok(ByteStream::from(range.to_vec()))
        }
    }
}
