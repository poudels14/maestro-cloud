#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::time::{SystemTime, UNIX_EPOCH};

use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use daemon::S3BackupObjectStore;
use logstore::{BackupObjectBody, BackupObjectStore, BackupObjectUpload};
use sha2::{Digest, Sha256};

const MULTIPART_BYTES: usize = 100 * 1024 * 1024;

#[tokio::test]
#[ignore = "requires a MinIO endpoint configured with KES through MAESTRO_MINIO_* variables"]
async fn minio_preserves_sse_kms_checksums_for_single_and_multipart_uploads()
-> Result<(), Box<dyn std::error::Error>> {
    let endpoint = required("MAESTRO_MINIO_ENDPOINT")?;
    let access_key = required("MAESTRO_MINIO_ACCESS_KEY")?;
    let secret_key = required("MAESTRO_MINIO_SECRET_KEY")?;
    let kms_key_id = required("MAESTRO_MINIO_KMS_KEY_ID")?;
    let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let bucket = format!("maestro-backup-{}-{suffix}", std::process::id());
    let credentials = Credentials::new(access_key, secret_key, None, None, "minio-test");
    let config = aws_sdk_s3::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .credentials_provider(credentials)
        .region(Region::new("us-east-1"))
        .endpoint_url(endpoint)
        .force_path_style(true)
        .build();
    let client = aws_sdk_s3::Client::from_conf(config);
    client.create_bucket().bucket(&bucket).send().await?;

    let object_keys = ["single/part.parquet", "multipart/part.parquet"];
    let result = exercise_uploads(&client, &bucket, &kms_key_id, &object_keys).await;
    let cleanup = cleanup_bucket(&client, &bucket, &object_keys).await;
    result?;
    cleanup?;
    Ok(())
}

async fn exercise_uploads(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    kms_key_id: &str,
    object_keys: &[&str; 2],
) -> Result<(), Box<dyn std::error::Error>> {
    let store = S3BackupObjectStore::new(client.clone(), bucket)?;
    let single = b"maestro-minio-sse-kms-contract".to_vec();
    upload_and_assert(&store, object_keys[0], single, kms_key_id).await?;
    let multipart = vec![0x5a; MULTIPART_BYTES];
    upload_and_assert(&store, object_keys[1], multipart, kms_key_id).await?;
    Ok(())
}

async fn upload_and_assert(
    store: &S3BackupObjectStore,
    key: &str,
    bytes: Vec<u8>,
    kms_key_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let size_bytes = u64::try_from(bytes.len())?;
    let sha256 = format!("{:x}", Sha256::digest(&bytes));
    let receipt = store
        .upload(&BackupObjectUpload {
            key: key.to_owned(),
            body: BackupObjectBody::Bytes(bytes),
            size_bytes,
            sha256: sha256.clone(),
            kms_key_id: kms_key_id.to_owned(),
            commit_marker: false,
        })
        .await?;
    assert_eq!(receipt.size_bytes, size_bytes);
    assert_eq!(receipt.sha256, sha256);
    assert_eq!(receipt.kms_key_id, kms_key_id);
    Ok(())
}

async fn cleanup_bucket(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    object_keys: &[&str; 2],
) -> Result<(), Box<dyn std::error::Error>> {
    for key in object_keys {
        client
            .delete_object()
            .bucket(bucket)
            .key(*key)
            .send()
            .await?;
    }
    client.delete_bucket().bucket(bucket).send().await?;
    Ok(())
}

fn required(name: &str) -> Result<String, Box<dyn std::error::Error>> {
    std::env::var(name).map_err(|_| format!("{name} must be set").into())
}
