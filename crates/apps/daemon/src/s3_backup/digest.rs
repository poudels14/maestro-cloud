use std::io::Read;
use std::path::Path;

use base64::Engine;
use sha2::{Digest, Sha256};

use super::S3BackupObjectStoreError;

pub(crate) const MULTIPART_THRESHOLD_BYTES: u64 = 100 * 1024 * 1024;
pub(crate) const DEFAULT_MULTIPART_PART_BYTES: u64 = 64 * 1024 * 1024;
pub(crate) const MAX_MULTIPART_PARTS: u64 = 10_000;
pub(crate) const MAX_MULTIPART_PART_BYTES: u64 = 5 * 1024 * 1024 * 1024;

pub(crate) struct ObjectDigest {
    pub(crate) size: u64,
    pub(crate) whole_base64: String,
    pub(crate) whole_hex: String,
    pub(crate) parts: Vec<PartDigest>,
}

pub(crate) struct PartDigest {
    pub(crate) number: i32,
    pub(crate) offset: u64,
    pub(crate) length: u64,
    pub(crate) checksum_base64: String,
}

pub(crate) fn digest_file(path: &Path) -> Result<ObjectDigest, S3BackupObjectStoreError> {
    let mut file = std::fs::File::open(path).map_err(|error| unavailable("open object", error))?;
    let size = file
        .metadata()
        .map_err(|error| unavailable("inspect object", error))?
        .len();
    digest_reader(&mut file, size, None)
}

pub(crate) fn digest_bytes(bytes: &[u8]) -> Result<ObjectDigest, S3BackupObjectStoreError> {
    let size = u64::try_from(bytes.len()).map_err(|error| rejected(error.to_string()))?;
    digest_reader(&mut std::io::Cursor::new(bytes), size, None)
}

#[cfg(test)]
pub(crate) fn digest_bytes_with_part_size(
    bytes: &[u8],
    part_size: u64,
) -> Result<ObjectDigest, S3BackupObjectStoreError> {
    if part_size == 0 {
        return Err(rejected("multipart part size must be greater than zero"));
    }
    let size = u64::try_from(bytes.len()).map_err(|error| rejected(error.to_string()))?;
    digest_reader(&mut std::io::Cursor::new(bytes), size, Some(part_size))
}

fn digest_reader(
    reader: &mut impl Read,
    size: u64,
    part_size_override: Option<u64>,
) -> Result<ObjectDigest, S3BackupObjectStoreError> {
    let part_size = match part_size_override {
        Some(part_size) => Some(part_size),
        None if size >= MULTIPART_THRESHOLD_BYTES => Some(multipart_part_size(size)?),
        None => None,
    };
    let expected_parts = part_size.map_or(0, |part_size| size.div_ceil(part_size));
    let capacity = usize::try_from(expected_parts).map_err(|error| rejected(error.to_string()))?;
    let mut parts = Vec::with_capacity(capacity);
    let mut whole_digest = Sha256::new();
    let mut part_digest = Sha256::new();
    let mut buffer = [0_u8; 1024 * 1024];
    let mut offset = 0_u64;
    let mut part_offset = 0_u64;
    while offset < size {
        let part_remaining = part_size.map_or(size - offset, |size| size - (offset - part_offset));
        let requested = (size - offset).min(part_remaining).min(buffer.len() as u64);
        let requested = usize::try_from(requested).map_err(|error| rejected(error.to_string()))?;
        let chunk = buffer
            .get_mut(..requested)
            .ok_or_else(|| rejected("object hash read exceeded its buffer"))?;
        reader
            .read_exact(chunk)
            .map_err(|error| unavailable("hash object", error))?;
        whole_digest.update(&*chunk);
        if let Some(part_size) = part_size {
            part_digest.update(&*chunk);
            let length = offset + requested as u64 - part_offset;
            if length == part_size || offset + requested as u64 == size {
                let checksum = std::mem::replace(&mut part_digest, Sha256::new()).finalize();
                parts.push(PartDigest {
                    number: i32::try_from(parts.len() + 1)
                        .map_err(|error| rejected(error.to_string()))?,
                    offset: part_offset,
                    length,
                    checksum_base64: base64::engine::general_purpose::STANDARD.encode(checksum),
                });
                part_offset = offset + requested as u64;
            }
        }
        offset += requested as u64;
    }
    let mut extra = [0_u8; 1];
    if reader
        .read(&mut extra)
        .map_err(|error| unavailable("verify object length", error))?
        != 0
    {
        return Err(rejected("object changed while hashing"));
    }
    if parts.len() != capacity {
        return Err(rejected("object part count changed while hashing"));
    }
    let whole_digest = whole_digest.finalize();
    Ok(ObjectDigest {
        size,
        whole_base64: base64::engine::general_purpose::STANDARD.encode(whole_digest),
        whole_hex: format!("{whole_digest:x}"),
        parts,
    })
}

pub(crate) fn multipart_part_size(size: u64) -> Result<u64, S3BackupObjectStoreError> {
    let minimum = size.div_ceil(MAX_MULTIPART_PARTS);
    let mebibyte = 1024 * 1024;
    let part_size = DEFAULT_MULTIPART_PART_BYTES.max(minimum.div_ceil(mebibyte) * mebibyte);
    if part_size > MAX_MULTIPART_PART_BYTES || size.div_ceil(part_size) > MAX_MULTIPART_PARTS {
        return Err(rejected(format!(
            "object is too large for S3 multipart upload: {size} bytes"
        )));
    }
    Ok(part_size)
}

pub(crate) fn composite_sha256(
    part_checksums: &[String],
) -> Result<String, S3BackupObjectStoreError> {
    if part_checksums.is_empty() {
        return Err(rejected("multipart upload requires at least one part"));
    }
    let mut digest = Sha256::new();
    for checksum in part_checksums {
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(checksum)
            .map_err(|error| rejected(error.to_string()))?;
        digest.update(decoded);
    }
    Ok(format!(
        "{}-{}",
        base64::engine::general_purpose::STANDARD.encode(digest.finalize()),
        part_checksums.len()
    ))
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
