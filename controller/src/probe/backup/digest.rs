use std::io::Read;
use std::path::Path;

use anyhow::{Result, bail};
use base64::Engine;
use sha2::{Digest, Sha256};

pub(super) const MULTIPART_THRESHOLD_BYTES: u64 = 100 * 1024 * 1024;
pub(super) const DEFAULT_MULTIPART_PART_BYTES: u64 = 64 * 1024 * 1024;
pub(super) const MAX_MULTIPART_PARTS: u64 = 10_000;
pub(super) const MAX_MULTIPART_PART_BYTES: u64 = 5 * 1024 * 1024 * 1024;

pub(super) struct FileDigest {
    pub(super) size: u64,
    pub(super) whole_base64: String,
    pub(super) whole_hex: String,
    pub(super) parts: Vec<PartDigest>,
}

pub(super) struct PartDigest {
    pub(super) number: i32,
    pub(super) offset: u64,
    pub(super) length: u64,
    pub(super) checksum_base64: String,
}

pub(super) fn multipart_part_size(size: u64) -> Result<u64> {
    let minimum = size.div_ceil(MAX_MULTIPART_PARTS);
    let mebibyte = 1024 * 1024;
    let part_size = DEFAULT_MULTIPART_PART_BYTES.max(minimum.div_ceil(mebibyte) * mebibyte);
    if part_size > MAX_MULTIPART_PART_BYTES || size.div_ceil(part_size) > MAX_MULTIPART_PARTS {
        bail!("backup object is too large for S3 multipart upload: {size} bytes");
    }
    Ok(part_size)
}

pub(super) fn digest_file(path: &Path) -> Result<FileDigest> {
    digest_file_with_part_size(path, None)
}

pub(super) fn digest_file_with_part_size(
    path: &Path,
    part_size_override: Option<u64>,
) -> Result<FileDigest> {
    let mut file = std::fs::File::open(path)?;
    let size = file.metadata()?.len();
    let part_size = match part_size_override {
        Some(part_size) if part_size > 0 => Some(part_size),
        Some(_) => bail!("multipart part size must be greater than zero"),
        None if size >= MULTIPART_THRESHOLD_BYTES => Some(multipart_part_size(size)?),
        None => None,
    };
    let expected_parts = part_size.map_or(0, |part_size| size.div_ceil(part_size));
    let mut parts = Vec::with_capacity(usize::try_from(expected_parts)?);
    let mut whole_digest = Sha256::new();
    let mut part_digest = Sha256::new();
    let mut buffer = [0_u8; 1024 * 1024];
    let mut offset = 0u64;
    let mut part_offset = 0u64;
    while offset < size {
        let part_remaining = part_size.map_or(size - offset, |part_size| {
            part_size - (offset - part_offset)
        });
        let requested =
            usize::try_from((size - offset).min(part_remaining).min(buffer.len() as u64))?;
        file.read_exact(&mut buffer[..requested])?;
        whole_digest.update(&buffer[..requested]);
        if let Some(part_size) = part_size {
            part_digest.update(&buffer[..requested]);
            let length = offset + requested as u64 - part_offset;
            if length == part_size || offset + requested as u64 == size {
                let checksum = std::mem::replace(&mut part_digest, Sha256::new()).finalize();
                parts.push(PartDigest {
                    number: i32::try_from(parts.len() + 1)?,
                    offset: part_offset,
                    length,
                    checksum_base64: base64::engine::general_purpose::STANDARD.encode(checksum),
                });
                part_offset = offset + requested as u64;
            }
        }
        offset += requested as u64;
    }
    let mut extra = [0u8; 1];
    if file.read(&mut extra)? != 0 || file.metadata()?.len() != size {
        bail!("backup object changed while hashing: {}", path.display());
    }
    if parts.len() != usize::try_from(expected_parts)? {
        bail!(
            "backup object part count changed while hashing: expected {expected_parts}, got {}",
            parts.len()
        );
    }
    let whole_digest = whole_digest.finalize();
    Ok(FileDigest {
        size,
        whole_base64: base64::engine::general_purpose::STANDARD.encode(whole_digest),
        whole_hex: format!("{whole_digest:x}"),
        parts,
    })
}

pub(super) fn composite_sha256(part_checksums: &[String]) -> Result<String> {
    if part_checksums.is_empty() {
        bail!("multipart upload requires at least one part");
    }
    let mut digest = Sha256::new();
    for checksum in part_checksums {
        digest.update(base64::engine::general_purpose::STANDARD.decode(checksum)?);
    }
    Ok(format!(
        "{}-{}",
        base64::engine::general_purpose::STANDARD.encode(digest.finalize()),
        part_checksums.len()
    ))
}
