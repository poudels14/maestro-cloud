use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use serde::{Deserialize, Serialize};

use crate::snapshot::{
    MAXIMUM_ENTRY_COUNT, MAXIMUM_KEY_BYTES, MAXIMUM_SNAPSHOT_BYTES, MAXIMUM_VALUE_BYTES,
};
use crate::{LegacyEntry, LegacySnapshot, SnapshotError};

const ARTIFACT_SCHEMA_VERSION: u32 = 1;
const MAXIMUM_ENCODED_VALUE_BYTES: usize = MAXIMUM_VALUE_BYTES.div_ceil(3) * 4;
const MAXIMUM_ARTIFACT_BYTES: usize = MAXIMUM_SNAPSHOT_BYTES * 2 + 16 * 1_024 * 1_024;

impl LegacySnapshot {
    /// Returns the maximum accepted serialized logical snapshot size.
    pub const fn maximum_artifact_bytes() -> usize {
        MAXIMUM_ARTIFACT_BYTES
    }

    /// Encodes a portable, digest-bound logical snapshot for review and rehearsal.
    pub fn encode_artifact(&self) -> Result<Vec<u8>, SnapshotArtifactError> {
        let artifact = SnapshotArtifact {
            schema_version: ARTIFACT_SCHEMA_VERSION,
            source_sha256: hex::encode(self.digest()),
            entries: self
                .entries()
                .iter()
                .map(|entry| ArtifactEntry {
                    key: entry.key().to_owned(),
                    value_base64: BASE64.encode(entry.value()),
                })
                .collect(),
        };
        let document = serde_json::to_vec_pretty(&artifact).map_err(|error| {
            SnapshotArtifactError::Encode {
                message: error.to_string(),
            }
        })?;
        if document.len() > MAXIMUM_ARTIFACT_BYTES {
            return Err(SnapshotArtifactError::ArtifactTooLarge {
                maximum: MAXIMUM_ARTIFACT_BYTES,
            });
        }
        Ok(document)
    }

    /// Validates the declared digest and decodes one portable logical snapshot artifact.
    pub fn decode_artifact(document: &[u8]) -> Result<Self, SnapshotArtifactError> {
        if document.len() > MAXIMUM_ARTIFACT_BYTES {
            return Err(SnapshotArtifactError::ArtifactTooLarge {
                maximum: MAXIMUM_ARTIFACT_BYTES,
            });
        }
        let artifact: SnapshotArtifact =
            serde_json::from_slice(document).map_err(|error| SnapshotArtifactError::Malformed {
                message: error.to_string(),
            })?;
        if artifact.schema_version != ARTIFACT_SCHEMA_VERSION {
            return Err(SnapshotArtifactError::UnsupportedSchema {
                version: artifact.schema_version,
            });
        }
        if artifact.entries.len() > MAXIMUM_ENTRY_COUNT {
            return Err(SnapshotArtifactError::Snapshot(
                SnapshotError::TooManyEntries {
                    count: artifact.entries.len(),
                    maximum: MAXIMUM_ENTRY_COUNT,
                },
            ));
        }
        let declared_digest = decode_digest(&artifact.source_sha256)?;
        let mut entries = Vec::with_capacity(artifact.entries.len());
        for entry in artifact.entries {
            if entry.key.len() > MAXIMUM_KEY_BYTES
                || entry.value_base64.len() > MAXIMUM_ENCODED_VALUE_BYTES
            {
                return Err(SnapshotArtifactError::OversizedEntry { key: entry.key });
            }
            let value = BASE64
                .decode(entry.value_base64.as_bytes())
                .map_err(|error| SnapshotArtifactError::InvalidValue {
                    key: entry.key.clone(),
                    message: error.to_string(),
                })?;
            if BASE64.encode(&value) != entry.value_base64 {
                return Err(SnapshotArtifactError::InvalidValue {
                    key: entry.key,
                    message: "value is not canonical base64".to_owned(),
                });
            }
            entries.push(LegacyEntry::new(entry.key, value));
        }
        let snapshot = LegacySnapshot::new(entries)?;
        if snapshot.digest() != declared_digest {
            return Err(SnapshotArtifactError::DigestMismatch {
                declared: artifact.source_sha256,
                actual: hex::encode(snapshot.digest()),
            });
        }
        Ok(snapshot)
    }
}

fn decode_digest(value: &str) -> Result<[u8; 32], SnapshotArtifactError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(SnapshotArtifactError::InvalidDigest);
    }
    let mut digest = [0; 32];
    hex::decode_to_slice(value, &mut digest).map_err(|_| SnapshotArtifactError::InvalidDigest)?;
    Ok(digest)
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SnapshotArtifact {
    schema_version: u32,
    source_sha256: String,
    entries: Vec<ArtifactEntry>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ArtifactEntry {
    key: String,
    value_base64: String,
}

/// A portable logical snapshot artifact failed validation.
#[derive(Debug, thiserror::Error)]
pub enum SnapshotArtifactError {
    /// The snapshot itself violates its bounded input contract.
    #[error(transparent)]
    Snapshot(#[from] SnapshotError),
    /// The serialized document exceeds the bounded artifact contract.
    #[error("legacy snapshot artifact exceeds the {maximum}-byte input bound")]
    ArtifactTooLarge { maximum: usize },
    /// The artifact could not be encoded as JSON.
    #[error("could not encode legacy snapshot artifact: {message}")]
    Encode { message: String },
    /// The artifact is not strict JSON in the reviewed schema.
    #[error("legacy snapshot artifact is malformed: {message}")]
    Malformed { message: String },
    /// The artifact uses a schema this binary cannot safely interpret.
    #[error("legacy snapshot artifact schema {version} is not supported")]
    UnsupportedSchema { version: u32 },
    /// The declared digest is not canonical SHA-256 text.
    #[error("legacy snapshot artifact digest is not canonical SHA-256")]
    InvalidDigest,
    /// An entry exceeds the encoded defensive bounds.
    #[error("legacy snapshot artifact entry `{key}` exceeds its size bound")]
    OversizedEntry { key: String },
    /// An entry value is not canonical base64 data.
    #[error("legacy snapshot artifact value at `{key}` is invalid: {message}")]
    InvalidValue { key: String, message: String },
    /// The decoded snapshot does not match the digest captured with it.
    #[error("legacy snapshot artifact digest {declared} does not match decoded digest {actual}")]
    DigestMismatch { declared: String, actual: String },
}
