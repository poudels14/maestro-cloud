use sha2::{Digest, Sha256};

const LEGACY_PREFIX: &str = "/maetro/";
pub(crate) const MAXIMUM_ENTRY_COUNT: usize = 100_000;
pub(crate) const MAXIMUM_KEY_BYTES: usize = 1_024;
pub(crate) const MAXIMUM_VALUE_BYTES: usize = 16 * 1_024 * 1_024;
pub(crate) const MAXIMUM_SNAPSHOT_BYTES: usize = 1_024 * 1_024 * 1_024;
const SNAPSHOT_DIGEST_DOMAIN: &[u8] = b"maestro-cutover-snapshot-v1\0";

/// One exact key/value pair read from the stopped legacy control plane.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegacyEntry {
    key: String,
    value: Vec<u8>,
}

impl LegacyEntry {
    /// Captures one legacy key and its unmodified bytes.
    pub fn new(key: impl Into<String>, value: Vec<u8>) -> Self {
        Self {
            key: key.into(),
            value,
        }
    }

    /// Returns the exact legacy etcd key.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Returns the exact legacy etcd value.
    pub fn value(&self) -> &[u8] {
        &self.value
    }
}

/// Bounded, canonical legacy-state snapshot used as the migration input fence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegacySnapshot {
    entries: Vec<LegacyEntry>,
    digest: [u8; 32],
}

impl LegacySnapshot {
    /// Validates, sorts, and hashes the legacy entries deterministically.
    pub fn new(mut entries: Vec<LegacyEntry>) -> Result<Self, SnapshotError> {
        if entries.len() > MAXIMUM_ENTRY_COUNT {
            return Err(SnapshotError::TooManyEntries {
                count: entries.len(),
                maximum: MAXIMUM_ENTRY_COUNT,
            });
        }

        for entry in &entries {
            validate_entry(entry)?;
        }
        let total_bytes = entries.iter().try_fold(0_usize, |total, entry| {
            total
                .checked_add(entry.key.len())
                .and_then(|total| total.checked_add(entry.value.len()))
        });
        if total_bytes.is_none_or(|total| total > MAXIMUM_SNAPSHOT_BYTES) {
            return Err(SnapshotError::SnapshotTooLarge {
                maximum: MAXIMUM_SNAPSHOT_BYTES,
            });
        }
        entries.sort_by(|left, right| left.key.cmp(&right.key));

        if let Some(key) = entries.windows(2).find_map(|pair| match pair {
            [left, right] if left.key == right.key => Some(left.key.clone()),
            _ => None,
        }) {
            return Err(SnapshotError::DuplicateKey { key });
        }

        let digest = digest_entries(&entries);
        Ok(Self { entries, digest })
    }

    /// Returns entries in canonical lexicographic key order.
    pub fn entries(&self) -> &[LegacyEntry] {
        &self.entries
    }

    /// Returns the SHA-256 input fence bound into the completion marker.
    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }
}

fn validate_entry(entry: &LegacyEntry) -> Result<(), SnapshotError> {
    if !entry.key.starts_with(LEGACY_PREFIX) {
        return Err(SnapshotError::OutsideLegacyNamespace {
            key: entry.key.clone(),
        });
    }
    if entry.key.len() > MAXIMUM_KEY_BYTES {
        return Err(SnapshotError::KeyTooLarge {
            key: entry.key.clone(),
            length: entry.key.len(),
            maximum: MAXIMUM_KEY_BYTES,
        });
    }
    if entry.value.len() > MAXIMUM_VALUE_BYTES {
        return Err(SnapshotError::ValueTooLarge {
            key: entry.key.clone(),
            length: entry.value.len(),
            maximum: MAXIMUM_VALUE_BYTES,
        });
    }
    Ok(())
}

fn digest_entries(entries: &[LegacyEntry]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(SNAPSHOT_DIGEST_DOMAIN);
    hash_length(&mut hasher, entries.len());
    for entry in entries {
        hash_length(&mut hasher, entry.key.len());
        hasher.update(entry.key.as_bytes());
        hash_length(&mut hasher, entry.value.len());
        hasher.update(&entry.value);
    }
    hasher.finalize().into()
}

fn hash_length(hasher: &mut Sha256, length: usize) {
    let length = u64::try_from(length).unwrap_or(u64::MAX);
    hasher.update(length.to_be_bytes());
}

/// A legacy snapshot violated the migration's bounded input contract.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SnapshotError {
    /// The snapshot is too large to plan safely in memory.
    #[error("legacy snapshot has {count} entries; maximum is {maximum}")]
    TooManyEntries {
        /// Observed entry count.
        count: usize,
        /// Maximum accepted entry count.
        maximum: usize,
    },
    /// The combined key and value bytes exceed the in-memory planning bound.
    #[error("legacy snapshot exceeds the {maximum}-byte total input bound")]
    SnapshotTooLarge {
        /// Maximum combined key and value bytes.
        maximum: usize,
    },
    /// A key is not owned by the misspelled legacy namespace.
    #[error("legacy snapshot key is outside /maetro/: {key}")]
    OutsideLegacyNamespace {
        /// Rejected key.
        key: String,
    },
    /// A key exceeds the defensive input bound.
    #[error("legacy key `{key}` is {length} bytes; maximum is {maximum}")]
    KeyTooLarge {
        /// Rejected key.
        key: String,
        /// Observed byte length.
        length: usize,
        /// Maximum accepted byte length.
        maximum: usize,
    },
    /// A value exceeds the defensive input bound.
    #[error("legacy value at `{key}` is {length} bytes; maximum is {maximum}")]
    ValueTooLarge {
        /// Rejected key.
        key: String,
        /// Observed byte length.
        length: usize,
        /// Maximum accepted byte length.
        maximum: usize,
    },
    /// The etcd snapshot contained one key more than once.
    #[error("legacy snapshot contains duplicate key `{key}`")]
    DuplicateKey {
        /// Repeated key.
        key: String,
    },
}
