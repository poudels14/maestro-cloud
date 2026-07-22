use std::collections::BTreeSet;
use std::fmt::{Debug, Formatter};

use etcd_client::{
    Certificate, Client, ConnectOptions, GetOptions, Identity, KvClient, TlsOptions,
};
use http::Uri;
use kernel_store::{EtcdStore, EtcdTlsConfig, derive_key};
use zeroize::Zeroizing;

use crate::snapshot::{
    MAXIMUM_ENTRY_COUNT, MAXIMUM_KEY_BYTES, MAXIMUM_SNAPSHOT_BYTES, MAXIMUM_VALUE_BYTES,
};
use crate::{LegacyEntry, LegacySnapshot, SnapshotError};

const LEGACY_PREFIX: &[u8] = b"/maetro/";
const PAGE_ENTRIES: i64 = 8;
const MAXIMUM_ENDPOINTS: usize = 9;
const MAXIMUM_ENDPOINT_BYTES: usize = 2_048;
const MAXIMUM_TLS_FILE_BYTES: usize = 1024 * 1024;
const MAXIMUM_RESPONSE_BYTES: usize = 129 * 1024 * 1024;

/// Validated mutual-TLS connection material shared by source and destination clients.
pub struct CutoverEtcdConnection {
    endpoints: Vec<String>,
    certificate_authority: Vec<u8>,
    client_certificate: Vec<u8>,
    client_private_key: Zeroizing<Vec<u8>>,
}

impl CutoverEtcdConnection {
    /// Validates endpoint and PEM input bounds without contacting etcd.
    pub fn new(
        endpoints: Vec<String>,
        certificate_authority: Vec<u8>,
        client_certificate: Vec<u8>,
        client_private_key: Vec<u8>,
    ) -> Result<Self, CutoverEtcdError> {
        validate_endpoints(&endpoints)?;
        validate_tls_file("certificate authority", &certificate_authority)?;
        validate_tls_file("client certificate", &client_certificate)?;
        validate_tls_file("client private key", &client_private_key)?;
        Ok(Self {
            endpoints,
            certificate_authority,
            client_certificate,
            client_private_key: Zeroizing::new(client_private_key),
        })
    }

    /// Opens a raw legacy-prefix reader with a bounded response contract.
    pub async fn legacy_source(&self) -> Result<LegacyEtcdSource, CutoverEtcdError> {
        let identity = Identity::from_pem(
            self.client_certificate.clone(),
            self.client_private_key.as_slice(),
        );
        let tls = TlsOptions::new()
            .ca_certificate(Certificate::from_pem(self.certificate_authority.clone()))
            .identity(identity);
        let client = Client::connect(&self.endpoints, Some(ConnectOptions::new().with_tls(tls)))
            .await
            .map_err(|error| CutoverEtcdError::Connect {
                message: error.to_string(),
            })?;
        Ok(LegacyEtcdSource {
            client: client
                .kv_client()
                .max_decoding_message_size(MAXIMUM_RESPONSE_BYTES),
        })
    }

    /// Opens the encrypted destination store used by the rewrite.
    pub async fn destination_store(
        &self,
        master_secret: &str,
    ) -> Result<EtcdStore, CutoverEtcdError> {
        let encryption_key =
            derive_key(master_secret).map_err(|error| CutoverEtcdError::Destination {
                message: error.to_string(),
            })?;
        let tls = EtcdTlsConfig::for_endpoints(
            self.certificate_authority.clone(),
            self.client_certificate.clone(),
            self.client_private_key.as_slice().to_vec(),
        );
        EtcdStore::connect_with_tls_and_encryption(self.endpoints.clone(), tls, encryption_key)
            .await
            .map_err(|error| CutoverEtcdError::Destination {
                message: error.to_string(),
            })
    }
}

impl Debug for CutoverEtcdConnection {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CutoverEtcdConnection")
            .field("endpoints", &self.endpoints)
            .field("certificate_authority", &"[PEM]")
            .field("client_certificate", &"[PEM]")
            .field("client_private_key", &"[REDACTED]")
            .finish()
    }
}

/// Raw, revision-consistent reader for the misspelled legacy namespace.
pub struct LegacyEtcdSource {
    client: KvClient,
}

impl LegacyEtcdSource {
    /// Captures every legacy key at one fixed etcd revision using bounded pages.
    pub async fn capture(&mut self) -> Result<CapturedLegacySnapshot, CutoverEtcdError> {
        let range_end = prefix_range_end(LEGACY_PREFIX).ok_or(CutoverEtcdError::InvalidPrefix)?;
        let mut start = LEGACY_PREFIX.to_vec();
        let mut revision = None;
        let mut entries = Vec::new();
        let mut total_bytes = 0_usize;
        let mut previous_key: Option<Vec<u8>> = None;
        loop {
            let mut options = GetOptions::new()
                .with_range(range_end.clone())
                .with_limit(PAGE_ENTRIES);
            if let Some(revision) = revision {
                options = options.with_revision(revision);
            }
            let mut response = self
                .client
                .get(start.clone(), Some(options))
                .await
                .map_err(|error| CutoverEtcdError::Read {
                    message: error.to_string(),
                })?;
            let page_revision = response
                .header()
                .map(|header| header.revision())
                .filter(|revision| *revision > 0)
                .ok_or(CutoverEtcdError::MissingRevision)?;
            if revision.is_some_and(|revision| revision != page_revision) {
                return Err(CutoverEtcdError::RevisionChanged);
            }
            revision = Some(page_revision);
            let more = response.more();
            let page = response.take_kvs();
            if more && page.is_empty() {
                return Err(CutoverEtcdError::EmptyPage);
            }
            for entry in page {
                let key = entry.key();
                if !key.starts_with(LEGACY_PREFIX)
                    || key >= range_end.as_slice()
                    || previous_key
                        .as_ref()
                        .is_some_and(|previous| previous.as_slice() >= key)
                {
                    return Err(CutoverEtcdError::UnorderedKeyspace);
                }
                let key =
                    std::str::from_utf8(key).map_err(|error| CutoverEtcdError::MalformedKey {
                        message: error.to_string(),
                    })?;
                previous_key = Some(entry.key().to_vec());
                push_bounded(&mut entries, &mut total_bytes, key, entry.value())?;
            }
            if !more {
                break;
            }
            start = previous_key.clone().ok_or(CutoverEtcdError::EmptyPage)?;
            start.push(0);
        }
        Ok(CapturedLegacySnapshot {
            revision: revision.ok_or(CutoverEtcdError::MissingRevision)?,
            snapshot: LegacySnapshot::new(entries)?,
        })
    }
}

/// One consistent logical snapshot and the etcd revision that produced it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapturedLegacySnapshot {
    revision: i64,
    snapshot: LegacySnapshot,
}

impl CapturedLegacySnapshot {
    /// Returns the fixed etcd revision read by every page.
    pub const fn revision(&self) -> i64 {
        self.revision
    }

    /// Returns the validated logical snapshot.
    pub const fn snapshot(&self) -> &LegacySnapshot {
        &self.snapshot
    }

    /// Consumes the capture into its validated logical snapshot.
    pub fn into_snapshot(self) -> LegacySnapshot {
        self.snapshot
    }
}

fn validate_endpoints(endpoints: &[String]) -> Result<(), CutoverEtcdError> {
    if endpoints.is_empty() || endpoints.len() > MAXIMUM_ENDPOINTS {
        return Err(CutoverEtcdError::InvalidEndpoints);
    }
    let mut unique = BTreeSet::new();
    for endpoint in endpoints {
        let uri = endpoint
            .parse::<Uri>()
            .map_err(|_| CutoverEtcdError::InvalidEndpoint)?;
        if endpoint.len() > MAXIMUM_ENDPOINT_BYTES
            || uri.scheme_str() != Some("https")
            || uri.authority().is_none()
            || uri
                .authority()
                .is_some_and(|authority| authority.as_str().contains('@'))
            || !matches!(uri.path(), "" | "/")
            || uri.query().is_some()
            || !unique.insert(
                uri.authority()
                    .map(|authority| authority.as_str().to_ascii_lowercase())
                    .unwrap_or_default(),
            )
        {
            return Err(CutoverEtcdError::InvalidEndpoint);
        }
    }
    Ok(())
}

fn push_bounded(
    entries: &mut Vec<LegacyEntry>,
    total_bytes: &mut usize,
    key: &str,
    value: &[u8],
) -> Result<(), CutoverEtcdError> {
    if entries.len() >= MAXIMUM_ENTRY_COUNT {
        return Err(SnapshotError::TooManyEntries {
            count: entries.len().saturating_add(1),
            maximum: MAXIMUM_ENTRY_COUNT,
        }
        .into());
    }
    if key.len() > MAXIMUM_KEY_BYTES {
        return Err(SnapshotError::KeyTooLarge {
            key: key.to_owned(),
            length: key.len(),
            maximum: MAXIMUM_KEY_BYTES,
        }
        .into());
    }
    if value.len() > MAXIMUM_VALUE_BYTES {
        return Err(SnapshotError::ValueTooLarge {
            key: key.to_owned(),
            length: value.len(),
            maximum: MAXIMUM_VALUE_BYTES,
        }
        .into());
    }
    *total_bytes = total_bytes
        .checked_add(key.len())
        .and_then(|total| total.checked_add(value.len()))
        .ok_or(SnapshotError::SnapshotTooLarge {
            maximum: MAXIMUM_SNAPSHOT_BYTES,
        })?;
    if *total_bytes > MAXIMUM_SNAPSHOT_BYTES {
        return Err(SnapshotError::SnapshotTooLarge {
            maximum: MAXIMUM_SNAPSHOT_BYTES,
        }
        .into());
    }
    entries.push(LegacyEntry::new(key, value.to_vec()));
    Ok(())
}

fn validate_tls_file(name: &'static str, value: &[u8]) -> Result<(), CutoverEtcdError> {
    if value.is_empty() || value.len() > MAXIMUM_TLS_FILE_BYTES {
        Err(CutoverEtcdError::InvalidTlsFile { name })
    } else {
        Ok(())
    }
}

fn prefix_range_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    for index in (0..end.len()).rev() {
        if end.get(index).copied() == Some(u8::MAX) {
            continue;
        }
        if let Some(byte) = end.get_mut(index) {
            *byte += 1;
            end.truncate(index + 1);
            return Some(end);
        }
    }
    None
}

/// The cutover tool could not safely read or connect to etcd.
#[derive(Debug, thiserror::Error)]
pub enum CutoverEtcdError {
    #[error("cutover etcd endpoints must contain between 1 and {MAXIMUM_ENDPOINTS} entries")]
    InvalidEndpoints,
    #[error("each cutover etcd endpoint must be a unique absolute HTTPS origin")]
    InvalidEndpoint,
    #[error("cutover {name} PEM is empty or exceeds {MAXIMUM_TLS_FILE_BYTES} bytes")]
    InvalidTlsFile { name: &'static str },
    #[error("could not connect to legacy etcd: {message}")]
    Connect { message: String },
    #[error("could not read a consistent legacy etcd snapshot: {message}")]
    Read { message: String },
    #[error("could not connect to the encrypted rewrite store: {message}")]
    Destination { message: String },
    #[error("legacy etcd response had no positive revision")]
    MissingRevision,
    #[error("legacy etcd returned different revisions for one paged snapshot")]
    RevisionChanged,
    #[error("legacy etcd indicated another page but returned no keys")]
    EmptyPage,
    #[error("legacy etcd returned keys outside strict ascending /maetro/ order")]
    UnorderedKeyspace,
    #[error("legacy etcd returned a non-UTF-8 key: {message}")]
    MalformedKey { message: String },
    #[error("the legacy etcd prefix has no finite range end")]
    InvalidPrefix,
    #[error(transparent)]
    Snapshot(#[from] SnapshotError),
}
