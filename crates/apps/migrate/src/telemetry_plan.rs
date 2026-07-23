use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::{Component, Path, PathBuf};

use duckdb::{AccessMode, Config, Connection};
use kernel_api::{ClusterId, NodeId};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const PLAN_SCHEMA_VERSION: u32 = 1;
const MAXIMUM_PLAN_BYTES: usize = 256 * 1024 * 1024;
const HASH_BUFFER_BYTES: usize = 1024 * 1024;
const REQUIRED_DATABASES: [&str; 3] = [
    "metrics.duckdb",
    "service-logs.duckdb",
    "system-logs.duckdb",
];

/// Reviewable inventory of one stopped legacy node's observability files.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LegacyTelemetryPlan {
    /// Plan wire version.
    pub schema_version: u32,
    /// Cluster that will own the imported records.
    pub cluster_id: ClusterId,
    /// Node that owns the source files and imported records.
    pub node_id: NodeId,
    /// SHA-256 fence over the ordered file inventory.
    pub source_sha256: String,
    /// Ordered source files relative to the legacy probe data root.
    pub files: Vec<LegacyTelemetryFile>,
    /// Validated logical row inventory.
    pub counts: LegacyTelemetryCounts,
}

impl LegacyTelemetryPlan {
    /// Returns the maximum accepted serialized plan size.
    pub const fn maximum_artifact_bytes() -> usize {
        MAXIMUM_PLAN_BYTES
    }

    /// Captures a source-fenced plan without modifying the legacy stores.
    pub fn capture(
        legacy_data_directory: &Path,
        cluster_id: ClusterId,
        node_id: NodeId,
    ) -> Result<Self, LegacyTelemetryPlanError> {
        let root = validate_root(legacy_data_directory)?;
        let files = inventory(&root)?;
        validate_required_databases(&files)?;
        let counts = count_rows(&root, &files)?;
        let source_sha256 = inventory_digest(&files);
        let plan = Self {
            schema_version: PLAN_SCHEMA_VERSION,
            cluster_id,
            node_id,
            source_sha256,
            files,
            counts,
        };
        let bytes = serde_json::to_vec_pretty(&plan)
            .map_err(|error| LegacyTelemetryPlanError::Encode(error.to_string()))?
            .len();
        if bytes > MAXIMUM_PLAN_BYTES {
            return Err(LegacyTelemetryPlanError::PlanTooLarge {
                bytes,
                maximum: MAXIMUM_PLAN_BYTES,
            });
        }
        Ok(plan)
    }

    /// Recaptures a stopped source and requires it to match this reviewed plan exactly.
    pub fn verify_source(
        &self,
        legacy_data_directory: &Path,
    ) -> Result<PathBuf, LegacyTelemetryPlanError> {
        if self.schema_version != PLAN_SCHEMA_VERSION {
            return Err(LegacyTelemetryPlanError::UnsupportedSchema {
                version: self.schema_version,
            });
        }
        let root = validate_root(legacy_data_directory)?;
        let captured = Self::capture(&root, self.cluster_id.clone(), self.node_id.clone())?;
        if captured == *self {
            Ok(root)
        } else {
            Err(LegacyTelemetryPlanError::SourceChanged {
                expected: self.source_sha256.clone(),
                actual: captured.source_sha256,
            })
        }
    }
}

/// One immutable input file bound into a telemetry plan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LegacyTelemetryFile {
    /// Slash-separated path relative to the legacy probe data root.
    pub path: String,
    /// File size observed while hashing.
    pub size_bytes: u64,
    /// Lowercase SHA-256 of the exact file bytes.
    pub sha256: String,
}

/// Logical records found across the legacy hot and cold tiers.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LegacyTelemetryCounts {
    /// Service log records in DuckDB and committed Parquet.
    pub service_logs: u64,
    /// System/build log records in DuckDB and committed Parquet.
    pub system_logs: u64,
    /// Host resource samples represented by the legacy `node` source.
    pub host_metrics: u64,
    /// Per-container samples eligible for workload-history conversion.
    pub container_metrics: u64,
    /// Redundant `cluster` and `service:*` aggregates derivable after import.
    pub derived_metric_aggregates: u64,
    /// Historical controller and backup operational samples.
    pub operational_metrics: u64,
    /// Retired Prometheus-derived traffic aggregates retained in the source archive.
    pub retired_traffic_metrics: u64,
    /// Whether persisted backup health is available.
    pub backup_stats: bool,
    /// Committed cold-tier manifests.
    pub cold_partitions: u64,
    /// Verified Parquet objects referenced by those manifests.
    pub parquet_files: u64,
}

fn validate_root(path: &Path) -> Result<PathBuf, LegacyTelemetryPlanError> {
    validate_absolute(path)?;
    let canonical = std::fs::canonicalize(path).map_err(|source| LegacyTelemetryPlanError::Io {
        action: "canonicalize",
        path: path.to_path_buf(),
        source,
    })?;
    if canonical != path {
        return Err(LegacyTelemetryPlanError::NonCanonical {
            path: path.to_path_buf(),
            canonical,
        });
    }
    let metadata =
        std::fs::symlink_metadata(path).map_err(|source| LegacyTelemetryPlanError::Io {
            action: "inspect",
            path: path.to_path_buf(),
            source,
        })?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(LegacyTelemetryPlanError::InvalidRoot {
            path: path.to_path_buf(),
        });
    }
    Ok(path.to_path_buf())
}

fn validate_absolute(path: &Path) -> Result<(), LegacyTelemetryPlanError> {
    if path.is_absolute()
        && path
            .components()
            .all(|component| !matches!(component, Component::ParentDir))
    {
        Ok(())
    } else {
        Err(LegacyTelemetryPlanError::InvalidPath {
            path: path.to_path_buf(),
        })
    }
}

fn inventory(root: &Path) -> Result<Vec<LegacyTelemetryFile>, LegacyTelemetryPlanError> {
    let mut paths = Vec::new();
    walk(&root.join("duckdb"), SourceTree::Databases, &mut paths)?;
    walk(&root.join("parts"), SourceTree::Partitions, &mut paths)?;
    paths.sort();
    let mut files = Vec::with_capacity(paths.len());
    for path in paths {
        let relative = relative_path(root, &path)?;
        let (size_bytes, sha256) = hash_file(&path)?;
        files.push(LegacyTelemetryFile {
            path: relative,
            size_bytes,
            sha256,
        });
    }
    Ok(files)
}

#[derive(Clone, Copy)]
enum SourceTree {
    Databases,
    Partitions,
}

fn walk(
    directory: &Path,
    tree: SourceTree,
    files: &mut Vec<PathBuf>,
) -> Result<(), LegacyTelemetryPlanError> {
    let metadata =
        std::fs::symlink_metadata(directory).map_err(|source| LegacyTelemetryPlanError::Io {
            action: "inspect",
            path: directory.to_path_buf(),
            source,
        })?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(LegacyTelemetryPlanError::UnsafeEntry {
            path: directory.to_path_buf(),
        });
    }
    let mut entries = std::fs::read_dir(directory)
        .map_err(|source| LegacyTelemetryPlanError::Io {
            action: "read",
            path: directory.to_path_buf(),
            source,
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| LegacyTelemetryPlanError::Io {
            action: "read",
            path: directory.to_path_buf(),
            source,
        })?;
    entries.sort_by_key(std::fs::DirEntry::file_name);
    for entry in entries {
        let path = entry.path();
        let metadata =
            std::fs::symlink_metadata(&path).map_err(|source| LegacyTelemetryPlanError::Io {
                action: "inspect",
                path: path.clone(),
                source,
            })?;
        if metadata.file_type().is_symlink() {
            return Err(LegacyTelemetryPlanError::UnsafeEntry { path });
        }
        if metadata.is_dir() {
            if matches!(tree, SourceTree::Databases) {
                return Err(LegacyTelemetryPlanError::UnexpectedEntry { path });
            }
            walk(&path, tree, files)?;
        } else if metadata.is_file() && allowed_file(&path, tree) {
            files.push(path);
        } else {
            return Err(LegacyTelemetryPlanError::UnexpectedEntry { path });
        }
    }
    Ok(())
}

fn allowed_file(path: &Path, tree: SourceTree) -> bool {
    let name = path.file_name().and_then(|name| name.to_str());
    match tree {
        SourceTree::Databases => name.is_some_and(|name| REQUIRED_DATABASES.contains(&name)),
        SourceTree::Partitions => {
            name == Some("manifest.json")
                || name.is_some_and(|name| name.starts_with("part-") && name.ends_with(".parquet"))
        }
    }
}

fn relative_path(root: &Path, path: &Path) -> Result<String, LegacyTelemetryPlanError> {
    path.strip_prefix(root)
        .ok()
        .and_then(Path::to_str)
        .map(|path| path.replace(std::path::MAIN_SEPARATOR, "/"))
        .ok_or_else(|| LegacyTelemetryPlanError::InvalidPath {
            path: path.to_path_buf(),
        })
}

fn hash_file(path: &Path) -> Result<(u64, String), LegacyTelemetryPlanError> {
    let file = File::open(path).map_err(|source| LegacyTelemetryPlanError::Io {
        action: "open",
        path: path.to_path_buf(),
        source,
    })?;
    let metadata = file
        .metadata()
        .map_err(|source| LegacyTelemetryPlanError::Io {
            action: "inspect",
            path: path.to_path_buf(),
            source,
        })?;
    let mut reader = BufReader::new(file);
    let mut digest = Sha256::new();
    let mut buffer = vec![0_u8; HASH_BUFFER_BYTES];
    loop {
        let read = reader
            .read(&mut buffer)
            .map_err(|source| LegacyTelemetryPlanError::Io {
                action: "hash",
                path: path.to_path_buf(),
                source,
            })?;
        if read == 0 {
            break;
        }
        let chunk = buffer
            .get(..read)
            .ok_or_else(|| LegacyTelemetryPlanError::InvalidRead {
                path: path.to_path_buf(),
                bytes: read,
                capacity: buffer.len(),
            })?;
        digest.update(chunk);
    }
    Ok((metadata.len(), hex::encode(digest.finalize())))
}

fn validate_required_databases(
    files: &[LegacyTelemetryFile],
) -> Result<(), LegacyTelemetryPlanError> {
    let present = files
        .iter()
        .filter_map(|file| file.path.strip_prefix("duckdb/"))
        .collect::<BTreeSet<_>>();
    for required in REQUIRED_DATABASES {
        if !present.contains(required) {
            return Err(LegacyTelemetryPlanError::MissingDatabase {
                name: required.to_owned(),
            });
        }
    }
    Ok(())
}

fn count_rows(
    root: &Path,
    files: &[LegacyTelemetryFile],
) -> Result<LegacyTelemetryCounts, LegacyTelemetryPlanError> {
    let service = open_read_only(&root.join("duckdb/service-logs.duckdb"))?;
    let system = open_read_only(&root.join("duckdb/system-logs.duckdb"))?;
    let metrics = open_read_only(&root.join("duckdb/metrics.duckdb"))?;
    let mut counts = LegacyTelemetryCounts {
        service_logs: count(&service, "SELECT COUNT(*) FROM logs")?,
        system_logs: count(&system, "SELECT COUNT(*) FROM logs")?,
        host_metrics: count(
            &metrics,
            "SELECT COUNT(*) FROM metrics WHERE source = 'node'",
        )?,
        container_metrics: count(
            &metrics,
            "SELECT COUNT(*) FROM metrics WHERE starts_with(source, 'container:')",
        )?,
        derived_metric_aggregates: count(
            &metrics,
            "SELECT COUNT(*) FROM metrics
             WHERE source = 'cluster' OR starts_with(source, 'service:')",
        )?,
        operational_metrics: count(&metrics, "SELECT COUNT(*) FROM stats_metrics")?,
        retired_traffic_metrics: count(&metrics, "SELECT COUNT(*) FROM traffic_metrics")?,
        backup_stats: count(
            &metrics,
            "SELECT COUNT(*) FROM probe_state WHERE key = 'backup-stats'",
        )? == 1,
        ..LegacyTelemetryCounts::default()
    };
    let known_metrics = counts
        .host_metrics
        .saturating_add(counts.container_metrics)
        .saturating_add(counts.derived_metric_aggregates);
    let all_metrics = count(&metrics, "SELECT COUNT(*) FROM metrics")?;
    if known_metrics != all_metrics {
        return Err(LegacyTelemetryPlanError::UnexpectedMetricSources {
            rows: all_metrics.saturating_sub(known_metrics),
        });
    }
    validate_partitions(root, files, &service, &system, &mut counts)?;
    Ok(counts)
}

fn open_read_only(path: &Path) -> Result<Connection, LegacyTelemetryPlanError> {
    let config = Config::default()
        .access_mode(AccessMode::ReadOnly)
        .map_err(|source| database(path, source))?;
    Connection::open_with_flags(path, config).map_err(|source| database(path, source))
}

fn count(connection: &Connection, sql: &str) -> Result<u64, LegacyTelemetryPlanError> {
    let value = connection
        .query_row(sql, [], |row| row.get::<_, i64>(0))
        .map_err(|source| LegacyTelemetryPlanError::Database {
            path: PathBuf::from("<legacy-schema>"),
            source,
        })?;
    u64::try_from(value).map_err(|_| LegacyTelemetryPlanError::InvalidCount { value })
}

fn validate_partitions(
    root: &Path,
    files: &[LegacyTelemetryFile],
    service: &Connection,
    system: &Connection,
    counts: &mut LegacyTelemetryCounts,
) -> Result<(), LegacyTelemetryPlanError> {
    let by_path = files
        .iter()
        .map(|file| (file.path.as_str(), file))
        .collect::<BTreeMap<_, _>>();
    let manifests = files
        .iter()
        .filter(|file| file.path.ends_with("/manifest.json"))
        .collect::<Vec<_>>();
    let parquets = files
        .iter()
        .filter(|file| file.path.ends_with(".parquet"))
        .collect::<Vec<_>>();
    let mut referenced = BTreeSet::new();
    for manifest_file in manifests {
        let path = root.join(&manifest_file.path);
        let manifest: LegacyPartitionManifest =
            serde_json::from_reader(BufReader::new(File::open(&path).map_err(|source| {
                LegacyTelemetryPlanError::Io {
                    action: "open",
                    path: path.clone(),
                    source,
                }
            })?))
            .map_err(|source| LegacyTelemetryPlanError::Manifest {
                path: path.clone(),
                message: source.to_string(),
            })?;
        if manifest.version != 1 || !matches!(manifest.tier.as_str(), "service" | "system") {
            return Err(LegacyTelemetryPlanError::Manifest {
                path,
                message: "unsupported manifest version or tier".to_owned(),
            });
        }
        let parent = Path::new(&manifest_file.path)
            .parent()
            .ok_or_else(|| LegacyTelemetryPlanError::InvalidPath { path: path.clone() })?;
        for part in manifest.parts {
            if part.file.contains('/') || part.file.contains('\\') {
                return Err(LegacyTelemetryPlanError::Manifest {
                    path: path.clone(),
                    message: "part filename must be a basename".to_owned(),
                });
            }
            let relative = parent.join(&part.file).to_string_lossy().replace('\\', "/");
            let actual = by_path.get(relative.as_str()).ok_or_else(|| {
                LegacyTelemetryPlanError::Manifest {
                    path: path.clone(),
                    message: format!("referenced part `{}` is missing", part.file),
                }
            })?;
            if actual.sha256 != part.sha256 || actual.size_bytes != part.size_bytes {
                return Err(LegacyTelemetryPlanError::Manifest {
                    path: path.clone(),
                    message: format!("part `{}` failed size or digest verification", part.file),
                });
            }
            let connection = if manifest.tier == "service" {
                service
            } else {
                system
            };
            let absolute = root.join(&relative);
            let (rows, low, high) = parquet_bounds(connection, &absolute)?;
            if rows != part.row_count || low != part.seq_lo || high != part.seq_hi {
                return Err(LegacyTelemetryPlanError::Manifest {
                    path: path.clone(),
                    message: format!("part `{}` failed row-bound verification", part.file),
                });
            }
            if manifest.tier == "service" {
                counts.service_logs = counts.service_logs.saturating_add(rows);
            } else {
                counts.system_logs = counts.system_logs.saturating_add(rows);
            }
            referenced.insert(relative);
        }
    }
    let actual = parquets
        .iter()
        .map(|file| file.path.clone())
        .collect::<BTreeSet<_>>();
    if actual != referenced {
        let path = actual
            .symmetric_difference(&referenced)
            .next()
            .cloned()
            .unwrap_or_else(|| "parts".to_owned());
        return Err(LegacyTelemetryPlanError::UncommittedPartition { path });
    }
    counts.cold_partitions = u64::try_from(
        files
            .iter()
            .filter(|file| file.path.ends_with("/manifest.json"))
            .count(),
    )
    .unwrap_or(u64::MAX);
    counts.parquet_files = u64::try_from(parquets.len()).unwrap_or(u64::MAX);
    Ok(())
}

fn parquet_bounds(
    connection: &Connection,
    path: &Path,
) -> Result<(u64, i64, i64), LegacyTelemetryPlanError> {
    let sql = "SELECT COUNT(*), MIN(seq), MAX(seq) FROM read_parquet(?1)";
    let (rows, low, high) = connection
        .query_row(sql, [path.to_string_lossy().as_ref()], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })
        .map_err(|source| database(path, source))?;
    let rows =
        u64::try_from(rows).map_err(|_| LegacyTelemetryPlanError::InvalidCount { value: rows })?;
    Ok((rows, low, high))
}

fn inventory_digest(files: &[LegacyTelemetryFile]) -> String {
    let mut digest = Sha256::new();
    digest.update(b"maestro-legacy-telemetry-plan-v1\0");
    for file in files {
        digest.update(file.path.len().to_be_bytes());
        digest.update(file.path.as_bytes());
        digest.update(file.size_bytes.to_be_bytes());
        digest.update(file.sha256.as_bytes());
    }
    hex::encode(digest.finalize())
}

fn database(path: &Path, source: duckdb::Error) -> LegacyTelemetryPlanError {
    LegacyTelemetryPlanError::Database {
        path: path.to_path_buf(),
        source,
    }
}

#[derive(Deserialize)]
struct LegacyPartitionManifest {
    version: u8,
    tier: String,
    #[allow(dead_code)]
    partition_key: String,
    parts: Vec<LegacyPartitionPart>,
}

#[derive(Deserialize)]
struct LegacyPartitionPart {
    file: String,
    row_count: u64,
    seq_lo: i64,
    seq_hi: i64,
    sha256: String,
    size_bytes: u64,
}

/// A legacy node's telemetry could not be fenced into a reviewable plan.
#[derive(Debug, thiserror::Error)]
pub enum LegacyTelemetryPlanError {
    /// Only plans produced by this migration generation are accepted.
    #[error("legacy telemetry plan schema {version} is not supported")]
    UnsupportedSchema { version: u32 },
    /// A source inventory cannot exceed the bounded review artifact size.
    #[error("legacy telemetry plan uses {bytes} bytes, exceeding the {maximum}-byte bound")]
    PlanTooLarge { bytes: usize, maximum: usize },
    /// A validated inventory could not be represented as JSON.
    #[error("legacy telemetry plan could not be encoded: {0}")]
    Encode(String),
    /// The stopped source changed after operator review.
    #[error("legacy telemetry source digest {actual} does not match reviewed digest {expected}")]
    SourceChanged { expected: String, actual: String },
    /// Paths must be explicit and traversal-free.
    #[error("legacy telemetry path must be absolute without parent traversal: {}", path.display())]
    InvalidPath { path: PathBuf },
    /// Symlink aliases are rejected so the reviewed path remains stable.
    #[error(
        "legacy telemetry path `{}` must be canonical; use `{}`",
        path.display(),
        canonical.display()
    )]
    NonCanonical { path: PathBuf, canonical: PathBuf },
    /// The selected source is not a normal directory.
    #[error("legacy telemetry root `{}` is not a regular directory", path.display())]
    InvalidRoot { path: PathBuf },
    /// Source trees must not contain symlinks or special files.
    #[error("legacy telemetry entry `{}` is a symlink or special file", path.display())]
    UnsafeEntry { path: PathBuf },
    /// Only the known stopped-store layout is accepted.
    #[error("legacy telemetry entry `{}` is not part of the supported layout", path.display())]
    UnexpectedEntry { path: PathBuf },
    /// A required hot-tier database is absent.
    #[error("legacy telemetry database `duckdb/{name}` is missing")]
    MissingDatabase { name: String },
    /// A legacy metrics source was outside the known node/container/derived namespaces.
    #[error("legacy metrics contain {rows} rows with unsupported source names")]
    UnexpectedMetricSources { rows: u64 },
    /// A cold-tier manifest is malformed or does not match its objects.
    #[error("legacy partition manifest `{}` is invalid: {message}", path.display())]
    Manifest { path: PathBuf, message: String },
    /// Every visible Parquet object must be committed by a manifest.
    #[error("legacy Parquet inventory and committed manifests differ at `{path}`")]
    UncommittedPartition { path: String },
    /// A count could not be represented safely.
    #[error("legacy telemetry row count {value} is invalid")]
    InvalidCount { value: i64 },
    /// A reader violated the standard `Read` contract.
    #[error(
        "legacy telemetry read from `{}` returned {bytes} bytes for a {capacity}-byte buffer",
        path.display()
    )]
    InvalidRead {
        path: PathBuf,
        bytes: usize,
        capacity: usize,
    },
    /// A filesystem operation failed.
    #[error("could not {action} legacy telemetry path `{}`: {source}", path.display())]
    Io {
        action: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    /// A stopped DuckDB or Parquet source could not be read.
    #[error("could not inspect legacy telemetry database `{}`: {source}", path.display())]
    Database {
        path: PathBuf,
        #[source]
        source: duckdb::Error,
    },
}
