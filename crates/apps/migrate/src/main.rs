use std::cell::Cell;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use clap::{Args, Parser, Subcommand};
use kernel_api::ResourceName;
use kernel_store::{Keyspace, Store};
use migrate::{
    CapturedLegacySnapshot, CutoverEtcdConnection, CutoverMigration, LegacyEtcdSource,
    LegacySnapshot, LegacyTelemetryPlan, MigrationOutcome, MigrationPlanReport,
    MigrationVerification, apply_legacy_telemetry, plan_legacy_snapshot, plan_legacy_store_restore,
    restore_legacy_store, verify_legacy_store_restore, verify_legacy_telemetry,
};
use serde::Serialize;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::util::SubscriberInitExt;

mod cutover_files;

use cutover_files::{
    read_master_secret, read_private_pem, read_public_pem, read_snapshot, read_telemetry_plan,
    write_new_private,
};

const DEFAULT_REWRITE_STORE_PEER_PORT: u16 = 2_380;

#[derive(Debug, Parser)]
#[command(
    name = "maestro-migrate",
    version,
    about = "One-shot Maestro cutover tool"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Captures a consistent logical snapshot from the stopped legacy control plane.
    Capture {
        #[command(flatten)]
        etcd: EtcdArguments,
        #[arg(long)]
        output: PathBuf,
    },
    /// Validates an artifact and emits its secret-free migration plan report.
    Plan {
        #[arg(long)]
        snapshot: PathBuf,
        #[arg(long)]
        master_secret_file: PathBuf,
        #[arg(long)]
        output: Option<PathBuf>,
    },
    /// Applies one reviewed artifact after fencing it against live legacy state.
    Apply {
        #[command(flatten)]
        etcd: EtcdArguments,
        #[arg(long)]
        snapshot: PathBuf,
        #[arg(long)]
        master_secret_file: PathBuf,
        #[arg(long, default_value = "legacy-v1")]
        migration_id: ResourceName,
    },
    /// Verifies a completed migration before rewrite daemons can mutate resources.
    Verify {
        #[command(flatten)]
        etcd: EtcdArguments,
        #[arg(long)]
        snapshot: PathBuf,
        #[arg(long)]
        master_secret_file: PathBuf,
        #[arg(long, default_value = "legacy-v1")]
        migration_id: ResourceName,
        #[arg(long)]
        output: Option<PathBuf>,
    },
    /// Emits the rewritten etcd topology restored from a post-migration native snapshot.
    StorePlan {
        /// Reviewed logical snapshot captured before schema conversion.
        #[arg(long)]
        snapshot: PathBuf,
        /// Shared rewrite etcd peer port used by every restored member.
        #[arg(long, default_value_t = DEFAULT_REWRITE_STORE_PEER_PORT)]
        store_peer_port: u16,
        #[arg(long)]
        output: Option<PathBuf>,
    },
    /// Restores one rewritten store member into an empty daemon data directory.
    StoreRestore {
        /// Reviewed logical snapshot captured before schema conversion.
        #[arg(long)]
        snapshot: PathBuf,
        /// Native etcd snapshot captured after migration verification.
        #[arg(long)]
        native_snapshot: PathBuf,
        /// Migrated control-plane node that will own this member.
        #[arg(long)]
        node_id: String,
        /// Rewrite cluster data root on this node.
        #[arg(long)]
        data_directory: PathBuf,
        /// Exact etcdutl executable used for snapshot restoration.
        #[arg(long)]
        etcdutl_binary: PathBuf,
        /// Shared rewrite etcd peer port used by every restored member.
        #[arg(long, default_value_t = DEFAULT_REWRITE_STORE_PEER_PORT)]
        store_peer_port: u16,
    },
    /// Verifies one restored member against both reviewed snapshot artifacts.
    StoreVerify {
        /// Reviewed logical snapshot captured before schema conversion.
        #[arg(long)]
        snapshot: PathBuf,
        /// Native etcd snapshot captured after migration verification.
        #[arg(long)]
        native_snapshot: PathBuf,
        /// Migrated control-plane node that owns this member.
        #[arg(long)]
        node_id: String,
        /// Rewrite cluster data root on this node.
        #[arg(long)]
        data_directory: PathBuf,
        /// Shared rewrite etcd peer port used by every restored member.
        #[arg(long, default_value_t = DEFAULT_REWRITE_STORE_PEER_PORT)]
        store_peer_port: u16,
        #[arg(long)]
        output: Option<PathBuf>,
    },
    /// Captures a reviewable, read-only inventory of one stopped node's legacy telemetry.
    TelemetryPlan {
        /// Legacy probe data root, normally `<cluster>/system/probe/data`.
        #[arg(long)]
        legacy_data_directory: PathBuf,
        #[arg(long)]
        cluster_id: kernel_api::ClusterId,
        #[arg(long)]
        node_id: kernel_api::NodeId,
        #[arg(long)]
        output: Option<PathBuf>,
    },
    /// Converts a reviewed node-local telemetry plan into rewrite stores.
    TelemetryApply {
        /// Owner-only plan emitted by `telemetry-plan`.
        #[arg(long)]
        plan: PathBuf,
        /// Legacy probe data root, normally `<cluster>/system/probe/data`.
        #[arg(long)]
        legacy_data_directory: PathBuf,
        /// Rewrite cluster data root containing the node-owned `agent` directory.
        #[arg(long)]
        data_directory: PathBuf,
    },
    /// Independently reprojects a source and verifies completed rewrite stores.
    TelemetryVerify {
        /// Owner-only plan emitted by `telemetry-plan`.
        #[arg(long)]
        plan: PathBuf,
        /// Legacy probe data root, normally `<cluster>/system/probe/data`.
        #[arg(long)]
        legacy_data_directory: PathBuf,
        /// Rewrite cluster data root containing the node-owned `agent` directory.
        #[arg(long)]
        data_directory: PathBuf,
        #[arg(long)]
        output: Option<PathBuf>,
    },
}

#[derive(Debug, Args)]
struct EtcdArguments {
    #[arg(long = "endpoint", required = true)]
    endpoints: Vec<String>,
    #[arg(long)]
    certificate_authority: PathBuf,
    #[arg(long)]
    client_certificate: PathBuf,
    #[arg(long)]
    client_private_key: PathBuf,
}

impl EtcdArguments {
    fn load(self) -> Result<CutoverEtcdConnection, Box<dyn std::error::Error>> {
        Ok(CutoverEtcdConnection::new(
            self.endpoints,
            read_public_pem(&self.certificate_authority)?,
            read_public_pem(&self.client_certificate)?,
            read_private_pem(&self.client_private_key)?,
        )?)
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(error) => error.exit(),
    };
    initialize_tracing();
    if let Err(error) = run(cli).await {
        tracing::error!(error = %error, "maestro migration failed");
        std::process::exit(1);
    }
}

fn initialize_tracing() {
    let filter = EnvFilter::builder()
        .with_default_directive(tracing::Level::INFO.into())
        .from_env_lossy();
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .json()
        .flatten_event(true)
        .with_ansi(false)
        .with_writer(std::io::stderr)
        .finish()
        .init();
}

async fn run(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
    match cli.command {
        Command::Capture { etcd, output } => capture(etcd.load()?, &output).await,
        Command::Plan {
            snapshot,
            master_secret_file,
            output,
        } => plan(&snapshot, &master_secret_file, output.as_deref()),
        Command::Apply {
            etcd,
            snapshot,
            master_secret_file,
            migration_id,
        } => apply(etcd.load()?, &snapshot, &master_secret_file, migration_id).await,
        Command::Verify {
            etcd,
            snapshot,
            master_secret_file,
            migration_id,
            output,
        } => {
            verify(
                etcd.load()?,
                &snapshot,
                &master_secret_file,
                migration_id,
                output.as_deref(),
            )
            .await
        }
        Command::StorePlan {
            snapshot,
            store_peer_port,
            output,
        } => {
            let snapshot = load_snapshot(&snapshot)?;
            let plan = plan_legacy_store_restore(&snapshot, store_peer_port)?;
            write_json(&plan, output.as_deref())
        }
        Command::StoreRestore {
            snapshot,
            native_snapshot,
            node_id,
            data_directory,
            etcdutl_binary,
            store_peer_port,
        } => {
            let snapshot = load_snapshot(&snapshot)?;
            let plan = plan_legacy_store_restore(&snapshot, store_peer_port)?;
            let node_id = parse_node_id(node_id)?;
            let report = restore_legacy_store(
                &plan,
                &node_id,
                &native_snapshot,
                &data_directory,
                &etcdutl_binary,
            )?;
            write_json(&report, None)
        }
        Command::StoreVerify {
            snapshot,
            native_snapshot,
            node_id,
            data_directory,
            store_peer_port,
            output,
        } => {
            let snapshot = load_snapshot(&snapshot)?;
            let plan = plan_legacy_store_restore(&snapshot, store_peer_port)?;
            let node_id = parse_node_id(node_id)?;
            let report =
                verify_legacy_store_restore(&plan, &node_id, &native_snapshot, &data_directory)?;
            write_json(&report, output.as_deref())
        }
        Command::TelemetryPlan {
            legacy_data_directory,
            cluster_id,
            node_id,
            output,
        } => {
            let plan = LegacyTelemetryPlan::capture(&legacy_data_directory, cluster_id, node_id)?;
            write_json(&plan, output.as_deref())
        }
        Command::TelemetryApply {
            plan,
            legacy_data_directory,
            data_directory,
        } => {
            let plan = load_telemetry_plan(&plan)?;
            let report =
                apply_legacy_telemetry(&plan, &legacy_data_directory, &data_directory).await?;
            write_json(&report, None)
        }
        Command::TelemetryVerify {
            plan,
            legacy_data_directory,
            data_directory,
            output,
        } => {
            let plan = load_telemetry_plan(&plan)?;
            let verification =
                verify_legacy_telemetry(&plan, &legacy_data_directory, &data_directory).await?;
            write_json(&verification, output.as_deref())
        }
    }
}

async fn capture(
    connection: CutoverEtcdConnection,
    output: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut source = connection.legacy_source().await?;
    let captured = source.capture().await?;
    let artifact = captured.snapshot().encode_artifact()?;
    write_new_private(output, &artifact)?;
    write_json(&CaptureReport::new(&captured, output), None)
}

fn plan(
    snapshot_path: &Path,
    master_secret_path: &Path,
    output: Option<&Path>,
) -> Result<(), Box<dyn std::error::Error>> {
    let snapshot = load_snapshot(snapshot_path)?;
    let master_secret = read_master_secret(master_secret_path)?;
    let plan = plan_legacy_snapshot(&snapshot, master_secret.as_str())?;
    write_json(&plan.report(), output)
}

async fn apply(
    connection: CutoverEtcdConnection,
    snapshot_path: &Path,
    master_secret_path: &Path,
    migration_id: ResourceName,
) -> Result<(), Box<dyn std::error::Error>> {
    let snapshot = load_snapshot(snapshot_path)?;
    let master_secret = read_master_secret(master_secret_path)?;
    let mut source = connection.legacy_source().await?;
    let initial = verify_source(&mut source, &snapshot).await?;
    let plan = plan_legacy_snapshot(&snapshot, master_secret.as_str())?;
    let report = plan.report();
    let store: Arc<dyn Store> =
        Arc::new(connection.destination_store(master_secret.as_str()).await?);
    let migration = CutoverMigration::new(store, Keyspace::new(plan.cluster_id()), migration_id);
    let final_revision = Cell::new(initial.revision());
    let outcome = migration
        .apply_guarded(&plan, || async {
            let captured = verify_source(&mut source, &snapshot).await?;
            final_revision.set(captured.revision());
            Ok::<(), SourceMismatch>(())
        })
        .await?;
    let verification = migration.verify(&plan).await?;
    let legacy_auth_was_enabled = connection.disable_legacy_auth().await?;
    write_json(
        &ApplyReport {
            schema_version: 3,
            source_verified_at_revision: final_revision.get(),
            legacy_auth_was_enabled,
            plan: report,
            outcome,
            verification,
        },
        None,
    )
}

async fn verify(
    connection: CutoverEtcdConnection,
    snapshot_path: &Path,
    master_secret_path: &Path,
    migration_id: ResourceName,
    output: Option<&Path>,
) -> Result<(), Box<dyn std::error::Error>> {
    let snapshot = load_snapshot(snapshot_path)?;
    let master_secret = read_master_secret(master_secret_path)?;
    let plan = plan_legacy_snapshot(&snapshot, master_secret.as_str())?;
    let store: Arc<dyn Store> =
        Arc::new(connection.destination_store(master_secret.as_str()).await?);
    let migration = CutoverMigration::new(store, Keyspace::new(plan.cluster_id()), migration_id);
    let verification = migration.verify(&plan).await?;
    write_json(
        &VerificationReport {
            schema_version: 1,
            plan: plan.report(),
            verification,
        },
        output,
    )
}

fn load_snapshot(path: &Path) -> Result<LegacySnapshot, Box<dyn std::error::Error>> {
    Ok(LegacySnapshot::decode_artifact(&read_snapshot(path)?)?)
}

fn load_telemetry_plan(path: &Path) -> Result<LegacyTelemetryPlan, Box<dyn std::error::Error>> {
    Ok(serde_json::from_slice(&read_telemetry_plan(path)?)?)
}

fn parse_node_id(value: String) -> Result<kernel_api::NodeId, Box<dyn std::error::Error>> {
    Ok(kernel_api::NodeId::new(value)?)
}

async fn verify_source(
    source: &mut LegacyEtcdSource,
    expected: &LegacySnapshot,
) -> Result<CapturedLegacySnapshot, SourceMismatch> {
    let captured = source
        .capture()
        .await
        .map_err(|error| SourceMismatch::Read(error.to_string()))?;
    if captured.snapshot().digest() != expected.digest() {
        Err(SourceMismatch::Changed {
            expected: hex::encode(expected.digest()),
            actual: hex::encode(captured.snapshot().digest()),
        })
    } else {
        Ok(captured)
    }
}

fn write_json<Value: Serialize>(
    value: &Value,
    output: Option<&Path>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut encoded = serde_json::to_vec_pretty(value)?;
    encoded.push(b'\n');
    if let Some(output) = output {
        write_new_private(output, &encoded)?;
    } else {
        let mut stdout = std::io::stdout().lock();
        stdout.write_all(&encoded)?;
        stdout.flush()?;
    }
    Ok(())
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct CaptureReport<'a> {
    schema_version: u32,
    source_revision: i64,
    source_sha256: String,
    entries: usize,
    output: &'a Path,
}

impl<'a> CaptureReport<'a> {
    fn new(captured: &CapturedLegacySnapshot, output: &'a Path) -> Self {
        Self {
            schema_version: 1,
            source_revision: captured.revision(),
            source_sha256: hex::encode(captured.snapshot().digest()),
            entries: captured.snapshot().entries().len(),
            output,
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ApplyReport {
    schema_version: u32,
    source_verified_at_revision: i64,
    legacy_auth_was_enabled: bool,
    plan: MigrationPlanReport,
    outcome: MigrationOutcome,
    verification: MigrationVerification,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct VerificationReport {
    schema_version: u32,
    plan: MigrationPlanReport,
    verification: MigrationVerification,
}

#[derive(Debug, thiserror::Error)]
enum SourceMismatch {
    #[error("could not recapture the live legacy source: {0}")]
    Read(String),
    #[error("live legacy digest {actual} does not match reviewed artifact {expected}")]
    Changed { expected: String, actual: String },
}
