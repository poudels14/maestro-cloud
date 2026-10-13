use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use maestro_telemetry::{
    DeploymentFromReplicaId, LogRecord, LogStore, ReplicaIdentity, ReplicaResolver, Stream, Tailer,
};

const IDLE_AFTER: Duration = Duration::from_secs(3600);

struct Harness {
    _root: tempfile::TempDir,
    log_dir: PathBuf,
    store: Arc<LogStore>,
    tailer: Tailer,
}

struct LiveReplicas(Vec<String>);

impl ReplicaResolver for LiveReplicas {
    fn resolve(&self, replica_id: &str) -> ReplicaIdentity {
        DeploymentFromReplicaId.resolve(replica_id)
    }

    fn is_live(&self, replica_id: &str) -> bool {
        self.0.iter().any(|live| live == replica_id)
    }
}

impl Harness {
    fn new() -> Self {
        Self::with_resolver(Arc::new(DeploymentFromReplicaId))
    }

    fn with_resolver(resolver: Arc<dyn ReplicaResolver>) -> Self {
        let root = tempfile::tempdir().unwrap();
        let log_dir = root.path().join("logs");
        let store = Arc::new(LogStore::open(&root.path().join("telemetry")).unwrap());
        let tailer = Tailer::new(log_dir.clone(), store.clone(), resolver).unwrap();
        Self {
            _root: root,
            log_dir,
            store,
            tailer,
        }
    }

    fn container(&self, replica_id: &str) -> PathBuf {
        self.log_dir.join(replica_id)
    }
}

fn write_lines(directory: &Path, name: &str, lines: &[&str]) {
    std::fs::create_dir_all(directory).unwrap();
    let contents = lines
        .iter()
        .map(|line| {
            let record = LogRecord {
                ts: 1_800_000_000_000,
                stream: Stream::Stdout,
                line: line.to_string(),
            };
            format!("{}\n", serde_json::to_string(&record).unwrap())
        })
        .collect::<String>();
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(directory.join(name))
        .unwrap()
        .write_all(contents.as_bytes())
        .unwrap();
}

fn age(directory: &Path, by: Duration) {
    let past = SystemTime::now() - by;
    std::fs::read_dir(directory)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .chain(std::iter::once(directory.to_path_buf()))
        .for_each(|path| {
            std::fs::File::open(&path)
                .unwrap()
                .set_modified(past)
                .unwrap()
        });
}

#[test]
fn removes_idle_fully_ingested_dirs_and_forgets_their_checkpoints() {
    let mut harness = Harness::new();
    let directory = harness.container("dep1-0");
    write_lines(&directory, "current.log.1", &["old"]);
    write_lines(&directory, "current.log", &["new"]);
    assert_eq!(harness.tailer.poll().unwrap(), 2);
    age(&directory, IDLE_AFTER * 2);

    assert_eq!(harness.tailer.sweep_idle(IDLE_AFTER).unwrap(), ["dep1-0"]);
    assert!(!directory.exists());
    assert!(harness.tailer.checkpoint("dep1-0").is_none());
    assert!(harness.store.checkpoints().unwrap().is_empty());
}

#[test]
fn keeps_dirs_with_recent_writes() {
    let mut harness = Harness::new();
    let directory = harness.container("dep1-0");
    write_lines(&directory, "current.log", &["fresh"]);
    harness.tailer.poll().unwrap();

    assert!(harness.tailer.sweep_idle(IDLE_AFTER).unwrap().is_empty());
    assert!(directory.exists());
}

#[test]
fn keeps_idle_dirs_that_are_not_fully_ingested() {
    let mut harness = Harness::new();
    let partially_read = harness.container("dep1-0");
    write_lines(&partially_read, "current.log", &["read"]);
    harness.tailer.poll().unwrap();
    write_lines(&partially_read, "current.log", &["unread"]);
    age(&partially_read, IDLE_AFTER * 2);
    let never_read = harness.container("dep2-0");
    write_lines(&never_read, "current.log", &["unread"]);
    age(&never_read, IDLE_AFTER * 2);

    assert!(harness.tailer.sweep_idle(IDLE_AFTER).unwrap().is_empty());
    assert!(partially_read.exists());
    assert!(never_read.exists());
}

#[test]
fn removes_idle_dirs_whose_only_unread_bytes_are_a_partial_line() {
    let mut harness = Harness::new();
    let directory = harness.container("dep1-0");
    write_lines(&directory, "current.log", &["complete"]);
    std::fs::OpenOptions::new()
        .append(true)
        .open(directory.join("current.log"))
        .unwrap()
        .write_all(b"{\"ts\":1,")
        .unwrap();
    harness.tailer.poll().unwrap();
    age(&directory, IDLE_AFTER * 2);

    assert_eq!(harness.tailer.sweep_idle(IDLE_AFTER).unwrap(), ["dep1-0"]);
}

#[test]
fn removes_idle_empty_dirs() {
    let mut harness = Harness::new();
    let directory = harness.container("dep1-0");
    std::fs::create_dir_all(&directory).unwrap();
    age(&directory, IDLE_AFTER * 2);

    assert_eq!(harness.tailer.sweep_idle(IDLE_AFTER).unwrap(), ["dep1-0"]);
}

#[test]
fn keeps_dirs_of_live_replicas_even_when_idle() {
    let mut harness = Harness::with_resolver(Arc::new(LiveReplicas(vec!["dep1-0".to_string()])));
    let live = harness.container("dep1-0");
    let gone = harness.container("dep0-0");
    write_lines(&live, "current.log", &["quiet"]);
    write_lines(&gone, "current.log", &["done"]);
    harness.tailer.poll().unwrap();
    age(&live, IDLE_AFTER * 2);
    age(&gone, IDLE_AFTER * 2);

    assert_eq!(harness.tailer.sweep_idle(IDLE_AFTER).unwrap(), ["dep0-0"]);
    assert!(live.exists());
}
