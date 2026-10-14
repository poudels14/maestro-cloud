use std::collections::HashMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use maestro_telemetry::{
    DeploymentFromReplicaId, LogDriverSettings, LogQuery, LogRecord, LogStore, ReplicaIdentity,
    Stream, Tailer, pump,
};

struct Harness {
    _root: tempfile::TempDir,
    log_dir: PathBuf,
    store: Arc<LogStore>,
    tailer: Tailer,
}

impl Harness {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let log_dir = root.path().join("logs");
        let data_dir = root.path().join("telemetry");
        std::fs::create_dir_all(&log_dir).unwrap();
        let store = Arc::new(LogStore::open(&data_dir).unwrap());
        let tailer = Tailer::new(
            log_dir.clone(),
            store.clone(),
            Arc::new(DeploymentFromReplicaId),
        )
        .unwrap();
        Self {
            _root: root,
            log_dir,
            store,
            tailer,
        }
    }

    fn restart_tailer(&mut self) {
        self.tailer = Tailer::new(
            self.log_dir.clone(),
            self.store.clone(),
            Arc::new(DeploymentFromReplicaId),
        )
        .unwrap();
    }

    fn current(&self, replica_id: &str) -> PathBuf {
        self.log_dir.join(replica_id).join("current.log")
    }

    async fn lines(&self) -> Vec<String> {
        let mut logs = self
            .store
            .read_logs(LogQuery {
                before_ts: None,
                after_ts: None,
                limit: 10_000,
                ..LogQuery::default()
            })
            .await
            .unwrap();
        logs.reverse();
        logs.into_iter().map(|log| log.line).collect()
    }
}

fn encode(lines: &[&str]) -> String {
    lines
        .iter()
        .map(|line| {
            let record = LogRecord {
                ts: 1_800_000_000_000,
                stream: Stream::Stdout,
                line: line.to_string(),
            };
            format!("{}\n", serde_json::to_string(&record).unwrap())
        })
        .collect()
}

fn append(path: &Path, contents: &str) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .unwrap()
        .write_all(contents.as_bytes())
        .unwrap();
}

fn rotate(directory: &Path, max_files: usize) {
    let rotated = |index: usize| directory.join(format!("current.log.{index}"));
    std::fs::remove_file(rotated(max_files)).ok();
    (1..max_files)
        .rev()
        .for_each(|index| drop(std::fs::rename(rotated(index), rotated(index + 1))));
    std::fs::rename(directory.join("current.log"), rotated(1)).unwrap();
}

#[tokio::test]
async fn ingests_existing_and_newly_discovered_container_dirs() {
    let mut harness = Harness::new();
    append(&harness.current("dep1-0"), &encode(&["a1", "a2"]));
    assert_eq!(harness.tailer.poll().unwrap(), 2);

    append(&harness.current("dep2-0"), &encode(&["b1"]));
    append(&harness.current("dep1-0"), &encode(&["a3"]));
    assert_eq!(harness.tailer.poll().unwrap(), 2);
    assert_eq!(harness.tailer.poll().unwrap(), 0);

    let mut lines = harness.lines().await;
    lines.sort();
    assert_eq!(lines, ["a1", "a2", "a3", "b1"]);
    let logs = harness
        .store
        .read_logs(LogQuery {
            before_ts: None,
            after_ts: None,
            search: Some("replica:dep2-0".parse().unwrap()),
            before: None,
            limit: 10,
        })
        .await
        .unwrap();
    assert_eq!(logs[0].deployment_id.as_deref(), Some("dep2"));
}

#[tokio::test]
async fn finishes_the_rotated_file_before_switching_to_the_new_one() {
    let mut harness = Harness::new();
    let directory = harness.log_dir.join("dep1-0");
    append(&harness.current("dep1-0"), &encode(&["one", "two"]));
    assert_eq!(harness.tailer.poll().unwrap(), 2);

    append(&harness.current("dep1-0"), &encode(&["three", "four"]));
    rotate(&directory, 4);
    append(&harness.current("dep1-0"), &encode(&["five"]));
    rotate(&directory, 4);
    append(&harness.current("dep1-0"), &encode(&["six"]));

    assert_eq!(harness.tailer.poll().unwrap(), 4);
    assert_eq!(
        harness.lines().await,
        ["one", "two", "three", "four", "five", "six"]
    );
    let newest_inode =
        std::os::unix::fs::MetadataExt::ino(&std::fs::metadata(harness.current("dep1-0")).unwrap());
    assert_eq!(
        harness.tailer.checkpoint("dep1-0").unwrap().inode,
        newest_inode
    );
}

#[tokio::test]
async fn waits_for_partial_lines_to_complete() {
    let mut harness = Harness::new();
    let encoded = encode(&["complete", "partial"]);
    let split = encoded.find("partial").unwrap();
    append(&harness.current("dep1-0"), &encoded[..split]);
    assert_eq!(harness.tailer.poll().unwrap(), 1);
    append(&harness.current("dep1-0"), &encoded[split..]);
    assert_eq!(harness.tailer.poll().unwrap(), 1);
    assert_eq!(harness.lines().await, ["complete", "partial"]);
}

#[tokio::test]
async fn restart_resumes_from_the_checkpoint_without_duplicates() {
    let mut harness = Harness::new();
    let directory = harness.log_dir.join("dep1-0");
    append(&harness.current("dep1-0"), &encode(&["one", "two"]));
    harness.tailer.poll().unwrap();
    append(&harness.current("dep1-0"), &encode(&["three"]));
    rotate(&directory, 4);
    append(&harness.current("dep1-0"), &encode(&["four"]));

    harness.restart_tailer();
    assert_eq!(harness.tailer.poll().unwrap(), 2);
    assert_eq!(harness.lines().await, ["one", "two", "three", "four"]);
}

#[tokio::test]
async fn reopening_the_store_restores_checkpoints() {
    let root = tempfile::tempdir().unwrap();
    let log_dir = root.path().join("logs");
    let data_dir = root.path().join("telemetry");
    let current = log_dir.join("dep1-0").join("current.log");
    append(&current, &encode(&["one", "two"]));
    {
        let store = Arc::new(LogStore::open(&data_dir).unwrap());
        let mut tailer =
            Tailer::new(log_dir.clone(), store, Arc::new(DeploymentFromReplicaId)).unwrap();
        assert_eq!(tailer.poll().unwrap(), 2);
    }
    append(&current, &encode(&["three"]));

    let store = Arc::new(LogStore::open(&data_dir).unwrap());
    let mut tailer = Tailer::new(
        log_dir.clone(),
        store.clone(),
        Arc::new(DeploymentFromReplicaId),
    )
    .unwrap();
    assert_eq!(tailer.poll().unwrap(), 1);
    let logs = store
        .read_logs(LogQuery {
            before_ts: None,
            after_ts: None,
            limit: 10,
            ..LogQuery::default()
        })
        .await
        .unwrap();
    assert_eq!(
        logs.iter().map(|log| log.line.as_str()).collect::<Vec<_>>(),
        ["three", "two", "one"]
    );
}

#[tokio::test]
async fn resumes_at_the_oldest_file_when_the_checkpoint_rotated_away() {
    let mut harness = Harness::new();
    let directory = harness.log_dir.join("dep1-0");
    append(&harness.current("dep1-0"), &encode(&["lost-1"]));
    harness.tailer.poll().unwrap();
    append(&harness.current("dep1-0"), &encode(&["lost-2"]));
    rotate(&directory, 1);
    append(&harness.current("dep1-0"), &encode(&["kept-1"]));
    rotate(&directory, 1);
    append(&harness.current("dep1-0"), &encode(&["kept-2"]));

    assert_eq!(harness.tailer.poll().unwrap(), 2);
    assert_eq!(harness.lines().await, ["lost-1", "kept-1", "kept-2"]);
}

#[tokio::test]
async fn rereads_a_truncated_file_from_the_start() {
    let mut harness = Harness::new();
    append(&harness.current("dep1-0"), &encode(&["first", "second"]));
    harness.tailer.poll().unwrap();
    std::fs::OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(harness.current("dep1-0"))
        .unwrap();
    append(&harness.current("dep1-0"), &encode(&["x"]));

    assert_eq!(harness.tailer.poll().unwrap(), 1);
    assert_eq!(harness.lines().await, ["first", "second", "x"]);
}

#[tokio::test]
async fn skips_malformed_records_and_keeps_going() {
    let mut harness = Harness::new();
    let contents = format!("{}not json\n{}", encode(&["before"]), encode(&["after"]));
    append(&harness.current("dep1-0"), &contents);
    assert_eq!(harness.tailer.poll().unwrap(), 2);
    assert_eq!(harness.lines().await, ["before", "after"]);
}

#[tokio::test]
async fn resolves_service_and_deployment_identity() {
    let root = tempfile::tempdir().unwrap();
    let log_dir = root.path().join("logs");
    let store = Arc::new(LogStore::open(&root.path().join("telemetry")).unwrap());
    let resolver = HashMap::from([(
        "dep1-0".to_string(),
        ReplicaIdentity {
            service_id: Some("api".to_string()),
            deployment_id: Some("dep1".to_string()),
        },
    )]);
    let mut tailer = Tailer::new(log_dir.clone(), store.clone(), Arc::new(resolver)).unwrap();
    append(
        &log_dir.join("dep1-0").join("current.log"),
        &encode(&["hi"]),
    );
    append(&log_dir.join("other").join("current.log"), &encode(&["yo"]));
    tailer.poll().unwrap();

    let api = store
        .read_logs(LogQuery {
            before_ts: None,
            after_ts: None,
            search: Some("service:api".parse().unwrap()),
            before: None,
            limit: 10,
        })
        .await
        .unwrap();
    assert_eq!(api.len(), 1);
    assert_eq!(api[0].line, "hi");
    let unresolved = store
        .read_logs(LogQuery {
            before_ts: None,
            after_ts: None,
            search: Some("replica:other".parse().unwrap()),
            before: None,
            limit: 10,
        })
        .await
        .unwrap();
    assert_eq!(unresolved[0].service_id, None);
    assert_eq!(unresolved[0].deployment_id, None);
}

#[tokio::test]
async fn forgets_checkpoints_of_removed_dirs() {
    let mut harness = Harness::new();
    append(&harness.current("dep1-0"), &encode(&["one"]));
    harness.tailer.poll().unwrap();
    assert!(harness.tailer.checkpoint("dep1-0").is_some());
    std::fs::remove_dir_all(harness.log_dir.join("dep1-0")).unwrap();
    harness.tailer.poll().unwrap();
    assert!(harness.tailer.checkpoint("dep1-0").is_none());
    assert!(harness.store.checkpoints().unwrap().is_empty());
}

#[tokio::test]
async fn ingests_everything_the_log_driver_writes_across_rotations() {
    let mut harness = Harness::new();
    let input = (0..40)
        .map(|index| format!("line number {index:02}\n"))
        .collect::<String>();
    pump(
        "dep1-0",
        std::io::Cursor::new(input.into_bytes()),
        tokio::io::empty(),
        LogDriverSettings {
            log_dir: harness.log_dir.clone(),
            max_file_bytes: 200,
            max_files: 64,
        },
    )
    .await
    .unwrap();
    assert!(
        harness
            .log_dir
            .join("dep1-0")
            .join("current.log.3")
            .exists()
    );

    assert_eq!(harness.tailer.poll().unwrap(), 40);
    let expected = (0..40)
        .map(|index| format!("line number {index:02}"))
        .collect::<Vec<_>>();
    assert_eq!(harness.lines().await, expected);
}

#[tokio::test]
async fn interleaved_writes_and_rotations_never_lose_or_duplicate_lines() {
    let mut harness = Harness::new();
    let directory = harness.log_dir.join("dep1-0");
    let mut expected = Vec::new();
    (0..30).for_each(|round| {
        let line = format!("round {round}");
        append(&harness.current("dep1-0"), &encode(&[&line]));
        expected.push(line);
        if round % 3 == 0 {
            rotate(&directory, 8);
        }
        if round % 4 == 0 {
            harness.tailer.poll().unwrap();
        }
        if round % 10 == 0 {
            harness.restart_tailer();
        }
    });
    harness.tailer.poll().unwrap();
    assert_eq!(harness.lines().await, expected);
}
