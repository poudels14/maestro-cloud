use super::*;

#[test]
fn init_creates_file_and_refuses_overwrite() {
    use std::time::{SystemTime, UNIX_EPOCH};

    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time")
        .as_nanos();
    let start_path = std::env::temp_dir().join(format!(
        "maestro-init-start-{}-{unique}.jsonc",
        std::process::id()
    ));
    let cluster_path = std::env::temp_dir().join(format!(
        "maestro-init-cluster-{}-{unique}.jsonc",
        std::process::id()
    ));

    init_config(&start_path, &cluster_path).expect("first create should work");
    let cluster_contents =
        std::fs::read_to_string(&cluster_path).expect("cluster file should exist");
    assert!(cluster_contents.contains("\"services\""));
    assert!(cluster_contents.contains("\"service-1\""));
    let start_contents = std::fs::read_to_string(&start_path).expect("start file should exist");
    assert!(start_contents.contains("encryption-key"));

    let err = init_config(&start_path, &cluster_path).expect_err("second create should fail");
    assert!(err.to_string().contains("already exists"));

    let _ = std::fs::remove_file(start_path);
    let _ = std::fs::remove_file(cluster_path);
}
