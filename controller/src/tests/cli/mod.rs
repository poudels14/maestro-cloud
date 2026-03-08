use super::*;

#[test]
fn init_creates_file_and_refuses_overwrite() {
    use std::time::{SystemTime, UNIX_EPOCH};

    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time")
        .as_nanos();
    let path = std::env::temp_dir().join(format!(
        "maestro-init-test-{}-{unique}.jsonc",
        std::process::id()
    ));

    init_config(path.as_path()).expect("first create should work");
    let contents = std::fs::read_to_string(&path).expect("file should exist");
    assert!(contents.contains("\"services\""));
    assert!(contents.contains("\"service-1\""));

    let err = init_config(path.as_path()).expect_err("second create should fail");
    assert!(err.to_string().contains("already exists"));

    let _ = std::fs::remove_file(path);
}
