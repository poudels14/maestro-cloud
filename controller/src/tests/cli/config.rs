use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use super::*;

fn temp_path(label: &str, ext: &str) -> PathBuf {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "maestro-{label}-{}-{unique}.{ext}",
        std::process::id()
    ))
}

#[test]
fn write_template_refuses_overwrite() {
    let path = temp_path("write-overwrite", "jsonc");
    write_template(&path, DEFAULT_CLUSTER_TEMPLATE).expect("first create should work");
    let err =
        write_template(&path, DEFAULT_CLUSTER_TEMPLATE).expect_err("second create should fail");
    assert!(err.to_string().contains("already exists"));
    let _ = std::fs::remove_file(path);
}

#[test]
fn cluster_template_validates_as_cluster_config() {
    let path = temp_path("validate-cluster", "jsonc");
    write_template(&path, DEFAULT_START_TEMPLATE).expect("write");
    run_validate(&path).expect("start template should validate as cluster config");
    let _ = std::fs::remove_file(path);
}

#[test]
fn services_template_validates_as_services_config() {
    let path = temp_path("validate-services", "jsonc");
    write_template(&path, DEFAULT_CLUSTER_TEMPLATE).expect("write");
    run_validate(&path).expect("cluster template should validate as services config");
    let _ = std::fs::remove_file(path);
}

#[test]
fn validate_rejects_unrecognized_config() {
    let path = temp_path("validate-unknown", "jsonc");
    std::fs::write(&path, r#"{"foo":1}"#).expect("write");
    let err = run_validate(&path).expect_err("unknown config should fail");
    assert!(err.to_string().contains("unrecognized config"));
    let _ = std::fs::remove_file(path);
}
