mod cancel;
mod rollout;

use std::{io::Write, path::Path};

const DEFAULT_CONFIG_TEMPLATE: &str = include_str!("../default.jsonc");

pub async fn run_rollout(config_path: &Path, host: &str) -> Result<(), String> {
    rollout::run_rollout(config_path, host).await
}

pub async fn run_cancel(host: &str, service_id: &str, deployment_id: &str) -> Result<(), String> {
    cancel::run_cancel(host, service_id, deployment_id).await
}

pub fn run_init(config_path: &Path) -> Result<(), String> {
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(config_path)
        .map_err(|err| {
            if err.kind() == std::io::ErrorKind::AlreadyExists {
                format!("{} already exists", config_path.display())
            } else {
                format!("failed to create {}: {err}", config_path.display())
            }
        })?;

    file.write_all(DEFAULT_CONFIG_TEMPLATE.as_bytes())
        .map_err(|err| format!("failed to write {}: {err}", config_path.display()))?;

    println!("created {}", config_path.display());
    Ok(())
}

#[cfg(test)]
mod tests {
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

        run_init(path.as_path()).expect("first create should work");
        let contents = std::fs::read_to_string(&path).expect("file should exist");
        assert!(contents.contains("\"services\""));
        assert!(contents.contains("\"service-1\""));

        let err = run_init(path.as_path()).expect_err("second create should fail");
        assert!(err.contains("already exists"));

        let _ = std::fs::remove_file(path);
    }
}
