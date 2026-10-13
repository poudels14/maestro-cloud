use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::process::Stdio;

use serde_json::{Value, json};
use tokio::io::AsyncWriteExt;
use tokio::process::Command;

use crate::spec::RuntimeError;

const CNI_VERSION: &str = "1.0.0";
const INTERFACE_NAME: &str = "eth0";

#[derive(Clone, Debug)]
pub struct CniNetwork {
    name: String,
    plugins: Vec<Value>,
    plugin_dirs: Vec<PathBuf>,
    cache_dir: PathBuf,
}

#[derive(Clone, Copy)]
enum CniCommand {
    Add,
    Del,
}

impl CniNetwork {
    pub fn bridge(
        bridge_name: &str,
        subnet: &str,
        plugin_dirs: Vec<PathBuf>,
        state_dir: &Path,
    ) -> Self {
        let plugins = vec![
            json!({
                "type": "bridge",
                "bridge": bridge_name,
                "isGateway": true,
                "ipMasq": false,
                "hairpinMode": true,
                "ipam": {
                    "type": "host-local",
                    "ranges": [[{ "subnet": subnet }]],
                    "routes": [{ "dst": "0.0.0.0/0" }],
                    "dataDir": state_dir.join("ipam"),
                },
            }),
            json!({ "type": "firewall" }),
        ];
        Self {
            name: "maestro".to_string(),
            plugins,
            plugin_dirs,
            cache_dir: state_dir.join("results"),
        }
    }

    pub async fn add(
        &self,
        container_id: &str,
        netns_path: &str,
    ) -> Result<Option<IpAddr>, RuntimeError> {
        tokio::fs::create_dir_all(&self.cache_dir).await?;
        let mut previous = Value::Null;
        for plugin in &self.plugins {
            previous = self
                .invoke(CniCommand::Add, plugin, container_id, netns_path, &previous)
                .await?;
            tokio::fs::write(
                self.cache_path(container_id),
                serde_json::to_vec(&previous)?,
            )
            .await?;
        }
        Ok(first_ip(&previous))
    }

    pub async fn del(&self, container_id: &str, netns_path: &str) -> Result<(), RuntimeError> {
        match tokio::fs::read(self.cache_path(container_id)).await {
            Ok(bytes) => {
                let cached: Value = serde_json::from_slice(&bytes)?;
                for plugin in self.plugins.iter().rev() {
                    self.invoke(CniCommand::Del, plugin, container_id, netns_path, &cached)
                        .await?;
                }
                Ok(tokio::fs::remove_file(self.cache_path(container_id)).await?)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    async fn invoke(
        &self,
        command: CniCommand,
        plugin: &Value,
        container_id: &str,
        netns_path: &str,
        previous: &Value,
    ) -> Result<Value, RuntimeError> {
        let plugin_type = plugin["type"].as_str().unwrap_or_default();
        let binary = self.find_plugin(plugin_type)?;
        let mut config = plugin.clone();
        config["cniVersion"] = json!(CNI_VERSION);
        config["name"] = json!(self.name);
        if !previous.is_null() {
            config["prevResult"] = previous.clone();
        }
        let plugin_path = std::env::join_paths(&self.plugin_dirs)
            .map_err(|error| RuntimeError::Network(error.to_string()))?;
        let mut child = Command::new(&binary)
            .env("CNI_COMMAND", command.as_str())
            .env("CNI_CONTAINERID", container_id)
            .env("CNI_NETNS", netns_path)
            .env("CNI_IFNAME", INTERFACE_NAME)
            .env("CNI_PATH", plugin_path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        if let Some(mut stdin) = child.stdin.take() {
            stdin.write_all(&serde_json::to_vec(&config)?).await?;
        }
        let output = child.wait_with_output().await?;
        match (output.status.success(), command) {
            (true, CniCommand::Add) => Ok(serde_json::from_slice(&output.stdout)?),
            (true, CniCommand::Del) => Ok(previous.clone()),
            (false, _) => Err(RuntimeError::Network(format!(
                "{plugin_type} {} failed: {} {}",
                command.as_str(),
                String::from_utf8_lossy(&output.stdout).trim(),
                String::from_utf8_lossy(&output.stderr).trim(),
            ))),
        }
    }

    pub async fn cached_ip(&self, container_id: &str) -> Option<IpAddr> {
        let bytes = tokio::fs::read(self.cache_path(container_id)).await.ok()?;
        let cached: Value = serde_json::from_slice(&bytes).ok()?;
        first_ip(&cached)
    }

    fn find_plugin(&self, plugin_type: &str) -> Result<PathBuf, RuntimeError> {
        self.plugin_dirs
            .iter()
            .map(|directory| directory.join(plugin_type))
            .find(|candidate| candidate.is_file())
            .ok_or_else(|| {
                RuntimeError::Network(format!(
                    "CNI plugin {plugin_type} not found in {:?}",
                    self.plugin_dirs
                ))
            })
    }

    fn cache_path(&self, container_id: &str) -> PathBuf {
        self.cache_dir.join(format!("{container_id}.json"))
    }
}

impl CniCommand {
    fn as_str(self) -> &'static str {
        match self {
            Self::Add => "ADD",
            Self::Del => "DEL",
        }
    }
}

fn first_ip(result: &Value) -> Option<IpAddr> {
    result["ips"]
        .as_array()
        .and_then(|ips| ips.first())
        .and_then(|ip| ip["address"].as_str())
        .and_then(|address| address.split('/').next())
        .and_then(|address| address.parse().ok())
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt;

    use super::*;

    fn write_plugin(directory: &Path, name: &str, script: &str) {
        let path = directory.join(name);
        std::fs::write(&path, format!("#!/bin/sh\n{script}\n")).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    #[tokio::test]
    async fn chains_plugins_and_caches_result_for_delete() {
        let plugins = tempfile::tempdir().unwrap();
        let state = tempfile::tempdir().unwrap();
        let log = state.path().join("calls.log");
        write_plugin(
            plugins.path(),
            "bridge",
            &format!(
                r#"echo "bridge $CNI_COMMAND $CNI_NETNS" >> {log}
cat > /dev/null
[ "$CNI_COMMAND" = "ADD" ] && echo '{{"cniVersion":"1.0.0","ips":[{{"address":"10.200.1.7/24"}}]}}'
exit 0"#,
                log = log.display()
            ),
        );
        write_plugin(
            plugins.path(),
            "firewall",
            &format!(
                r#"input=$(cat)
echo "firewall $CNI_COMMAND" >> {log}
case "$input" in *'"prevResult"'*10.200.1.7*) ;; *) exit 1 ;; esac
[ "$CNI_COMMAND" = "ADD" ] && echo '{{"cniVersion":"1.0.0","ips":[{{"address":"10.200.1.7/24"}}]}}'
exit 0"#,
                log = log.display()
            ),
        );
        let network = CniNetwork::bridge(
            "maestro0",
            "10.200.1.0/24",
            vec![plugins.path().to_path_buf()],
            state.path(),
        );

        let ip = network.add("c1", "/proc/42/ns/net").await.unwrap();
        assert_eq!(ip, Some("10.200.1.7".parse().unwrap()));
        assert!(state.path().join("results/c1.json").exists());

        network.del("c1", "").await.unwrap();
        assert!(!state.path().join("results/c1.json").exists());
        let calls = std::fs::read_to_string(&log).unwrap();
        assert_eq!(
            calls.lines().collect::<Vec<_>>(),
            [
                "bridge ADD /proc/42/ns/net",
                "firewall ADD",
                "firewall DEL",
                "bridge DEL "
            ]
        );
    }

    #[tokio::test]
    async fn reports_missing_plugins() {
        let state = tempfile::tempdir().unwrap();
        let network = CniNetwork::bridge(
            "maestro0",
            "10.200.1.0/24",
            vec![state.path().join("none")],
            state.path(),
        );
        assert!(matches!(
            network.add("c1", "/proc/1/ns/net").await,
            Err(RuntimeError::Network(_))
        ));
    }
}
