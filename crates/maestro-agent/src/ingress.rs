use std::path::{Path, PathBuf};

use maestro_core::{RouteTable, ServiceId};
use serde_json::{Map, Value, json};

const CONFIG_FILE: &str = "routes.yaml";
const RETRY_MIDDLEWARE: &str = "maestro-retry";
const SERVERS_TRANSPORT: &str = "maestro-transport";
const RETRY_ATTEMPTS: u32 = 3;
const RETRY_INTERVAL: &str = "50ms";
const DIAL_TIMEOUT: &str = "1s";
const HEALTH_INTERVAL: &str = "2s";
const HEALTH_TIMEOUT: &str = "1s";

pub struct IngressWriter {
    directory: PathBuf,
    entry_points: Vec<String>,
    last_written: Option<Value>,
}

impl IngressWriter {
    pub fn new(directory: PathBuf, ports: &[u16]) -> Self {
        Self {
            directory,
            entry_points: ports.iter().map(|port| entry_point_name(*port)).collect(),
            last_written: None,
        }
    }

    pub fn directory(&self) -> &Path {
        &self.directory
    }

    pub async fn write(&mut self, table: &RouteTable) -> std::io::Result<bool> {
        let config = render(table, &self.entry_points);
        match self.last_written.as_ref() == Some(&config) {
            true => Ok(false),
            false => {
                tokio::fs::create_dir_all(&self.directory).await?;
                let temporary = self.directory.join(format!(".{CONFIG_FILE}.tmp"));
                tokio::fs::write(&temporary, serde_json::to_vec_pretty(&config)?).await?;
                tokio::fs::rename(&temporary, self.directory.join(CONFIG_FILE)).await?;
                self.last_written = Some(config);
                Ok(true)
            }
        }
    }
}

pub fn entry_point_name(port: u16) -> String {
    format!("web{port}")
}

fn render(table: &RouteTable, entry_points: &[String]) -> Value {
    let routable = table
        .routes
        .iter()
        .filter(|(_, route)| !route.hosts.is_empty() && !route.backends.is_empty())
        .collect::<Vec<_>>();
    let routers = routable
        .iter()
        .map(|(service_id, route)| {
            let rule = route
                .hosts
                .iter()
                .map(|host| format!("Host(`{host}`)"))
                .collect::<Vec<_>>()
                .join(" || ");
            let router = json!({
                "rule": rule,
                "service": service_name(service_id),
                "entryPoints": entry_points,
                "middlewares": [RETRY_MIDDLEWARE],
            });
            (service_name(service_id), router)
        })
        .collect::<Map<_, _>>();
    let services = routable
        .iter()
        .map(|(service_id, route)| {
            let servers = route
                .backends
                .iter()
                .map(|backend| json!({ "url": format!("http://{}", backend.address) }))
                .collect::<Vec<_>>();
            let mut load_balancer = json!({
                "servers": servers,
                "passHostHeader": true,
                "serversTransport": SERVERS_TRANSPORT,
            });
            if let Some(path) = &route.health_path {
                load_balancer["healthCheck"] = json!({
                    "path": path,
                    "interval": HEALTH_INTERVAL,
                    "timeout": HEALTH_TIMEOUT,
                });
            }
            (
                service_name(service_id),
                json!({ "loadBalancer": load_balancer }),
            )
        })
        .collect::<Map<_, _>>();
    match routers.is_empty() {
        true => json!({}),
        false => json!({
            "http": {
                "routers": routers,
                "services": services,
                "middlewares": {
                    RETRY_MIDDLEWARE: { "retry": { "attempts": RETRY_ATTEMPTS, "initialInterval": RETRY_INTERVAL } },
                },
                "serversTransports": {
                    SERVERS_TRANSPORT: { "forwardingTimeouts": { "dialTimeout": DIAL_TIMEOUT } },
                },
            }
        }),
    }
}

fn service_name(service_id: &ServiceId) -> String {
    format!("svc-{service_id}")
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use maestro_core::{Backend, ReplicaId, ServiceRoute};

    use super::*;

    fn table() -> RouteTable {
        RouteTable {
            routes: BTreeMap::from([
                (
                    ServiceId::from("web"),
                    ServiceRoute {
                        health_path: Some("/health".to_string()),
                        hosts: vec!["web.example.com".to_string(), "www.example.com".to_string()],
                        backends: vec![
                            Backend {
                                replica_id: ReplicaId::from("d1-0"),
                                address: "10.200.1.2:8080".parse().unwrap(),
                            },
                            Backend {
                                replica_id: ReplicaId::from("d1-1"),
                                address: "10.200.1.3:8080".parse().unwrap(),
                            },
                        ],
                    },
                ),
                (
                    ServiceId::from("idle"),
                    ServiceRoute {
                        health_path: None,
                        hosts: vec!["idle.example.com".to_string()],
                        backends: vec![],
                    },
                ),
            ]),
        }
    }

    #[test]
    fn renders_routers_and_load_balancers() {
        let config = render(&table(), &["web80".to_string()]);
        let router = &config["http"]["routers"]["svc-web"];
        assert_eq!(
            router["rule"],
            "Host(`web.example.com`) || Host(`www.example.com`)"
        );
        assert_eq!(router["entryPoints"], json!(["web80"]));
        assert_eq!(
            config["http"]["services"]["svc-web"]["loadBalancer"]["servers"],
            json!([{ "url": "http://10.200.1.2:8080" }, { "url": "http://10.200.1.3:8080" }])
        );
        assert!(config["http"]["routers"].get("svc-idle").is_none());
        assert_eq!(router["middlewares"], json!(["maestro-retry"]));
        let load_balancer = &config["http"]["services"]["svc-web"]["loadBalancer"];
        assert_eq!(load_balancer["healthCheck"]["path"], "/health");
        assert_eq!(load_balancer["serversTransport"], "maestro-transport");
        assert_eq!(
            config["http"]["serversTransports"]["maestro-transport"]["forwardingTimeouts"]["dialTimeout"],
            "1s"
        );
    }

    #[tokio::test]
    async fn writes_atomically_and_skips_unchanged_tables() {
        let directory = tempfile::tempdir().unwrap();
        let mut writer = IngressWriter::new(directory.path().to_path_buf(), &[80]);
        assert!(writer.write(&table()).await.unwrap());
        assert!(!writer.write(&table()).await.unwrap());
        let written: Value =
            serde_json::from_slice(&std::fs::read(directory.path().join(CONFIG_FILE)).unwrap())
                .unwrap();
        assert!(written["http"]["routers"]["svc-web"].is_object());
        assert!(writer.write(&RouteTable::default()).await.unwrap());
    }
}
