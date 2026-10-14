use std::path::{Path, PathBuf};

use maestro_core::{Backend, RouteTable, ServiceId, ServiceRoute, affinity_token};
use serde_json::{Map, Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const CONFIG_FILE: &str = "routes.yaml";
const RETRY_MIDDLEWARE: &str = "maestro-retry";
const SERVERS_TRANSPORT: &str = "maestro-transport";
const RETRY_ATTEMPTS: u32 = 3;
const RETRY_INTERVAL: &str = "50ms";
const DIAL_TIMEOUT: &str = "1s";
const HEALTH_INTERVAL: &str = "2s";
const HEALTH_TIMEOUT: &str = "1s";
const AFFINITY_COOKIE: &str = "maestro-affinity";
const BLOCKED_ROUTER_PREFIX: &str = "maestro-blocked-";
const BLOCKED_IPS_PER_ROUTER: usize = 256;
const BLOCKED_PRIORITY: u32 = 10_000;
const DENIED_SERVICE: &str = "maestro-denied";
const DENIED_ADDRESS: &str = "127.0.0.1:7480";
const DENIED_RESPONSE: &[u8] = b"HTTP/1.1 403 Forbidden\r\nContent-Type: text/plain\r\nContent-Length: 10\r\nConnection: close\r\n\r\nForbidden\n";

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

pub async fn serve_denied() -> std::io::Result<()> {
    let listener = tokio::net::TcpListener::bind(DENIED_ADDRESS).await?;
    loop {
        let (mut connection, _) = listener.accept().await?;
        tokio::spawn(async move {
            let mut request = [0u8; 4096];
            if connection.read(&mut request).await.is_ok() {
                let _ = connection.write_all(DENIED_RESPONSE).await;
            }
        });
    }
}

fn render(table: &RouteTable, entry_points: &[String]) -> Value {
    let mut routers = Map::new();
    let mut services = Map::new();
    table
        .routes
        .iter()
        .filter(|(_, route)| !route.hosts.is_empty() && !route.backends.is_empty())
        .for_each(|(service_id, route)| {
            let name = service_name(service_id);
            let host_rule = route
                .hosts
                .iter()
                .map(|host| format!("Host(`{host}`)"))
                .collect::<Vec<_>>()
                .join(" || ");
            routers.insert(name.clone(), router(&host_rule, &name, entry_points));
            services.insert(
                name.clone(),
                load_balancer(route, &route.backends, route.affinity_header.is_some()),
            );
            if let Some(header) = &route.affinity_header {
                route.backends.iter().for_each(|backend| {
                    let replica_name = format!("{name}-{}", backend.replica_id);
                    let token = affinity_token(&backend.replica_id);
                    let rule = format!("({host_rule}) && Header(`{header}`, `{token}`)");
                    routers.insert(
                        replica_name.clone(),
                        router(&rule, &replica_name, entry_points),
                    );
                    services.insert(
                        replica_name,
                        load_balancer(route, std::slice::from_ref(backend), false),
                    );
                });
            }
        });
    table
        .blocked_ips
        .chunks(BLOCKED_IPS_PER_ROUTER)
        .enumerate()
        .for_each(|(index, chunk)| {
            routers.insert(
                format!("{BLOCKED_ROUTER_PREFIX}{index}"),
                json!({
                    "rule": blocked_ip_rule(chunk),
                    "service": DENIED_SERVICE,
                    "entryPoints": entry_points,
                    "priority": BLOCKED_PRIORITY,
                }),
            );
            services.insert(
                DENIED_SERVICE.to_string(),
                json!({ "loadBalancer": { "servers": [{ "url": format!("http://{DENIED_ADDRESS}") }] } }),
            );
        });
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

fn router(rule: &str, service: &str, entry_points: &[String]) -> Value {
    json!({
        "rule": rule,
        "service": service,
        "entryPoints": entry_points,
        "middlewares": [RETRY_MIDDLEWARE],
    })
}

fn load_balancer(route: &ServiceRoute, backends: &[Backend], sticky: bool) -> Value {
    let servers = backends
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
    if sticky {
        load_balancer["sticky"] =
            json!({ "cookie": { "name": AFFINITY_COOKIE, "httpOnly": true } });
    }
    json!({ "loadBalancer": load_balancer })
}

fn blocked_ip_rule(blocked_ips: &[String]) -> String {
    let direct = blocked_ips
        .iter()
        .map(|ip| format!("ClientIP(`{ip}`)"))
        .collect::<Vec<_>>()
        .join(" || ");
    let alternatives = blocked_ips
        .iter()
        .map(|ip| ip.replace('.', r"\."))
        .collect::<Vec<_>>()
        .join("|");
    format!(
        "{direct} || HeaderRegexp(`CF-Connecting-IP`, `^({alternatives})$`) || HeaderRegexp(`X-Real-IP`, `^({alternatives})$`) || HeaderRegexp(`X-Forwarded-For`, `(^[[:space:]]*|,[[:space:]]*)({alternatives})([[:space:]]*,|$)`)"
    )
}

fn service_name(service_id: &ServiceId) -> String {
    format!("svc-{service_id}")
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use maestro_core::ReplicaId;

    use super::*;

    fn table() -> RouteTable {
        RouteTable {
            routes: BTreeMap::from([
                (
                    ServiceId::from("web"),
                    ServiceRoute {
                        health_path: Some("/health".to_string()),
                        affinity_header: None,
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
                        affinity_header: None,
                        hosts: vec!["idle.example.com".to_string()],
                        backends: vec![],
                    },
                ),
            ]),
            blocked_ips: Vec::new(),
        }
    }

    #[test]
    fn routes_affinity_headers_to_their_replica() {
        let mut table = table();
        table
            .routes
            .get_mut(&ServiceId::from("web"))
            .unwrap()
            .affinity_header = Some("X-Session-Affinity".to_string());
        let config = render(&table, &["web80".to_string()]);
        assert_eq!(
            config["http"]["services"]["svc-web"]["loadBalancer"]["sticky"]["cookie"]["name"],
            "maestro-affinity"
        );
        let token = affinity_token(&ReplicaId::from("d1-1"));
        let router = &config["http"]["routers"]["svc-web-d1-1"];
        assert_eq!(
            router["rule"],
            format!(
                "(Host(`web.example.com`) || Host(`www.example.com`)) && Header(`X-Session-Affinity`, `{token}`)"
            )
        );
        assert_eq!(
            config["http"]["services"]["svc-web-d1-1"]["loadBalancer"]["servers"],
            json!([{ "url": "http://10.200.1.3:8080" }])
        );
    }

    #[test]
    fn sends_blocked_ips_to_the_denied_responder() {
        let table = RouteTable {
            blocked_ips: vec!["192.0.2.1".to_string(), "2001:db8::9".to_string()],
            ..RouteTable::default()
        };
        let config = render(&table, &["web80".to_string()]);
        let router = &config["http"]["routers"]["maestro-blocked-0"];
        let rule = router["rule"].as_str().unwrap();
        assert!(rule.starts_with("ClientIP(`192.0.2.1`) || ClientIP(`2001:db8::9`)"));
        assert!(rule.contains(r"HeaderRegexp(`X-Real-IP`, `^(192\.0\.2\.1|2001:db8::9)$`)"));
        assert_eq!(router["priority"], 10_000);
        assert_eq!(
            config["http"]["services"]["maestro-denied"]["loadBalancer"]["servers"][0]["url"],
            "http://127.0.0.1:7480"
        );
    }

    #[tokio::test]
    async fn denied_responder_returns_forbidden() {
        tokio::spawn(serve_denied());
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let mut stream = tokio::net::TcpStream::connect(DENIED_ADDRESS)
            .await
            .unwrap();
        stream
            .write_all(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n")
            .await
            .unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();
        assert!(response.starts_with("HTTP/1.1 403 Forbidden"));
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
