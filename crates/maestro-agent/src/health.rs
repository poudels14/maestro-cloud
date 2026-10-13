use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

use maestro_core::{ReplicaId, ReplicaPlan};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio::time::Instant;

use maestro_core::ReplicaPhase;

use crate::status::StatusBoard;

const TICK: Duration = Duration::from_secs(1);
const PROBE_TIMEOUT: Duration = Duration::from_secs(3);
const UNHEALTHY_AFTER_FAILURES: u32 = 3;

pub struct HealthMonitor {
    board: StatusBoard,
    plans: watch::Receiver<Vec<ReplicaPlan>>,
    next_due: HashMap<ReplicaId, Instant>,
}

struct ProbeResult {
    replica_id: ReplicaId,
    outcome: Result<u16, String>,
}

impl HealthMonitor {
    pub fn new(board: StatusBoard, plans: watch::Receiver<Vec<ReplicaPlan>>) -> Self {
        Self {
            board,
            plans,
            next_due: HashMap::new(),
        }
    }

    pub async fn run(mut self) {
        let mut probes = JoinSet::new();
        let mut ticker = tokio::time::interval(TICK);
        loop {
            tokio::select! {
                _ = ticker.tick() => self.schedule(&mut probes),
                Some(Ok(result)) = probes.join_next() => self.record(result),
            }
        }
    }

    fn schedule(&mut self, probes: &mut JoinSet<ProbeResult>) {
        let now = Instant::now();
        let plans = self.plans.borrow().clone();
        self.next_due
            .retain(|replica_id, _| plans.iter().any(|plan| &plan.replica_id == replica_id));
        plans.iter().for_each(|plan| {
            let status = self.board.get(&plan.replica_id);
            let target = plan.healthcheck.as_ref().zip(
                status
                    .filter(|status| status.phase.is_live())
                    .and_then(|status| status.ip),
            );
            let due = self
                .next_due
                .get(&plan.replica_id)
                .is_none_or(|due| *due <= now);
            if let (Some((healthcheck, ip)), true) = (target, due) {
                let interval = Duration::from_secs(u64::from(healthcheck.interval_secs.max(1)));
                self.next_due
                    .insert(plan.replica_id.clone(), now + interval);
                let replica_id = plan.replica_id.clone();
                let path = healthcheck.path.clone();
                let port = healthcheck.port;
                probes.spawn(async move {
                    ProbeResult {
                        replica_id,
                        outcome: probe(ip, port, &path, PROBE_TIMEOUT).await,
                    }
                });
            }
        });
    }

    fn record(&self, result: ProbeResult) {
        self.board.update(&result.replica_id, |status| {
            let still_live = status.phase.is_live();
            match (still_live, result.outcome) {
                (false, _) => {}
                (true, Ok(_)) => {
                    status.phase = ReplicaPhase::Healthy;
                    status.consecutive_failures = 0;
                    status.last_error = None;
                }
                (true, Err(error)) => {
                    status.consecutive_failures += 1;
                    status.last_error = Some(error);
                    if status.consecutive_failures >= UNHEALTHY_AFTER_FAILURES {
                        status.phase = ReplicaPhase::Unhealthy;
                    }
                }
            }
        });
    }
}

pub async fn probe(ip: IpAddr, port: u16, path: &str, timeout: Duration) -> Result<u16, String> {
    let request = async {
        let mut stream = TcpStream::connect(SocketAddr::new(ip, port))
            .await
            .map_err(|error| format!("connect failed: {error}"))?;
        let host = SocketAddr::new(ip, port);
        stream
            .write_all(
                format!(
                    "GET {path} HTTP/1.1\r\nHost: {host}\r\nUser-Agent: maestro-healthcheck\r\nConnection: close\r\n\r\n"
                )
                .as_bytes(),
            )
            .await
            .map_err(|error| format!("write failed: {error}"))?;
        let mut status_line = String::new();
        BufReader::new(stream)
            .read_line(&mut status_line)
            .await
            .map_err(|error| format!("read failed: {error}"))?;
        status_line
            .split_whitespace()
            .nth(1)
            .and_then(|code| code.parse::<u16>().ok())
            .ok_or_else(|| format!("invalid HTTP response: {}", status_line.trim()))
    };
    match tokio::time::timeout(timeout, request).await {
        Ok(Ok(code)) if (200..400).contains(&code) => Ok(code),
        Ok(Ok(code)) => Err(format!("unhealthy status {code}")),
        Ok(Err(error)) => Err(error),
        Err(_) => Err(format!("timed out after {}s", timeout.as_secs())),
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::AsyncReadExt;
    use tokio::net::TcpListener;

    use super::*;

    async fn serve_once(response: &'static str) -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buffer = [0; 512];
            let _ = socket.read(&mut buffer).await;
            let _ = socket.write_all(response.as_bytes()).await;
        });
        port
    }

    #[tokio::test]
    async fn accepts_success_and_redirect_codes() {
        let port = serve_once("HTTP/1.1 204 No Content\r\n\r\n").await;
        let localhost = "127.0.0.1".parse().unwrap();
        assert_eq!(
            probe(localhost, port, "/health", PROBE_TIMEOUT).await,
            Ok(204)
        );
        let port = serve_once("HTTP/1.1 302 Found\r\n\r\n").await;
        assert_eq!(
            probe(localhost, port, "/health", PROBE_TIMEOUT).await,
            Ok(302)
        );
    }

    #[tokio::test]
    async fn rejects_errors_and_refused_connections() {
        let localhost = "127.0.0.1".parse().unwrap();
        let port = serve_once("HTTP/1.1 503 Service Unavailable\r\n\r\n").await;
        assert_eq!(
            probe(localhost, port, "/health", PROBE_TIMEOUT).await,
            Err("unhealthy status 503".to_string())
        );
        let closed = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let closed_port = closed.local_addr().unwrap().port();
        drop(closed);
        assert!(
            probe(localhost, closed_port, "/", PROBE_TIMEOUT)
                .await
                .is_err()
        );
    }
}
