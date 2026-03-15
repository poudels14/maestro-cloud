mod healthcheck;
mod types;

use std::time::Duration;

use anyhow::Result;
use etcd_client::Client as EtcdClient;
use tokio::signal::unix::{SignalKind, signal};
use tokio::time::sleep;

const POLL_INTERVAL: Duration = Duration::from_secs(2);
const HEALTH_TIMEOUT: Duration = Duration::from_secs(5);

#[tokio::main]
async fn main() -> Result<()> {
    let etcd_endpoint =
        std::env::var("ETCD_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:6479".to_string());
    eprintln!("[sidecar]: starting, etcd={etcd_endpoint}");

    let mut client = EtcdClient::connect([&etcd_endpoint], None).await?;
    let http_client = reqwest::Client::builder().timeout(HEALTH_TIMEOUT).build()?;

    let mut sigterm = signal(SignalKind::terminate())?;
    let mut sigint = signal(SignalKind::interrupt())?;

    loop {
        let poll = async {
            sleep(POLL_INTERVAL).await;
            if let Err(err) = healthcheck::check_deployments(&mut client, &http_client).await {
                eprintln!("[sidecar]: poll error: {err}");
            }
        };

        tokio::select! {
            _ = sigterm.recv() => {
                eprintln!("[sidecar]: received SIGTERM, shutting down");
                break;
            }
            _ = sigint.recv() => {
                eprintln!("[sidecar]: received SIGINT, shutting down");
                break;
            }
            _ = poll => {}
        }
    }

    Ok(())
}
