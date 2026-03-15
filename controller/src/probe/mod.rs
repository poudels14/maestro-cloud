mod healthcheck;

use std::time::Duration;

use anyhow::Result;
use tokio::signal::unix::{SignalKind, signal};
use tokio::time::sleep;

use crate::deployment::etcd::EtcdStateStore;

const POLL_INTERVAL: Duration = Duration::from_secs(2);
const HEALTH_TIMEOUT: Duration = Duration::from_secs(5);

pub async fn run(etcd_endpoint: &str) -> Result<()> {
    eprintln!("[probe]: starting, etcd={etcd_endpoint}");

    let store = EtcdStateStore::new(etcd_endpoint).await?;
    let http_client = reqwest::Client::builder().timeout(HEALTH_TIMEOUT).build()?;

    let mut sigterm = signal(SignalKind::terminate())?;
    let mut sigint = signal(SignalKind::interrupt())?;

    loop {
        let poll = async {
            sleep(POLL_INTERVAL).await;
            if let Err(err) = healthcheck::check_deployments(&store, &http_client).await {
                eprintln!("[probe]: poll error: {err}");
            }
        };

        tokio::select! {
            _ = sigterm.recv() => {
                eprintln!("[probe]: received SIGTERM, shutting down");
                break;
            }
            _ = sigint.recv() => {
                eprintln!("[probe]: received SIGINT, shutting down");
                break;
            }
            _ = poll => {}
        }
    }

    Ok(())
}
