use anyhow::{Result, anyhow};
use etcd_client::{Client as EtcdClient, Compare, CompareOp, GetOptions, Txn, TxnOp};

use crate::types::{ServiceDeployment, ServiceInfo};

const SERVICES_PREFIX: &str = "/maetro/services/";

pub async fn check_deployments(client: &mut EtcdClient, http: &reqwest::Client) -> Result<()> {
    let range_end =
        prefix_range_end(SERVICES_PREFIX.as_bytes()).ok_or_else(|| anyhow!("bad prefix"))?;
    let opts = GetOptions::new().with_range(range_end);
    let resp = client.get(SERVICES_PREFIX.as_bytes(), Some(opts)).await?;

    for kv in resp.kvs() {
        let key = std::str::from_utf8(kv.key())?;
        if !key.contains("/deployments/history/") {
            continue;
        }

        let deployment: ServiceDeployment = match serde_json::from_slice(kv.value()) {
            Ok(d) => d,
            Err(err) => {
                eprintln!("[sidecar]: bad JSON at {key}: {err}");
                continue;
            }
        };

        if deployment.status != "PENDING_READY" {
            continue;
        }

        let health_path = match deployment
            .config
            .deploy
            .healthcheck_path
            .as_deref()
            .filter(|p| !p.is_empty())
        {
            Some(p) => p,
            None => continue,
        };

        let url = match build_health_url(&deployment, health_path) {
            Some(u) => u,
            None => continue,
        };

        eprintln!(
            "[sidecar]: probing {}/{} -> {url}",
            deployment.config.id, deployment.id
        );

        let healthy = match http.get(&url).send().await {
            Ok(resp) => resp.status().is_success(),
            Err(_) => false,
        };

        if !healthy {
            continue;
        }

        eprintln!(
            "[sidecar]: healthy! marking {}/{} READY",
            deployment.config.id, deployment.id
        );

        if let Err(err) = mark_ready(client, key, kv.value(), kv.mod_revision()).await {
            eprintln!("[sidecar]: mark ready failed: {err}");
            continue;
        }

        if let Err(err) = update_service_info_status(client, &deployment.config.id, "READY").await {
            eprintln!("[sidecar]: update service info failed: {err}");
        }
    }

    Ok(())
}

fn build_health_url(deployment: &ServiceDeployment, health_path: &str) -> Option<String> {
    let ports = &deployment.config.deploy.ports;

    if let Some(first_port) = ports.first() {
        // Port format is "hostPort:containerPort" — use hostPort.
        let host_port = first_port.split(':').next().unwrap_or(first_port);
        return Some(format!("http://localhost:{host_port}{health_path}"));
    }

    // No port mappings — try reaching the container by name on the Docker network.
    let short_id: String = deployment.id.chars().take(6).collect();
    let container_name = format!("{}-{short_id}", deployment.config.id);
    Some(format!("http://{container_name}{health_path}"))
}

async fn mark_ready(
    client: &mut EtcdClient,
    key: &str,
    raw_value: &[u8],
    mod_revision: i64,
) -> Result<()> {
    let mut doc: serde_json::Value = serde_json::from_slice(raw_value)?;

    doc["status"] = serde_json::Value::String("READY".to_string());
    doc["deployedAt"] = serde_json::json!(current_time_millis()?);

    let data = serde_json::to_string(&doc)?;

    let txn = Txn::new()
        .when(vec![Compare::mod_revision(
            key.as_bytes().to_vec(),
            CompareOp::Equal,
            mod_revision,
        )])
        .and_then(vec![TxnOp::put(
            key.as_bytes().to_vec(),
            data.as_bytes().to_vec(),
            None,
        )]);

    let resp = client.txn(txn).await?;
    if !resp.succeeded() {
        return Err(anyhow!("CAS conflict on {key}"));
    }

    Ok(())
}

async fn update_service_info_status(
    client: &mut EtcdClient,
    service_id: &str,
    status: &str,
) -> Result<()> {
    let info_key = format!("/maetro/services/{service_id}/info");
    let resp = client.get(info_key.as_bytes(), None).await?;

    let Some(kv) = resp.kvs().first() else {
        return Ok(());
    };

    let mut info: ServiceInfo = serde_json::from_slice(kv.value())?;
    info.status = Some(status.to_string());
    let data = serde_json::to_string(&info)?;

    let txn = Txn::new()
        .when(vec![Compare::mod_revision(
            info_key.as_bytes().to_vec(),
            CompareOp::Equal,
            kv.mod_revision(),
        )])
        .and_then(vec![TxnOp::put(
            info_key.as_bytes().to_vec(),
            data.as_bytes().to_vec(),
            None,
        )]);

    let resp = client.txn(txn).await?;
    if !resp.succeeded() {
        return Err(anyhow!("CAS conflict on {info_key}"));
    }

    Ok(())
}

fn current_time_millis() -> Result<u64> {
    Ok(std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis() as u64)
}

fn prefix_range_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    for idx in (0..end.len()).rev() {
        if end[idx] < 0xff {
            end[idx] += 1;
            end.truncate(idx + 1);
            return Some(end);
        }
    }
    None
}
