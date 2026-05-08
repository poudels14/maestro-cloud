use std::{
    env,
    io::{self, IsTerminal},
};

use serde::Deserialize;

use crate::deployment::types::{DeploymentStatus, DeploymentWithReplicas, ServiceConfig};
use crate::error::{Error, Result};

const ANSI_RED: &str = "\x1b[31m";
const ANSI_RESET: &str = "\x1b[0m";

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ServiceListItem {
    #[serde(flatten)]
    service: ServiceConfig,
    status: Option<DeploymentStatus>,
    #[serde(default)]
    system: bool,
    #[serde(default)]
    deploy_frozen: bool,
}

pub async fn run_services(host: &str) -> Result<()> {
    let base = normalize_base_url(host)?;
    let client = reqwest::Client::new();
    let endpoint = format!("{base}/api/services");
    let response = client
        .get(&endpoint)
        .send()
        .await
        .map_err(|err| Error::external(format!("failed to call services endpoint: {err}")))?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(Error::external(format!(
            "services request failed with status {status}: {body}"
        )));
    }

    let mut services = response
        .json::<Vec<ServiceListItem>>()
        .await
        .map_err(|err| Error::external(format!("failed to decode services response: {err}")))?;
    services.sort_by(|a, b| {
        a.system
            .cmp(&b.system)
            .then_with(|| a.service.id.cmp(&b.service.id))
    });

    if services.is_empty() {
        println!("[maestro]: no services found");
        return Ok(());
    }

    let mut rows = Vec::with_capacity(services.len());
    for item in services {
        let replicas = if item.system {
            format!("-/{}", item.service.deploy.replicas)
        } else {
            let ready = ready_replica_count(&client, &base, &item.service.id).await?;
            format!("{ready}/{}", item.service.deploy.replicas)
        };
        rows.push(ServiceRow {
            endpoint: service_endpoint(&item.service),
            id: item.service.id,
            name: item.service.name,
            status: item
                .status
                .map(|status| format!("{status:?}"))
                .unwrap_or_else(|| "-".to_string()),
            replicas,
            deploy_frozen: item.deploy_frozen,
            system: item.system,
        });
    }

    print_rows(&rows);
    Ok(())
}

async fn ready_replica_count(
    client: &reqwest::Client,
    base: &str,
    service_id: &str,
) -> Result<usize> {
    let endpoint = format!("{base}/api/services/{service_id}/deployments");
    let response = client.get(&endpoint).send().await.map_err(|err| {
        Error::external(format!(
            "failed to call deployments endpoint for service `{service_id}`: {err}"
        ))
    })?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(Error::external(format!(
            "deployment request for service `{service_id}` failed with status {status}: {body}"
        )));
    }

    let deployments = response
        .json::<Vec<DeploymentWithReplicas>>()
        .await
        .map_err(|err| {
            Error::external(format!(
                "failed to decode deployments response for service `{service_id}`: {err}"
            ))
        })?;

    let Some(active) = deployments.into_iter().find(|deployment| {
        matches!(
            deployment.deployment.status,
            DeploymentStatus::Ready | DeploymentStatus::PendingReady | DeploymentStatus::Draining
        )
    }) else {
        return Ok(0);
    };

    Ok(active
        .replicas
        .iter()
        .filter(|replica| replica.status == DeploymentStatus::Ready)
        .count())
}

struct ServiceRow {
    id: String,
    name: String,
    status: String,
    replicas: String,
    deploy_frozen: bool,
    system: bool,
    endpoint: String,
}

fn print_rows(rows: &[ServiceRow]) {
    let use_color = should_color_output();
    let widths = ColumnWidths::for_rows(rows);
    let user_rows = rows.iter().filter(|row| !row.system).collect::<Vec<_>>();
    let system_rows = rows.iter().filter(|row| row.system).collect::<Vec<_>>();

    if !user_rows.is_empty() {
        print_section("USER SERVICES", &user_rows, widths, use_color, true);
    }
    if !system_rows.is_empty() {
        if !user_rows.is_empty() {
            println!();
        }
        print_section("SYSTEM SERVICES", &system_rows, widths, use_color, false);
    }
}

fn print_section(
    title: &str,
    rows: &[&ServiceRow],
    widths: ColumnWidths,
    use_color: bool,
    include_endpoint: bool,
) {
    println!("{title}");
    if include_endpoint {
        println!(
            "{:<id_width$}  {:<name_width$}  {:<status_width$}  {:>replicas_width$}  {:<6}  {:<6}  {:<endpoint_width$}",
            "ID",
            "NAME",
            "STATUS",
            "REPLICAS",
            "FROZEN",
            "SYSTEM",
            "ENDPOINTS",
            id_width = widths.id,
            name_width = widths.name,
            status_width = widths.status,
            replicas_width = widths.replicas,
            endpoint_width = widths.endpoint,
        );
    } else {
        println!(
            "{:<id_width$}  {:<name_width$}  {:<status_width$}  {:>replicas_width$}  {:<6}  {:<6}",
            "ID",
            "NAME",
            "STATUS",
            "REPLICAS",
            "FROZEN",
            "SYSTEM",
            id_width = widths.id,
            name_width = widths.name,
            status_width = widths.status,
            replicas_width = widths.replicas,
        );
    }
    for row in rows {
        let status = format_status_cell(row, widths, use_color);
        if include_endpoint {
            println!(
                "{:<id_width$}  {:<name_width$}  {}  {:>replicas_width$}  {:<6}  {:<6}  {:<endpoint_width$}",
                row.id,
                row.name,
                status,
                row.replicas,
                if row.deploy_frozen { "yes" } else { "no" },
                if row.system { "yes" } else { "no" },
                row.endpoint,
                id_width = widths.id,
                name_width = widths.name,
                replicas_width = widths.replicas,
                endpoint_width = widths.endpoint,
            );
        } else {
            println!(
                "{:<id_width$}  {:<name_width$}  {}  {:>replicas_width$}  {:<6}  {:<6}",
                row.id,
                row.name,
                status,
                row.replicas,
                if row.deploy_frozen { "yes" } else { "no" },
                if row.system { "yes" } else { "no" },
                id_width = widths.id,
                name_width = widths.name,
                replicas_width = widths.replicas,
            );
        }
    }
}

fn format_status_cell(row: &ServiceRow, widths: ColumnWidths, use_color: bool) -> String {
    let status = format!(
        "{:<status_width$}",
        row.status,
        status_width = widths.status
    );
    if use_color && row.status == "Crashed" {
        format!("{ANSI_RED}{status}{ANSI_RESET}")
    } else {
        status
    }
}

fn should_color_output() -> bool {
    io::stdout().is_terminal()
        && env::var_os("NO_COLOR").is_none()
        && env::var("TERM").map_or(true, |term| term != "dumb")
}

#[derive(Clone, Copy)]
struct ColumnWidths {
    id: usize,
    name: usize,
    status: usize,
    replicas: usize,
    endpoint: usize,
}

impl ColumnWidths {
    fn for_rows(rows: &[ServiceRow]) -> Self {
        Self {
            id: width("ID", rows.iter().map(|row| row.id.as_str())),
            name: width("NAME", rows.iter().map(|row| row.name.as_str())),
            status: width("STATUS", rows.iter().map(|row| row.status.as_str())),
            replicas: width("REPLICAS", rows.iter().map(|row| row.replicas.as_str())),
            endpoint: width("ENDPOINTS", rows.iter().map(|row| row.endpoint.as_str())),
        }
    }
}

fn width<'a>(header: &str, values: impl Iterator<Item = &'a str>) -> usize {
    values.fold(header.len(), |max, value| max.max(value.len()))
}

fn service_endpoint(service: &ServiceConfig) -> String {
    service
        .ingress
        .as_ref()
        .map(|ingress| format!("{}:{}", ingress.host, ingress.port.unwrap_or(80)))
        .unwrap_or_else(|| "-".to_string())
}

fn normalize_base_url(host: &str) -> Result<String> {
    let host = host.trim();
    if host.is_empty() {
        return Err(Error::invalid_input("host cannot be empty"));
    }

    let base = if host.starts_with("http://") || host.starts_with("https://") {
        host.to_string()
    } else {
        format!("http://{host}")
    };

    Ok(base.trim_end_matches('/').to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn service_row(status: &str) -> ServiceRow {
        ServiceRow {
            id: "service-1".to_string(),
            name: "Service 1".to_string(),
            status: status.to_string(),
            replicas: "0/1".to_string(),
            deploy_frozen: false,
            system: false,
            endpoint: "service.local:80".to_string(),
        }
    }

    #[test]
    fn formats_crashed_status_red_when_color_is_enabled() {
        let widths = ColumnWidths {
            id: 2,
            name: 4,
            status: 9,
            replicas: 8,
            endpoint: 9,
        };

        assert_eq!(
            format_status_cell(&service_row("Crashed"), widths, true),
            "\x1b[31mCrashed  \x1b[0m"
        );
    }

    #[test]
    fn leaves_status_uncolored_when_color_is_disabled_or_status_is_not_crashed() {
        let widths = ColumnWidths {
            id: 2,
            name: 4,
            status: 9,
            replicas: 8,
            endpoint: 9,
        };

        assert_eq!(
            format_status_cell(&service_row("Crashed"), widths, false),
            "Crashed  "
        );
        assert_eq!(
            format_status_cell(&service_row("Ready"), widths, true),
            "Ready    "
        );
    }
}
