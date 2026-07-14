use std::net::Ipv4Addr;

use anyhow::{Result, bail};
use etcd_client::{
    Client, Compare, CompareOp, ConnectOptions, Permission, RoleRevokePermissionOptions,
    TlsOptions, Txn, TxnOp, UserAddOptions,
};

use crate::cluster::types::{ClusterRuntime, NodeRole};

const READY_KEY: &str = "/maetro/system/rbac-ready";
const TRAEFIK_ROLE: &str = "maestro-traefik";
const RBAC_VERSION: u8 = 3;

pub async fn bootstrap_initial(runtime: &ClusterRuntime, tls: TlsOptions) -> Result<()> {
    if !runtime.is_seed() {
        bail!("only the bootstrap seed may initialize etcd RBAC");
    }
    let endpoint = format!("https://{}:{}", runtime.host_ip, runtime.etcd_client_port);
    let mut client = Client::connect([endpoint], Some(ConnectOptions::new().with_tls(tls))).await?;
    ensure_role(&mut client, "root", &[]).await?;
    ensure_role(&mut client, TRAEFIK_ROLE, &traefik_permissions()).await?;
    ensure_user(&mut client, "root", "root").await?;
    for node in &runtime.initial_voters {
        provision_voter_transport_users(&mut client, node.host_ip, node.identity_api_port).await?;
    }
    provision_node_users_with_client(
        &mut client,
        runtime.host_ip,
        runtime.identity_api_port,
        &runtime.node_id,
        NodeRole::Voter,
    )
    .await?;
    client
        .put(
            READY_KEY,
            serde_json::to_vec(&serde_json::json!({
                "version": RBAC_VERSION,
                "initializedBy": runtime.node_id,
            }))?,
            None,
        )
        .await?;
    enable_auth(&mut client).await
}

async fn enable_auth(client: &mut Client) -> Result<()> {
    match client.auth_enable().await {
        Ok(_) => Ok(()),
        Err(err) if err.to_string().contains("already enabled") => Ok(()),
        Err(err) => Err(err.into()),
    }
}

pub async fn provision_node_users(
    endpoints: &[String],
    tls: TlsOptions,
    host_ip: Ipv4Addr,
    identity_api_port: Option<u16>,
    node_id: &str,
    subnet: &str,
    role: NodeRole,
) -> Result<()> {
    let mut client = Client::connect(endpoints, Some(ConnectOptions::new().with_tls(tls))).await?;
    provision_node_users_with_client(&mut client, host_ip, identity_api_port, node_id, role)
        .await?;
    let control_reservation = identity_api_port.map_or_else(
        || serde_json::json!({"hostIp": host_ip, "nodeId": null, "state": "reserved"}),
        |api_port| {
            serde_json::json!({
                "hostIp": host_ip,
                "apiPort": api_port,
                "nodeId": null,
                "state": "reserved"
            })
        },
    );
    reserve_resource(
        &mut client,
        &format!(
            "/maetro/cluster/control-addresses/{}",
            identity_suffix(host_ip, identity_api_port)
        ),
        control_reservation,
        if identity_api_port.is_some() {
            "apiPort"
        } else {
            "hostIp"
        },
    )
    .await?;
    reserve_resource(
        &mut client,
        &format!(
            "/maetro/cluster/subnets/{}",
            subnet.replace('.', "-").replace('/', "_")
        ),
        serde_json::json!({"cidr": subnet, "nodeId": null, "state": "reserved"}),
        "cidr",
    )
    .await
}

pub fn ready_key() -> &'static str {
    READY_KEY
}

async fn provision_node_users_with_client(
    client: &mut Client,
    host_ip: Ipv4Addr,
    identity_api_port: Option<u16>,
    node_id: &str,
    role: NodeRole,
) -> Result<()> {
    validate_node_id(node_id)?;
    ensure_role(client, TRAEFIK_ROLE, &traefik_permissions()).await?;
    let suffix = identity_suffix(host_ip, identity_api_port);
    let daemon_user = format!(
        "maestro-{}-{suffix}",
        match role {
            NodeRole::Voter => "voter",
            NodeRole::Worker => "worker",
        }
    );
    let daemon_role = match role {
        NodeRole::Voter => "root".to_string(),
        NodeRole::Worker => {
            let role_name = format!("maestro-worker-{suffix}");
            ensure_role(client, &role_name, &worker_permissions(node_id)).await?;
            role_name
        }
    };
    ensure_user(client, &daemon_user, &daemon_role).await?;

    let probe_role = format!("maestro-probe-{suffix}");
    ensure_role(client, &probe_role, &probe_permissions(node_id)).await?;
    ensure_user(client, &format!("maestro-probe-{suffix}"), &probe_role).await?;
    ensure_user(client, &format!("maestro-traefik-{suffix}"), TRAEFIK_ROLE).await
}

fn traefik_permissions() -> Vec<Permission> {
    vec![
        Permission::read("traefik").with_prefix(),
        Permission::read("maestro-gateway").with_prefix(),
    ]
}

async fn provision_voter_transport_users(
    client: &mut Client,
    host_ip: Ipv4Addr,
    identity_api_port: Option<u16>,
) -> Result<()> {
    let suffix = identity_suffix(host_ip, identity_api_port);
    ensure_user(client, &format!("maestro-voter-{suffix}"), "root").await?;
    ensure_user(client, &format!("maestro-traefik-{suffix}"), TRAEFIK_ROLE).await
}

pub async fn provision_local_voter_users(
    endpoints: &[String],
    tls: TlsOptions,
    host_ip: Ipv4Addr,
    identity_api_port: Option<u16>,
    node_id: &str,
) -> Result<()> {
    let mut client = Client::connect(endpoints, Some(ConnectOptions::new().with_tls(tls))).await?;
    provision_node_users_with_client(
        &mut client,
        host_ip,
        identity_api_port,
        node_id,
        NodeRole::Voter,
    )
    .await
}

fn identity_suffix(host_ip: Ipv4Addr, api_port: Option<u16>) -> String {
    api_port.map_or_else(
        || format!("{:08x}", u32::from(host_ip)),
        |port| format!("{:08x}-{port:04x}", u32::from(host_ip)),
    )
}

fn worker_permissions(node_id: &str) -> Vec<Permission> {
    vec![
        Permission::read("/maetro/").with_prefix(),
        Permission::read_write(format!("/maetro/cluster/nodes/{node_id}")),
        Permission::read_write(format!("/maetro/cluster/replica-states/{node_id}/")).with_prefix(),
        Permission::read_write(format!("/maetro/cluster/stats/{node_id}")),
        Permission::read_write(format!("/maetro/cluster/disks/{node_id}")),
        Permission::read_write(format!("/maetro/system/upgrade-request/{node_id}")),
        Permission::read_write(format!("/maetro/system/restart-request/{node_id}")),
    ]
}

fn probe_permissions(node_id: &str) -> Vec<Permission> {
    vec![
        Permission::read("/maetro/").with_prefix(),
        Permission::read_write(format!("/maetro/cluster/replica-states/{node_id}/")).with_prefix(),
        Permission::read_write(format!("/maetro/cluster/stats/{node_id}")),
        Permission::read_write(format!("/maetro/cluster/disks/{node_id}")),
        Permission::read_write(format!("/maetro/system/upgrade-request/{node_id}")),
        Permission::read_write(format!("/maetro/system/restart-request/{node_id}")),
        Permission::read_write("/maetro/system/log-migration/").with_prefix(),
    ]
}

fn validate_node_id(node_id: &str) -> Result<()> {
    if node_id.len() != 12
        || !node_id
            .chars()
            .all(|character| character.is_ascii_lowercase() || character.is_ascii_digit())
    {
        bail!("invalid cluster node id `{node_id}` for RBAC provisioning");
    }
    Ok(())
}

async fn ensure_role(client: &mut Client, name: &str, permissions: &[Permission]) -> Result<()> {
    let roles = client.role_list().await?.roles().to_vec();
    if !roles.iter().any(|role| role == name) {
        client.role_add(name).await?;
    }
    let current = client.role_get(name).await?.permissions();
    if same_permissions(&current, permissions) {
        return Ok(());
    }
    for permission in current {
        let options = if permission.is_prefix() {
            Some(RoleRevokePermissionOptions::new().with_prefix())
        } else if permission.is_from_key() {
            Some(RoleRevokePermissionOptions::new().with_from_key())
        } else if permission.range_end().is_empty() {
            None
        } else {
            Some(RoleRevokePermissionOptions::new().with_range_end(permission.range_end().to_vec()))
        };
        client
            .role_revoke_permission(name, permission.key().to_vec(), options)
            .await?;
    }
    for permission in permissions {
        client
            .role_grant_permission(name, permission.clone())
            .await?;
    }
    Ok(())
}

fn same_permissions(left: &[Permission], right: &[Permission]) -> bool {
    let canonical = |permissions: &[Permission]| {
        let mut values = permissions
            .iter()
            .map(|permission| {
                (
                    permission.get_type(),
                    permission.key().to_vec(),
                    permission.range_end().to_vec(),
                )
            })
            .collect::<Vec<_>>();
        values.sort();
        values
    };
    canonical(left) == canonical(right)
}

async fn ensure_user(client: &mut Client, name: &str, role: &str) -> Result<()> {
    let users = client.user_list().await?.users().to_vec();
    if !users.iter().any(|user| user == name) {
        client
            .user_add(name, "", Some(UserAddOptions::new().with_no_pwd()))
            .await?;
    }
    let roles = client.user_get(name).await?.roles().to_vec();
    for existing in &roles {
        if existing != role {
            client.user_revoke_role(name, existing).await?;
        }
    }
    if !roles.iter().any(|existing| existing == role) {
        client.user_grant_role(name, role).await?;
    }
    Ok(())
}

async fn reserve_resource(
    client: &mut Client,
    key: &str,
    value: serde_json::Value,
    identity_field: &str,
) -> Result<()> {
    let response = client.get(key, None).await?;
    if let Some(existing) = response.kvs().first() {
        let existing: serde_json::Value = serde_json::from_slice(existing.value())?;
        if existing.get(identity_field) != value.get(identity_field) {
            bail!("cluster resource `{key}` is already reserved differently");
        }
        return Ok(());
    }
    let transaction = Txn::new()
        .when([Compare::version(key, CompareOp::Equal, 0)])
        .and_then([TxnOp::put(key, serde_json::to_vec(&value)?, None)]);
    if !client.txn(transaction).await?.succeeded() {
        bail!("cluster resource `{key}` was reserved concurrently; retry to validate it");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn writable_prefixes(permissions: &[Permission]) -> Vec<String> {
        permissions
            .iter()
            .filter(|permission| permission.get_type() != 0)
            .map(|permission| permission.key_str().expect("utf8 permission").to_string())
            .collect()
    }

    #[test]
    fn worker_writes_are_node_scoped() {
        let writes = writable_prefixes(&worker_permissions("abc123def456"));
        assert!(writes.iter().all(|key| key.contains("abc123def456")));
        assert!(!writes.iter().any(|key| key.contains("/assignments/")));
        assert!(
            !writes
                .iter()
                .any(|key| key.starts_with("/maetro/services/"))
        );
        assert!(!writes.iter().any(|key| key.contains("/leader")));
    }

    #[test]
    fn probe_writes_are_health_or_node_local_only() {
        let writes = writable_prefixes(&probe_permissions("abc123def456"));
        assert!(
            writes.iter().all(|key| {
                key.contains("abc123def456") || key == "/maetro/system/log-migration/"
            })
        );
        assert!(!writes.iter().any(|key| key.contains("/assignments/")));
        assert!(
            !writes
                .iter()
                .any(|key| key.starts_with("/maetro/services/"))
        );
        assert!(!writes.iter().any(|key| key.contains("/node-state/")));
    }

    #[test]
    fn rejects_untrusted_node_ids_in_role_namespaces() {
        assert!(validate_node_id("abc123def456").is_ok());
        assert!(validate_node_id("../../leader").is_err());
        assert!(validate_node_id("UPPERCASE123").is_err());
    }

    #[test]
    fn traefik_can_read_public_and_node_gateway_routes_only() {
        let permissions = traefik_permissions();
        let keys = permissions
            .iter()
            .map(|permission| permission.key_str().expect("utf8 permission"))
            .collect::<Vec<_>>();
        assert_eq!(keys, ["traefik", "maestro-gateway"]);
        assert!(
            permissions
                .iter()
                .all(|permission| permission.get_type() == 0 && permission.is_prefix())
        );
        let reversed = permissions.iter().cloned().rev().collect::<Vec<_>>();
        assert!(same_permissions(&permissions, &reversed));
    }
}
