use std::collections::BTreeSet;
use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::sync::Arc;

use cluster::{EmbeddedEtcdProvider, StoreProvider, StoreShutdown, StoreStartMode};
use kernel_api::{NodeId, ResourceKind, ResourceName};
use kernel_store::{Keyspace, Store, TokioClock};
use serde_json::json;
use tokio::task::JoinSet;

use crate::legacy_tests::{MASTER_SECRET, encrypted as encrypted_fixture};
use crate::real_etcd_fixture::{
    allocate_shared_ports, bind_three_member_topology, cutover_connection, embedded_settings,
    provider_configs, raw_client, save_native_snapshot, three_member_cutover_snapshot,
};
use crate::{
    CutoverMigration, LegacyEtcdSource, MigrationError, MigrationOutcome, MigrationVerification,
    plan_legacy_snapshot, plan_legacy_store_restore, restore_legacy_store,
    verify_legacy_store_restore,
};

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
#[ignore = "requires MAESTRO_ETCD_BIN, MAESTRO_ETCDUTL_BIN, and MAESTRO_ETCD_TEST_IPS"]
async fn real_cutover_restores_three_member_encrypted_store() -> TestResult {
    let binary = PathBuf::from(std::env::var("MAESTRO_ETCD_BIN")?);
    let etcdutl_binary = PathBuf::from(std::env::var("MAESTRO_ETCDUTL_BIN")?);
    let host_addresses = parse_host_addresses(&std::env::var("MAESTRO_ETCD_TEST_IPS")?)?;
    let bootstrap_address = host_addresses
        .first()
        .copied()
        .ok_or("rehearsal has no bootstrap address")?;
    let directory = tempfile::tempdir()?;
    let ports = allocate_shared_ports(host_addresses)?;

    let source_configs = provider_configs(
        &directory.path().join("legacy"),
        host_addresses,
        ports,
        MASTER_SECRET,
    )?;
    let bootstrap_node = NodeId::new("node-a")?;
    let source_config = source_configs
        .get(&bootstrap_node)
        .cloned()
        .ok_or("bootstrap provider config is missing")?;
    let endpoint = format!("https://{bootstrap_address}:{}", ports.store_client);
    let connection = cutover_connection(&source_config, endpoint.clone())?;
    let provider = EmbeddedEtcdProvider::new(
        source_config.clone(),
        binary.clone(),
        Arc::new(TokioClock::new()),
        embedded_settings()?,
    )?;
    let runtime = provider.start(StoreStartMode::Bootstrap).await?;
    let mut raw = raw_client(&source_config, &endpoint).await?;

    let snapshot = bind_three_member_topology(
        three_member_cutover_snapshot(vec![
            encrypted_fixture(
                "/maetro/services/api/deploy-1/deploy/env",
                json!({"PUBLIC_NAME": "api"}),
            )?,
            encrypted_fixture(
                "/maetro/services/api/deploy-1/deploy/secrets",
                json!({"TOKEN": "secret"}),
            )?,
        ])?,
        host_addresses,
        ports,
    )?;
    for entry in snapshot.entries() {
        raw.put(entry.key(), entry.value(), None).await?;
    }

    let mut source = connection.legacy_source().await?;
    let captured = source.capture().await?;
    assert_eq!(captured.snapshot(), &snapshot);
    let plan = plan_legacy_snapshot(captured.snapshot(), MASTER_SECRET)?;
    let keyspace = Keyspace::new(plan.cluster_id());
    let store: Arc<dyn Store> = Arc::new(connection.destination_store(MASTER_SECRET).await?);
    let migration = CutoverMigration::new(
        store.clone(),
        keyspace.clone(),
        ResourceName::new("real-etcd-rehearsal")?,
    );
    let expected_digest = snapshot.digest();
    let outcome = migration
        .apply_guarded(&plan, || source_matches(&mut source, expected_digest))
        .await?;
    assert_eq!(
        outcome,
        MigrationOutcome::Applied {
            resources: plan.writes().len(),
            request_claims: plan.request_claims().len(),
            written: plan.writes().len() + plan.request_claims().len(),
            reused: 0,
        }
    );
    assert_eq!(
        migration.verify(&plan).await?,
        MigrationVerification::Verified {
            resources: plan.writes().len(),
            request_claims: plan.request_claims().len(),
            source_sha256: hex::encode(expected_digest),
        }
    );
    verify_plan(&plan, &keyspace, store.as_ref()).await?;
    let native_snapshot = directory.path().join("post-migration.db");
    save_native_snapshot(&mut raw, &native_snapshot).await?;
    let restore_plan = plan_legacy_store_restore(&snapshot, ports.store_peer)?;
    assert_eq!(restore_plan.members().len(), 3);

    raw.put(
        "/maetro/services/api/deployments/history-next-index",
        "2",
        None,
    )
    .await?;
    assert!(matches!(
        migration
            .apply_guarded(&plan, || source_matches(&mut source, expected_digest))
            .await,
        Err(MigrationError::SourceFence { .. })
    ));

    drop(source);
    drop(raw);
    drop(store);
    drop(migration);
    runtime.shutdown(StoreShutdown::Immediate).await?;

    let restored_root = directory.path().join("rewrite");
    for node_id in restore_plan.members().keys() {
        let node_root = restored_root.join(node_id.as_str());
        restore_legacy_store(
            &restore_plan,
            node_id,
            &native_snapshot,
            &node_root,
            &etcdutl_binary,
        )?;
        verify_legacy_store_restore(&restore_plan, node_id, &native_snapshot, &node_root)?;
    }

    let restored_configs = provider_configs(&restored_root, host_addresses, ports, MASTER_SECRET)?;
    let restored_bootstrap_config = restored_configs
        .get(&bootstrap_node)
        .cloned()
        .ok_or("restored bootstrap provider config is missing")?;
    let mut starts = JoinSet::new();
    for config in restored_configs.into_values() {
        let provider = EmbeddedEtcdProvider::new(
            config,
            binary.clone(),
            Arc::new(TokioClock::new()),
            embedded_settings()?,
        )?;
        starts.spawn(async move { provider.start(StoreStartMode::Restart).await });
    }
    let mut restored_runtimes = Vec::new();
    while let Some(started) = starts.join_next().await {
        restored_runtimes.push(started??);
    }
    assert_eq!(restored_runtimes.len(), 3);
    for restored_runtime in &restored_runtimes {
        let restored_store = restored_runtime.store();
        verify_plan(&plan, &keyspace, restored_store.as_ref()).await?;
    }
    let mut restored_raw = raw_client(&restored_bootstrap_config, &endpoint).await?;
    assert_eq!(restored_raw.member_list().await?.members().len(), 3);
    drop(restored_raw);
    for restored_runtime in restored_runtimes {
        restored_runtime.shutdown(StoreShutdown::Immediate).await?;
    }
    Ok(())
}

async fn source_matches(
    source: &mut LegacyEtcdSource,
    expected_digest: [u8; 32],
) -> Result<(), String> {
    let captured = source.capture().await.map_err(|error| error.to_string())?;
    if captured.snapshot().digest() == expected_digest {
        Ok(())
    } else {
        Err("legacy digest changed".to_owned())
    }
}

async fn verify_plan(
    plan: &crate::MigrationPlan,
    keyspace: &Keyspace,
    store: &dyn Store,
) -> TestResult {
    for write in plan.writes() {
        let key = keyspace.resource(&ResourceKind::new(write.kind().as_str())?, write.id());
        assert_eq!(
            store.get(&key).await?.map(|stored| stored.value),
            Some(write.value().to_vec())
        );
    }
    Ok(())
}

fn parse_host_addresses(value: &str) -> Result<[Ipv4Addr; 3], Box<dyn std::error::Error>> {
    let addresses = value
        .split(',')
        .map(str::trim)
        .map(str::parse::<Ipv4Addr>)
        .collect::<Result<Vec<_>, _>>()?;
    let addresses: [Ipv4Addr; 3] = addresses
        .try_into()
        .map_err(|_| "MAESTRO_ETCD_TEST_IPS must contain exactly three addresses")?;
    if addresses
        .iter()
        .any(|address| !address.is_private() || address.is_loopback())
        || addresses.iter().copied().collect::<BTreeSet<_>>().len() != 3
    {
        return Err(
            "MAESTRO_ETCD_TEST_IPS must contain three unique private non-loopback addresses".into(),
        );
    }
    Ok(addresses)
}
