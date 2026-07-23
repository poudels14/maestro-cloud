use std::collections::BTreeSet;
use std::process::Command;

use async_trait::async_trait;
use clustertest::{ClusterSetupCluster, FixtureNodeName};
use daemon::StoreLaunchMode;
use kernel_api::{ConditionState, NodeNetwork, ResourceKind, ResourceName};
use kernel_store::{CasOutcome, ExpectedVersion, Keyspace, PutRequest, Store};

use super::{
    RETRY_DELAY, RealClusterError, RealProcessCluster, SETUP_TIMEOUT, node_namespace_diagnostics,
    read_log, workload_namespace_diagnostics,
};

#[async_trait]
impl ClusterSetupCluster for RealProcessCluster {
    type Error = RealClusterError;

    fn nodes(&self) -> Vec<FixtureNodeName> {
        self.nodes.iter().map(|node| node.fixture.clone()).collect()
    }

    async fn bootstrap_seed(&mut self) -> Result<(), Self::Error> {
        self.launch_node(0, StoreLaunchMode::Bootstrap).await?;
        self.wait_store().await?;
        self.ensure_workload_ready(0).await?;
        Ok(())
    }

    async fn join_node(&mut self, node: &FixtureNodeName) -> Result<(), Self::Error> {
        let index = self.index_for(node)?;
        let ticket = self.admit_and_stage_member(index).await?;
        self.launch_node(
            index,
            StoreLaunchMode::Join {
                ticket: ticket.clone(),
            },
        )
        .await?;
        self.activate_member(&ticket).await?;
        self.wait_store().await?;
        self.ensure_workload_ready(index).await?;
        Ok(())
    }

    async fn await_mesh(
        &mut self,
        expected: &BTreeSet<FixtureNodeName>,
    ) -> Result<BTreeSet<FixtureNodeName>, Self::Error> {
        let deadline = tokio::time::Instant::now() + SETUP_TIMEOUT;
        let prefix = Keyspace::new(&self.cluster.cluster_id).resource_kind(
            &ResourceKind::new("NodeNetwork").map_err(RealClusterError::from_display)?,
        );
        loop {
            self.ensure_children_running()?;
            if let Ok(store) = self.connect_store().await
                && let Ok(snapshot) = store.list(&prefix).await
            {
                let resources = snapshot
                    .values
                    .iter()
                    .filter_map(|stored| serde_json::from_slice::<NodeNetwork>(&stored.value).ok())
                    .filter(|resource| {
                        resource.status.applied_generation == resource.meta.generation
                            && resource.status.conditions.iter().any(|condition| {
                                condition.state == ConditionState::True
                                    && condition.condition_type.0 == "MeshReady"
                            })
                    })
                    .map(|resource| FixtureNodeName::new(resource.spec.node_id.as_str()))
                    .collect::<BTreeSet<_>>();
                if resources == *expected {
                    return Ok(resources);
                }
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(RealClusterError::new(format!(
                    "mesh did not converge before deadline; logs: {}",
                    self.nodes
                        .iter()
                        .map(|node| format!("{}={}", node.node_id, read_log(&node.log_path)))
                        .collect::<Vec<_>>()
                        .join(" | ")
                )));
            }
            tokio::time::sleep(RETRY_DELAY).await;
        }
    }

    async fn ping_workload(
        &mut self,
        source: &FixtureNodeName,
        target: &FixtureNodeName,
    ) -> Result<(), Self::Error> {
        let source_index = self.index_for(source)?;
        let target_index = self.index_for(target)?;
        let source_node_namespace = self.node(source_index)?.namespace.clone();
        let source_workload_namespace = self.node(source_index)?.workload_namespace.clone();
        let source_address = self.node(source_index)?.workload_address;
        let target_node_namespace = self.node(target_index)?.namespace.clone();
        let target_workload_namespace = self.node(target_index)?.workload_namespace.clone();
        let target_address = self.node(target_index)?.workload_address;
        let deadline = tokio::time::Instant::now() + SETUP_TIMEOUT;
        loop {
            self.ensure_children_running()?;
            let output = Command::new("ip")
                .args([
                    "netns",
                    "exec",
                    &source_workload_namespace,
                    "ping",
                    "-c",
                    "1",
                    "-W",
                    "1",
                    "-I",
                    &source_address.to_string(),
                ])
                .arg(target_address.to_string())
                .output()
                .map_err(RealClusterError::from_display)?;
            if output.status.success() {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(RealClusterError::new(format!(
                    "workload ping from `{}` to `{}` did not converge: {}; source node: {}; source workload: {}; target node: {}; target workload: {}",
                    source.as_str(),
                    target.as_str(),
                    String::from_utf8_lossy(&output.stderr).trim(),
                    node_namespace_diagnostics(&source_node_namespace),
                    workload_namespace_diagnostics(&source_workload_namespace),
                    node_namespace_diagnostics(&target_node_namespace),
                    workload_namespace_diagnostics(&target_workload_namespace),
                )));
            }
            tokio::time::sleep(RETRY_DELAY).await;
        }
    }

    async fn stop_node(&mut self, node: &FixtureNodeName) -> Result<(), Self::Error> {
        let index = self.index_for(node)?;
        self.stop_process(index).await
    }

    async fn restart_node(&mut self, node: &FixtureNodeName) -> Result<(), Self::Error> {
        let index = self.index_for(node)?;
        self.launch_node(index, StoreLaunchMode::Restart).await?;
        self.wait_store().await?;
        self.ensure_workload_ready(index).await?;
        Ok(())
    }

    async fn verify_store_write(&mut self) -> Result<(), Self::Error> {
        self.write_sequence = self.write_sequence.saturating_add(1);
        let store = self.wait_store().await?;
        let key = Keyspace::new(&self.cluster.cluster_id).resource(
            &ResourceKind::new("SetupProbe").map_err(RealClusterError::from_display)?,
            &ResourceName::new(format!("write-{}", self.write_sequence))
                .map_err(RealClusterError::from_display)?,
        );
        let value = format!("quorum-write-{}", self.write_sequence).into_bytes();
        let outcome = store
            .put_cas(PutRequest {
                key: key.clone(),
                value: value.clone(),
                expected: ExpectedVersion::Missing,
                session: None,
            })
            .await
            .map_err(RealClusterError::from_display)?;
        if !matches!(outcome, CasOutcome::Applied(_)) {
            return Err(RealClusterError::new("setup probe write conflicted"));
        }
        let observed = store
            .get(&key)
            .await
            .map_err(RealClusterError::from_display)?
            .map(|stored| stored.value);
        if observed == Some(value) {
            Ok(())
        } else {
            Err(RealClusterError::new(
                "setup probe read did not match write",
            ))
        }
    }
}
