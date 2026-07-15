use std::{net::IpAddr, path::Path, sync::Arc, time::Duration};

use anyhow::{Context, Result, bail};
use reqwest::{Certificate, Client, Identity, StatusCode};
use tokio::sync::broadcast;

use crate::{
    cluster::{
        NodeInfo, NodeRole, NodeState,
        assignment_store::AssignmentStore,
        elector::LeaderElector,
        registry::NodeRegistry,
        types::{
            ClusterFreeze, ClusterMaintenanceKind, LeadershipState, LeadershipToken, UpgradeEvent,
            UpgradeNodeStatus, UpgradeNodeStep, UpgradePhase, UpgradeRun,
        },
    },
    deployment::{store::ClusterStore, types::DeploymentStatus},
    logs::Logger,
};

const DRAIN_TIMEOUT_MS: i64 = 60_000;
const VERIFY_TIMEOUT_MS: i64 = 120_000;
const UPGRADE_RETRY_MS: i64 = 15_000;

#[derive(Clone)]
pub struct ClusterUpgradeOrchestrator {
    local_node_id: String,
    elector: Arc<dyn LeaderElector>,
    registry: Arc<dyn NodeRegistry>,
    assignments: Arc<dyn AssignmentStore>,
    store: Arc<dyn ClusterStore>,
    http: Client,
    node_api_scheme: &'static str,
    jwt_secret: Option<String>,
    logger: Logger,
}

impl ClusterUpgradeOrchestrator {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        local_node_id: String,
        local_host_ip: IpAddr,
        certs_dir: &Path,
        elector: Arc<dyn LeaderElector>,
        registry: Arc<dyn NodeRegistry>,
        assignments: Arc<dyn AssignmentStore>,
        store: Arc<dyn ClusterStore>,
        jwt_secret: Option<String>,
        logger: Logger,
    ) -> Result<Self> {
        let ca = std::fs::read(certs_dir.join("ca.pem"))?;
        let mut identity = std::fs::read(certs_dir.join("client.pem"))?;
        identity.extend_from_slice(b"\n");
        identity.extend_from_slice(&std::fs::read(certs_dir.join("client-key.pem"))?);
        let http = Client::builder()
            .https_only(true)
            .add_root_certificate(Certificate::from_pem(&ca)?)
            .identity(Identity::from_pem(&identity)?)
            .local_address(local_host_ip)
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(10))
            .build()?;
        Ok(Self {
            local_node_id,
            elector,
            registry,
            assignments,
            store,
            http,
            node_api_scheme: "https",
            jwt_secret,
            logger,
        })
    }

    #[cfg(test)]
    pub(crate) fn new_for_test(
        local_node_id: String,
        elector: Arc<dyn LeaderElector>,
        registry: Arc<dyn NodeRegistry>,
        assignments: Arc<dyn AssignmentStore>,
        store: Arc<dyn ClusterStore>,
        http: Client,
    ) -> Self {
        Self {
            local_node_id,
            elector,
            registry,
            assignments,
            store,
            http,
            node_api_scheme: "http",
            jwt_secret: None,
            logger: Logger::noop(),
        }
    }

    pub async fn create_run(
        &self,
        token: &LeadershipToken,
        target_version: &str,
    ) -> Result<UpgradeRun> {
        let target = semver::Version::parse(target_version.trim())
            .with_context(|| format!("invalid target version `{target_version}`"))?;
        self.create_maintenance_run(token, ClusterMaintenanceKind::Upgrade, Some(target), None)
            .await
    }

    pub async fn create_restart_run(
        &self,
        token: &LeadershipToken,
        node_id: Option<&str>,
    ) -> Result<UpgradeRun> {
        self.create_maintenance_run(token, ClusterMaintenanceKind::Restart, None, node_id)
            .await
    }

    async fn create_maintenance_run(
        &self,
        token: &LeadershipToken,
        kind: ClusterMaintenanceKind,
        target: Option<semver::Version>,
        selected_node_id: Option<&str>,
    ) -> Result<UpgradeRun> {
        require_leadership(self.elector.as_ref(), token)?;
        let mut nodes = self.registry.list_nodes().await?;
        if nodes.is_empty() {
            bail!("cannot {kind} a cluster without live nodes");
        }
        for service in self.store.list_service_infos().await? {
            let active_rollout = self
                .store
                .list_service_deployments(&service.config.id)
                .await?
                .into_iter()
                .any(|deployment| {
                    matches!(
                        deployment.status,
                        DeploymentStatus::Queued
                            | DeploymentStatus::Building
                            | DeploymentStatus::PendingReady
                            | DeploymentStatus::Draining
                    )
                });
            if active_rollout {
                bail!(
                    "service `{}` has an active rollout; wait for it to settle before starting a cluster {kind}",
                    service.config.id
                );
            }
        }
        let live_ids = nodes
            .iter()
            .map(|node| node.node_id.as_str())
            .collect::<std::collections::HashSet<_>>();
        let offline = self
            .store
            .list_cluster_node_records()
            .await?
            .into_iter()
            .filter(|record| !live_ids.contains(record.last_info.node_id.as_str()))
            .map(|record| record.last_info.node_id)
            .collect::<Vec<_>>();
        if !offline.is_empty() {
            bail!(
                "all cluster nodes must be live before a cluster {kind}; offline: {}",
                offline.join(", ")
            );
        }
        let maintenance_reason = kind.to_string();
        for node in &nodes {
            let state = self.registry.get_node_state(&node.node_id).await?;
            if state.unschedulable && state.reason.as_deref() != Some(maintenance_reason.as_str()) {
                bail!(
                    "node `{}` is already unschedulable for `{}`; restore it before starting a cluster {kind}",
                    node.node_id,
                    state.reason.as_deref().unwrap_or("an operator action")
                );
            }
        }

        if let Some(target) = target.as_ref() {
            for node in &nodes {
                let current = semver::Version::parse(&node.version).with_context(|| {
                    format!(
                        "node `{}` reported invalid version `{}`",
                        node.node_id, node.version
                    )
                })?;
                if target < &current {
                    bail!(
                        "target version {target} is older than node `{}` version {current}",
                        node.node_id
                    );
                }
            }
            nodes.retain(|node| node.version != target.to_string());
            if nodes.is_empty() {
                bail!("every live cluster node already reports version {target}");
            }
        }

        if let Some(selected_node_id) = selected_node_id {
            let selected_node_id = selected_node_id.trim();
            if selected_node_id.is_empty() {
                bail!("restart node id cannot be empty");
            }
            nodes.retain(|node| node.node_id == selected_node_id);
            if nodes.is_empty() {
                bail!("cluster node `{selected_node_id}` is not live");
            }
        }
        order_nodes(&mut nodes, &token.info.node_id);
        let now_ms = now_millis();
        let run_id = crate::utils::nanoid::unique_id(20).to_lowercase();
        let target_version = target.map_or_else(String::new, |target| target.to_string());
        let scope = selected_node_id.map_or_else(
            || "all nodes".to_string(),
            |node_id| format!("node `{node_id}`"),
        );
        let run = UpgradeRun {
            run_id: run_id.clone(),
            kind,
            target_version: target_version.clone(),
            requested_at_ms: now_ms,
            updated_at_ms: now_ms,
            requested_by_node_id: self.local_node_id.clone(),
            phase: UpgradePhase::Draining,
            phase_started_at_ms: now_ms,
            current_node_index: 0,
            nodes: nodes
                .into_iter()
                .map(|node| UpgradeNodeStep {
                    node_id: node.node_id,
                    hostname: node.hostname,
                    role: node.role,
                    from_version: node.version,
                    from_instance_id: Some(node.instance_id),
                    status: UpgradeNodeStatus::Pending,
                    started_at_ms: None,
                    completed_at_ms: None,
                    upgrade_started_at_ms: None,
                    last_upgrade_request_at_ms: None,
                    error: None,
                })
                .collect(),
            history: vec![UpgradeEvent {
                at_ms: now_ms,
                phase: UpgradePhase::Draining,
                node_id: None,
                message: match kind {
                    ClusterMaintenanceKind::Upgrade => {
                        format!("cluster frozen for rolling upgrade to {target_version}")
                    }
                    ClusterMaintenanceKind::Restart => {
                        format!("cluster frozen for rolling restart of {scope}")
                    }
                },
            }],
            failure: None,
        };
        let freeze = ClusterFreeze {
            reason: match kind {
                ClusterMaintenanceKind::Upgrade => {
                    format!("rolling upgrade to {target_version}")
                }
                ClusterMaintenanceKind::Restart => format!("rolling restart of {scope}"),
            },
            upgrade_run_id: run_id,
            at_ms: now_ms,
        };
        if !self
            .store
            .create_cluster_upgrade(token, &run, &freeze)
            .await?
        {
            bail!("leadership changed while creating the cluster {kind}");
        }
        Ok(run)
    }

    pub async fn run(
        self: Arc<Self>,
        mut shutdown: broadcast::Receiver<crate::signal::ShutdownEvent>,
    ) {
        let mut leadership = self.elector.watch();
        let mut interval = tokio::time::interval(Duration::from_secs(5));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = shutdown.recv() => break,
                changed = leadership.changed() => {
                    if changed.is_err() { break; }
                }
                _ = interval.tick() => {}
            }
            let LeadershipState::Leading(token) = leadership.borrow().clone() else {
                continue;
            };
            if let Err(error) = self.tick(&token).await {
                self.logger.emit(
                    "error",
                    &format!("cluster maintenance tick failed: {error}"),
                );
            }
        }
    }

    pub async fn manual_unfreeze(
        &self,
        token: &LeadershipToken,
        run_id: &str,
    ) -> Result<UpgradeRun> {
        require_leadership(self.elector.as_ref(), token)?;
        self.store
            .manually_unfreeze_cluster_upgrade(token, run_id, now_millis())
            .await
    }

    pub async fn tick(&self, token: &LeadershipToken) -> Result<()> {
        require_leadership(self.elector.as_ref(), token)?;
        let Some(run) = self.store.read_cluster_upgrade().await? else {
            return Ok(());
        };
        if run.phase.is_terminal() {
            return Ok(());
        }
        if run.current_node().is_none() {
            return self.finish_success(token, run).await;
        }
        match run.phase {
            UpgradePhase::Draining => self.drain(token, run).await,
            UpgradePhase::AwaitingLeadershipTransfer => {
                self.await_leadership_transfer(token, run).await
            }
            UpgradePhase::UpgradeRequested => self.request_upgrade(token, run).await,
            UpgradePhase::SelfRestartPending => self.self_restart(token, run).await,
            UpgradePhase::Verifying => self.verify(token, run).await,
            UpgradePhase::Restoring => self.restore(token, run).await,
            UpgradePhase::Succeeded | UpgradePhase::Failed => Ok(()),
        }
    }

    async fn drain(&self, token: &LeadershipToken, mut run: UpgradeRun) -> Result<()> {
        let now_ms = now_millis();
        let operation = run.operation_name();
        let ongoing_operation = match run.kind {
            ClusterMaintenanceKind::Upgrade => "upgrading",
            ClusterMaintenanceKind::Restart => "restarting",
        };
        let node_id = run
            .current_node()
            .expect("checked current node")
            .node_id
            .clone();
        if run.current_node().expect("checked current node").status == UpgradeNodeStatus::Pending {
            self.registry
                .set_node_state(
                    token,
                    &node_id,
                    NodeState {
                        unschedulable: true,
                        drained_at_ms: Some(now_ms),
                        reason: Some(operation.to_string()),
                    },
                )
                .await?;
            let node = run.current_node_mut().expect("checked current node");
            node.status = UpgradeNodeStatus::Draining;
            node.started_at_ms = Some(now_ms);
            transition(
                &mut run,
                UpgradePhase::Draining,
                now_ms,
                Some(node_id.clone()),
                format!("draining node `{node_id}`"),
            );
            return self.persist(token, &run, false).await;
        }
        let manifest_empty = self
            .assignments
            .get_for_node(&node_id)
            .await?
            .is_none_or(|manifest| manifest.assignments.is_empty());
        if !manifest_empty {
            if now_ms.saturating_sub(run.phase_started_at_ms) >= DRAIN_TIMEOUT_MS {
                return self
                    .fail(
                        token,
                        run,
                        format!(
                            "node `{node_id}` did not drain within 60 seconds; pinned or capacity-constrained workloads were left running"
                        ),
                    )
                    .await;
            }
            run.updated_at_ms = now_ms;
            return self.persist(token, &run, false).await;
        }

        if node_id == token.info.node_id {
            let another_voter = self
                .registry
                .list_nodes()
                .await?
                .into_iter()
                .any(|node| node.node_id != node_id && node.role == NodeRole::Voter);
            if another_voter {
                transition(
                    &mut run,
                    UpgradePhase::AwaitingLeadershipTransfer,
                    now_ms,
                    Some(node_id),
                    "drained leader is transferring leadership".to_string(),
                );
                self.persist(token, &run, false).await?;
                self.elector.resign().await?;
                return Ok(());
            }
            if let Some(node) = run.current_node_mut() {
                node.status = UpgradeNodeStatus::Upgrading;
            }
            transition(
                &mut run,
                UpgradePhase::SelfRestartPending,
                now_ms,
                Some(node_id),
                format!("single voter is {ongoing_operation} and will resume after restart"),
            );
            self.persist(token, &run, false).await?;
            return self.request_node_action(token, run, true).await;
        }

        if let Some(node) = run.current_node_mut() {
            node.status = UpgradeNodeStatus::Upgrading;
        }
        transition(
            &mut run,
            UpgradePhase::UpgradeRequested,
            now_ms,
            Some(node_id),
            format!("node drained; requesting node-local {operation}"),
        );
        self.persist(token, &run, false).await
    }

    async fn await_leadership_transfer(
        &self,
        token: &LeadershipToken,
        mut run: UpgradeRun,
    ) -> Result<()> {
        let node_id = run
            .current_node()
            .expect("checked current node")
            .node_id
            .clone();
        let operation = run.operation_name();
        if node_id != token.info.node_id {
            if let Some(node) = run.current_node_mut() {
                node.status = UpgradeNodeStatus::Upgrading;
            }
            transition(
                &mut run,
                UpgradePhase::UpgradeRequested,
                now_millis(),
                Some(node_id),
                format!("new leader resumed the {operation} run"),
            );
            return self.persist(token, &run, false).await;
        }
        run.updated_at_ms = now_millis();
        self.persist(token, &run, false).await?;
        self.elector.resign().await?;
        Ok(())
    }

    async fn request_upgrade(&self, token: &LeadershipToken, run: UpgradeRun) -> Result<()> {
        self.request_node_action(token, run, false).await
    }

    async fn request_node_action(
        &self,
        token: &LeadershipToken,
        mut run: UpgradeRun,
        self_restart: bool,
    ) -> Result<()> {
        let now_ms = now_millis();
        let operation = run.operation_name();
        let node_id = run
            .current_node()
            .expect("checked current node")
            .node_id
            .clone();
        if let Some(step) = run.current_node_mut()
            && step.upgrade_started_at_ms.is_none()
        {
            step.upgrade_started_at_ms = Some(now_ms);
        }
        let live_node = self.live_node(&node_id).await?;
        if live_node
            .as_ref()
            .is_some_and(|node| node_completed_action(&run, node))
        {
            let message = match run.kind {
                ClusterMaintenanceKind::Upgrade => {
                    "node reports the target version; verifying health".to_string()
                }
                ClusterMaintenanceKind::Restart => {
                    "node reports a new process instance; verifying health".to_string()
                }
            };
            if let Some(node) = run.current_node_mut() {
                node.status = UpgradeNodeStatus::Verifying;
            }
            transition(
                &mut run,
                UpgradePhase::Verifying,
                now_ms,
                Some(node_id),
                message,
            );
            return self.persist(token, &run, false).await;
        }
        let Some(node) = live_node else {
            if self_restart {
                run.updated_at_ms = now_ms;
            } else {
                if let Some(node) = run.current_node_mut() {
                    node.status = UpgradeNodeStatus::Verifying;
                }
                transition(
                    &mut run,
                    UpgradePhase::Verifying,
                    now_ms,
                    Some(node_id),
                    "node went offline before the response; waiting for restart recovery"
                        .to_string(),
                );
            }
            return self.persist(token, &run, false).await;
        };
        let path = match run.kind {
            ClusterMaintenanceKind::Upgrade => "/api/system/upgrade",
            ClusterMaintenanceKind::Restart => "/api/system/restart",
        };
        let mut request = self.http.post(self.node_api_url(&node, path)).header(
            "x-maestro-request-id",
            format!("{operation}-{}-{node_id}", run.run_id),
        );
        if run.kind == ClusterMaintenanceKind::Upgrade {
            request = request.json(&serde_json::json!({ "version": run.target_version }));
        }
        if let Some(token) = self.operator_token()? {
            request = request.bearer_auth(token);
        }
        let response = request.send().await;
        if let Some(step) = run.current_node_mut() {
            step.last_upgrade_request_at_ms = Some(now_ms);
        }
        match response {
            Ok(response) if response.status().is_success() => {}
            Ok(response)
                if run.kind == ClusterMaintenanceKind::Upgrade
                    && response.status() == StatusCode::CONFLICT =>
            {
                self.logger.emit(
                    "info",
                    &format!(
                        "node `{node_id}` reported an upgrade version conflict; verifying its live version"
                    ),
                );
            }
            Ok(response) => {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                return self
                    .fail(
                        token,
                        run,
                        format!("node `{node_id}` rejected {operation} ({status}): {body}"),
                    )
                    .await;
            }
            Err(error) => {
                self.logger.emit(
                    "info",
                    &format!(
                        "{operation} request to `{node_id}` disconnected while the node may be restarting: {error}"
                    ),
                );
            }
        }
        if self_restart {
            run.updated_at_ms = now_ms;
            self.persist(token, &run, false).await
        } else {
            let message = match run.kind {
                ClusterMaintenanceKind::Upgrade => {
                    "upgrade accepted; waiting for exact version and health".to_string()
                }
                ClusterMaintenanceKind::Restart => {
                    "restart accepted; waiting for a new process instance and health".to_string()
                }
            };
            if let Some(node) = run.current_node_mut() {
                node.status = UpgradeNodeStatus::Verifying;
            }
            transition(
                &mut run,
                UpgradePhase::Verifying,
                now_ms,
                Some(node_id),
                message,
            );
            self.persist(token, &run, false).await
        }
    }

    async fn self_restart(&self, token: &LeadershipToken, mut run: UpgradeRun) -> Result<()> {
        let now_ms = now_millis();
        let operation = run.operation_name();
        let node_id = run
            .current_node()
            .expect("checked current node")
            .node_id
            .clone();
        if self
            .live_node(&node_id)
            .await?
            .as_ref()
            .is_some_and(|node| node_completed_action(&run, node))
        {
            if let Some(node) = run.current_node_mut() {
                node.status = UpgradeNodeStatus::Verifying;
            }
            transition(
                &mut run,
                UpgradePhase::Verifying,
                now_ms,
                Some(node_id),
                format!("single voter completed its {operation}; verifying health"),
            );
            return self.persist(token, &run, false).await;
        }
        if upgrade_timed_out(&run, now_ms) {
            return self
                .fail(
                    token,
                    run,
                    format!("single voter did not complete its {operation}"),
                )
                .await;
        }
        let should_retry = run
            .current_node()
            .and_then(|node| node.last_upgrade_request_at_ms)
            .is_none_or(|last| now_ms.saturating_sub(last) >= UPGRADE_RETRY_MS);
        if should_retry {
            return self.request_node_action(token, run, true).await;
        }
        run.updated_at_ms = now_ms;
        self.persist(token, &run, false).await
    }

    async fn verify(&self, token: &LeadershipToken, mut run: UpgradeRun) -> Result<()> {
        let now_ms = now_millis();
        let operation = run.operation_name();
        let node_id = run
            .current_node()
            .expect("checked current node")
            .node_id
            .clone();
        let live = self.registry.list_nodes().await?;
        let node = live.iter().find(|node| node.node_id == node_id);
        let action_completed = node.is_some_and(|node| node_completed_action(&run, node));
        let healthy = match node {
            Some(node) => self.node_healthy(node).await,
            None => false,
        };
        if action_completed && healthy {
            let message = match run.kind {
                ClusterMaintenanceKind::Upgrade => {
                    "target version and node health verified".to_string()
                }
                ClusterMaintenanceKind::Restart => {
                    "new process instance and node health verified".to_string()
                }
            };
            if let Some(node) = run.current_node_mut() {
                node.status = UpgradeNodeStatus::Restoring;
            }
            transition(
                &mut run,
                UpgradePhase::Restoring,
                now_ms,
                Some(node_id),
                message,
            );
            return self.persist(token, &run, false).await;
        }
        if upgrade_timed_out(&run, now_ms) {
            let message = match run.kind {
                ClusterMaintenanceKind::Upgrade => {
                    let reported = node.map_or("offline", |node| node.version.as_str());
                    format!(
                        "node `{node_id}` failed verification: expected version {}, reported {reported}, healthy={healthy}",
                        run.target_version
                    )
                }
                ClusterMaintenanceKind::Restart => format!(
                    "node `{node_id}` failed restart verification: new_instance={action_completed}, healthy={healthy}"
                ),
            };
            return self.fail(token, run, message).await;
        }
        let should_retry = node.is_some()
            && !action_completed
            && run
                .current_node()
                .and_then(|node| node.last_upgrade_request_at_ms)
                .is_none_or(|last| now_ms.saturating_sub(last) >= UPGRADE_RETRY_MS);
        if should_retry {
            transition(
                &mut run,
                UpgradePhase::UpgradeRequested,
                now_ms,
                Some(node_id),
                format!(
                    "node has not completed its {operation}; retrying the idempotent {operation} request"
                ),
            );
            return self.persist(token, &run, false).await;
        }
        run.updated_at_ms = now_ms;
        self.persist(token, &run, false).await
    }

    async fn restore(&self, token: &LeadershipToken, mut run: UpgradeRun) -> Result<()> {
        let now_ms = now_millis();
        let operation = run.operation_name();
        let node_id = run
            .current_node()
            .expect("checked current node")
            .node_id
            .clone();
        self.registry
            .set_node_state(token, &node_id, NodeState::default())
            .await?;
        if let Some(node) = run.current_node_mut() {
            node.status = UpgradeNodeStatus::Succeeded;
            node.completed_at_ms = Some(now_ms);
        }
        run.history.push(UpgradeEvent {
            at_ms: now_ms,
            phase: UpgradePhase::Restoring,
            node_id: Some(node_id),
            message: "node restored to scheduling".to_string(),
        });
        run.current_node_index = run.current_node_index.saturating_add(1);
        if run.current_node().is_none() {
            return self.finish_success(token, run).await;
        }
        let next = run
            .current_node()
            .expect("checked next node")
            .node_id
            .clone();
        transition(
            &mut run,
            UpgradePhase::Draining,
            now_ms,
            Some(next.clone()),
            format!("advancing rolling {operation} to node `{next}`"),
        );
        self.persist(token, &run, false).await
    }

    async fn finish_success(&self, token: &LeadershipToken, mut run: UpgradeRun) -> Result<()> {
        let now_ms = now_millis();
        let operation = run.operation_name();
        transition(
            &mut run,
            UpgradePhase::Succeeded,
            now_ms,
            None,
            format!("cluster {operation} verified; cluster unfrozen"),
        );
        self.persist(token, &run, true).await
    }

    async fn fail(
        &self,
        token: &LeadershipToken,
        mut run: UpgradeRun,
        message: String,
    ) -> Result<()> {
        let now_ms = now_millis();
        let operation = run.operation_name();
        if let Some(node) = run.current_node_mut() {
            node.status = UpgradeNodeStatus::Failed;
            node.error = Some(message.clone());
            node.completed_at_ms = Some(now_ms);
        }
        run.failure = Some(message.clone());
        let current_node_id = run.current_node().map(|node| node.node_id.clone());
        transition(
            &mut run,
            UpgradePhase::Failed,
            now_ms,
            current_node_id,
            format!("{message}; remaining {operation} nodes skipped and cluster unfrozen"),
        );
        self.persist(token, &run, true).await
    }

    async fn persist(
        &self,
        token: &LeadershipToken,
        run: &UpgradeRun,
        clear_freeze: bool,
    ) -> Result<()> {
        if !self
            .store
            .update_cluster_upgrade(token, run, clear_freeze)
            .await?
        {
            bail!("leadership changed while advancing cluster maintenance");
        }
        Ok(())
    }

    async fn live_node(&self, node_id: &str) -> Result<Option<NodeInfo>> {
        Ok(self
            .registry
            .list_nodes()
            .await?
            .into_iter()
            .find(|node| node.node_id == node_id))
    }

    async fn node_healthy(&self, node: &NodeInfo) -> bool {
        self.http
            .get(self.node_api_url(node, "/_healthy"))
            .send()
            .await
            .is_ok_and(|response| response.status() == StatusCode::OK)
    }

    fn operator_token(&self) -> Result<Option<String>> {
        let Some(secret) = &self.jwt_secret else {
            return Ok(None);
        };
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs();
        let claims = serde_json::json!({
            "sub": "maestro-upgrade-orchestrator",
            "scope": "operator",
            "iat": now,
            "exp": now.saturating_add(300),
        });
        let token = jsonwebtoken::encode(
            &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::HS256),
            &claims,
            &jsonwebtoken::EncodingKey::from_secret(secret.as_bytes()),
        )?;
        Ok(Some(token))
    }

    fn node_api_url(&self, node: &NodeInfo, path: &str) -> String {
        format!(
            "{}://{}:{}{path}",
            self.node_api_scheme, node.cluster_host_ip, node.cluster_api_port
        )
    }
}

fn order_nodes(nodes: &mut [NodeInfo], leader_node_id: &str) {
    nodes.sort_by(|left, right| {
        upgrade_order(left, leader_node_id)
            .cmp(&upgrade_order(right, leader_node_id))
            .then_with(|| left.node_id.cmp(&right.node_id))
    });
}

fn upgrade_order(node: &NodeInfo, leader_node_id: &str) -> u8 {
    if node.node_id == leader_node_id {
        2
    } else if node.role == NodeRole::Worker {
        0
    } else {
        1
    }
}

fn upgrade_timed_out(run: &UpgradeRun, now_ms: i64) -> bool {
    let started_at_ms = run
        .current_node()
        .and_then(|node| node.upgrade_started_at_ms)
        .unwrap_or(run.phase_started_at_ms);
    now_ms.saturating_sub(started_at_ms) >= VERIFY_TIMEOUT_MS
}

fn node_completed_action(run: &UpgradeRun, node: &NodeInfo) -> bool {
    match run.kind {
        ClusterMaintenanceKind::Upgrade => node.version == run.target_version,
        ClusterMaintenanceKind::Restart => run
            .current_node()
            .and_then(|step| step.from_instance_id.as_deref())
            .is_some_and(|instance_id| instance_id != node.instance_id),
    }
}

fn transition(
    run: &mut UpgradeRun,
    phase: UpgradePhase,
    now_ms: i64,
    node_id: Option<String>,
    message: String,
) {
    run.phase = phase;
    run.phase_started_at_ms = now_ms;
    run.updated_at_ms = now_ms;
    run.history.push(UpgradeEvent {
        at_ms: now_ms,
        phase,
        node_id,
        message,
    });
}

fn require_leadership(elector: &dyn LeaderElector, token: &LeadershipToken) -> Result<()> {
    if elector.state() != LeadershipState::Leading(token.clone()) {
        bail!("local daemon is no longer cluster leader");
    }
    Ok(())
}

fn now_millis() -> i64 {
    crate::utils::time::current_time_millis()
        .ok()
        .and_then(|value| i64::try_from(value).ok())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, net::Ipv4Addr};

    use super::*;

    fn node(id: &str, role: NodeRole) -> NodeInfo {
        NodeInfo {
            node_id: id.to_string(),
            instance_id: format!("instance-{id}"),
            hostname: id.to_string(),
            role,
            scheduling: true,
            cluster_host_ip: Ipv4Addr::new(10, 20, 0, 11),
            cluster_api_port: 3001,
            cluster_gateway_port: 3002,
            subnet: "172.22.1.0/24".to_string(),
            tailscale_ip: None,
            data_plane_ready: true,
            data_plane_checked_at_ms: 1,
            data_plane_error: None,
            version: "1.0.0".to_string(),
            started_at_ms: 1,
            labels: BTreeMap::new(),
        }
    }

    #[test]
    fn workers_then_follower_voters_then_leader() {
        let mut nodes = vec![
            node("leader", NodeRole::Voter),
            node("voter", NodeRole::Voter),
            node("worker-b", NodeRole::Worker),
            node("worker-a", NodeRole::Worker),
        ];
        order_nodes(&mut nodes, "leader");
        assert_eq!(
            nodes
                .iter()
                .map(|node| node.node_id.as_str())
                .collect::<Vec<_>>(),
            vec!["worker-a", "worker-b", "voter", "leader"]
        );
    }

    #[test]
    fn terminal_phases_are_explicit() {
        assert!(!UpgradePhase::Verifying.is_terminal());
        assert!(UpgradePhase::Succeeded.is_terminal());
        assert!(UpgradePhase::Failed.is_terminal());
    }

    #[test]
    fn retry_transitions_do_not_extend_the_version_verification_deadline() {
        let mut run = UpgradeRun {
            run_id: "run-1".to_string(),
            kind: ClusterMaintenanceKind::Upgrade,
            target_version: "2.0.0".to_string(),
            requested_at_ms: 0,
            updated_at_ms: 0,
            requested_by_node_id: "leader".to_string(),
            phase: UpgradePhase::Verifying,
            phase_started_at_ms: 10_000,
            current_node_index: 0,
            nodes: vec![UpgradeNodeStep {
                node_id: "worker".to_string(),
                hostname: "worker".to_string(),
                role: NodeRole::Worker,
                from_version: "1.0.0".to_string(),
                from_instance_id: Some("instance-worker".to_string()),
                status: UpgradeNodeStatus::Verifying,
                started_at_ms: Some(0),
                completed_at_ms: None,
                upgrade_started_at_ms: Some(1_000),
                last_upgrade_request_at_ms: Some(10_000),
                error: None,
            }],
            history: Vec::new(),
            failure: None,
        };
        transition(
            &mut run,
            UpgradePhase::UpgradeRequested,
            100_000,
            Some("worker".to_string()),
            "retry".to_string(),
        );
        assert!(upgrade_timed_out(&run, 121_000));
    }

    #[test]
    fn restart_completion_requires_a_new_process_instance() {
        let mut current = node("worker", NodeRole::Worker);
        let run = UpgradeRun {
            run_id: "restart-1".to_string(),
            kind: ClusterMaintenanceKind::Restart,
            target_version: String::new(),
            requested_at_ms: 0,
            updated_at_ms: 0,
            requested_by_node_id: "leader".to_string(),
            phase: UpgradePhase::Verifying,
            phase_started_at_ms: 0,
            current_node_index: 0,
            nodes: vec![UpgradeNodeStep {
                node_id: "worker".to_string(),
                hostname: "worker".to_string(),
                role: NodeRole::Worker,
                from_version: "1.0.0".to_string(),
                from_instance_id: Some(current.instance_id.clone()),
                status: UpgradeNodeStatus::Verifying,
                started_at_ms: Some(0),
                completed_at_ms: None,
                upgrade_started_at_ms: Some(0),
                last_upgrade_request_at_ms: Some(0),
                error: None,
            }],
            history: Vec::new(),
            failure: None,
        };

        assert!(!node_completed_action(&run, &current));
        current.instance_id = "instance-after-restart".to_string();
        assert!(node_completed_action(&run, &current));
        current.version = "9.9.9".to_string();
        assert!(node_completed_action(&run, &current));

        let mut legacy = serde_json::to_value(&run).expect("serialize maintenance run");
        legacy.as_object_mut().expect("run object").remove("kind");
        legacy["nodes"][0]
            .as_object_mut()
            .expect("node step object")
            .remove("fromInstanceId");
        let decoded: UpgradeRun =
            serde_json::from_value(legacy).expect("decode pre-restart upgrade schema");
        assert_eq!(decoded.kind, ClusterMaintenanceKind::Upgrade);
        assert_eq!(decoded.nodes[0].from_instance_id, None);
    }
}
