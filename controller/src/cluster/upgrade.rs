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
    deployment::{
        store::{ClusterStore, SystemUpgradeProgress},
        types::DeploymentStatus,
    },
    logs::Logger,
};

const DRAIN_TIMEOUT_MS: i64 = 60_000;
const UPGRADE_TIMEOUT_MS: i64 = 6 * 60 * 60 * 1_000;
const RESTART_VERIFY_TIMEOUT_MS: i64 = 120_000;
const UPGRADE_RETRY_MS: i64 = 15_000;

enum UpgradeProgressObservation {
    Missing,
    Active,
    Failed(String),
}

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
        let live_voter_count = nodes.iter().filter(|node| node.role.is_voter()).count();
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
            let mut pending = Vec::with_capacity(nodes.len());
            for node in nodes {
                if node_requires_upgrade(&node, target)? {
                    pending.push(node);
                }
            }
            nodes = pending;
            if nodes.is_empty() {
                bail!("every live cluster node already meets minimum version {target}");
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
        if nodes.iter().any(|node| node.role.is_voter())
            && !single_voter_maintenance_preserves_quorum(live_voter_count)
        {
            bail!(
                "cluster {kind} cannot safely restart a voter in a two-voter cluster; add a third voter first so the remaining nodes preserve quorum"
            );
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
                    upgrade_stage: None,
                    restart_started_at_ms: None,
                    error: None,
                })
                .collect(),
            history: vec![UpgradeEvent {
                at_ms: now_ms,
                phase: UpgradePhase::Draining,
                node_id: None,
                message: match kind {
                    ClusterMaintenanceKind::Upgrade => {
                        format!("cluster frozen for rolling upgrade to at least {target_version}")
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
                    format!("rolling upgrade to at least {target_version}")
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
            self.restore_maintenance_nodes(token, &run).await?;
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
                .any(|node| node.node_id != node_id && node.role.is_voter());
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
                    "node reports at least the requested version; verifying health".to_string()
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
        let progress = self.observe_upgrade_progress(&mut run, now_ms).await?;
        match progress {
            UpgradeProgressObservation::Failed(error) => {
                return self.fail(token, run, error).await;
            }
            UpgradeProgressObservation::Active => {
                if !self_restart {
                    transition(
                        &mut run,
                        UpgradePhase::Verifying,
                        now_ms,
                        Some(node_id),
                        "node reports active upgrade work; waiting for its next stage".to_string(),
                    );
                }
                run.updated_at_ms = now_ms;
                return self.persist(token, &run, false).await;
            }
            UpgradeProgressObservation::Missing => {}
        }
        let Some(node) = live_node else {
            if should_infer_restart_from_offline(&run) {
                record_restart_started(&mut run, now_ms, &node_id, "node went offline");
            }
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
            request = request.json(&serde_json::json!({
                "version": run.target_version,
                "runId": run.run_id,
            }));
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
                    "upgrade accepted; waiting for the requested-or-newer version and health"
                        .to_string()
                }
                ClusterMaintenanceKind::Restart => {
                    "restart accepted; waiting for a new process instance and health".to_string()
                }
            };
            let status = match run.kind {
                ClusterMaintenanceKind::Upgrade => UpgradeNodeStatus::Upgrading,
                ClusterMaintenanceKind::Restart => UpgradeNodeStatus::Verifying,
            };
            if let Some(node) = run.current_node_mut() {
                node.status = status;
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

    async fn observe_upgrade_progress(
        &self,
        run: &mut UpgradeRun,
        now_ms: i64,
    ) -> Result<UpgradeProgressObservation> {
        if run.kind != ClusterMaintenanceKind::Upgrade {
            return Ok(UpgradeProgressObservation::Missing);
        }
        let node_id = run
            .current_node()
            .expect("checked current node")
            .node_id
            .clone();
        let Some(progress) = self
            .store
            .read_system_upgrade_progress(Some(&node_id))
            .await?
        else {
            return Ok(UpgradeProgressObservation::Missing);
        };
        Ok(apply_reported_upgrade_progress(run, progress, now_ms))
    }

    async fn self_restart(&self, token: &LeadershipToken, mut run: UpgradeRun) -> Result<()> {
        let now_ms = now_millis();
        let operation = run.operation_name();
        let node_id = run
            .current_node()
            .expect("checked current node")
            .node_id
            .clone();
        let live_node = self.live_node(&node_id).await?;
        if live_node
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
        let progress = self.observe_upgrade_progress(&mut run, now_ms).await?;
        if let UpgradeProgressObservation::Failed(error) = &progress {
            return self.fail(token, run, error.clone()).await;
        }
        if live_node.is_none() && should_start_restart_deadline_for_offline(&run, &progress) {
            record_restart_started(&mut run, now_ms, &node_id, "node went offline");
        }
        if node_action_timed_out(&run, now_ms) {
            return self
                .fail(
                    token,
                    run,
                    format!("single voter did not complete its {operation}"),
                )
                .await;
        }
        let should_retry = upgrade_request_retry_due(&run, now_ms, &progress);
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
                    let version = node
                        .expect("completed upgrade node is live")
                        .version
                        .as_str();
                    format!("node version {version} and health verified")
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
        let progress = self.observe_upgrade_progress(&mut run, now_ms).await?;
        if let UpgradeProgressObservation::Failed(error) = &progress {
            return self.fail(token, run, error.clone()).await;
        }
        if action_completed
            && run.kind == ClusterMaintenanceKind::Upgrade
            && run
                .current_node()
                .and_then(|node| node.restart_started_at_ms)
                .is_none()
        {
            record_restart_started(
                &mut run,
                now_ms,
                &node_id,
                "node reported at least the requested version",
            );
        }
        if node.is_none() && should_start_restart_deadline_for_offline(&run, &progress) {
            record_restart_started(&mut run, now_ms, &node_id, "node went offline");
        }
        if node_action_timed_out(&run, now_ms) {
            let message = match run.kind {
                ClusterMaintenanceKind::Upgrade => {
                    let reported = node.map_or("offline", |node| node.version.as_str());
                    let stage = run
                        .current_node()
                        .and_then(|node| node.upgrade_stage)
                        .map_or_else(|| "unreported".to_string(), |stage| stage.to_string());
                    format!(
                        "node `{node_id}` did not complete its upgrade within six hours: expected at least version {}, reported {reported}, healthy={healthy}, last stage={stage}",
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
            && upgrade_request_retry_due(&run, now_ms, &progress);
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
            message: "node restored to placement eligibility".to_string(),
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
        self.persist(token, &run, true).await?;
        self.restore_maintenance_nodes(token, &run).await
    }

    async fn restore_maintenance_nodes(
        &self,
        token: &LeadershipToken,
        run: &UpgradeRun,
    ) -> Result<()> {
        for node_id in restore_maintenance_node_states(self.registry.as_ref(), token, run).await? {
            self.logger.emit(
                "info",
                &format!(
                    "node `{node_id}` restored to placement eligibility after cluster {}",
                    run.operation_name()
                ),
            );
        }
        Ok(())
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
        if !node.data_plane_ready {
            return false;
        }
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

async fn restore_maintenance_node_states(
    registry: &dyn NodeRegistry,
    token: &LeadershipToken,
    run: &UpgradeRun,
) -> Result<Vec<String>> {
    let reason = run.operation_name();
    let mut restored = Vec::new();
    for node in &run.nodes {
        let state = registry.get_node_state(&node.node_id).await?;
        if state.unschedulable && state.reason.as_deref() == Some(reason) {
            registry
                .set_node_state(token, &node.node_id, NodeState::default())
                .await?;
            restored.push(node.node_id.clone());
        }
    }
    Ok(restored)
}

fn node_requires_upgrade(node: &NodeInfo, minimum: &semver::Version) -> Result<bool> {
    let current = semver::Version::parse(&node.version).with_context(|| {
        format!(
            "node `{}` reported invalid version `{}`",
            node.node_id, node.version
        )
    })?;
    Ok(current < *minimum)
}

fn single_voter_maintenance_preserves_quorum(live_voter_count: usize) -> bool {
    live_voter_count != 2
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

fn node_action_timed_out(run: &UpgradeRun, now_ms: i64) -> bool {
    let started_at_ms = run
        .current_node()
        .and_then(|node| node.upgrade_started_at_ms)
        .unwrap_or(run.phase_started_at_ms);
    let timeout_ms = match run.kind {
        ClusterMaintenanceKind::Upgrade => UPGRADE_TIMEOUT_MS,
        ClusterMaintenanceKind::Restart => RESTART_VERIFY_TIMEOUT_MS,
    };
    now_ms.saturating_sub(started_at_ms) >= timeout_ms
}

fn upgrade_request_retry_due(
    run: &UpgradeRun,
    now_ms: i64,
    progress: &UpgradeProgressObservation,
) -> bool {
    !matches!(progress, UpgradeProgressObservation::Active)
        && run
            .current_node()
            .and_then(|node| node.last_upgrade_request_at_ms)
            .is_none_or(|last| now_ms.saturating_sub(last) >= UPGRADE_RETRY_MS)
}

fn should_infer_restart_from_offline(run: &UpgradeRun) -> bool {
    run.kind == ClusterMaintenanceKind::Upgrade
        && run
            .current_node()
            .and_then(|node| node.last_upgrade_request_at_ms)
            .is_some()
}

fn should_start_restart_deadline_for_offline(
    run: &UpgradeRun,
    progress: &UpgradeProgressObservation,
) -> bool {
    matches!(progress, UpgradeProgressObservation::Missing)
        && should_infer_restart_from_offline(run)
}

fn apply_reported_upgrade_progress(
    run: &mut UpgradeRun,
    progress: SystemUpgradeProgress,
    now_ms: i64,
) -> UpgradeProgressObservation {
    let node_id = run
        .current_node()
        .expect("checked current node")
        .node_id
        .clone();
    if progress.run_id.as_deref() != Some(run.run_id.as_str())
        || progress.target_version != run.target_version
    {
        return UpgradeProgressObservation::Missing;
    }
    if progress.stage.is_failed() {
        return UpgradeProgressObservation::Failed(
            progress
                .error
                .unwrap_or_else(|| format!("node `{node_id}` reported an upgrade failure")),
        );
    }

    let changed = run.current_node().and_then(|node| node.upgrade_stage) != Some(progress.stage);
    if changed {
        if let Some(node) = run.current_node_mut() {
            node.upgrade_stage = Some(progress.stage);
            node.status = if progress.stage.is_restarting() {
                UpgradeNodeStatus::Verifying
            } else {
                UpgradeNodeStatus::Upgrading
            };
        }
        run.updated_at_ms = now_ms;
        run.history.push(UpgradeEvent {
            at_ms: now_ms,
            phase: run.phase,
            node_id: Some(node_id.clone()),
            message: format!("node reported upgrade stage: {}", progress.stage),
        });
    }
    if progress.stage.is_restarting() {
        record_restart_started(run, now_ms, &node_id, "node reported restart start");
    }
    UpgradeProgressObservation::Active
}

fn record_restart_started(run: &mut UpgradeRun, now_ms: i64, node_id: &str, reason: &str) {
    let Some(node) = run.current_node_mut() else {
        return;
    };
    if node.restart_started_at_ms.is_some() {
        return;
    }
    node.restart_started_at_ms = Some(now_ms);
    node.status = UpgradeNodeStatus::Verifying;
    run.updated_at_ms = now_ms;
    run.history.push(UpgradeEvent {
        at_ms: now_ms,
        phase: run.phase,
        node_id: Some(node_id.to_string()),
        message: format!("{reason}; waiting for the requested-or-newer version and health"),
    });
}

fn node_completed_action(run: &UpgradeRun, node: &NodeInfo) -> bool {
    match run.kind {
        ClusterMaintenanceKind::Upgrade => semver::Version::parse(&node.version)
            .ok()
            .zip(semver::Version::parse(&run.target_version).ok())
            .is_some_and(|(reported, minimum)| reported >= minimum),
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
    fn workers_then_follower_consensus_nodes_then_leader() {
        let mut nodes = vec![
            node("leader", NodeRole::Hybrid),
            node("hybrid", NodeRole::Hybrid),
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
            vec!["worker-a", "worker-b", "hybrid", "voter", "leader"]
        );
    }

    #[test]
    fn one_at_a_time_voter_maintenance_preserves_quorum() {
        assert!(single_voter_maintenance_preserves_quorum(1));
        assert!(!single_voter_maintenance_preserves_quorum(2));
        assert!(single_voter_maintenance_preserves_quorum(3));
        assert!(single_voter_maintenance_preserves_quorum(4));
    }

    #[test]
    fn terminal_phases_are_explicit() {
        assert!(!UpgradePhase::Verifying.is_terminal());
        assert!(UpgradePhase::Succeeded.is_terminal());
        assert!(UpgradePhase::Failed.is_terminal());
    }

    #[test]
    fn only_nodes_below_the_requested_minimum_require_upgrade() {
        let minimum = semver::Version::new(2, 0, 0);
        let mut current = node("worker", NodeRole::Worker);
        current.version = "1.9.9".to_string();
        assert!(node_requires_upgrade(&current, &minimum).unwrap());
        current.version = "2.0.0".to_string();
        assert!(!node_requires_upgrade(&current, &minimum).unwrap());
        current.version = "2.1.0".to_string();
        assert!(!node_requires_upgrade(&current, &minimum).unwrap());
        current.version = "invalid".to_string();
        assert!(node_requires_upgrade(&current, &minimum).is_err());
    }

    #[tokio::test]
    async fn terminal_maintenance_restores_only_its_own_node_drains() {
        let registry = crate::cluster::registry::InMemoryNodeRegistry::new("leader".to_string());
        let token = LeadershipToken {
            info: crate::cluster::types::LeaderInfo {
                node_id: "leader".to_string(),
            },
            election_key: b"leader".to_vec(),
            create_revision: 1,
            lease_id: 1,
        };
        registry
            .set_node_state(
                &token,
                &"upgrade-node".to_string(),
                NodeState {
                    unschedulable: true,
                    drained_at_ms: Some(10),
                    reason: Some("upgrade".to_string()),
                },
            )
            .await
            .unwrap();
        let operator_state = NodeState {
            unschedulable: true,
            drained_at_ms: Some(20),
            reason: Some("drain".to_string()),
        };
        registry
            .set_node_state(&token, &"operator-node".to_string(), operator_state.clone())
            .await
            .unwrap();
        let step = |node_id: &str| UpgradeNodeStep {
            node_id: node_id.to_string(),
            hostname: node_id.to_string(),
            role: NodeRole::Worker,
            from_version: "1.0.0".to_string(),
            from_instance_id: Some(format!("instance-{node_id}")),
            status: UpgradeNodeStatus::Failed,
            started_at_ms: Some(1),
            completed_at_ms: Some(2),
            upgrade_started_at_ms: None,
            last_upgrade_request_at_ms: None,
            upgrade_stage: None,
            restart_started_at_ms: None,
            error: Some("failed".to_string()),
        };
        let run = UpgradeRun {
            run_id: "failed-run".to_string(),
            kind: ClusterMaintenanceKind::Upgrade,
            target_version: "2.0.0".to_string(),
            requested_at_ms: 0,
            updated_at_ms: 2,
            requested_by_node_id: "leader".to_string(),
            phase: UpgradePhase::Failed,
            phase_started_at_ms: 2,
            current_node_index: 0,
            nodes: vec![step("upgrade-node"), step("operator-node")],
            history: Vec::new(),
            failure: Some("failed".to_string()),
        };

        assert_eq!(
            restore_maintenance_node_states(&registry, &token, &run)
                .await
                .unwrap(),
            ["upgrade-node"]
        );
        assert_eq!(
            registry
                .get_node_state(&"upgrade-node".to_string())
                .await
                .unwrap(),
            NodeState::default()
        );
        assert_eq!(
            registry
                .get_node_state(&"operator-node".to_string())
                .await
                .unwrap(),
            operator_state
        );
    }

    #[test]
    fn reported_progress_is_correlated_deduplicated_and_records_restart() {
        let mut run = UpgradeRun {
            run_id: "run-1".to_string(),
            kind: ClusterMaintenanceKind::Upgrade,
            target_version: "2.0.0".to_string(),
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
                from_instance_id: Some("instance-worker".to_string()),
                status: UpgradeNodeStatus::Upgrading,
                started_at_ms: Some(0),
                completed_at_ms: None,
                upgrade_started_at_ms: Some(1_000),
                last_upgrade_request_at_ms: Some(1_000),
                upgrade_stage: None,
                restart_started_at_ms: None,
                error: None,
            }],
            history: Vec::new(),
            failure: None,
        };
        let progress = |run_id: &str, stage| SystemUpgradeProgress {
            run_id: Some(run_id.to_string()),
            target_version: "2.0.0".to_string(),
            stage,
            updated_at_ms: 2_000,
            error: None,
        };

        assert!(matches!(
            apply_reported_upgrade_progress(
                &mut run,
                progress(
                    "stale-run",
                    crate::cluster::SystemUpgradeStage::RebuildingSystem
                ),
                2_000,
            ),
            UpgradeProgressObservation::Missing
        ));
        assert!(run.history.is_empty());

        assert!(matches!(
            apply_reported_upgrade_progress(
                &mut run,
                progress(
                    "run-1",
                    crate::cluster::SystemUpgradeStage::RebuildingSystem
                ),
                3_000,
            ),
            UpgradeProgressObservation::Active
        ));
        assert_eq!(run.history.len(), 1);
        assert_eq!(
            run.current_node().expect("node").restart_started_at_ms,
            None
        );
        assert!(!upgrade_request_retry_due(
            &run,
            20_000,
            &UpgradeProgressObservation::Active,
        ));
        assert!(upgrade_request_retry_due(
            &run,
            20_000,
            &UpgradeProgressObservation::Missing,
        ));
        assert!(!should_start_restart_deadline_for_offline(
            &run,
            &UpgradeProgressObservation::Active,
        ));
        assert!(should_start_restart_deadline_for_offline(
            &run,
            &UpgradeProgressObservation::Missing,
        ));

        apply_reported_upgrade_progress(
            &mut run,
            progress(
                "run-1",
                crate::cluster::SystemUpgradeStage::RebuildingSystem,
            ),
            4_000,
        );
        assert_eq!(run.history.len(), 1);

        apply_reported_upgrade_progress(
            &mut run,
            progress("run-1", crate::cluster::SystemUpgradeStage::Restarting),
            5_000,
        );
        let node = run.current_node().expect("node");
        assert_eq!(node.restart_started_at_ms, Some(5_000));
        assert_eq!(node.status, UpgradeNodeStatus::Verifying);
        assert_eq!(run.history.len(), 3);
    }

    #[test]
    fn reported_upgrade_failure_is_immediately_actionable() {
        let mut progress = SystemUpgradeProgress {
            run_id: Some("run-1".to_string()),
            target_version: "2.0.0".to_string(),
            stage: crate::cluster::SystemUpgradeStage::Failed,
            updated_at_ms: 2_000,
            error: Some("nixos-rebuild failed".to_string()),
        };
        let mut run: UpgradeRun = serde_json::from_value(serde_json::json!({
            "runId": "run-1",
            "targetVersion": "2.0.0",
            "requestedAtMs": 0,
            "updatedAtMs": 0,
            "requestedByNodeId": "leader",
            "phase": "verifying",
            "phaseStartedAtMs": 0,
            "currentNodeIndex": 0,
            "nodes": [{
                "nodeId": "worker",
                "hostname": "worker",
                "role": "worker",
                "fromVersion": "1.0.0",
                "status": "upgrading",
                "startedAtMs": 0,
                "completedAtMs": null,
                "error": null
            }],
            "history": [],
            "failure": null
        }))
        .expect("upgrade run");

        match apply_reported_upgrade_progress(&mut run, progress.clone(), 2_000) {
            UpgradeProgressObservation::Failed(error) => {
                assert_eq!(error, "nixos-rebuild failed")
            }
            _ => panic!("expected reported failure"),
        }

        progress.error = None;
        match apply_reported_upgrade_progress(&mut run, progress, 2_000) {
            UpgradeProgressObservation::Failed(error) => {
                assert!(error.contains("reported an upgrade failure"))
            }
            _ => panic!("expected reported failure"),
        }
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
                upgrade_stage: None,
                restart_started_at_ms: None,
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
        assert!(should_infer_restart_from_offline(&run));
        assert!(!node_action_timed_out(&run, 1_000 + UPGRADE_TIMEOUT_MS - 1));
        assert!(node_action_timed_out(&run, 1_000 + UPGRADE_TIMEOUT_MS));

        run.current_node_mut()
            .expect("upgrade node")
            .restart_started_at_ms = Some(50_000);
        assert!(!node_action_timed_out(
            &run,
            50_000 + RESTART_VERIFY_TIMEOUT_MS
        ));
        assert!(node_action_timed_out(&run, 1_000 + UPGRADE_TIMEOUT_MS));

        run.kind = ClusterMaintenanceKind::Restart;
        assert!(!should_infer_restart_from_offline(&run));
        assert!(!node_action_timed_out(
            &run,
            1_000 + RESTART_VERIFY_TIMEOUT_MS - 1
        ));
        assert!(node_action_timed_out(
            &run,
            1_000 + RESTART_VERIFY_TIMEOUT_MS
        ));
    }

    #[test]
    fn upgrade_completion_accepts_the_requested_or_a_newer_version() {
        let mut current = node("worker", NodeRole::Worker);
        let run = UpgradeRun {
            run_id: "upgrade-1".to_string(),
            kind: ClusterMaintenanceKind::Upgrade,
            target_version: "2.0.0".to_string(),
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
                upgrade_stage: Some(crate::cluster::SystemUpgradeStage::Restarting),
                restart_started_at_ms: Some(0),
                error: None,
            }],
            history: Vec::new(),
            failure: None,
        };

        current.version = "1.9.9".to_string();
        assert!(!node_completed_action(&run, &current));
        current.version = "2.0.0".to_string();
        assert!(node_completed_action(&run, &current));
        current.version = "2.1.0".to_string();
        assert!(node_completed_action(&run, &current));
        current.version = "invalid".to_string();
        assert!(!node_completed_action(&run, &current));
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
                upgrade_stage: None,
                restart_started_at_ms: None,
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
