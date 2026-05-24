use super::build_health_url_for_replica;
use crate::deployment::store::ClusterStore;
use crate::deployment::types::{
    Deployment, DeploymentStatus, QueuedDeployment, ReplicaState, ServiceConfig,
    ServiceDeployConfig, ServiceDeployment,
};
use crate::health::{DEFAULT_MAX_HEALTHCHECK_FAILURES, DefaultHealthMonitor};
use anyhow::Result;
use async_trait::async_trait;
use std::{collections::HashMap, sync::Mutex};

#[derive(Default)]
struct ProbeTestStore {
    deployment: Mutex<Option<ServiceDeployment>>,
    replica_states: Mutex<HashMap<String, Vec<ReplicaState>>>,
}

impl ProbeTestStore {
    fn key(service_id: &str, deployment_id: &str) -> String {
        format!("{service_id}/{deployment_id}")
    }
}

#[async_trait]
impl ClusterStore for ProbeTestStore {
    async fn list_service_ids(&self) -> Result<Vec<String>> {
        let deployment = self.deployment.lock().expect("deployment lock");
        Ok(deployment
            .as_ref()
            .map(|deployment| vec![deployment.config.id.clone()])
            .unwrap_or_default())
    }

    async fn list_service_deployments(&self, service_id: &str) -> Result<Vec<ServiceDeployment>> {
        let deployment = self.deployment.lock().expect("deployment lock");
        Ok(deployment
            .as_ref()
            .filter(|deployment| deployment.config.id == service_id)
            .map(|deployment| vec![deployment.clone()])
            .unwrap_or_default())
    }

    async fn list_replica_states(
        &self,
        service_id: &str,
        deployment_id: &str,
    ) -> Result<Vec<ReplicaState>> {
        let replica_states = self.replica_states.lock().expect("replica state lock");
        Ok(replica_states
            .get(&Self::key(service_id, deployment_id))
            .cloned()
            .unwrap_or_default())
    }

    async fn update_replica_status(
        &self,
        service_id: &str,
        deployment_id: &str,
        replica_index: u32,
        status: DeploymentStatus,
    ) -> Result<()> {
        let mut replica_states = self.replica_states.lock().expect("replica state lock");
        let replicas = replica_states
            .entry(Self::key(service_id, deployment_id))
            .or_default();
        if let Some(replica) = replicas
            .iter_mut()
            .find(|replica| replica.replica_index == replica_index)
        {
            replica.status = status;
        } else {
            replicas.push(ReplicaState {
                replica_index,
                status,
                healthcheck_failures: 0,
                restart_attempts: 0,
            });
        }
        Ok(())
    }

    async fn upsert_replica_state(
        &self,
        service_id: &str,
        deployment_id: &str,
        state: ReplicaState,
    ) -> Result<()> {
        let mut replica_states = self.replica_states.lock().expect("replica state lock");
        let replicas = replica_states
            .entry(Self::key(service_id, deployment_id))
            .or_default();
        if let Some(replica) = replicas
            .iter_mut()
            .find(|replica| replica.replica_index == state.replica_index)
        {
            *replica = state;
        } else {
            replicas.push(state);
        }
        Ok(())
    }

    async fn list_queued_deployments(&self) -> Result<Vec<QueuedDeployment>> {
        Ok(Vec::new())
    }

    async fn claim_deployment_building(
        &self,
        _queued_deployment: &QueuedDeployment,
    ) -> Result<bool> {
        Ok(false)
    }

    async fn update_deployment_status(
        &self,
        deployment: &Deployment,
        status: DeploymentStatus,
    ) -> Result<()> {
        let mut current = self.deployment.lock().expect("deployment lock");
        let Some(current) = current.as_mut() else {
            return Ok(());
        };
        if current.config.id == deployment.service_id && current.id == deployment.id {
            current.status = status;
        }
        Ok(())
    }

    async fn stop_service_deployment(
        &self,
        deployment: &Deployment,
    ) -> Result<Option<ServiceDeployment>> {
        let mut current = self.deployment.lock().expect("deployment lock");
        let Some(current) = current.as_mut() else {
            return Ok(None);
        };
        if current.config.id != deployment.service_id || current.id != deployment.id {
            return Ok(None);
        }
        current.status = DeploymentStatus::Draining;
        Ok(Some(current.clone()))
    }
}

fn deployment_with_ports(ingress_port: Option<u16>, expose_ports: Vec<u16>) -> ServiceDeployment {
    ServiceDeployment {
        id: "abc123xyz9".to_string(),
        created_at: 0,
        deployed_at: None,
        drained_at: None,
        status: DeploymentStatus::PendingReady,
        config: ServiceConfig {
            id: "svc".to_string(),
            name: "svc".to_string(),
            version: "v1".to_string(),
            build: None,
            image: Some("svc:latest".to_string()),
            deploy: ServiceDeployConfig {
                flags: vec![],
                expose_ports,
                command: None,
                healthcheck_path: Some("/health".to_string()),
                replicas: 1,
                max_restarts: None,
                env: Default::default(),
                secrets: None,
                volumes: vec![],
                healthcheck_interval: 60,
                node_affinity: None,
            },
            ingress: ingress_port.map(|port| crate::deployment::types::IngressConfig {
                host: Some("svc.local".to_string()),
                hosts: Vec::new(),
                port: Some(port),
            }),
        },
        git_commit: None,
        build: None,
        upload_archive: None,
    }
}

#[test]
fn health_url_prefers_ingress_port() {
    let deployment = deployment_with_ports(Some(8080), vec![3000]);
    let url = build_health_url_for_replica(&deployment, 0, "/health", None)
        .expect("ingress.port should produce health URL");
    assert_eq!(url, "http://svc-abc123:8080/health");
}

#[test]
fn health_url_requires_ingress_port_even_when_expose_ports_exist() {
    let deployment = deployment_with_ports(None, vec![3000, 5000]);
    let url = build_health_url_for_replica(&deployment, 0, "/health", None);
    assert!(url.is_none());
}

#[test]
fn health_url_requires_explicit_port() {
    let deployment = deployment_with_ports(None, vec![]);
    let url = build_health_url_for_replica(&deployment, 0, "/health", None);
    assert!(url.is_none());
}

#[test]
fn health_url_uses_fqdn_when_dns_domain_is_provided() {
    let deployment = deployment_with_ports(Some(8080), vec![]);
    let url = build_health_url_for_replica(
        &deployment,
        0,
        "/health",
        Some("cluster-1.maestro.internal"),
    )
    .expect("ingress.port should produce health URL");
    assert_eq!(
        url,
        "http://svc-abc123.cluster-1.maestro.internal:8080/health"
    );
}

#[tokio::test]
async fn marks_replica_crashed_after_tenth_consecutive_healthcheck_failure() {
    use std::sync::Arc;
    let deployment = deployment_with_ports(Some(1), vec![]);
    let store = Arc::new(ProbeTestStore {
        deployment: Mutex::new(Some(deployment.clone())),
        replica_states: Mutex::new(HashMap::from([(
            ProbeTestStore::key(&deployment.config.id, &deployment.id),
            vec![ReplicaState {
                replica_index: 0,
                status: DeploymentStatus::PendingReady,
                healthcheck_failures: 9,
                restart_attempts: 0,
            }],
        )])),
    });
    let http = reqwest::Client::builder()
        .build()
        .expect("build reqwest client");
    let mut state = HashMap::new();
    let mut last_polled = HashMap::new();

    let store_arc: Arc<dyn ClusterStore> = store.clone();
    let monitor = DefaultHealthMonitor::new(store_arc, DEFAULT_MAX_HEALTHCHECK_FAILURES);
    crate::probe::healthcheck::check_deployments(
        store.as_ref(),
        &monitor,
        &http,
        &mut state,
        &mut last_polled,
        None,
    )
    .await
    .expect("healthcheck should complete");

    let deployment_after = store
        .deployment
        .lock()
        .expect("deployment lock")
        .clone()
        .expect("deployment should exist");
    assert_eq!(deployment_after.status, DeploymentStatus::PendingReady);

    let replica_states = store
        .replica_states
        .lock()
        .expect("replica state lock")
        .get(&ProbeTestStore::key(&deployment.config.id, &deployment.id))
        .cloned()
        .expect("replica state should exist");
    assert_eq!(replica_states[0].healthcheck_failures, 10);
    assert_eq!(replica_states[0].status, DeploymentStatus::Crashed);
}
