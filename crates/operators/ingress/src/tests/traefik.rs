use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use kernel_api::{ClusterId, TrafficGenerationId};

use super::plan::World;
use crate::{
    BackendChange, IngressBackend, IngressBackendError, PublishedTraffic, TraefikBackend,
    TraefikCutover, TraefikProvider, TraefikStage, plan, traefik_service_router_prefix,
};

#[tokio::test]
async fn traefik_backend_stages_services_before_atomic_router_cutover()
-> Result<(), Box<dyn std::error::Error>> {
    let provider = Arc::new(RecordingProvider::default());
    let backend = TraefikBackend::new(ClusterId::new("cluster-1")?, provider.clone());
    let change = active_change();
    backend.apply(&change).await?;

    assert_eq!(provider.events(), [Event::Stage, Event::Cutover]);
    let stage = provider.stages().remove(0);
    assert!(
        stage
            .entries
            .values()
            .any(|value| value == "http://10.42.1.10:8080")
    );
    assert!(
        stage
            .entries
            .keys()
            .all(|key| key.starts_with("http/services/"))
    );
    let cutover = provider.cutovers().remove(0);
    assert_eq!(
        cutover.router_prefix,
        format!(
            "http/routers/{}",
            traefik_service_router_prefix(&change.service_id)
        )
    );
    assert!(
        cutover
            .routers
            .keys()
            .all(|key| key.starts_with(&cutover.router_prefix))
    );
    assert!(
        cutover
            .routers
            .values()
            .any(|value| value.contains("PathPrefix(`/v1`)"))
    );
    Ok(())
}

#[tokio::test]
async fn affinity_tokens_and_labels_are_stable_opaque_and_node_scoped()
-> Result<(), Box<dyn std::error::Error>> {
    let first_provider = Arc::new(RecordingProvider::default());
    let first = TraefikBackend::new(ClusterId::new("cluster-1")?, first_provider.clone());
    first.apply(&active_change()).await?;
    let first_cutover = first_provider.cutovers().remove(0);
    let affinity_rule = first_cutover
        .routers
        .iter()
        .find(|(key, _)| key.ends_with("/rule") && key.contains("-a-"))
        .map(|(_, value)| value.clone())
        .ok_or("affinity rule missing")?;
    assert!(affinity_rule.contains("Header(`X-Maestro-Affinity`, `"));
    assert!(!affinity_rule.contains("node-1"));
    assert!(
        first_cutover
            .routers
            .keys()
            .all(|key| !key.contains("node-1"))
    );

    let same_provider = Arc::new(RecordingProvider::default());
    TraefikBackend::new(ClusterId::new("cluster-1")?, same_provider.clone())
        .apply(&active_change())
        .await?;
    assert_eq!(
        same_provider.cutovers().remove(0).routers,
        first_cutover.routers
    );

    let other_provider = Arc::new(RecordingProvider::default());
    TraefikBackend::new(ClusterId::new("other-cluster")?, other_provider.clone())
        .apply(&active_change())
        .await?;
    assert_ne!(
        other_provider.cutovers().remove(0).routers,
        first_cutover.routers
    );
    Ok(())
}

#[tokio::test]
async fn wildcard_routes_render_host_regexp_without_backend_injection()
-> Result<(), Box<dyn std::error::Error>> {
    let mut world = World::ready();
    world.routes[0].spec.hosts = vec!["*.preview.example.test".to_string()];
    world.routes[0].spec.path_prefix = None;
    let generation = plan(world.input())?.create_generations.remove(0);
    let provider = Arc::new(RecordingProvider::default());
    TraefikBackend::new(ClusterId::new("cluster-1")?, provider.clone())
        .apply(&BackendChange {
            service_id: generation.spec.service_id.clone(),
            active: Some(PublishedTraffic {
                generation_id: generation.meta.id,
                spec: generation.spec,
            }),
            remove: Vec::new(),
        })
        .await?;
    assert!(
        provider.cutovers()[0]
            .routers
            .values()
            .any(|value| value.contains("HostRegexp(`^[^.]+\\.preview\\.example\\.test$`)"))
    );
    Ok(())
}

#[tokio::test]
async fn failed_staging_never_switches_stable_routers() -> Result<(), Box<dyn std::error::Error>> {
    let provider = Arc::new(RecordingProvider {
        fail_stage: true,
        ..Default::default()
    });
    let backend = TraefikBackend::new(ClusterId::new("cluster-1")?, provider.clone());
    assert!(backend.apply(&active_change()).await.is_err());
    assert_eq!(provider.events(), [Event::Stage]);
    assert!(provider.cutovers().is_empty());
    Ok(())
}

#[tokio::test]
async fn retirement_removes_only_hashed_generation_prefixes()
-> Result<(), Box<dyn std::error::Error>> {
    let provider = Arc::new(RecordingProvider::default());
    let backend = TraefikBackend::new(ClusterId::new("cluster-1")?, provider.clone());
    let mut change = active_change();
    let removed = TrafficGenerationId::new("traffic-retired")?;
    change.remove = vec![removed.clone()];
    backend.apply(&change).await?;
    let cutover = provider.cutovers().remove(0);
    assert_eq!(cutover.remove_prefixes.len(), 1);
    assert!(cutover.remove_prefixes[0].starts_with("http/services/maestro-s-"));
    assert!(!cutover.remove_prefixes[0].contains(removed.as_str()));
    assert_eq!(crate::traefik::owned_prefixes(&change).len(), 1);
    Ok(())
}

fn active_change() -> BackendChange {
    let world = World::ready();
    let generation = plan(world.input())
        .expect("traffic plan")
        .create_generations
        .remove(0);
    BackendChange {
        service_id: generation.spec.service_id.clone(),
        active: Some(PublishedTraffic {
            generation_id: generation.meta.id,
            spec: generation.spec,
        }),
        remove: Vec::new(),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Event {
    Stage,
    Cutover,
}

#[derive(Default)]
struct RecordingProvider {
    events: Mutex<Vec<Event>>,
    stages: Mutex<Vec<TraefikStage>>,
    cutovers: Mutex<Vec<TraefikCutover>>,
    fail_stage: bool,
}

impl RecordingProvider {
    fn events(&self) -> Vec<Event> {
        self.events.lock().expect("events").clone()
    }

    fn stages(&self) -> Vec<TraefikStage> {
        self.stages.lock().expect("stages").clone()
    }

    fn cutovers(&self) -> Vec<TraefikCutover> {
        self.cutovers.lock().expect("cutovers").clone()
    }
}

#[async_trait]
impl TraefikProvider for RecordingProvider {
    async fn stage(&self, stage: &TraefikStage) -> Result<(), IngressBackendError> {
        self.events.lock().expect("events").push(Event::Stage);
        if self.fail_stage {
            return Err(IngressBackendError::new("injected stage failure"));
        }
        self.stages.lock().expect("stages").push(stage.clone());
        Ok(())
    }

    async fn cutover(&self, cutover: &TraefikCutover) -> Result<(), IngressBackendError> {
        self.events.lock().expect("events").push(Event::Cutover);
        self.cutovers
            .lock()
            .expect("cutovers")
            .push(cutover.clone());
        Ok(())
    }
}
