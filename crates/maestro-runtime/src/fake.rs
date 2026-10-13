use std::collections::{BTreeMap, HashSet};
use std::net::{IpAddr, Ipv4Addr};
use std::sync::Mutex;
use std::time::Duration;

use tokio::sync::mpsc;

use crate::spec::{
    ContainerSpec, ContainerState, ContainerStatus, Runtime, RuntimeError, RuntimeEvent,
    StartedContainer,
};

#[derive(Default)]
pub struct FakeRuntime {
    state: Mutex<FakeState>,
}

#[derive(Default)]
struct FakeState {
    containers: BTreeMap<String, FakeContainer>,
    pulled: HashSet<String>,
    failing_images: HashSet<String>,
    subscribers: Vec<mpsc::Sender<RuntimeEvent>>,
    next_ip: u8,
    started: Vec<String>,
}

struct FakeContainer {
    spec: ContainerSpec,
    state: ContainerState,
    ip: IpAddr,
}

impl FakeRuntime {
    pub fn fail_image(&self, image: &str) {
        self.lock().failing_images.insert(image.to_string());
    }

    pub fn exit(&self, id: &str, exit_code: u32) {
        let mut state = self.lock();
        if let Some(container) = state.containers.get_mut(id) {
            container.state = ContainerState::Stopped { exit_code };
        }
        let event = RuntimeEvent::Exited {
            id: id.to_string(),
            exit_code,
        };
        state
            .subscribers
            .retain(|subscriber| subscriber.try_send(event.clone()).is_ok());
    }

    pub fn spec(&self, id: &str) -> Option<ContainerSpec> {
        self.lock()
            .containers
            .get(id)
            .map(|container| container.spec.clone())
    }

    pub fn start_count(&self, id: &str) -> usize {
        self.lock()
            .started
            .iter()
            .filter(|started| started.as_str() == id)
            .count()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, FakeState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl Runtime for FakeRuntime {
    async fn pull(&self, image: &str) -> Result<(), RuntimeError> {
        let mut state = self.lock();
        match state.failing_images.contains(image) {
            true => Err(RuntimeError::Image {
                image: image.to_string(),
                reason: "fake pull failure".to_string(),
            }),
            false => {
                state.pulled.insert(image.to_string());
                Ok(())
            }
        }
    }

    async fn start(&self, spec: &ContainerSpec) -> Result<StartedContainer, RuntimeError> {
        let mut state = self.lock();
        match state.pulled.contains(&spec.image) {
            false => Err(RuntimeError::Image {
                image: spec.image.clone(),
                reason: "image not pulled".to_string(),
            }),
            true => {
                state.next_ip = state.next_ip.wrapping_add(1).max(2);
                let ip = IpAddr::V4(Ipv4Addr::new(10, 200, 0, state.next_ip));
                state.containers.insert(
                    spec.id.clone(),
                    FakeContainer {
                        spec: spec.clone(),
                        state: ContainerState::Running,
                        ip,
                    },
                );
                state.started.push(spec.id.clone());
                Ok(StartedContainer {
                    id: spec.id.clone(),
                    ip: Some(ip),
                })
            }
        }
    }

    async fn stop(&self, id: &str, _grace: Duration) -> Result<(), RuntimeError> {
        let running = self
            .lock()
            .containers
            .get(id)
            .is_some_and(|container| container.state == ContainerState::Running);
        if running {
            self.exit(id, 143);
        }
        Ok(())
    }

    async fn remove(&self, id: &str) -> Result<(), RuntimeError> {
        self.lock().containers.remove(id);
        Ok(())
    }

    async fn list(&self) -> Result<Vec<ContainerStatus>, RuntimeError> {
        Ok(self
            .lock()
            .containers
            .iter()
            .map(|(id, container)| ContainerStatus {
                id: id.clone(),
                labels: container.spec.labels.clone(),
                state: container.state,
                ip: Some(container.ip),
            })
            .collect())
    }

    async fn events(&self) -> Result<mpsc::Receiver<RuntimeEvent>, RuntimeError> {
        let (sender, receiver) = mpsc::channel(256);
        self.lock().subscribers.push(sender);
        Ok(receiver)
    }
}
