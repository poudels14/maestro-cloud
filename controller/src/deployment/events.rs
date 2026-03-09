use std::collections::VecDeque;

use crate::deployment::store::QueuedDeployment;

#[derive(Debug, Clone)]
pub enum DeploymentEvent {
    ClaimQueuedDeployment(QueuedDeployment),
    MarkDeploymentReady {
        service_id: String,
        deployment_key: String,
        deployment_id: String,
    },
    MarkDeploymentCrashed {
        service_id: String,
        deployment_id: String,
    },
    MarkDeploymentTerminated {
        service_id: String,
        deployment_id: String,
    },
}

#[derive(Default)]
pub struct DeploymentEventQueue {
    events: VecDeque<DeploymentEvent>,
}

impl DeploymentEventQueue {
    pub fn push(&mut self, event: DeploymentEvent) {
        self.events.push_back(event);
    }

    pub fn drain(&mut self) -> Vec<DeploymentEvent> {
        self.events.drain(..).collect()
    }
}
