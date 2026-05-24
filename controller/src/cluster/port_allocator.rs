//! Stable per-service port allocation. Once a service is assigned a port it
//! keeps that port across redeploys and node migrations — that's what makes
//! DNS-based discovery work without per-replica Traefik rewriting.

use std::collections::HashMap;
use std::sync::Mutex;

use anyhow::{Result, anyhow};
use async_trait::async_trait;

pub use super::scheduling::Port;

#[async_trait]
pub trait PortAllocator: Send + Sync {
    /// Return the stable port for `service_id`, allocating one from the pool
    /// if none has been assigned yet.
    async fn allocate(&self, service_id: &str) -> Result<Port>;

    /// Look up the port for `service_id` without allocating.
    async fn get(&self, service_id: &str) -> Result<Option<Port>>;

    /// Free the port back into the pool when a service is deleted.
    async fn release(&self, service_id: &str) -> Result<()>;
}

#[derive(Debug, Clone, Copy)]
pub struct PortRange {
    pub start: Port,
    pub end: Port,
}

impl PortRange {
    pub fn new(start: Port, end: Port) -> Self {
        Self { start, end }
    }
}

pub struct InMemoryPortAllocator {
    range: PortRange,
    state: Mutex<HashMap<String, Port>>,
}

impl InMemoryPortAllocator {
    pub fn new(range: PortRange) -> Self {
        Self {
            range,
            state: Mutex::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl PortAllocator for InMemoryPortAllocator {
    async fn allocate(&self, service_id: &str) -> Result<Port> {
        let mut state = self.state.lock().expect("port allocator state");
        if let Some(existing) = state.get(service_id) {
            return Ok(*existing);
        }
        let used: std::collections::HashSet<Port> = state.values().copied().collect();
        for candidate in self.range.start..=self.range.end {
            if !used.contains(&candidate) {
                state.insert(service_id.to_string(), candidate);
                return Ok(candidate);
            }
        }
        Err(anyhow!(
            "port pool exhausted ({}..={}; {} services already allocated)",
            self.range.start,
            self.range.end,
            state.len()
        ))
    }

    async fn get(&self, service_id: &str) -> Result<Option<Port>> {
        Ok(self
            .state
            .lock()
            .expect("port allocator state")
            .get(service_id)
            .copied())
    }

    async fn release(&self, service_id: &str) -> Result<()> {
        self.state
            .lock()
            .expect("port allocator state")
            .remove(service_id);
        Ok(())
    }
}
