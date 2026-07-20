use std::collections::BTreeMap;
use std::sync::Mutex;

use async_trait::async_trait;
use kernel_api::WorkloadId;
use runtime::{
    AddressLease, AddressRequest, NetworkAttachment, NetworkHandle, NetworkProvider,
    NetworkProviderError, NetworkSpec, WorkloadHandle, WorkloadNetworkStatus,
};

#[derive(Default)]
pub(crate) struct FakeNetworkProvider {
    state: Mutex<FakeNetworkState>,
}

#[derive(Default)]
struct FakeNetworkState {
    network: Option<NetworkSpec>,
    leases: BTreeMap<WorkloadId, std::net::IpAddr>,
    attachments: BTreeMap<WorkloadId, NetworkAttachment>,
}

impl FakeNetworkProvider {
    pub(crate) fn lease_count(&self) -> usize {
        self.state
            .lock()
            .map(|state| state.leases.len())
            .unwrap_or_default()
    }
}

#[async_trait]
impl NetworkProvider for FakeNetworkProvider {
    async fn ensure_network(
        &self,
        spec: &NetworkSpec,
    ) -> Result<NetworkHandle, NetworkProviderError> {
        let mut state = self.lock()?;
        match &state.network {
            Some(existing) if existing != spec => {
                return Err(NetworkProviderError::Rejected {
                    message: "fake network already has different IPAM".to_owned(),
                });
            }
            Some(_) => {}
            None => state.network = Some(spec.clone()),
        }
        NetworkHandle::new(&spec.name)
    }

    async fn allocate_address(
        &self,
        network: &NetworkHandle,
        workload_id: &WorkloadId,
        request: AddressRequest,
    ) -> Result<AddressLease, NetworkProviderError> {
        let mut state = self.lock()?;
        validate_network(&state, network)?;
        let AddressRequest::Exact(address) = request else {
            return Err(NetworkProviderError::Rejected {
                message: "fake assignment network requires an exact address".to_owned(),
            });
        };
        if state
            .leases
            .iter()
            .any(|(owner, reserved)| owner != workload_id && *reserved == address)
        {
            return Err(NetworkProviderError::AddressConflict { address });
        }
        state.leases.insert(workload_id.clone(), address);
        Ok(AddressLease {
            workload_id: workload_id.clone(),
            address,
        })
    }

    async fn attach(
        &self,
        workload: &WorkloadHandle,
        network: &NetworkHandle,
        lease: &AddressLease,
    ) -> Result<NetworkAttachment, NetworkProviderError> {
        let mut state = self.lock()?;
        validate_network(&state, network)?;
        if state.leases.get(workload.workload_id()) != Some(&lease.address)
            || lease.workload_id != *workload.workload_id()
        {
            return Err(NetworkProviderError::AddressConflict {
                address: lease.address,
            });
        }
        let attachment = NetworkAttachment {
            network: network.clone(),
            address: lease.address,
            interface_name: Some("eth0".to_owned()),
        };
        state
            .attachments
            .insert(workload.workload_id().clone(), attachment.clone());
        Ok(attachment)
    }

    async fn detach(
        &self,
        workload: &WorkloadHandle,
        network: &NetworkHandle,
    ) -> Result<(), NetworkProviderError> {
        let mut state = self.lock()?;
        validate_network(&state, network)?;
        state.attachments.remove(workload.workload_id());
        Ok(())
    }

    async fn inspect(
        &self,
        workload: &WorkloadHandle,
    ) -> Result<WorkloadNetworkStatus, NetworkProviderError> {
        let state = self.lock()?;
        Ok(WorkloadNetworkStatus {
            attachments: state
                .attachments
                .get(workload.workload_id())
                .cloned()
                .into_iter()
                .collect(),
        })
    }

    async fn release_address(
        &self,
        network: &NetworkHandle,
        lease: &AddressLease,
    ) -> Result<(), NetworkProviderError> {
        let mut state = self.lock()?;
        validate_network(&state, network)?;
        if state.leases.get(&lease.workload_id) == Some(&lease.address) {
            state.leases.remove(&lease.workload_id);
            Ok(())
        } else {
            Err(NetworkProviderError::AddressConflict {
                address: lease.address,
            })
        }
    }
}

impl FakeNetworkProvider {
    fn lock(&self) -> Result<std::sync::MutexGuard<'_, FakeNetworkState>, NetworkProviderError> {
        self.state
            .lock()
            .map_err(|_| NetworkProviderError::Unavailable {
                message: "fake network lock was poisoned".to_owned(),
            })
    }
}

fn validate_network(
    state: &FakeNetworkState,
    network: &NetworkHandle,
) -> Result<(), NetworkProviderError> {
    if state
        .network
        .as_ref()
        .is_some_and(|configured| configured.name == network.name())
    {
        Ok(())
    } else {
        Err(NetworkProviderError::NetworkNotFound {
            name: network.name().to_owned(),
        })
    }
}
