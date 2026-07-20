use std::collections::{BTreeMap, VecDeque};

use kernel_api::{ClusterId, NodeId, WorkloadId};

use crate::{
    FakeRuntimeCall, FakeRuntimeOperation, LogFrame, RuntimeError, RuntimeEvent, WorkloadHandle,
    WorkloadMetadata, WorkloadStatus,
};

#[derive(Default)]
pub(crate) struct FakeState {
    pub(crate) sequence: u64,
    pub(crate) workloads: BTreeMap<WorkloadId, FakeWorkload>,
    pub(crate) events: Vec<FakeEventRecord>,
    pub(crate) directives: BTreeMap<FakeRuntimeOperation, VecDeque<FakeDirective>>,
    pub(crate) calls: Vec<FakeRuntimeCall>,
}

impl FakeState {
    pub(crate) fn next_sequence(&mut self) -> u64 {
        self.sequence = self.sequence.saturating_add(1);
        self.sequence
    }
}

pub(crate) struct FakeWorkload {
    pub(crate) fingerprint: Vec<u8>,
    pub(crate) handle: WorkloadHandle,
    pub(crate) metadata: WorkloadMetadata,
    pub(crate) status: WorkloadStatus,
    pub(crate) logs: Vec<LogFrame>,
}

#[derive(Clone)]
pub(crate) struct FakeEventRecord {
    pub(crate) cluster_id: ClusterId,
    pub(crate) node_id: NodeId,
    pub(crate) event: RuntimeEvent,
}

pub(crate) enum FakeDirective {
    Fail(RuntimeError),
    Hang,
}

pub(crate) fn checked_record<'a>(
    state: &'a FakeState,
    handle: &WorkloadHandle,
) -> Result<&'a FakeWorkload, RuntimeError> {
    let record =
        state
            .workloads
            .get(handle.workload_id())
            .ok_or_else(|| RuntimeError::NotFound {
                workload_id: handle.workload_id().clone(),
            })?;
    validate_handle(record, handle)?;
    Ok(record)
}

pub(crate) fn checked_record_mut<'a>(
    state: &'a mut FakeState,
    handle: &WorkloadHandle,
) -> Result<&'a mut FakeWorkload, RuntimeError> {
    let record = state
        .workloads
        .get_mut(handle.workload_id())
        .ok_or_else(|| RuntimeError::NotFound {
            workload_id: handle.workload_id().clone(),
        })?;
    validate_handle(record, handle)?;
    Ok(record)
}

pub(crate) fn validate_handle(
    record: &FakeWorkload,
    handle: &WorkloadHandle,
) -> Result<(), RuntimeError> {
    if record.handle == *handle {
        Ok(())
    } else {
        Err(RuntimeError::Conflict {
            workload_id: handle.workload_id().clone(),
            message: "backend handle does not match the managed object".to_owned(),
        })
    }
}
