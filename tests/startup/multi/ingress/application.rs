// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Independent downstream temporary admission, retaining the full oracle.
use super::*;
use std::cell::{Cell, RefCell};
type PendingCommands = Vec<(OperationId, Vec<u8>)>;
#[derive(Default)]
pub(super) struct Probe {
    pub(super) factories: Cell<usize>,
    pub(super) calls: Cell<usize>,
    pub(super) live: Cell<usize>,
    pub(super) drops: Cell<usize>,
    pub(super) pending: RefCell<Vec<PendingCommands>>,
}
struct Lease(Rc<Probe>);
impl Drop for Lease {
    fn drop(&mut self) {
        self.0.live.set(self.0.live.get() - 1);
        self.0.drops.set(self.0.drops.get() + 1);
    }
}
#[derive(Clone)]
pub(super) struct PreparedHost {
    pub(super) inner: HostApplication,
    pub(super) probe: Rc<Probe>,
}
impl PreparedHost {
    fn idle(&self) {
        assert_eq!(
            self.probe.live.get(),
            0,
            "prepared scope escaped into mutation"
        );
    }
}
impl StateMachine for PreparedHost {
    type Receipt = CounterReceipt;
    fn deployment_requirements(&self) -> Option<voteboat::raft::ReadinessRequirements> {
        self.inner.deployment_requirements()
    }
    fn applied_index(&self) -> u64 {
        self.inner.applied_index()
    }
    fn apply_batch(
        &mut self,
        entries: &[LogEntry],
    ) -> Result<Vec<CounterReceipt>, ApplicationError> {
        self.idle();
        self.inner.apply_batch(entries)
    }
}
impl BoundedStateMachine for PreparedHost {
    fn receipt_bytes_bound(&self, entries: &[LogEntry]) -> Result<usize, ApplicationError> {
        self.inner.receipt_bytes_bound(entries)
    }
}
impl ProposalAdmission for PreparedHost {
    fn validate_proposal<'pending>(
        &self,
        operation: OperationId,
        bytes: &[u8],
        pending: impl Iterator<Item = (OperationId, &'pending [u8])>,
    ) -> Result<usize, ApplicationError> {
        self.inner.validate_proposal(operation, bytes, pending)
    }
    fn prepare_proposal_admission<'pending>(
        &self,
        pending: impl Iterator<Item = (OperationId, &'pending [u8])>,
    ) -> Option<PreparedAdmission<'_>> {
        assert_eq!(self.probe.live.get(), 0);
        self.probe.factories.set(self.probe.factories.get() + 1);
        self.probe.live.set(1);
        let lease = Lease(Rc::clone(&self.probe));
        let mut reserved: PendingCommands =
            pending.map(|(id, bytes)| (id, bytes.to_vec())).collect();
        self.probe.pending.borrow_mut().push(reserved.clone());
        Some(Box::new(move |operation, bytes| {
            let _retained = &lease;
            self.probe.calls.set(self.probe.calls.get() + 1);
            // Independent conformance provider, deliberately using the full
            // reference each time. No production prepared implementation or
            // private Counter membership/schema is used by this callback.
            let bound = self.inner.validate_proposal(
                operation,
                bytes,
                reserved.iter().map(|(id, bytes)| (*id, bytes.as_slice())),
            )?;
            reserved.push((operation, bytes.to_vec()));
            Ok(bound)
        }))
    }
}
impl ReadableStateMachine for PreparedHost {
    type Query = ();
    type ReadResult = i64;
    fn read_at(&self, index: u64, query: ()) -> Result<i64, ApplicationError> {
        self.inner.read_at(index, query)
    }
}
impl BoundedReadableStateMachine for PreparedHost {
    fn query_bytes(&self, query: &(), limit: usize) -> Result<usize, ApplicationError> {
        self.inner.query_bytes(query, limit)
    }
    fn read_result_bound(&self, query: &()) -> Result<usize, ApplicationError> {
        self.inner.read_result_bound(query)
    }
    fn read_result_bytes(&self, result: &i64, limit: usize) -> Result<usize, ApplicationError> {
        self.inner.read_result_bytes(result, limit)
    }
}
impl CheckpointStateMachine for PreparedHost {
    fn schema_version(&self) -> u64 {
        self.inner.schema_version()
    }
    fn checkpoint(&self, limit: usize) -> Result<Vec<u8>, ApplicationError> {
        self.idle();
        self.inner.checkpoint(limit)
    }
    fn restore_checkpoint(
        &mut self,
        schema: u64,
        index: u64,
        bytes: &[u8],
    ) -> Result<(), ApplicationError> {
        self.idle();
        self.inner.restore_checkpoint(schema, index, bytes)
    }
}
