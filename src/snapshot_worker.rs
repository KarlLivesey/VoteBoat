// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
// Unless explicitly acquired and licensed from Licensor under another license,
// the contents of this file are subject to the Reciprocal Public License
// ("RPL") Version 1.5, or subsequent versions as allowed by the RPL, and You may
// not copy or use this file in either source code or executable form, except
// in compliance with the terms and conditions of the RPL.
//
// All software distributed under the RPL is provided strictly on an "AS IS"
// basis, WITHOUT WARRANTY OF ANY KIND, EITHER EXPRESS OR IMPLIED, AND LICENSOR
// HEREBY DISCLAIMS ALL SUCH WARRANTIES, INCLUDING WITHOUT LIMITATION, ANY
// WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, QUIET
// ENJOYMENT, OR NON-INFRINGEMENT. See the RPL for specific language governing
// rights and limitations under the RPL.
//! Explicit asynchronous snapshot I/O. Publication is not log durability;
//! application installation and every Raft transition remain on the owner.
use crate::{
    application::CheckpointStateMachine,
    contracts::StorageError,
    identity::*,
    raft::{Effect, Raft, RaftError, Rpc},
    runtime::VisitTicket,
    snapshot::*,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SnapshotWorkerBinding {
    /// The authoritative WAL's recovered binding, not a snapshot store session.
    pub store: StoreBinding,
    pub generation: SnapshotWorkerGeneration,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SnapshotWorkTicket {
    pub binding: SnapshotWorkerBinding,
    pub sequence: u64,
}
#[derive(Debug)]
pub enum SnapshotJob {
    /// Reconcile only against the owner's exact durable log anchor. Publish,
    /// pin and verify the new image before returning its reference.
    Publish {
        snapshot: Snapshot,
        durable: Option<SnapshotRef>,
    },
    /// Install loads reconcile retention to this already-durable log reference.
    /// Send loads preserve all existing anchors.
    Load {
        reference: SnapshotRef,
        install: bool,
    },
}
#[derive(Debug)]
pub struct SnapshotWork {
    pub visit: VisitTicket,
    pub job: SnapshotJob,
}
#[derive(Debug)]
pub enum SnapshotOutput {
    Published(SnapshotRef),
    Loaded {
        reference: SnapshotRef,
        snapshot: Snapshot,
        reconciled: bool,
    },
}
#[derive(Debug)]
pub struct SnapshotWorkEvent {
    pub request: SnapshotWorkTicket,
    pub visit: VisitTicket,
    pub result: Result<SnapshotOutput, StorageError>,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SnapshotWorkError {
    InvalidLimits,
    WrongBinding,
    TooLarge,
    Overloaded,
    Closed,
    Fenced,
    Exhausted,
}
#[derive(Debug)]
pub struct SnapshotWorkRejected {
    pub reason: SnapshotWorkError,
    pub work: Box<SnapshotWork>,
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SnapshotWorkUsage {
    pub requests: usize,
    pub bytes: usize,
}
#[derive(Clone, Copy, Debug)]
pub struct SnapshotWorkLimits {
    pub max_groups: usize,
    pub max_requests: usize,
    pub max_bytes: usize,
    pub control_requests: usize,
    pub control_bytes: usize,
}
impl Default for SnapshotWorkLimits {
    fn default() -> Self {
        Self {
            max_groups: 4096,
            max_requests: 16,
            max_bytes: 256 * 1024 * 1024,
            control_requests: 2,
            control_bytes: 64 * 1024 * 1024,
        }
    }
}
impl SnapshotWorkLimits {
    pub fn validate(self) -> Result<Self, SnapshotWorkError> {
        if self.max_groups == 0
            || self.max_groups > 65536
            || self.max_requests < 2
            || self.max_requests > 65536
            || self.max_bytes == 0
            || self.max_bytes as u128 > 4 * 1024 * 1024 * 1024u128
            || self.control_requests == 0
            || self.control_requests >= self.max_requests
            || self.control_bytes == 0
            || self.control_bytes >= self.max_bytes
        {
            return Err(SnapshotWorkError::InvalidLimits);
        }
        Ok(self)
    }
}
/// Object-safe, construction-selected snapshot work. One accepted request per
/// group. Credits cover request retention and the maximum completion image and
/// remain charged until terminal poll. Provider scratch/codec/file memory is
/// separate. Poll transfers outputs into a separately reserved owner budget.
/// Admissions use the fixed binding and checked, strictly increasing nonzero
/// sequences; shared owners may observe gaps. Rejection returns original
/// ownership; dropping observation is not rollback.
/// Close drains accepted work and never closes a host-owned shared executor.
pub trait SnapshotWorker {
    fn binding(&self) -> SnapshotWorkerBinding;
    fn limits(&self) -> SnapshotWorkLimits;
    fn usage(&self) -> SnapshotWorkUsage;
    /// Maximum capacity-costed loaded image for this selected group. None
    /// rejects an unassigned group before I/O. Must remain stable while live.
    fn load_reservation(&self, group: GroupIdentity) -> Option<usize>;
    fn submit(&mut self, work: SnapshotWork) -> Result<SnapshotWorkTicket, SnapshotWorkRejected>;
    fn poll(&mut self, limit: usize) -> Vec<SnapshotWorkEvent>;
    fn close(&mut self);
    fn is_drained(&self) -> bool {
        self.usage().requests == 0
    }
}

/// Public-interface preflight, called before transferring work. The caller keeps
/// the original effect lease live through completion and separately reserves
/// loaded-image capacity before submitting a Load. Application validation is
/// deterministic, on a clone, and cannot establish storage durability.
pub fn prepare_snapshot_work<A: CheckpointStateMachine>(
    raft: &Raft,
    application: &A,
    visit: VisitTicket,
    effect: &Effect,
    binding: SnapshotWorkerBinding,
) -> Result<SnapshotWork, CheckpointError> {
    if visit.owner.store != binding.store
        || raft.storage_binding() != binding.store
        || visit.group != raft.state().bootstrap.group
    {
        return Err(CheckpointError::InvalidBinding);
    }
    let job = match effect {
        Effect::StageSnapshot(message) if raft.staged_matches(message) => {
            let Rpc::Snapshot { snapshot } = &message.rpc else {
                return Err(CheckpointError::InvalidBoundary);
            };
            let mut check = application.clone();
            check.restore_checkpoint(
                snapshot.metadata.application_schema,
                snapshot.metadata.index,
                &snapshot.application,
            )?;
            SnapshotJob::Publish {
                snapshot: (**snapshot).clone(),
                durable: raft.state().snapshot,
            }
        }
        Effect::SnapshotRequired { reference, .. } => SnapshotJob::Load {
            reference: *reference,
            install: false,
        },
        Effect::SnapshotInstalled(reference) if raft.state().snapshot == Some(*reference) => {
            SnapshotJob::Load {
                reference: *reference,
                install: true,
            }
        }
        _ => return Err(CheckpointError::Consensus(RaftError::WrongCompletion)),
    };
    Ok(SnapshotWork { visit, job })
}

/// Validate exact envelope before touching the core. The expected ticket and
/// visit must be recorded at successful admission, scoped to the original lease.
/// Any actual storage/install failure fences the core; an obsolete envelope is
/// rejected without poisoning a current owner. No file I/O runs here.
pub fn complete_snapshot_work<A: CheckpointStateMachine>(
    raft: &mut Raft,
    application: &mut A,
    effect: &Effect,
    expected: SnapshotWorkTicket,
    visit: VisitTicket,
    event: SnapshotWorkEvent,
) -> Result<Vec<Effect>, CheckpointError> {
    if event.request != expected
        || event.visit != visit
        || expected.sequence == 0
        || expected.binding.store != raft.storage_binding()
        || visit.owner.store != raft.storage_binding()
        || visit.group != raft.state().bootstrap.group
    {
        return Err(CheckpointError::InvalidBinding);
    }
    let result = (|| {
        let output = event.result?;
        match (effect, output) {
            (Effect::StageSnapshot(message), SnapshotOutput::Published(reference))
                if raft.staged_matches(message) =>
            {
                Ok(raft.snapshot_stored(reference)?)
            }
            (
                Effect::SnapshotRequired {
                    to,
                    context,
                    reference,
                },
                SnapshotOutput::Loaded {
                    reference: loaded,
                    snapshot,
                    reconciled: false,
                },
            ) if *reference == loaded => Ok(raft.snapshot_send(*to, *context, loaded, snapshot)?),
            (
                Effect::SnapshotInstalled(reference),
                SnapshotOutput::Loaded {
                    reference: loaded,
                    snapshot,
                    reconciled: true,
                },
            ) if *reference == loaded => {
                if raft.state().snapshot != Some(loaded)
                    || !loaded.matches(&snapshot)
                    || snapshot.metadata.bootstrap != raft.state().bootstrap
                    || application.applied_index() > loaded.index
                {
                    return Err(CheckpointError::InvalidBoundary);
                }
                let mut next = application.clone();
                next.restore_checkpoint(
                    loaded.application_schema,
                    loaded.index,
                    &snapshot.application,
                )?;
                next.apply_batch(raft.replay_committed())?;
                let effects = raft.snapshot_applied(loaded, next.applied_index())?;
                *application = next;
                Ok(effects)
            }
            _ => Err(CheckpointError::Consensus(RaftError::WrongCompletion)),
        }
    })();
    if result.is_err() {
        raft.storage_failed();
    }
    result
}

/// Capacity-based retained image cost, independent of its encoded file length.
/// Validated policy structure still receives finite traversal/metadata checks.
pub fn snapshot_image_bytes(snapshot: &Snapshot) -> Option<usize> {
    use crate::quorum::{Tree, WeightedChild};
    use std::mem::size_of;
    let b = &snapshot.metadata.bootstrap;
    if b.voter_stores.len() > 4096 || b.policy.voters().len() > 4096 {
        return None;
    }
    let mut bytes = size_of::<Snapshot>()
        .checked_add(snapshot.application.capacity())?
        .checked_add((b.voter_stores.len() + b.policy.voters().len()).checked_mul(128)?)?;
    let mut stack = vec![(b.policy.tree(), 0)];
    let mut count = 0;
    while let Some((tree, depth)) = stack.pop() {
        count += 1;
        if count > 16384 || depth > 32 {
            return None;
        }
        match tree {
            Tree::Voter(_) => (),
            Tree::Majority(children) => {
                bytes = bytes.checked_add(children.capacity().checked_mul(size_of::<Tree>())?)?;
                if children.len() > 16384 - count - stack.len() {
                    return None;
                }
                stack.extend(children.iter().map(|t| (t, depth + 1)));
            }
            Tree::Weighted(children) => {
                bytes = bytes.checked_add(
                    children
                        .capacity()
                        .checked_mul(size_of::<WeightedChild>())?,
                )?;
                if children.len() > 16384 - count - stack.len() {
                    return None;
                }
                stack.extend(children.iter().map(|t| (&t.node, depth + 1)));
            }
        }
    }
    Some(bytes)
}
/// A load reserves this finite output allowance before I/O. Providers must
/// return capacity-costed images within it; exceeding it is a provider failure.
pub fn snapshot_load_reservation(limits: SnapshotLimits) -> Option<usize> {
    limits.validate().ok()?;
    limits
        .max_application_bytes
        .checked_add(limits.max_metadata_bytes.checked_mul(8)?)?
        .checked_add(4096)
}
