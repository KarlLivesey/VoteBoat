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
//! Asynchronous WAL work: accepted ownership, written admission and durable
//! completion are distinct. The owner still controls every Raft transition.
use crate::{contracts::StorageError, identity::*, log::*, raft::*, runtime::*};
use std::{collections::BTreeSet, mem::size_of};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WorkerBinding {
    pub store: StoreBinding,
    pub generation: StorageWorkerGeneration,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WorkerTicket {
    pub binding: WorkerBinding,
    pub sequence: u64,
}
/// Physical maintenance admission, never a Raft durability token.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReclaimTicket {
    pub binding: WorkerBinding,
    pub sequence: u64,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReclaimEvent {
    pub request: ReclaimTicket,
    pub result: Result<LogReclaimed, StorageError>,
}
#[derive(Debug)]
pub struct PersistUnit {
    pub id: PersistUnitId,
    pub update: LogUpdate,
}
/// Exact host continuation and optional Core append-window correlation.
/// Neither identity is durability evidence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PersistUnitId {
    pub effect: EffectTicket,
    pub append: Option<AppendUnitId>,
}
impl PersistUnitId {
    pub fn visit(self) -> VisitTicket {
        self.effect.visit
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WorkerError {
    Overloaded,
    BatchTooLarge,
    WrongBinding,
    Closed,
    Fenced,
    Exhausted,
    InvalidLimits,
    Unsupported,
    Runtime(RuntimeError),
    Consensus(RaftError),
}
#[derive(Debug)]
pub struct WorkerRejected {
    pub reason: WorkerError,
    pub units: Vec<PersistUnit>,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WorkerEvent {
    Written {
        request: WorkerTicket,
        admissions: Vec<(PersistUnitId, LogTicket)>,
    },
    Durable {
        request: WorkerTicket,
        units: Vec<PersistUnitId>,
        completion: DurableLog,
    },
    Failed {
        request: WorkerTicket,
        units: Vec<PersistUnitId>,
        error: StorageError,
    },
}
impl WorkerEvent {
    pub fn request(&self) -> WorkerTicket {
        match self {
            Self::Written { request, .. }
            | Self::Durable { request, .. }
            | Self::Failed { request, .. } => *request,
        }
    }
    pub fn terminal(&self) -> bool {
        !matches!(self, Self::Written { .. })
    }
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WorkerUsage {
    pub requests: usize,
    pub units: usize,
    pub bytes: usize,
}
#[derive(Clone, Copy, Debug)]
pub struct WorkerLimits {
    pub max_requests: usize,
    pub max_units: usize,
    pub max_bytes: usize,
    pub control_requests: usize,
    pub control_units: usize,
    pub control_bytes: usize,
    pub batch_units: usize,
    pub batch_bytes: usize,
}
impl Default for WorkerLimits {
    fn default() -> Self {
        Self {
            max_requests: 64,
            max_units: 1024,
            max_bytes: 16 * 1024 * 1024,
            control_requests: 8,
            control_units: 128,
            control_bytes: 1024 * 1024,
            batch_units: 256,
            batch_bytes: 1024 * 1024,
        }
    }
}
impl WorkerLimits {
    pub fn validate(self) -> Result<Self, WorkerError> {
        if self.max_requests < 2
            || self.max_requests > 1024
            || self.max_units < 2
            || self.max_units > 65536
            || self.max_bytes == 0
            || self.max_bytes > 1024 * 1024 * 1024
            || self.control_requests == 0
            || self.control_requests >= self.max_requests
            || self.control_units == 0
            || self.control_units >= self.max_units
            || self.control_bytes == 0
            || self.control_bytes >= self.max_bytes
            || self.batch_units == 0
            || self.batch_units > self.max_units
            || self.batch_bytes == 0
            || self.batch_bytes > self.max_bytes
        {
            return Err(WorkerError::InvalidLimits);
        }
        Ok(self)
    }
}
/// Waking is only a scheduling hint. Implementations must be nonblocking and
/// must not panic. Shared host reactors/executors remain owned by the host.
pub trait WorkerWake: Send + Sync {
    fn wake(&self);
}
/// Submit transfers an owned batch only on success. Rejection returns it whole.
/// The default permits one outstanding accepted unit per group. An explicit
/// immutable capability permits bounded successive requests; each atomic submit
/// still contains at most one unit per group. Poll delivers
/// Written before Durable/Failed, with exact visit and store scopes. A terminal
/// poll releases input/output credits; polling Written alone does not. Worker
/// tickets are admissions, never durable prefixes. Closing drains accepted work;
/// abandoning a wait cannot roll it back. The host fences affected groups on
/// failure and explicitly recovers uncertain persistence. No core is mutated
/// on the worker. Snapshot references require their existing durable pin first.
pub trait PersistenceWorker {
    fn binding(&self) -> WorkerBinding;
    fn limits(&self) -> WorkerLimits;
    fn usage(&self) -> WorkerUsage;
    /// Immutable same-group outstanding-unit bound in 1..=64, defaulting to one.
    /// Successive requests remain subject to ordinary resource/admission limits;
    /// this capability reserves no capacity and grants no durability evidence.
    fn max_outstanding_units_per_group(&self) -> usize {
        1
    }
    fn submit(&mut self, units: Vec<PersistUnit>) -> Result<WorkerTicket, WorkerRejected>;
    fn poll(&mut self, limit: usize) -> Vec<WorkerEvent>;
    /// Immutable replacement-image ceiling for automatic maintenance, or None
    /// when unsupported. This capability creates no work or durability evidence.
    /// Admission can still refuse transient pressure within this ceiling.
    fn reclaim_limit(&self) -> Option<usize> {
        None
    }
    /// Optional bounded background work; scoped close drains accepted cleanup.
    /// Terminal polling releases worker admission credits. Results cannot stand
    /// in for Written/Durable events. Rejected cleanup performs no work.
    fn submit_reclaim(&mut self, _max_bytes: usize) -> Result<ReclaimTicket, WorkerError> {
        Err(WorkerError::Unsupported)
    }
    fn poll_reclaims(&mut self, _limit: usize) -> Vec<ReclaimEvent> {
        Vec::new()
    }
    fn close(&mut self);
    fn is_drained(&self) -> bool {
        self.usage().requests == 0
    }
}

/// Conservative retained-request accounting; store-internal buffers have the
/// selected LogStore's separate budgets. Vector capacity is charged. Fixed
/// completion/correlation metadata is reserved per unit before submission.
pub(crate) const PERSIST_UNIT_METADATA: usize =
    size_of::<PersistUnit>() + 3 * size_of::<PersistUnitId>() + 2 * size_of::<LogTicket>();

pub(crate) fn persist_payload_cost(update: &LogUpdate) -> Result<(usize, bool), WorkerError> {
    let mut bytes = 0usize;
    let mut control = true;
    if let Some(base) = &update.snapshot_membership {
        bytes = bytes
            .checked_add(base.retained_bytes())
            .ok_or(WorkerError::BatchTooLarge)?;
        control = false;
    }
    if let Some(suffix) = &update.suffix {
        bytes = bytes
            .checked_add(
                suffix
                    .entries
                    .capacity()
                    .checked_mul(size_of::<LogEntry>())
                    .ok_or(WorkerError::BatchTooLarge)?,
            )
            .ok_or(WorkerError::BatchTooLarge)?;
        for entry in &suffix.entries {
            if !matches!(entry.payload, EntryPayload::Noop) {
                control = false;
                bytes = bytes
                    .checked_add(entry.retained_payload_bytes())
                    .ok_or(WorkerError::BatchTooLarge)?;
            }
        }
    }
    Ok((bytes, control))
}

pub fn batch_cost(
    units: &[PersistUnit],
    capacity: usize,
    limits: WorkerLimits,
) -> Result<(usize, bool), WorkerError> {
    if units.is_empty() || units.len() > limits.batch_units || capacity > limits.batch_units {
        return Err(WorkerError::BatchTooLarge);
    }
    let mut bytes = capacity
        .checked_mul(PERSIST_UNIT_METADATA)
        .ok_or(WorkerError::BatchTooLarge)?;
    let mut control = true;
    let mut groups = BTreeSet::new();
    for unit in units {
        if unit.id.visit().group != unit.update.group || !groups.insert(unit.update.group) {
            return Err(WorkerError::WrongBinding);
        }
        let (payload, is_control) = persist_payload_cost(&unit.update)?;
        bytes = bytes
            .checked_add(payload)
            .ok_or(WorkerError::BatchTooLarge)?;
        control &= is_control;
        if bytes > limits.batch_bytes {
            return Err(WorkerError::BatchTooLarge);
        }
    }
    if bytes > limits.batch_bytes {
        return Err(WorkerError::BatchTooLarge);
    }
    Ok((bytes, control))
}

/// Validate the exact core effects before ownership transfer to a worker.
/// This stage does not admit a LogTicket or release any dependent effect.
pub fn submit_for_shard<Q: ReadyScheduler, W: PersistenceWorker>(
    shard: &mut Shard<Q>,
    worker: &mut W,
    units: Vec<PersistUnit>,
) -> Result<WorkerTicket, WorkerRejected> {
    for unit in &units {
        if worker.binding().store != unit.id.visit().owner.store {
            return Err(WorkerRejected {
                reason: WorkerError::WrongBinding,
                units,
            });
        }
        let result = shard.with_persist_unit(unit.id, |core| match unit.id.append {
            Some(id) => core.validate_append_effect(id, &unit.update),
            None => core.validate_persist_effect(&unit.update),
        });
        let error = match result {
            Err(e) => Some(WorkerError::Runtime(e)),
            Ok(Err(e)) => Some(WorkerError::Consensus(e)),
            Ok(Ok(())) => None,
        };
        if let Some(reason) = error {
            return Err(WorkerRejected { reason, units });
        }
    }
    worker.submit(units)
}
pub fn submit_for_timed<
    Q: ReadyScheduler,
    T: TimerService,
    E: ElectionEntropy,
    W: PersistenceWorker,
>(
    shard: &mut TimedShard<Q, T, E>,
    worker: &mut W,
    units: Vec<PersistUnit>,
    now: MonoTime,
) -> Result<WorkerTicket, WorkerRejected> {
    for unit in &units {
        if worker.binding().store != unit.id.visit().owner.store {
            return Err(WorkerRejected {
                reason: WorkerError::WrongBinding,
                units,
            });
        }
        let result = shard.with_persist_unit(unit.id, now, |core| match unit.id.append {
            Some(id) => core.validate_append_effect(id, &unit.update),
            None => core.validate_persist_effect(&unit.update),
        });
        let error = match result {
            Err(e) => Some(WorkerError::Runtime(e)),
            Ok(Err(e)) => Some(WorkerError::Consensus(e)),
            Ok(Ok(())) => None,
        };
        if let Some(reason) = error {
            return Err(WorkerRejected { reason, units });
        }
    }
    worker.submit(units)
}

/// Apply a worker stage on the serialized owner. No generic backend can turn
/// queue admission or a wake into a durable Raft response. Failures fence before
/// any further service; callers retain the error for outcome/recovery reporting.
pub struct WorkerDelivery {
    pub id: PersistUnitId,
    pub result: Result<Vec<Effect>, WorkerError>,
    pub retired_units: Option<Vec<AppendUnitId>>,
}
enum Task<'a> {
    Written(LogTicket),
    Durable(&'a DurableLog),
    Failed(&'a StorageError),
}
struct Applied {
    effects: Vec<Effect>,
    retired: Option<Vec<AppendUnitId>>,
}
impl Task<'_> {
    fn apply(
        self,
        raft: &mut Raft,
        id: PersistUnitId,
        replicate: bool,
    ) -> Result<Applied, RaftError> {
        let mut retired = None;
        let effects = match (self, id.append) {
            (Self::Written(ticket), Some(unit)) if replicate => {
                raft.replicate_admitted_append(unit, ticket)?
            }
            (Self::Written(ticket), Some(unit)) => {
                raft.admitted_append(unit, ticket)?;
                Vec::new()
            }
            (Self::Durable(completion), Some(unit)) => {
                match raft.complete_append_unit(unit, completion)? {
                    AppendCompletion::Waiting => Vec::new(),
                    AppendCompletion::Retired { units, effects } => {
                        retired = Some(units);
                        effects
                    }
                }
            }
            (Self::Written(ticket), None) if replicate => raft.replicate_admitted(ticket)?,
            (Self::Written(ticket), None) => {
                raft.admitted(ticket)?;
                Vec::new()
            }
            (Self::Durable(completion), None) => raft.complete(completion)?,
            (Self::Failed(error), _) => {
                raft.storage_failed();
                return Err(RaftError::Storage(error.clone()));
            }
        };
        Ok(Applied { effects, retired })
    }
}
fn deliver(
    event: WorkerEvent,
    mut core: impl FnMut(PersistUnitId, Task<'_>) -> Result<Result<Applied, RaftError>, RuntimeError>,
) -> Vec<WorkerDelivery> {
    let binding = event.request().binding.store;
    let mut results = Vec::new();
    match event {
        WorkerEvent::Written { admissions, .. } => {
            for (id, ticket) in admissions {
                if ticket.binding != binding || ticket.group != id.visit().group {
                    results.push(WorkerDelivery {
                        id,
                        result: Err(WorkerError::WrongBinding),
                        retired_units: None,
                    });
                } else {
                    results.push(delivery(id, Task::Written(ticket), binding, &mut core));
                }
            }
        }
        WorkerEvent::Durable {
            units, completion, ..
        } => {
            for id in units {
                results.push(delivery(id, Task::Durable(&completion), binding, &mut core));
            }
        }
        WorkerEvent::Failed { units, error, .. } => {
            for id in units {
                results.push(delivery(id, Task::Failed(&error), binding, &mut core));
            }
        }
    }
    results
}
fn delivery(
    id: PersistUnitId,
    task: Task<'_>,
    binding: StoreBinding,
    core: &mut impl FnMut(PersistUnitId, Task<'_>) -> Result<Result<Applied, RaftError>, RuntimeError>,
) -> WorkerDelivery {
    let valid = id.visit().owner.store == binding && id.effect.sequence != 0;
    let result = if valid {
        core(id, task)
            .map_err(WorkerError::Runtime)
            .and_then(|r| r.map_err(WorkerError::Consensus))
    } else {
        Err(WorkerError::WrongBinding)
    };
    let (result, retired_units) = match result {
        Ok(v) => (Ok(v.effects), v.retired),
        Err(e) => (Err(e), None),
    };
    WorkerDelivery {
        id,
        result,
        retired_units,
    }
}
pub fn apply_to_shard<Q: ReadyScheduler>(
    shard: &mut Shard<Q>,
    event: WorkerEvent,
) -> Vec<WorkerDelivery> {
    deliver(event, |id, task| {
        shard.with_persist_unit(id, |core| task.apply(core, id, false))
    })
}
pub fn apply_to_timed<Q: ReadyScheduler, T: TimerService, E: ElectionEntropy>(
    shard: &mut TimedShard<Q, T, E>,
    event: WorkerEvent,
    now: MonoTime,
) -> Vec<WorkerDelivery> {
    deliver(event, |id, task| {
        shard.with_persist_unit(id, now, |core| task.apply(core, id, false))
    })
}
pub(crate) fn apply_to_timed_with_replication<
    Q: ReadyScheduler,
    T: TimerService,
    E: ElectionEntropy,
>(
    shard: &mut TimedShard<Q, T, E>,
    event: WorkerEvent,
    now: MonoTime,
    mut replicate: impl FnMut(PersistUnitId) -> bool,
) -> Vec<WorkerDelivery> {
    deliver(event, |id, task| {
        shard.with_persist_unit(id, now, |core| task.apply(core, id, replicate(id)))
    })
}
