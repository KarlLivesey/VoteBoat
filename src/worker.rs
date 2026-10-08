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
#[derive(Debug)]
pub struct PersistUnit {
    pub visit: VisitTicket,
    pub update: LogUpdate,
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
        admissions: Vec<(VisitTicket, LogTicket)>,
    },
    Durable {
        request: WorkerTicket,
        visits: Vec<VisitTicket>,
        completion: DurableLog,
    },
    Failed {
        request: WorkerTicket,
        visits: Vec<VisitTicket>,
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
/// At most one accepted unit per group remains outstanding. Poll delivers
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
    fn submit(&mut self, units: Vec<PersistUnit>) -> Result<WorkerTicket, WorkerRejected>;
    fn poll(&mut self, limit: usize) -> Vec<WorkerEvent>;
    fn close(&mut self);
    fn is_drained(&self) -> bool {
        self.usage().requests == 0
    }
}

/// Conservative retained-request accounting; store-internal buffers have the
/// selected LogStore's separate budgets. Vector capacity is charged. Fixed
/// completion/correlation metadata is reserved per unit before submission.
pub(crate) const PERSIST_UNIT_METADATA: usize =
    size_of::<PersistUnit>() + 3 * size_of::<VisitTicket>() + 2 * size_of::<LogTicket>();

pub(crate) fn persist_payload_cost(update: &LogUpdate) -> Result<(usize, bool), WorkerError> {
    let mut bytes = 0usize;
    let mut control = true;
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
            if let EntryPayload::Command { bytes: command, .. } = &entry.payload {
                control = false;
                bytes = bytes
                    .checked_add(command.capacity())
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
        if unit.visit.group != unit.update.group || !groups.insert(unit.update.group) {
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
        if worker.binding().store != unit.visit.owner.store {
            return Err(WorkerRejected {
                reason: WorkerError::WrongBinding,
                units,
            });
        }
        let result = shard.with_core(unit.visit, |core| {
            core.validate_persist_effect(&unit.update)
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
        if worker.binding().store != unit.visit.owner.store {
            return Err(WorkerRejected {
                reason: WorkerError::WrongBinding,
                units,
            });
        }
        let result = shard.with_core(unit.visit, now, |core| {
            core.validate_persist_effect(&unit.update)
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
    pub visit: VisitTicket,
    pub result: Result<Vec<Effect>, WorkerError>,
}
enum Task<'a> {
    Written(LogTicket),
    Durable(&'a DurableLog),
    Failed(&'a StorageError),
}
impl Task<'_> {
    fn apply(self, raft: &mut Raft) -> Result<Vec<Effect>, RaftError> {
        match self {
            Self::Written(ticket) => {
                raft.admitted(ticket)?;
                Ok(vec![])
            }
            Self::Durable(completion) => raft.complete(completion),
            Self::Failed(error) => {
                raft.storage_failed();
                Err(RaftError::Storage(error.clone()))
            }
        }
    }
}
fn deliver(
    event: WorkerEvent,
    mut core: impl FnMut(VisitTicket, Task<'_>) -> Result<Result<Vec<Effect>, RaftError>, RuntimeError>,
) -> Vec<WorkerDelivery> {
    let binding = event.request().binding.store;
    let mut results = Vec::new();
    match event {
        WorkerEvent::Written { admissions, .. } => {
            for (visit, ticket) in admissions {
                let result = if visit.owner.store != binding
                    || ticket.binding != binding
                    || ticket.group != visit.group
                {
                    Err(WorkerError::WrongBinding)
                } else {
                    core(visit, Task::Written(ticket))
                        .map_err(WorkerError::Runtime)
                        .and_then(|r| r.map_err(WorkerError::Consensus))
                };
                results.push(WorkerDelivery { visit, result });
            }
        }
        WorkerEvent::Durable {
            visits, completion, ..
        } => {
            for visit in visits {
                let result = if visit.owner.store != binding {
                    Err(WorkerError::WrongBinding)
                } else {
                    core(visit, Task::Durable(&completion))
                        .map_err(WorkerError::Runtime)
                        .and_then(|r| r.map_err(WorkerError::Consensus))
                };
                results.push(WorkerDelivery { visit, result });
            }
        }
        WorkerEvent::Failed { visits, error, .. } => {
            for visit in visits {
                let result = if visit.owner.store != binding {
                    Err(WorkerError::WrongBinding)
                } else {
                    core(visit, Task::Failed(&error))
                        .map_err(WorkerError::Runtime)
                        .and_then(|r| r.map_err(WorkerError::Consensus))
                };
                results.push(WorkerDelivery { visit, result });
            }
        }
    }
    results
}
pub fn apply_to_shard<Q: ReadyScheduler>(
    shard: &mut Shard<Q>,
    event: WorkerEvent,
) -> Vec<WorkerDelivery> {
    deliver(event, |visit, task| {
        shard.with_core(visit, |core| task.apply(core))
    })
}
pub fn apply_to_timed<Q: ReadyScheduler, T: TimerService, E: ElectionEntropy>(
    shard: &mut TimedShard<Q, T, E>,
    event: WorkerEvent,
    now: MonoTime,
) -> Vec<WorkerDelivery> {
    deliver(event, |visit, task| {
        shard.with_core(visit, now, |core| task.apply(core))
    })
}
