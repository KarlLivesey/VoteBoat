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
//! One explicitly constructed blocking WAL thread, shared by many groups and
//! execution owners. It owns the selected LogStore, never another WAL/runtime.
//! Already queued independent requests can share one bounded durability barrier;
//! original appends, completion scopes, admission credits and FIFO are retained.
use crate::{contracts::StorageError, identity::*, log::*, runtime::VisitTicket, worker::*};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        mpsc::{self, Receiver, SyncSender, TryRecvError, TrySendError},
        Arc,
    },
    thread::{self, JoinHandle, Thread},
};

pub struct ThreadWake {
    thread: Thread,
}
impl ThreadWake {
    pub fn current() -> Self {
        Self {
            thread: thread::current(),
        }
    }
}
impl WorkerWake for ThreadWake {
    fn wake(&self) {
        self.thread.unpark();
    }
}
struct Batch {
    ticket: WorkerTicket,
    units: Vec<PersistUnit>,
}
enum Work {
    Persist(Batch),
    Reclaim {
        ticket: ReclaimTicket,
        max_bytes: usize,
    },
}
struct Retained {
    usage: WorkerUsage,
    control: bool,
    visits: Vec<VisitTicket>,
    reclaim: bool,
}
pub struct NativeLogWorker<L: LogStore + Send + 'static> {
    binding: WorkerBinding,
    limits: WorkerLimits,
    sequence: u64,
    sender: Option<SyncSender<Work>>,
    events: Receiver<WorkerEvent>,
    reclaims: Receiver<ReclaimEvent>,
    reclaim_supported: bool,
    max_reclaim_bytes: usize,
    reclaim_pending: bool,
    thread: Option<JoinHandle<L>>,
    retained: BTreeMap<u64, Retained>,
    groups: BTreeSet<GroupIdentity>,
    usage: WorkerUsage,
    data: WorkerUsage,
    fenced: bool,
}
impl<L: LogStore + Send + 'static> NativeLogWorker<L> {
    /// The supplied store must be quiescent. Recover group states/cores before
    /// moving it here. Worker generations cannot be reused within a store session.
    /// A failed construction creates no surviving thread. Dropping a handle
    /// abandons observation, not accepted writes; use close/poll/try_reclaim for
    /// deterministic shutdown and store return. No host executor is shut down.
    pub fn spawn(
        mut store: L,
        generation: StorageWorkerGeneration,
        limits: WorkerLimits,
        wake: Arc<dyn WorkerWake>,
    ) -> Result<Self, WorkerError> {
        let limits = limits.validate()?;
        if limits.batch_units > store.limits().max_batch_units
            || limits.batch_bytes > store.limits().max_batch_bytes
        {
            return Err(WorkerError::InvalidLimits);
        }
        let binding = WorkerBinding {
            store: store.binding(),
            generation,
        };
        let max_pending_units = store.limits().max_pending_units;
        let reclaim_supported = store.supports_reclaim();
        let max_reclaim_bytes = store.limits().max_wal_bytes;
        let (sender, requests) = mpsc::sync_channel::<Work>(limits.max_requests);
        let (out, events) = mpsc::sync_channel(limits.max_requests * 2);
        let (reclaim_out, reclaims) = mpsc::sync_channel(1);
        let thread = thread::Builder::new()
            .name("voteboat-wal".into())
            .spawn(move || {
                let mut fenced = false;
                let mut deferred = None;
                while let Some(work) = deferred.take().or_else(|| requests.recv().ok()) {
                    let batch = match work {
                        Work::Persist(batch) => batch,
                        Work::Reclaim { ticket, max_bytes } => {
                            let mut result = if fenced {
                                Err(StorageError::Fenced)
                            } else {
                                match store.reclaim(max_bytes) {
                                    Ok(r)
                                        if r.after_bytes <= r.before_bytes
                                            && r.after_bytes <= max_bytes =>
                                    {
                                        Ok(r)
                                    }
                                    Ok(_) => Err(StorageError::Corrupt(
                                        "reclamation result outside budget",
                                    )),
                                    Err(e) => Err(e),
                                }
                            };
                            if store.binding() != binding.store {
                                result =
                                    Err(StorageError::Corrupt("maintenance changed store binding"));
                            }
                            if result.as_ref().is_err_and(fatal) {
                                fenced = true;
                            }
                            let _ = reclaim_out.send(ReclaimEvent {
                                request: ticket,
                                result,
                            });
                            wake.wake();
                            continue;
                        }
                    };
                    let batches =
                        ready_window(batch, &requests, &mut deferred, limits, max_pending_units);
                    persist_window(&mut store, batches, binding, &mut fenced, &out, &*wake);
                }
                store
            })
            .map_err(|_| WorkerError::Exhausted)?;
        Ok(Self {
            binding,
            limits,
            sequence: 0,
            sender: Some(sender),
            events,
            reclaims,
            reclaim_supported,
            max_reclaim_bytes,
            reclaim_pending: false,
            thread: Some(thread),
            retained: BTreeMap::new(),
            groups: BTreeSet::new(),
            usage: WorkerUsage::default(),
            data: WorkerUsage::default(),
            fenced: false,
        })
    }
    fn admission(
        &self,
        units: &[PersistUnit],
        capacity: usize,
    ) -> Result<(usize, bool), WorkerError> {
        if self.fenced {
            return Err(WorkerError::Fenced);
        }
        if self.sender.is_none() {
            return Err(WorkerError::Closed);
        }
        let (bytes, control) = batch_cost(units, capacity, self.limits)?;
        if units
            .iter()
            .any(|u| u.visit.owner.store != self.binding.store)
        {
            return Err(WorkerError::WrongBinding);
        }
        if units.iter().any(|u| self.groups.contains(&u.visit.group)) {
            return Err(WorkerError::Overloaded);
        }
        let fits = |u: WorkerUsage, r: usize, n: usize, b: usize| {
            u.requests < r
                && units.len() <= n.saturating_sub(u.units)
                && bytes <= b.saturating_sub(u.bytes)
        };
        if !fits(
            self.usage,
            self.limits.max_requests,
            self.limits.max_units,
            self.limits.max_bytes,
        ) || (!control
            && !fits(
                self.data,
                self.limits.max_requests - self.limits.control_requests,
                self.limits.max_units - self.limits.control_units,
                self.limits.max_bytes - self.limits.control_bytes,
            ))
        {
            return Err(WorkerError::Overloaded);
        }
        Ok((bytes, control))
    }
    /// Nonblocking store return after explicit close and terminal delivery. A
    /// returned store may be fenced after an I/O error; recovery is still needed.
    pub fn try_reclaim(&mut self) -> Result<Option<L>, StorageError> {
        if self.sender.is_some() {
            return Err(StorageError::Rejected("close worker before reclaim"));
        }
        if !self.retained.is_empty() {
            return Ok(None);
        }
        let thread = self
            .thread
            .as_ref()
            .ok_or(StorageError::Rejected("worker already reclaimed"))?;
        if !thread.is_finished() {
            return Ok(None);
        }
        self.thread
            .take()
            .unwrap()
            .join()
            .map(Some)
            .map_err(|_| StorageError::Uncertain("WAL worker panicked; recover store".into()))
    }
    fn release(&mut self, sequence: u64) {
        if let Some(r) = self.retained.remove(&sequence) {
            if r.reclaim {
                self.reclaim_pending = false;
            }
            for v in r.visits {
                self.groups.remove(&v.group);
            }
            self.usage.requests -= r.usage.requests;
            self.usage.units -= r.usage.units;
            self.usage.bytes -= r.usage.bytes;
            if !r.control {
                self.data.requests -= r.usage.requests;
                self.data.units -= r.usage.units;
                self.data.bytes -= r.usage.bytes;
            }
        }
    }
}
// Gather only work already in the FIFO. A deferred item is never overtaken.
fn ready_window(
    first: Batch,
    requests: &Receiver<Work>,
    deferred: &mut Option<Work>,
    limits: WorkerLimits,
    max_pending_units: usize,
) -> Vec<Batch> {
    let ceiling = limits.batch_units.min(max_pending_units);
    let mut units = first.units.len();
    let mut bytes = batch_cost(&first.units, first.units.capacity(), limits)
        .expect("admitted request retains its validated cost")
        .0;
    let mut batches = vec![first];
    while batches.len() < limits.max_requests && units < ceiling {
        let Ok(work) = requests.try_recv() else {
            break;
        };
        let Work::Persist(next) = work else {
            *deferred = Some(work);
            break;
        };
        let cost = batch_cost(&next.units, next.units.capacity(), limits)
            .expect("admitted request retains its validated cost")
            .0;
        if next.units.len() > ceiling - units || cost > limits.batch_bytes.saturating_sub(bytes) {
            *deferred = Some(Work::Persist(next));
            break;
        }
        units += next.units.len();
        bytes += cost;
        batches.reserve_exact(1);
        batches.push(next);
    }
    batches
}
struct PendingBarrier {
    request: WorkerTicket,
    visits: Vec<VisitTicket>,
}
fn persist_window<L: LogStore>(
    store: &mut L,
    batches: Vec<Batch>,
    binding: WorkerBinding,
    fenced: &mut bool,
    out: &SyncSender<WorkerEvent>,
    wake: &dyn WorkerWake,
) {
    let count = batches.iter().map(|b| b.units.len()).sum();
    let mut tickets = Vec::with_capacity(count);
    let mut pending = Vec::with_capacity(batches.len());
    let mut window_failure = None;
    for batch in batches {
        let visits = batch.units.iter().map(|u| u.visit).collect::<Vec<_>>();
        let result = if *fenced {
            Err(StorageError::Fenced)
        } else {
            store.append_batch(
                batch
                    .units
                    .into_iter()
                    .map(|u| LogMutation::Update(u.update))
                    .collect(),
            )
        };
        let result = result.and_then(|admitted| {
            if admitted.len() != visits.len()
                || visits.iter().any(|v| {
                    admitted
                        .iter()
                        .filter(|t| t.binding == binding.store && t.group == v.group)
                        .count()
                        != 1
                })
            {
                Err(StorageError::Corrupt("worker admission scope mismatch"))
            } else {
                Ok(admitted)
            }
        });
        match result {
            Ok(admitted) => {
                let admissions = visits
                    .iter()
                    .map(|v| (*v, *admitted.iter().find(|t| t.group == v.group).unwrap()))
                    .collect();
                let _ = out.send(WorkerEvent::Written {
                    request: batch.ticket,
                    admissions,
                });
                wake.wake();
                tickets.extend(admitted);
                pending.push(PendingBarrier {
                    request: batch.ticket,
                    visits,
                });
            }
            Err(error) => {
                if fatal(&error) && !*fenced {
                    window_failure = Some(error.clone());
                }
                *fenced |= fatal(&error);
                let _ = out.send(WorkerEvent::Failed {
                    request: batch.ticket,
                    visits,
                    error,
                });
                wake.wake();
            }
        }
    }
    finish_persist_window(store, fenced, tickets, pending, window_failure, out, wake);
}

fn finish_persist_window<L: LogStore>(
    store: &mut L,
    fenced: &mut bool,
    tickets: Vec<LogTicket>,
    pending: Vec<PendingBarrier>,
    window_failure: Option<StorageError>,
    out: &SyncSender<WorkerEvent>,
    wake: &dyn WorkerWake,
) {
    if pending.is_empty() {
        return;
    }
    let result = if *fenced {
        Err(window_failure.unwrap_or(StorageError::Fenced))
    } else {
        store.barrier(&tickets).and_then(|completion| {
            if completion.tickets.len() == tickets.len()
                && tickets.iter().all(|t| completion.tickets.contains(t))
            {
                // Exact union validation permits projection to each original
                // request. The provider may return the union in another order.
                Ok(())
            } else {
                Err(StorageError::Corrupt("worker barrier scope mismatch"))
            }
        })
    };
    if let Err(error) = &result {
        *fenced |= fatal(error);
    }
    let mut tickets = tickets.into_iter();
    for p in pending {
        let terminal = match &result {
            Ok(()) => {
                let completion = DurableLog {
                    tickets: tickets.by_ref().take(p.visits.len()).collect(),
                };
                WorkerEvent::Durable {
                    request: p.request,
                    visits: p.visits,
                    completion,
                }
            }
            Err(error) => WorkerEvent::Failed {
                request: p.request,
                visits: p.visits,
                error: error.clone(),
            },
        };
        let _ = out.send(terminal);
        wake.wake();
    }
}

fn fatal(error: &StorageError) -> bool {
    matches!(
        error,
        StorageError::Uncertain(_) | StorageError::Corrupt(_) | StorageError::Fenced
    )
}
impl<L: LogStore + Send + 'static> PersistenceWorker for NativeLogWorker<L> {
    fn binding(&self) -> WorkerBinding {
        self.binding
    }
    fn limits(&self) -> WorkerLimits {
        self.limits
    }
    fn usage(&self) -> WorkerUsage {
        self.usage
    }
    fn reclaim_limit(&self) -> Option<usize> {
        self.reclaim_supported.then_some(self.max_reclaim_bytes)
    }
    fn submit_reclaim(&mut self, max_bytes: usize) -> Result<ReclaimTicket, WorkerError> {
        if self.fenced {
            return Err(WorkerError::Fenced);
        }
        if self.sender.is_none() {
            return Err(WorkerError::Closed);
        }
        if !self.reclaim_supported {
            return Err(WorkerError::Unsupported);
        }
        if max_bytes == 0 || max_bytes > self.max_reclaim_bytes {
            return Err(WorkerError::BatchTooLarge);
        }
        let bytes = std::mem::size_of::<Work>()
            + std::mem::size_of::<Retained>()
            + std::mem::size_of::<ReclaimEvent>();
        if self.reclaim_pending
            || self.usage.requests == self.limits.max_requests
            || self.data.requests >= self.limits.max_requests - self.limits.control_requests
            || bytes > self.limits.max_bytes.saturating_sub(self.usage.bytes)
            || bytes
                > (self.limits.max_bytes - self.limits.control_bytes)
                    .saturating_sub(self.data.bytes)
        {
            return Err(WorkerError::Overloaded);
        }
        let sequence = self.sequence.checked_add(1).ok_or(WorkerError::Exhausted)?;
        let ticket = ReclaimTicket {
            binding: self.binding,
            sequence,
        };
        match self
            .sender
            .as_ref()
            .unwrap()
            .try_send(Work::Reclaim { ticket, max_bytes })
        {
            Ok(()) => (),
            Err(TrySendError::Full(_)) => return Err(WorkerError::Overloaded),
            Err(TrySendError::Disconnected(_)) => {
                self.fenced = true;
                self.sender.take();
                return Err(WorkerError::Fenced);
            }
        }
        self.sequence = sequence;
        self.reclaim_pending = true;
        self.usage.requests += 1;
        self.usage.bytes += bytes;
        self.data.requests += 1;
        self.data.bytes += bytes;
        self.retained.insert(
            sequence,
            Retained {
                usage: WorkerUsage {
                    requests: 1,
                    units: 0,
                    bytes,
                },
                control: false,
                visits: Vec::new(),
                reclaim: true,
            },
        );
        Ok(ticket)
    }
    fn poll_reclaims(&mut self, limit: usize) -> Vec<ReclaimEvent> {
        if limit == 0 {
            return Vec::new();
        }
        let event = match self.reclaims.try_recv() {
            Ok(event) => event,
            Err(TryRecvError::Empty) => return Vec::new(),
            Err(TryRecvError::Disconnected) => {
                if self.sender.is_some() {
                    self.fenced = true;
                    self.sender.take();
                }
                let Some((&sequence, _)) = self.retained.iter().find(|(_, r)| r.reclaim) else {
                    return Vec::new();
                };
                self.fenced = true;
                ReclaimEvent {
                    request: ReclaimTicket {
                        binding: self.binding,
                        sequence,
                    },
                    result: Err(StorageError::Uncertain(
                        "worker disconnected; recover maintenance".into(),
                    )),
                }
            }
        };
        if event.result.as_ref().is_err_and(fatal) {
            self.fenced = true;
            self.sender.take();
        }
        self.release(event.request.sequence);
        vec![event]
    }
    fn submit(&mut self, units: Vec<PersistUnit>) -> Result<WorkerTicket, WorkerRejected> {
        let (bytes, control) = match self.admission(&units, units.capacity()) {
            Ok(v) => v,
            Err(reason) => return Err(WorkerRejected { reason, units }),
        };
        let Some(sequence) = self.sequence.checked_add(1) else {
            return Err(WorkerRejected {
                reason: WorkerError::Exhausted,
                units,
            });
        };
        let ticket = WorkerTicket {
            binding: self.binding,
            sequence,
        };
        let visits = units.iter().map(|u| u.visit).collect::<Vec<_>>();
        let count = units.len();
        match self
            .sender
            .as_ref()
            .unwrap()
            .try_send(Work::Persist(Batch { ticket, units }))
        {
            Ok(()) => {
                self.sequence = sequence;
                for v in &visits {
                    self.groups.insert(v.group);
                }
                let usage = WorkerUsage {
                    requests: 1,
                    units: count,
                    bytes,
                };
                self.usage.requests += 1;
                self.usage.units += count;
                self.usage.bytes += bytes;
                if !control {
                    self.data.requests += 1;
                    self.data.units += count;
                    self.data.bytes += bytes;
                }
                self.retained.insert(
                    sequence,
                    Retained {
                        usage,
                        control,
                        visits,
                        reclaim: false,
                    },
                );
                Ok(ticket)
            }
            Err(TrySendError::Full(Work::Persist(batch))) => Err(WorkerRejected {
                reason: WorkerError::Overloaded,
                units: batch.units,
            }),
            Err(TrySendError::Disconnected(Work::Persist(batch))) => {
                self.fenced = true;
                self.sender.take();
                Err(WorkerRejected {
                    reason: WorkerError::Fenced,
                    units: batch.units,
                })
            }
            Err(_) => unreachable!("submitted persistence work returned with another kind"),
        }
    }
    fn poll(&mut self, limit: usize) -> Vec<WorkerEvent> {
        let mut events = Vec::new();
        for _ in 0..limit.min(self.limits.max_requests * 2) {
            let event = match self.events.try_recv() {
                Ok(event) => event,
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    if self.sender.is_some() {
                        self.fenced = true;
                        self.sender.take();
                    }
                    let Some((&sequence, r)) = self.retained.iter().find(|(_, r)| !r.reclaim)
                    else {
                        break;
                    };
                    self.fenced = true;
                    WorkerEvent::Failed {
                        request: WorkerTicket {
                            binding: self.binding,
                            sequence,
                        },
                        visits: r.visits.clone(),
                        error: StorageError::Uncertain(
                            "worker disconnected; recover accepted work".into(),
                        ),
                    }
                }
            };
            if let WorkerEvent::Failed { error, .. } = &event {
                if fatal(error) {
                    self.fenced = true;
                    self.sender.take();
                }
            }
            if event.terminal() {
                self.release(event.request().sequence);
            }
            events.push(event);
        }
        events
    }
    fn close(&mut self) {
        self.sender.take();
    }
}
