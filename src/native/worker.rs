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
struct Retained {
    usage: WorkerUsage,
    control: bool,
    visits: Vec<VisitTicket>,
}
pub struct NativeLogWorker<L: LogStore + Send + 'static> {
    binding: WorkerBinding,
    limits: WorkerLimits,
    sequence: u64,
    sender: Option<SyncSender<Batch>>,
    events: Receiver<WorkerEvent>,
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
        let (sender, requests) = mpsc::sync_channel::<Batch>(limits.max_requests);
        let (out, events) = mpsc::sync_channel(limits.max_requests * 2);
        let thread = thread::Builder::new()
            .name("voteboat-wal".into())
            .spawn(move || {
                let mut fenced = false;
                while let Ok(batch) = requests.recv() {
                    let visits = batch.units.iter().map(|u| u.visit).collect::<Vec<_>>();
                    let result = if fenced {
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
                    let terminal = match result {
                        Ok(tickets) => {
                            if tickets.len() != visits.len()
                                || visits.iter().any(|v| {
                                    tickets
                                        .iter()
                                        .filter(|t| {
                                            t.binding == binding.store && t.group == v.group
                                        })
                                        .count()
                                        != 1
                                })
                            {
                                fenced = true;
                                WorkerEvent::Failed {
                                    request: batch.ticket,
                                    visits,
                                    error: StorageError::Corrupt("worker admission scope mismatch"),
                                }
                            } else {
                                let admissions = visits
                                    .iter()
                                    .map(|v| {
                                        (*v, *tickets.iter().find(|t| t.group == v.group).unwrap())
                                    })
                                    .collect();
                                let _ = out.send(WorkerEvent::Written {
                                    request: batch.ticket,
                                    admissions,
                                });
                                wake.wake();
                                match store.barrier(&tickets) {
                                    Ok(completion)
                                        if completion.tickets.len() == tickets.len()
                                            && tickets
                                                .iter()
                                                .all(|t| completion.tickets.contains(t)) =>
                                    {
                                        WorkerEvent::Durable {
                                            request: batch.ticket,
                                            visits,
                                            completion,
                                        }
                                    }
                                    Ok(_) => {
                                        fenced = true;
                                        WorkerEvent::Failed {
                                            request: batch.ticket,
                                            visits,
                                            error: StorageError::Corrupt(
                                                "worker barrier scope mismatch",
                                            ),
                                        }
                                    }
                                    Err(error) => {
                                        fenced |= fatal(&error);
                                        WorkerEvent::Failed {
                                            request: batch.ticket,
                                            visits,
                                            error,
                                        }
                                    }
                                }
                            }
                        }
                        Err(error) => {
                            fenced |= fatal(&error);
                            WorkerEvent::Failed {
                                request: batch.ticket,
                                visits,
                                error,
                            }
                        }
                    };
                    let _ = out.send(terminal);
                    wake.wake();
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
            .try_send(Batch { ticket, units })
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
                    },
                );
                Ok(ticket)
            }
            Err(TrySendError::Full(batch)) => Err(WorkerRejected {
                reason: WorkerError::Overloaded,
                units: batch.units,
            }),
            Err(TrySendError::Disconnected(batch)) => {
                self.fenced = true;
                self.sender.take();
                Err(WorkerRejected {
                    reason: WorkerError::Fenced,
                    units: batch.units,
                })
            }
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
                    let Some((&sequence, r)) = self.retained.first_key_value() else {
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
