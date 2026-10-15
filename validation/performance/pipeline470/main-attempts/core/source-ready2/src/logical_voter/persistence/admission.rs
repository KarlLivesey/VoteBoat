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
use super::*;
impl LogicalPersistenceWorker {
    fn admit(&self, units: &[PersistUnit], capacity: usize) -> Result<WorkerUsage, WorkerError> {
        if self.fenced {
            return Err(WorkerError::Fenced);
        }
        if self.sender.is_none() {
            return Err(WorkerError::Closed);
        }
        if self.retained.is_some() {
            return Err(WorkerError::Overloaded);
        }
        if units.len() != 1
            || units.iter().any(|u| {
                u.update.group != self.mapping.parent
                    || u.id.visit().group != self.mapping.parent
                    || u.id.visit().owner.store != self.binding.store
                    || u.id.visit().sequence == 0
                    || u.id.effect.sequence == 0
            })
        {
            return Err(WorkerError::WrongBinding);
        }
        if units.iter().any(|unit| unit.id.append.is_some()) {
            return Err(WorkerError::Unsupported);
        }
        let (bytes, control) = batch_cost(units, capacity, self.limits)?;
        let reserved = bytes
            .checked_add(std::mem::size_of::<RetainedPersist>())
            .and_then(|n| n.checked_add(std::mem::size_of::<PersistUnitId>()))
            .ok_or(WorkerError::BatchTooLarge)?;
        let ceiling = if control {
            self.limits.max_bytes
        } else {
            self.limits.max_bytes - self.limits.control_bytes
        };
        if reserved > ceiling {
            return Err(WorkerError::Overloaded);
        }
        Ok(WorkerUsage {
            requests: 1,
            units: units.len(),
            bytes: reserved,
        })
    }
    fn consume(&mut self, event: WorkerEvent) -> Option<WorkerEvent> {
        let retained = self.retained.as_mut()?;
        let valid = event.request() == retained.ticket
            && match &event {
                WorkerEvent::Written { admissions, .. } => {
                    retained.written.is_none()
                        && admissions.len() == retained.units.len()
                        && admissions
                            .iter()
                            .zip(&retained.units)
                            .all(|((v, t), original)| {
                                v == original
                                    && t.binding == self.binding.store
                                    && t.group == v.visit().group
                            })
                }
                WorkerEvent::Durable {
                    units, completion, ..
                } => {
                    units == &retained.units
                        && retained.written.as_ref().is_some_and(|ticket| {
                            completion.tickets.as_slice() == std::slice::from_ref(ticket)
                        })
                }
                WorkerEvent::Failed { units, .. } => units == &retained.units,
            };
        if !valid {
            self.fenced = true;
            return Some(self.failure("logical persistence output mismatch"));
        }
        if let WorkerEvent::Written { admissions, .. } = &event {
            retained.written = Some(admissions[0].1);
        }
        if event.terminal() {
            if matches!(event, WorkerEvent::Failed { .. }) {
                self.fenced = true;
            }
            self.retained = None;
            self.outstanding.store(false, Ordering::Release);
        }
        Some(event)
    }
    fn failure(&mut self, message: &str) -> WorkerEvent {
        let retained = self.retained.take().expect("original outstanding work");
        self.outstanding.store(false, Ordering::Release);
        self.fenced = true;
        WorkerEvent::Failed {
            request: retained.ticket,
            units: retained.units,
            error: StorageError::Uncertain(message.to_owned()),
        }
    }
}
impl PersistenceWorker for LogicalPersistenceWorker {
    fn binding(&self) -> WorkerBinding {
        self.binding
    }
    fn limits(&self) -> WorkerLimits {
        self.limits
    }
    fn usage(&self) -> WorkerUsage {
        self.retained
            .as_ref()
            .map_or(WorkerUsage::default(), |r| r.usage)
    }
    fn submit(&mut self, units: Vec<PersistUnit>) -> Result<WorkerTicket, WorkerRejected> {
        let usage = match self.admit(&units, units.capacity()) {
            Ok(u) => u,
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
        let ids = units.iter().map(|u| u.id).collect();
        self.outstanding.store(true, Ordering::Release);
        match self
            .sender
            .as_ref()
            .expect("checked open worker")
            .try_send(PersistBatch { ticket, units })
        {
            Ok(()) => {
                self.sequence = sequence;
                self.retained = Some(RetainedPersist {
                    ticket,
                    units: ids,
                    usage,
                    written: None,
                });
                Ok(ticket)
            }
            Err(error) => {
                self.outstanding.store(false, Ordering::Release);
                let (reason, batch) = match error {
                    TrySendError::Full(b) => (WorkerError::Overloaded, b),
                    TrySendError::Disconnected(b) => {
                        self.fenced = true;
                        (WorkerError::Fenced, b)
                    }
                };
                Err(WorkerRejected {
                    reason,
                    units: batch.units,
                })
            }
        }
    }
    fn poll(&mut self, limit: usize) -> Vec<WorkerEvent> {
        let mut events = Vec::new();
        for _ in 0..limit.min(2) {
            match self.events.try_recv() {
                Ok(event) => {
                    if let Some(event) = self.consume(event) {
                        events.push(event);
                    }
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    if self.retained.is_some() {
                        events.push(self.failure("logical child bridge lost"));
                    }
                    break;
                }
            }
        }
        events
    }
    fn close(&mut self) {
        self.sender = None;
    }
}
