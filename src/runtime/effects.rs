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
//! Reserved output ownership around a timed shard. No I/O or application runs
//! implicitly. Owned effects remain charged through rejection and external work.
use super::*;
use crate::{log::LogEntry, worker::*};
use std::collections::BTreeSet;

#[derive(Clone, Copy, Debug)]
pub struct EffectOwnerLimits {
    pub active_visits: usize,
    pub reserved_bytes: usize,
    pub control_visits: usize,
    pub control_bytes: usize,
}
impl Default for EffectOwnerLimits {
    fn default() -> Self {
        Self {
            active_visits: 256,
            reserved_bytes: 256 * 1024 * 1024,
            control_visits: 16,
            control_bytes: 32 * 1024 * 1024,
        }
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EffectOwnerError {
    InvalidLimits,
    Overloaded,
    ReservationTooLarge,
    StaleEffect,
    StaleCompletion,
    WrongWorker,
    PersistenceEffect,
    NotApplied,
    ProviderContract,
    Runtime(RuntimeError),
    Consensus(RaftError),
    Worker(WorkerError),
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EffectTicket {
    pub visit: VisitTicket,
    pub sequence: u64,
}
#[derive(Debug)]
pub struct EffectLease {
    pub ticket: EffectTicket,
    pub effect: Effect,
}
#[derive(Debug)]
pub struct EffectRejected {
    pub reason: EffectOwnerError,
    pub lease: Box<EffectLease>,
}
#[derive(Debug)]
pub struct EffectBatchRejected {
    pub reason: EffectOwnerError,
    pub leases: Vec<EffectLease>,
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct EffectOwnerUsage {
    pub active_visits: usize,
    pub reserved_bytes: usize,
    pub leased: usize,
    pub persistence_requests: usize,
}
#[derive(Debug)]
pub struct OwnerStep {
    pub admission: Option<AdmissionTicket>,
    pub proposed: Option<ProposalPosition>,
    pub visit: VisitTicket,
    pub operation: Option<OperationId>,
    pub read: Option<ReadRequestId>,
    pub error: Option<RaftError>,
}
struct Active {
    visit: VisitTicket,
    reserved: usize,
    effects: VecDeque<Effect>,
    leased: Option<EffectTicket>,
    kind: Option<LeaseKind>,
    waiting: bool,
    ready: bool,
    control: bool,
}
#[derive(Clone, Copy, Eq, PartialEq)]
enum LeaseKind {
    Persist,
    Send(NodeId),
    Committed(u64),
    Read(ReadBarrier),
    External,
}
fn kind(effect: &Effect) -> LeaseKind {
    match effect {
        Effect::Persist(_) => LeaseKind::Persist,
        Effect::Send(message) => LeaseKind::Send(message.to),
        Effect::Committed(entries) => LeaseKind::Committed(entries.last().map_or(0, |e| e.index)),
        Effect::ReadReady(barrier) => LeaseKind::Read(*barrier),
        _ => LeaseKind::External,
    }
}
struct Request {
    visits: Vec<VisitTicket>,
    written: bool,
}

/// One serialized owner with pre-reserved output space. It advances one event
/// per visit, batches persistence across visits and holds that visit until all
/// outputs, external leases and core dependencies resolve. Other ready groups
/// remain runnable. Mechanism providers and application/snapshot workers stay
/// host-owned; the owner creates no thread, store, listener or hidden queue.
///
/// A lease transfers the effect but retains its reservation. Release Send only
/// after a bounded outbound queue accepts it; acknowledge committed work only
/// after ordered application. ReadReady is consumed on the core before its
/// completion callback. Persist leases use submit_persists, never generic release.
/// Callbacks run on the serialized owner and must not perform blocking I/O.
/// SnapshotRouter retains snapshot leases through asynchronous work and
/// delivers exact completions through the same serialized callback path. Extend
/// a reservation before starting host work whose result can exceed the original bound.
pub struct EffectOwner<Q: ReadyScheduler, T: TimerService, E: ElectionEntropy> {
    runtime: TimedShard<Q, T, E>,
    worker: WorkerBinding,
    limits: EffectOwnerLimits,
    active: BTreeMap<GroupIdentity, Active>,
    ready: VecDeque<GroupIdentity>,
    requests: BTreeMap<u64, Request>,
    reserved: usize,
    sequence: u64,
    failed: Option<EffectOwnerError>,
}
impl<Q: ReadyScheduler, T: TimerService, E: ElectionEntropy> EffectOwner<Q, T, E> {
    pub fn new(
        runtime: TimedShard<Q, T, E>,
        worker: WorkerBinding,
        limits: EffectOwnerLimits,
    ) -> Result<Self, EffectOwnerError> {
        runtime
            .validate_quiescent()
            .map_err(EffectOwnerError::Runtime)?;
        if worker.store != runtime.owner().store {
            return Err(EffectOwnerError::WrongWorker);
        }
        if limits.active_visits == 0
            || limits.active_visits > 65536
            || limits.reserved_bytes == 0
            || limits.reserved_bytes > 4usize.saturating_mul(1024 * 1024 * 1024)
            || limits.control_visits == 0
            || limits.control_visits >= limits.active_visits
            || limits.control_bytes == 0
            || limits.control_bytes >= limits.reserved_bytes
        {
            return Err(EffectOwnerError::InvalidLimits);
        }
        for group in runtime.groups() {
            if runtime
                .core(group)
                .unwrap()
                .effect_reservation(runtime.limits().max_event_bytes)
                .is_none_or(|n| n > limits.reserved_bytes)
            {
                return Err(EffectOwnerError::ReservationTooLarge);
            }
        }
        Ok(Self {
            runtime,
            worker,
            limits,
            active: BTreeMap::new(),
            ready: VecDeque::new(),
            requests: BTreeMap::new(),
            reserved: 0,
            sequence: 0,
            failed: None,
        })
    }
    fn check(&self) -> Result<(), EffectOwnerError> {
        self.failed.clone().map_or(Ok(()), Err)
    }
    pub(super) fn fail<R>(&mut self, error: EffectOwnerError) -> Result<R, EffectOwnerError> {
        let groups = self.runtime.groups().collect::<Vec<_>>();
        for group in groups {
            let _ = self.runtime.stop_group(group);
        }
        self.active.retain(|_, a| {
            a.effects.clear();
            a.ready = false;
            if a.leased.is_none() {
                self.reserved -= a.reserved;
                false
            } else {
                true
            }
        });
        self.ready.clear();
        self.failed = Some(error.clone());
        Err(error)
    }
    pub fn identity(&self) -> RuntimeOwner {
        self.runtime.owner()
    }
    pub fn validate_quiescent(&self) -> Result<(), EffectOwnerError> {
        self.check()?;
        self.runtime
            .validate_quiescent()
            .map_err(EffectOwnerError::Runtime)
    }
    pub fn limits(&self) -> EffectOwnerLimits {
        self.limits
    }
    pub fn worker_binding(&self) -> WorkerBinding {
        self.worker
    }
    pub fn groups(&self) -> impl Iterator<Item = GroupIdentity> + '_ {
        self.runtime.groups()
    }
    pub fn is_failed(&self) -> bool {
        self.failed.is_some()
    }
    pub(super) fn reserve_snapshot(
        &mut self,
        lease: &EffectLease,
        extra: usize,
    ) -> Result<(), EffectOwnerError> {
        self.validate_lease(lease)?;
        let a = self.active.get_mut(&lease.ticket.visit.group).unwrap();
        let capacity = a.effects.capacity();
        let needed = effect_bytes(a.effects.make_contiguous(), capacity)
            .and_then(|n| n.checked_add(effect_bytes(std::slice::from_ref(&lease.effect), 1)?))
            .and_then(|n| n.checked_add(extra))
            .ok_or(EffectOwnerError::ReservationTooLarge)?;
        let ceiling = if a.control {
            self.limits.reserved_bytes
        } else {
            self.limits.reserved_bytes - self.limits.control_bytes
        };
        if needed > ceiling {
            return Err(EffectOwnerError::ReservationTooLarge);
        }
        let additional = needed.saturating_sub(a.reserved);
        self.extend_reservation(lease.ticket, additional)
    }
    pub fn core(&self, group: GroupIdentity) -> Option<&Raft> {
        self.runtime.core(group)
    }
    /// Includes queued effects and a Send lease held by a downstream driver.
    pub fn has_pending_send(&self, peer: NodeId) -> bool {
        self.active.values().any(|a| {
            a.kind == Some(LeaseKind::Send(peer))
                || a.effects
                    .iter()
                    .any(|e| matches!(e,Effect::Send(m) if m.to == peer))
        })
    }
    pub fn deadline(&self, group: GroupIdentity) -> Option<TimerToken> {
        self.runtime.deadline(group)
    }
    pub fn usage(&self) -> EffectOwnerUsage {
        EffectOwnerUsage {
            active_visits: self.active.len(),
            reserved_bytes: self.reserved,
            leased: self.active.values().filter(|a| a.leased.is_some()).count(),
            persistence_requests: self.requests.len(),
        }
    }
    pub fn set_connection_budget(
        &mut self,
        budget: ConnectionBudget,
    ) -> Result<(), EffectOwnerError> {
        self.check()?;
        self.runtime
            .set_connection_budget(budget)
            .map_err(EffectOwnerError::Runtime)
    }
    pub fn connection_budget(&self) -> Option<&ConnectionBudget> {
        self.runtime.connection_budget()
    }
    pub(super) fn prepare_connection_budget(
        &self,
        budget: ConnectionBudget,
    ) -> Result<ConnectionBudget, EffectOwnerError> {
        self.check()?;
        self.runtime
            .prepare_connection_budget(budget)
            .map_err(EffectOwnerError::Runtime)
    }
    pub(super) fn install_connection_budget(&mut self, budget: ConnectionBudget) {
        self.runtime.install_connection_budget(budget);
    }
    pub fn reserved_connection_peers(&self) -> Result<Option<usize>, EffectOwnerError> {
        self.check()?;
        self.runtime
            .reserved_connection_peers()
            .map_err(EffectOwnerError::Runtime)
    }
    pub fn admit(&mut self, group: GroupIdentity, event: Event) -> Result<(), Rejected> {
        if let Err(reason) = self.check_input_reservation(group, &event) {
            return Err(Rejected {
                reason,
                event: Box::new(event),
            });
        }
        self.runtime.admit(group, event)
    }
    pub fn admit_tracked(
        &mut self,
        group: GroupIdentity,
        event: Event,
    ) -> Result<AdmissionTicket, Rejected> {
        if let Err(reason) = self.check_input_reservation(group, &event) {
            return Err(Rejected {
                reason,
                event: Box::new(event),
            });
        }
        self.runtime.admit_tracked(group, event)
    }
    fn check_input_reservation(
        &self,
        group: GroupIdentity,
        event: &Event,
    ) -> Result<(), RuntimeError> {
        if self.failed.is_some() {
            return Err(RuntimeError::Fenced);
        }
        let (class, _) = self.runtime.admission_shape(group, event)?;
        let capacity = if class == Class::Control {
            self.limits.reserved_bytes
        } else {
            self.limits.reserved_bytes - self.limits.control_bytes
        };
        self.runtime
            .core(group)
            .unwrap()
            .event_effect_reservation(event, self.runtime.limits().max_event_bytes)
            .filter(|bound| *bound <= capacity)
            .map(|_| ())
            .ok_or(RuntimeError::EventTooLarge)
    }
    fn refresh(&mut self, group: GroupIdentity) -> Result<(), EffectOwnerError> {
        let a = self
            .active
            .get_mut(&group)
            .ok_or(EffectOwnerError::StaleEffect)?;
        if a.leased.is_none() && !a.effects.is_empty() && !a.ready {
            a.ready = true;
            self.ready.push_back(group);
        }
        if a.leased.is_none()
            && a.effects.is_empty()
            && !a.waiting
            && !self.runtime.core(group).unwrap().has_pending_dependency()
        {
            let a = self.active.remove(&group).unwrap();
            self.runtime
                .finish(a.visit)
                .map_err(EffectOwnerError::Runtime)?;
            self.reserved -= a.reserved;
        }
        Ok(())
    }
    fn install(
        &mut self,
        visit: VisitTicket,
        effects: Vec<Effect>,
    ) -> Result<(), EffectOwnerError> {
        let a = self
            .active
            .get_mut(&visit.group)
            .ok_or(EffectOwnerError::StaleEffect)?;
        if a.visit != visit || !a.effects.is_empty() || a.leased.is_some() {
            return self.fail(EffectOwnerError::ProviderContract);
        }
        if effect_bytes(&effects, effects.capacity()).is_none_or(|n| n > a.reserved) {
            return self.fail(EffectOwnerError::ReservationTooLarge);
        }
        a.effects = effects.into();
        self.refresh(visit.group)
    }
    /// Bounded ready visits. Returned invocation errors are not client success;
    /// the host separately budgets result retention and routes original IDs.
    pub fn advance(
        &mut self,
        now: MonoTime,
        limit: usize,
    ) -> Result<Vec<OwnerStep>, EffectOwnerError> {
        self.advance_checked(now, limit, |_, _| Ok(()))
    }
    pub(super) fn advance_checked(
        &mut self,
        now: MonoTime,
        limit: usize,
        check: impl FnMut(&Raft, &Event) -> Result<(), RaftError>,
    ) -> Result<Vec<OwnerStep>, EffectOwnerError> {
        let result = self.advance_inner(now, limit, check);
        match result {
            Err(error @ (EffectOwnerError::Runtime(_) | EffectOwnerError::ReservationTooLarge)) => {
                self.fail(error)
            }
            other => other,
        }
    }
    fn advance_inner(
        &mut self,
        now: MonoTime,
        limit: usize,
        mut check: impl FnMut(&Raft, &Event) -> Result<(), RaftError>,
    ) -> Result<Vec<OwnerStep>, EffectOwnerError> {
        self.check()?;
        if limit > 4096 {
            return Err(EffectOwnerError::InvalidLimits);
        }
        let mut steps = Vec::new();
        self.runtime
            .poll_timers(now)
            .map_err(EffectOwnerError::Runtime)?;
        for _ in 0..limit {
            if self.active.len() >= self.limits.active_visits {
                break;
            }
            let Some(visit) = self.runtime.poll(now).map_err(EffectOwnerError::Runtime)? else {
                break;
            };
            let Some(reserved) = self
                .runtime
                .next_effect_reservation(visit, now)
                .map_err(EffectOwnerError::Runtime)?
            else {
                self.runtime
                    .finish(visit)
                    .map_err(EffectOwnerError::Runtime)?;
                continue;
            };
            if reserved > self.limits.reserved_bytes {
                return self.fail(EffectOwnerError::ReservationTooLarge);
            }
            if reserved > self.limits.reserved_bytes - self.reserved {
                self.runtime
                    .finish(visit)
                    .map_err(EffectOwnerError::Runtime)?;
                continue;
            }
            let control = self
                .runtime
                .next_class(visit, now)
                .map_err(EffectOwnerError::Runtime)?
                == Some(MessageClass::Control);
            if !control {
                if reserved > self.limits.reserved_bytes - self.limits.control_bytes {
                    return self.fail(EffectOwnerError::ReservationTooLarge);
                }
                let bulk_count = self.active.values().filter(|a| !a.control).count();
                let bulk_bytes = self
                    .active
                    .values()
                    .filter(|a| !a.control)
                    .map(|a| a.reserved)
                    .sum::<usize>();
                if bulk_count >= self.limits.active_visits - self.limits.control_visits
                    || reserved
                        > (self.limits.reserved_bytes - self.limits.control_bytes)
                            .saturating_sub(bulk_bytes)
                {
                    self.runtime
                        .finish(visit)
                        .map_err(EffectOwnerError::Runtime)?;
                    continue;
                }
            }
            self.reserved += reserved;
            self.active.insert(
                visit.group,
                Active {
                    visit,
                    reserved,
                    effects: VecDeque::new(),
                    leased: None,
                    kind: None,
                    waiting: false,
                    ready: false,
                    control,
                },
            );
            let stepped = self
                .runtime
                .step_next_checked(visit, now, &mut check)
                .map_err(EffectOwnerError::Runtime)?;
            if let Some(step) = stepped {
                let (effects, error) = match step.result {
                    Ok(e) => (e, None),
                    Err(e) => (Vec::new(), Some(e)),
                };
                self.install(visit, effects)?;
                steps.push(OwnerStep {
                    admission: step.admission,
                    proposed: step.proposed,
                    visit,
                    operation: step.operation,
                    read: step.read,
                    error,
                });
            } else {
                self.refresh(visit.group)?;
            }
        }
        Ok(steps)
    }
    pub fn take_effect(&mut self) -> Result<Option<EffectLease>, EffectOwnerError> {
        self.check()?;
        while let Some(group) = self.ready.pop_front() {
            let Some(a) = self.active.get_mut(&group) else {
                continue;
            };
            a.ready = false;
            if a.leased.is_some() || a.effects.is_empty() {
                continue;
            }
            self.sequence = self
                .sequence
                .checked_add(1)
                .ok_or(EffectOwnerError::ProviderContract)?;
            let ticket = EffectTicket {
                visit: a.visit,
                sequence: self.sequence,
            };
            let effect = a.effects.pop_front().unwrap();
            if a.effects.is_empty() {
                a.effects = VecDeque::new();
            }
            a.leased = Some(ticket);
            a.kind = Some(kind(&effect));
            return Ok(Some(EffectLease { ticket, effect }));
        }
        Ok(None)
    }
    fn live(&self, ticket: EffectTicket) -> Result<(), EffectOwnerError> {
        self.check()?;
        if self
            .active
            .get(&ticket.visit.group)
            .is_none_or(|a| a.visit != ticket.visit || a.leased != Some(ticket))
        {
            return Err(EffectOwnerError::StaleEffect);
        }
        Ok(())
    }
    pub(super) fn validate_lease(&self, lease: &EffectLease) -> Result<(), EffectOwnerError> {
        self.live(lease.ticket)?;
        if self.active[&lease.ticket.visit.group].kind != Some(kind(&lease.effect)) {
            return Err(EffectOwnerError::StaleEffect);
        }
        Ok(())
    }
    /// Consume abandoned external output after the owner is fenced. Accepted
    /// storage still has an unknown outcome and must be drained/recovered by its owner.
    pub fn discard_failed(&mut self, lease: EffectLease) -> Result<(), EffectRejected> {
        let valid = self.failed.is_some()
            && self
                .active
                .get(&lease.ticket.visit.group)
                .is_some_and(|a| a.visit == lease.ticket.visit && a.leased == Some(lease.ticket));
        if !valid {
            return Err(EffectRejected {
                reason: EffectOwnerError::StaleEffect,
                lease: Box::new(lease),
            });
        }
        let a = self.active.remove(&lease.ticket.visit.group).unwrap();
        self.reserved -= a.reserved;
        Ok(())
    }
    pub(super) fn discard_failed_transfer(
        &mut self,
        ticket: EffectTicket,
    ) -> Result<(), EffectOwnerError> {
        if self.failed.is_none()
            || self
                .active
                .get(&ticket.visit.group)
                .is_none_or(|a| a.visit != ticket.visit || a.leased != Some(ticket))
        {
            return Err(EffectOwnerError::StaleEffect);
        }
        let a = self.active.remove(&ticket.visit.group).unwrap();
        self.reserved -= a.reserved;
        Ok(())
    }
    pub fn extend_reservation(
        &mut self,
        ticket: EffectTicket,
        bytes: usize,
    ) -> Result<(), EffectOwnerError> {
        self.live(ticket)?;
        if bytes > self.limits.reserved_bytes - self.reserved {
            return Err(EffectOwnerError::Overloaded);
        }
        if !self.active[&ticket.visit.group].control {
            let bulk = self
                .active
                .values()
                .filter(|a| !a.control)
                .map(|a| a.reserved)
                .sum::<usize>();
            if bytes > (self.limits.reserved_bytes - self.limits.control_bytes).saturating_sub(bulk)
            {
                return Err(EffectOwnerError::Overloaded);
            }
        }
        self.reserved += bytes;
        self.active.get_mut(&ticket.visit.group).unwrap().reserved += bytes;
        Ok(())
    }
    pub fn complete_effect<R>(
        &mut self,
        lease: EffectLease,
        applied_index: u64,
        now: MonoTime,
        callback: impl FnOnce(&mut Raft) -> Result<(Vec<Effect>, R), RaftError>,
    ) -> Result<R, EffectRejected> {
        self.complete_effect_with(lease, applied_index, now, |core, _| callback(core))
    }
    /// Complete against the original owned effect without cloning a potentially
    /// large snapshot. The callback still runs on the serialized core owner.
    pub fn complete_effect_with<R>(
        &mut self,
        lease: EffectLease,
        applied_index: u64,
        now: MonoTime,
        callback: impl FnOnce(&mut Raft, &Effect) -> Result<(Vec<Effect>, R), RaftError>,
    ) -> Result<R, EffectRejected> {
        self.complete_effect_inner(lease, applied_index, now, false, callback)
    }
    /// Abandon only this exact ready read, including while application lags.
    /// No query executes and no durability/application evidence is produced.
    pub fn cancel_read(&mut self, lease: EffectLease, now: MonoTime) -> Result<(), EffectRejected> {
        if !matches!(lease.effect, Effect::ReadReady(_)) {
            return Err(EffectRejected {
                reason: EffectOwnerError::StaleEffect,
                lease: Box::new(lease),
            });
        }
        self.complete_effect_inner(lease, 0, now, true, |_, _| Ok((vec![], ())))
    }
    fn complete_effect_inner<R>(
        &mut self,
        lease: EffectLease,
        applied_index: u64,
        now: MonoTime,
        cancel_read: bool,
        callback: impl FnOnce(&mut Raft, &Effect) -> Result<(Vec<Effect>, R), RaftError>,
    ) -> Result<R, EffectRejected> {
        let result = (|| {
            self.validate_lease(&lease)?;
            if matches!(lease.effect, Effect::Persist(_)) {
                return Err(EffectOwnerError::PersistenceEffect);
            }
            if let Effect::Committed(entries) = &lease.effect {
                if entries.last().is_some_and(|e| applied_index < e.index) {
                    return Err(EffectOwnerError::NotApplied);
                }
            }
            if let Effect::ReadReady(barrier) = &lease.effect {
                self.runtime
                    .with_core(lease.ticket.visit, now, |core| {
                        if cancel_read {
                            match core.step(Event::CancelRead {
                                request: barrier.request(),
                            }) {
                                Ok(effects) if effects.is_empty() => Ok(()),
                                Err(RaftError::StaleRead) => Ok(()),
                                Err(error) => Err(error),
                                Ok(_) => Err(RaftError::StaleRead),
                            }
                        } else {
                            core.finish_read(barrier, applied_index)
                        }
                    })
                    .map_err(EffectOwnerError::Runtime)?
                    .map_err(EffectOwnerError::Consensus)?;
            }
            let result = self
                .runtime
                .with_core(lease.ticket.visit, now, |core| {
                    callback(core, &lease.effect)
                })
                .map_err(EffectOwnerError::Runtime)?;
            let (effects, result) = match result {
                Ok(result) => result,
                Err(error) => return self.fail(EffectOwnerError::Consensus(error)),
            };
            let a = self.active.get_mut(&lease.ticket.visit.group).unwrap();
            let capacity = a.effects.capacity();
            let retained = effect_bytes(a.effects.make_contiguous(), capacity)
                .and_then(|n| n.checked_add(effect_bytes(&effects, effects.capacity())?))
                .and_then(|n| n.checked_add(effect_bytes(std::slice::from_ref(&lease.effect), 1)?));
            if retained.is_none_or(|n| n > a.reserved) {
                return self.fail(EffectOwnerError::ReservationTooLarge);
            }
            a.effects.extend(effects);
            a.leased = None;
            a.kind = None;
            self.refresh(lease.ticket.visit.group)?;
            Ok(result)
        })();
        if let Err(error @ EffectOwnerError::Runtime(_)) = &result {
            let _ = self.fail::<()>(error.clone());
        }
        result.map_err(|reason| EffectRejected {
            reason,
            lease: Box::new(lease),
        })
    }
    pub fn release(
        &mut self,
        lease: EffectLease,
        applied_index: u64,
        now: MonoTime,
    ) -> Result<(), EffectRejected> {
        self.complete_effect(lease, applied_index, now, |_| Ok((vec![], ())))
    }
    /// Release a Send lease after its owned message has moved into another
    /// bounded queue. This avoids cloning a message merely to return its lease.
    pub fn release_transferred_send(
        &mut self,
        ticket: EffectTicket,
    ) -> Result<(), EffectOwnerError> {
        self.live(ticket)?;
        let a = self.active.get_mut(&ticket.visit.group).unwrap();
        if !matches!(a.kind, Some(LeaseKind::Send(_))) {
            return Err(EffectOwnerError::StaleEffect);
        }
        a.leased = None;
        a.kind = None;
        self.refresh(ticket.visit.group)
    }
    pub fn submit_persists<W: PersistenceWorker>(
        &mut self,
        worker: &mut W,
        leases: Vec<EffectLease>,
        now: MonoTime,
    ) -> Result<WorkerTicket, EffectBatchRejected> {
        let checked = (|| {
            self.check()?;
            if worker.binding() != self.worker {
                return Err(EffectOwnerError::WrongWorker);
            }
            if leases.is_empty() || leases.capacity() > worker.limits().batch_units {
                return Err(EffectOwnerError::Worker(WorkerError::BatchTooLarge));
            }
            let mut groups = BTreeSet::new();
            for lease in &leases {
                self.validate_lease(lease)?;
                if !self.active[&lease.ticket.visit.group].effects.is_empty() {
                    return Err(EffectOwnerError::ProviderContract);
                }
                if !groups.insert(lease.ticket.visit.group)
                    || !matches!(lease.effect, Effect::Persist(_))
                {
                    return Err(EffectOwnerError::PersistenceEffect);
                }
            }
            Ok(())
        })();
        if let Err(reason) = checked {
            return Err(EffectBatchRejected { reason, leases });
        }
        let tickets = leases.iter().map(|l| l.ticket).collect::<Vec<_>>();
        let units = leases
            .into_iter()
            .map(|lease| {
                let Effect::Persist(update) = lease.effect else {
                    unreachable!()
                };
                PersistUnit {
                    visit: lease.ticket.visit,
                    update,
                }
            })
            .collect();
        match submit_for_timed(&mut self.runtime, worker, units, now) {
            Err(rejected) => {
                let leases = rejected
                    .units
                    .into_iter()
                    .zip(tickets)
                    .map(|(unit, ticket)| EffectLease {
                        ticket,
                        effect: Effect::Persist(unit.update),
                    })
                    .collect();
                Err(EffectBatchRejected {
                    reason: EffectOwnerError::Worker(rejected.reason),
                    leases,
                })
            }
            Ok(request) => {
                if request.binding != self.worker
                    || request.sequence == 0
                    || self.requests.contains_key(&request.sequence)
                {
                    for ticket in &tickets {
                        let a = self.active.get_mut(&ticket.visit.group).unwrap();
                        a.leased = None;
                        a.kind = None;
                    }
                    let _ = self.fail::<()>(EffectOwnerError::ProviderContract);
                    return Err(EffectBatchRejected {
                        reason: EffectOwnerError::ProviderContract,
                        leases: vec![],
                    });
                }
                let visits = tickets.iter().map(|t| t.visit).collect();
                self.requests.insert(
                    request.sequence,
                    Request {
                        visits,
                        written: false,
                    },
                );
                for ticket in tickets {
                    let a = self.active.get_mut(&ticket.visit.group).unwrap();
                    a.leased = None;
                    a.kind = None;
                    a.waiting = true;
                }
                Ok(request)
            }
        }
    }
    pub fn deliver_worker(
        &mut self,
        event: WorkerEvent,
        now: MonoTime,
    ) -> Result<(), EffectOwnerError> {
        self.check()?;
        let request = event.request();
        if request.binding != self.worker {
            return Err(EffectOwnerError::StaleCompletion);
        }
        let Some(pending) = self.requests.get(&request.sequence) else {
            return Err(EffectOwnerError::StaleCompletion);
        };
        let visits: Vec<_> = match &event {
            WorkerEvent::Written { admissions, .. } => admissions.iter().map(|(v, _)| *v).collect(),
            WorkerEvent::Durable { visits, .. } | WorkerEvent::Failed { visits, .. } => {
                visits.clone()
            }
        };
        let actual: BTreeSet<_> = visits.iter().map(|v| (v.group, v.sequence)).collect();
        let expected: BTreeSet<_> = pending
            .visits
            .iter()
            .map(|v| (v.group, v.sequence))
            .collect();
        if visits.len() != pending.visits.len()
            || actual != expected
            || visits.iter().any(|v| v.owner != self.runtime.owner())
        {
            return self.fail(EffectOwnerError::ProviderContract);
        }
        if matches!(event, WorkerEvent::Written { .. }) && pending.written {
            return Err(EffectOwnerError::StaleCompletion);
        }
        if matches!(event, WorkerEvent::Durable { .. }) && !pending.written {
            return self.fail(EffectOwnerError::ProviderContract);
        }
        let terminal = event.terminal();
        if terminal {
            self.requests.remove(&request.sequence);
        } else {
            self.requests.get_mut(&request.sequence).unwrap().written = true;
        }
        for delivery in apply_to_timed(&mut self.runtime, event, now) {
            if terminal {
                self.active
                    .get_mut(&delivery.visit.group)
                    .ok_or(EffectOwnerError::StaleCompletion)?
                    .waiting = false;
            }
            let effects = match delivery.result {
                Ok(e) => e,
                Err(e) => return self.fail(EffectOwnerError::Worker(e)),
            };
            self.install(delivery.visit, effects)?;
        }
        Ok(())
    }
    pub fn close_admission(&mut self) -> Result<(), EffectOwnerError> {
        match self.runtime.close_admission() {
            Ok(()) => Ok(()),
            Err(e) => self.fail(EffectOwnerError::Runtime(e)),
        }
    }
    pub fn is_drained(&self) -> bool {
        self.failed.is_none()
            && self.active.is_empty()
            && self.requests.is_empty()
            && self.runtime.is_drained()
    }
}
fn entries_bytes(entries: &[LogEntry]) -> Option<usize> {
    let mut bytes = 0usize;
    for entry in entries {
        bytes = bytes.checked_add(entry.retained_payload_bytes())?;
    }
    Some(bytes)
}
fn effect_bytes(effects: &[Effect], capacity: usize) -> Option<usize> {
    // Reserve metadata slack for Vec/VecDeque growth and transient conversion,
    // while charging every owned payload capacity separately below.
    let mut bytes = capacity.checked_mul(4 * size_of::<Effect>())?;
    for effect in effects {
        let extra = match effect {
            Effect::Send(message) | Effect::StageSnapshot(message) => {
                message_cost(message, usize::MAX).ok()?.1
            }
            Effect::Committed(entries) => entries
                .capacity()
                .checked_mul(size_of::<LogEntry>())?
                .checked_add(entries_bytes(entries)?)?,
            Effect::Persist(update) => (match &update.suffix {
                Some(suffix) => suffix
                    .entries
                    .capacity()
                    .checked_mul(size_of::<LogEntry>())?
                    .checked_add(entries_bytes(&suffix.entries)?)?,
                None => 0,
            })
            .checked_add(
                update
                    .snapshot_membership
                    .as_ref()
                    .map_or(0, |m| m.retained_bytes()),
            )?,
            _ => 0,
        };
        bytes = bytes.checked_add(extra)?;
    }
    Some(bytes)
}
