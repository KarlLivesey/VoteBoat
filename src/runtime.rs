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
//! Shared, single-owner ingress scheduling. No threads, clocks, stores or
//! transports are constructed. Effects transfer to host-owned bounded workers;
//! these ingress credits are not an outbound or application memory budget.
use crate::{
    identity::*,
    outbound::{message_cost, MessageClass},
    raft::*,
};
use std::{
    collections::{BTreeMap, VecDeque},
    mem::size_of,
};
mod administration;
mod applications;
mod checkpoint_schedule;
mod clients;
mod connections;
mod effects;
mod ingress;
mod maintenance;
mod node;
mod peers;
mod read_requests;
mod reads;
mod replica;
mod snapshots;
mod timed;
pub use administration::*;
pub use applications::*;
pub use checkpoint_schedule::*;
pub use clients::*;
pub use connections::ConnectionBudget;
pub use effects::*;
pub use ingress::*;
pub use maintenance::*;
pub use node::*;
pub use peers::*;
pub use read_requests::*;
pub use reads::*;
pub use replica::*;
pub use snapshots::*;
pub use timed::{TimedShard, TimerConfig, TimerProgress};

/// Milliseconds on one local monotonic clock domain, never a lease or term.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct MonoTime(pub u64);

pub trait Clock {
    fn now(&self) -> MonoTime;
}
/// Election jitter only; not a source of identities or cryptographic material.
pub trait ElectionEntropy {
    fn sample(&mut self) -> u64;
}
pub fn election_deadline<C: Clock, E: ElectionEntropy>(
    clock: &C,
    entropy: &mut E,
    minimum_ms: u64,
    spread_ms: u64,
) -> Result<MonoTime, RuntimeError> {
    if minimum_ms == 0 || spread_ms == 0 {
        return Err(RuntimeError::InvalidLimits);
    }
    clock
        .now()
        .0
        .checked_add(minimum_ms)
        .and_then(|t| t.checked_add(entropy.sample() % spread_ms))
        .map(MonoTime)
        .ok_or(RuntimeError::Exhausted)
}

/// The host must never reuse a generation for the same store session/lane.
/// Store recovery changes the session; local owner replacement changes generation.
/// One shard and one timer-service lifetime share an owner. Reconstructing either
/// requires a fresh generation; resetting a token sequence under an old owner
/// would allow old work to alias new work.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RuntimeOwner {
    pub store: StoreBinding,
    pub lane: ExecutionLaneId,
    pub generation: RuntimeGeneration,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VisitTicket {
    pub owner: RuntimeOwner,
    pub group: GroupIdentity,
    pub sequence: u64,
}
/// Exact volatile input allocation, independent of the visit that executes it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AdmissionTicket {
    pub owner: RuntimeOwner,
    pub group: GroupIdentity,
    pub sequence: u64,
}
/// Proposed position is not persistence, commitment or application evidence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProposalPosition {
    pub index: u64,
    pub term: u64,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RuntimeError {
    PeerUnavailable,
    PeerCapacity,
    PeerStoreConflict,
    InvalidLimits,
    Overloaded,
    EventTooLarge,
    UnknownGroup,
    WrongOwner,
    StaleTicket,
    DependencyPending,
    ClockRegressed,
    Closed,
    Fenced,
    Exhausted,
    SchedulerContract,
}

/// Exactly-once readiness, not a second event queue. `enqueue` is idempotent;
/// pop removes readiness; a still-ready group is re-enqueued at the tail. A
/// provider must fairly serve continuously ready groups and own no core state.
/// Rejected enqueue leaves readiness unchanged. Each shard supplies a dedicated
/// logical queue even when a host shares its underlying executor resources.
pub trait ReadyScheduler {
    fn capacity(&self) -> usize;
    fn enqueue(&mut self, group: GroupIdentity) -> Result<(), RuntimeError>;
    fn pop(&mut self) -> Option<GroupIdentity>;
    fn cancel(&mut self, group: GroupIdentity);
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum TimerKind {
    Election,
    Heartbeat,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TimerToken {
    pub owner: RuntimeOwner,
    pub group: GroupIdentity,
    pub kind: TimerKind,
    pub sequence: u64,
    pub deadline: MonoTime,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Expiration {
    pub token: TimerToken,
    pub late_ms: u64,
}
/// One deadline per (group, kind), bounded physical retention even across
/// replacement/cancellation. Poll transfers expirations to the caller, which
/// must compare exact expected tokens before admitting timer events. Queue
/// overload must retain/retry an expiration or explicitly rearm it; it must
/// never silently disable failure detection. No deadline proves leadership.
pub trait TimerService {
    fn owner(&self) -> RuntimeOwner;
    fn capacity(&self) -> usize;
    fn register(
        &mut self,
        group: GroupIdentity,
        kind: TimerKind,
        deadline: MonoTime,
    ) -> Result<TimerToken, RuntimeError>;
    fn cancel(&mut self, token: TimerToken) -> Result<(), RuntimeError>;
    fn poll(&mut self, now: MonoTime, limit: usize) -> Result<Vec<Expiration>, RuntimeError>;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ShardLimits {
    pub max_groups: usize,
    pub max_items: usize,
    pub max_bytes: usize,
    pub group_items: usize,
    pub group_bytes: usize,
    pub control_items: usize,
    pub control_bytes: usize,
    pub group_control_items: usize,
    pub group_control_bytes: usize,
    pub background_items: usize,
    pub background_bytes: usize,
    pub max_event_bytes: usize,
    pub visit_items: usize,
    pub visit_bytes: usize,
    pub visit_ms: u64,
}
impl Default for ShardLimits {
    fn default() -> Self {
        Self {
            max_groups: 4096,
            max_items: 8192,
            max_bytes: 64 * 1024 * 1024,
            group_items: 256,
            group_bytes: 4 * 1024 * 1024,
            control_items: 1024,
            control_bytes: 4 * 1024 * 1024,
            group_control_items: 16,
            group_control_bytes: 64 * 1024,
            background_items: 16,
            background_bytes: 16 * 1024 * 1024,
            max_event_bytes: 1024 * 1024,
            visit_items: 16,
            visit_bytes: 1024 * 1024,
            visit_ms: 2,
        }
    }
}
impl ShardLimits {
    pub fn validate(self) -> Result<Self, RuntimeError> {
        if self.max_groups == 0
            || self.max_groups > 65536
            || self.max_items == 0
            || self.max_items > 1_048_576
            || self.max_bytes == 0
            || self.max_bytes > 1024 * 1024 * 1024
            || self.group_items == 0
            || self.group_items > self.max_items
            || self.group_bytes == 0
            || self.group_bytes > self.max_bytes
            || self.control_items == 0
            || self.control_items >= self.max_items
            || self.control_bytes == 0
            || self.control_bytes >= self.max_bytes
            || self.group_control_items == 0
            || self.group_control_items >= self.group_items
            || self.group_control_bytes == 0
            || self.group_control_bytes >= self.group_bytes
            || self.background_items == 0
            || self.background_items > self.max_items - self.control_items
            || self.background_bytes == 0
            || self.background_bytes > self.max_bytes - self.control_bytes
            || self.max_event_bytes < size_of::<Event>()
            || self.max_event_bytes > self.group_bytes
            || self.visit_items == 0
            || self.visit_items > self.group_items
            || self.visit_bytes < self.max_event_bytes
            || self.visit_bytes > self.max_bytes
            || self.visit_ms == 0
        {
            return Err(RuntimeError::InvalidLimits);
        }
        Ok(self)
    }
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Usage {
    pub items: usize,
    pub bytes: usize,
}
impl Usage {
    fn add(&mut self, cost: usize) {
        self.items += 1;
        self.bytes += cost;
    }
    fn sub(&mut self, other: Usage) {
        self.items -= other.items;
        self.bytes -= other.bytes;
    }
    fn fits(self, cost: usize, items: usize, bytes: usize) -> bool {
        self.items < items && cost <= bytes.saturating_sub(self.bytes)
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Class {
    Control = 0,
    Data = 1,
    Background = 2,
}
struct Queued {
    event: Event,
    cost: usize,
    timer: Option<TimerToken>,
    admission: Option<AdmissionTicket>,
}
struct Visit {
    ticket: VisitTicket,
    used: [Usage; 3],
    deadline: MonoTime,
}
struct Group {
    core: Raft,
    queues: [VecDeque<Queued>; 3],
    usage: [Usage; 3],
    visit: Option<Visit>,
    turn: usize,
    fenced: bool,
    timer: Option<TimerToken>,
}
impl Group {
    fn ready(&self) -> bool {
        self.queues.iter().any(|q| !q.is_empty())
    }
    fn next_queued(&self, limits: ShardLimits, now: MonoTime) -> Option<(usize, usize)> {
        let visit = self.visit.as_ref()?;
        let used = sum(&visit.used);
        if now >= visit.deadline || used.items >= limits.visit_items {
            return None;
        }
        // Cursor survives visits, including visit_items=1.
        const ORDER: [usize; 4] = [0, 0, 1, 2];
        (0..4)
            .map(|offset| (self.turn + offset, ORDER[(self.turn + offset) % 4]))
            .find(|(_, c)| {
                self.queues[*c]
                    .front()
                    .is_some_and(|q| q.cost <= limits.visit_bytes.saturating_sub(used.bytes))
            })
    }
}
fn sum(usages: &[Usage; 3]) -> Usage {
    Usage {
        items: usages.iter().map(|u| u.items).sum(),
        bytes: usages.iter().map(|u| u.bytes).sum(),
    }
}

/// Rejection returns the exact owned input; no successful admission is implied.
#[derive(Debug)]
pub struct Rejected {
    pub reason: RuntimeError,
    pub event: Box<Event>,
}
pub struct Stepped {
    pub admission: Option<AdmissionTicket>,
    pub proposed: Option<ProposalPosition>,
    pub operation: Option<OperationId>,
    pub read: Option<ReadRequestId>,
    /// A consumed live expiration. Stale queued timers produce no core effects.
    pub timer: Option<TimerToken>,
    pub result: Result<Vec<Effect>, RaftError>,
}
pub struct Stopped {
    /// Exact tracked inputs still unprocessed; active/stepped work is excluded.
    pub admissions: Vec<AdmissionTicket>,
    /// Unprocessed inputs; return retryable failure to their callers.
    pub queued: Vec<Event>,
    /// Effects/work already handed out have unknown outcomes, not rollback.
    pub had_active_visit: bool,
}

/// Owns every registered core. Only an exact live visit ticket grants mutable
/// access for driving its effects/completions. Multiple groups can have suspended
/// visits; each group has at most one. A stalled worker does not requeue its group.
pub struct Shard<Q: ReadyScheduler> {
    owner: RuntimeOwner,
    limits: ShardLimits,
    scheduler: Q,
    groups: BTreeMap<GroupIdentity, Group>,
    usage: [Usage; 3],
    sequence: u64,
    admission_sequence: u64,
    connection_budget: Option<ConnectionBudget>,
    now: MonoTime,
    closed: bool,
}
impl<Q: ReadyScheduler> Shard<Q> {
    pub fn new(
        owner: RuntimeOwner,
        limits: ShardLimits,
        scheduler: Q,
    ) -> Result<Self, RuntimeError> {
        let limits = limits.validate()?;
        if scheduler.capacity() < limits.max_groups || !scheduler.is_empty() {
            return Err(RuntimeError::SchedulerContract);
        }
        Ok(Self {
            owner,
            limits,
            scheduler,
            groups: BTreeMap::new(),
            usage: [Usage::default(); 3],
            sequence: 0,
            admission_sequence: 0,
            connection_budget: None,
            now: MonoTime(0),
            closed: false,
        })
    }
    pub fn register(&mut self, core: Raft) -> Result<(), RuntimeError> {
        if self.closed {
            return Err(RuntimeError::Closed);
        }
        let group = core.state().bootstrap.group;
        if core.storage_binding() != self.owner.store {
            return Err(RuntimeError::WrongOwner);
        }
        if self.groups.contains_key(&group) || self.groups.keys().any(|g| g.id == group.id) {
            return Err(RuntimeError::WrongOwner);
        }
        if self.groups.len() >= self.limits.max_groups {
            return Err(RuntimeError::Overloaded);
        }
        if core.has_pending_dependency() {
            return Err(RuntimeError::DependencyPending);
        }
        if let Some(budget) = &self.connection_budget {
            self.reserved_connections(budget, None, Some(&core))?;
        }
        self.groups.insert(
            group,
            Group {
                core,
                queues: std::array::from_fn(|_| VecDeque::new()),
                usage: [Usage::default(); 3],
                visit: None,
                turn: 0,
                fenced: false,
                timer: None,
            },
        );
        self.retain_connection_history(group)?;
        Ok(())
    }
    pub fn core(&self, group: GroupIdentity) -> Option<&Raft> {
        self.groups.get(&group).map(|g| &g.core)
    }
    pub fn usage(&self) -> Usage {
        sum(&self.usage)
    }
    pub fn group_usage(&self, group: GroupIdentity) -> Option<Usage> {
        self.groups.get(&group).map(|g| sum(&g.usage))
    }
    pub fn active_visits(&self) -> usize {
        self.groups.values().filter(|g| g.visit.is_some()).count()
    }
    pub fn close_admission(&mut self) {
        self.closed = true;
    }
    pub fn is_drained(&self) -> bool {
        self.usage().items == 0 && self.active_visits() == 0
    }

    fn admission_shape(
        &self,
        group: GroupIdentity,
        event: &Event,
    ) -> Result<(Class, usize), RuntimeError> {
        if self.closed {
            return Err(RuntimeError::Closed);
        }
        let g = self.groups.get(&group).ok_or(RuntimeError::UnknownGroup)?;
        if g.fenced {
            return Err(RuntimeError::Fenced);
        }
        if matches!(event, Event::Receive(m) if m.group != group) {
            return Err(RuntimeError::WrongOwner);
        }
        event_cost(event, self.limits.max_event_bytes)
    }
    fn admit_inner(
        &mut self,
        group: GroupIdentity,
        event: &Event,
        timer: Option<TimerToken>,
        tracked: bool,
    ) -> Result<(Class, usize), RuntimeError> {
        let (class, mut cost) = self.admission_shape(group, event)?;
        if connections::changes_connections(event) {
            self.check_connection_event(group, event)?;
        }
        let g = self.groups.get(&group).unwrap();
        if tracked {
            cost = cost
                .checked_add(size_of::<AdmissionTicket>())
                .filter(|n| *n <= self.limits.max_event_bytes)
                .ok_or(RuntimeError::EventTooLarge)?;
        }
        if timer.is_some() {
            cost = cost
                .checked_add(size_of::<TimerToken>())
                .filter(|n| *n <= self.limits.max_event_bytes)
                .ok_or(RuntimeError::EventTooLarge)?;
        }
        let all = sum(&self.usage);
        let local = sum(&g.usage);
        let l = self.limits;
        if !all.fits(cost, l.max_items, l.max_bytes)
            || !local.fits(cost, l.group_items, l.group_bytes)
        {
            return Err(RuntimeError::Overloaded);
        }
        if class != Class::Control {
            let data = Usage {
                items: self.usage[1].items + self.usage[2].items,
                bytes: self.usage[1].bytes + self.usage[2].bytes,
            };
            let local_data = Usage {
                items: g.usage[1].items + g.usage[2].items,
                bytes: g.usage[1].bytes + g.usage[2].bytes,
            };
            if !data.fits(
                cost,
                l.max_items - l.control_items,
                l.max_bytes - l.control_bytes,
            ) || !local_data.fits(
                cost,
                l.group_items - l.group_control_items,
                l.group_bytes - l.group_control_bytes,
            ) {
                return Err(RuntimeError::Overloaded);
            }
        }
        if class == Class::Background
            && !self.usage[2].fits(cost, l.background_items, l.background_bytes)
        {
            return Err(RuntimeError::Overloaded);
        }
        if g.visit.is_none() && !g.ready() {
            self.scheduler.enqueue(group)?;
        }
        Ok((class, cost))
    }
    pub fn admit(&mut self, group: GroupIdentity, event: Event) -> Result<(), Rejected> {
        self.admit_tagged(group, event, None)
    }
    pub fn admit_tracked(
        &mut self,
        group: GroupIdentity,
        event: Event,
    ) -> Result<AdmissionTicket, Rejected> {
        self.admit_record(group, event, None, true)
            .map(|t| t.unwrap())
    }
    fn admit_tagged(
        &mut self,
        group: GroupIdentity,
        event: Event,
        timer: Option<TimerToken>,
    ) -> Result<(), Rejected> {
        self.admit_record(group, event, timer, false).map(|_| ())
    }
    fn admit_record(
        &mut self,
        group: GroupIdentity,
        event: Event,
        timer: Option<TimerToken>,
        tracked: bool,
    ) -> Result<Option<AdmissionTicket>, Rejected> {
        let sequence = self.admission_sequence.checked_add(1);
        if tracked && sequence.is_none() {
            return Err(Rejected {
                reason: RuntimeError::Exhausted,
                event: Box::new(event),
            });
        }
        match self.admit_inner(group, &event, timer, tracked) {
            Ok((class, cost)) => {
                let g = self.groups.get_mut(&group).unwrap();
                let admission = tracked.then(|| AdmissionTicket {
                    owner: self.owner,
                    group,
                    sequence: sequence.unwrap(),
                });
                if tracked {
                    self.admission_sequence = sequence.unwrap();
                }
                g.queues[class as usize].push_back(Queued {
                    event,
                    cost,
                    timer,
                    admission,
                });
                g.usage[class as usize].add(cost);
                self.usage[class as usize].add(cost);
                Ok(admission)
            }
            Err(reason) => Err(Rejected {
                reason,
                event: Box::new(event),
            }),
        }
    }
    fn observe(&mut self, now: MonoTime) -> Result<(), RuntimeError> {
        if now < self.now {
            return Err(RuntimeError::ClockRegressed);
        }
        self.now = now;
        Ok(())
    }
    pub fn poll(&mut self, now: MonoTime) -> Result<Option<VisitTicket>, RuntimeError> {
        self.observe(now)?;
        if self.scheduler.is_empty() {
            return Ok(None);
        }
        let sequence = self
            .sequence
            .checked_add(1)
            .ok_or(RuntimeError::Exhausted)?;
        let deadline = MonoTime(
            now.0
                .checked_add(self.limits.visit_ms)
                .ok_or(RuntimeError::Exhausted)?,
        );
        let Some(group) = self.scheduler.pop() else {
            return Err(RuntimeError::SchedulerContract);
        };
        let g = self
            .groups
            .get_mut(&group)
            .ok_or(RuntimeError::SchedulerContract)?;
        if g.visit.is_some() || !g.ready() || g.fenced {
            return Err(RuntimeError::SchedulerContract);
        }
        self.sequence = sequence;
        let ticket = VisitTicket {
            owner: self.owner,
            group,
            sequence: self.sequence,
        };
        g.visit = Some(Visit {
            ticket,
            used: [Usage::default(); 3],
            deadline,
        });
        Ok(Some(ticket))
    }
    fn live(&mut self, ticket: VisitTicket) -> Result<&mut Group, RuntimeError> {
        if ticket.owner != self.owner {
            return Err(RuntimeError::StaleTicket);
        }
        let g = self
            .groups
            .get_mut(&ticket.group)
            .ok_or(RuntimeError::StaleTicket)?;
        if g.visit.as_ref().is_none_or(|v| v.ticket != ticket) {
            return Err(RuntimeError::StaleTicket);
        }
        Ok(g)
    }
    /// Inspect the same priority choice used by step_next without consuming it.
    /// A serialized owner can reserve output capacity for that traffic class.
    pub fn next_class(
        &mut self,
        ticket: VisitTicket,
        now: MonoTime,
    ) -> Result<Option<MessageClass>, RuntimeError> {
        self.observe(now)?;
        let limits = self.limits;
        let g = self.live(ticket)?;
        if g.core.has_pending_dependency() {
            return Err(RuntimeError::DependencyPending);
        }
        Ok(g.next_queued(limits, now).map(|(_, c)| match c {
            0 => MessageClass::Control,
            1 => MessageClass::Data,
            _ => MessageClass::Background,
        }))
    }
    /// Non-consuming reservation for the exact next priority/byte-budget choice.
    /// Recheck immediately before stepping: earlier visits can change membership.
    pub fn next_effect_reservation(
        &mut self,
        ticket: VisitTicket,
        now: MonoTime,
    ) -> Result<Option<usize>, RuntimeError> {
        self.observe(now)?;
        let limits = self.limits;
        let g = self.live(ticket)?;
        if g.core.has_pending_dependency() {
            return Err(RuntimeError::DependencyPending);
        }
        g.next_queued(limits, now)
            .map(|(_, c)| {
                g.core
                    .event_effect_reservation(
                        &g.queues[c].front().unwrap().event,
                        limits.max_event_bytes,
                    )
                    .ok_or(RuntimeError::EventTooLarge)
            })
            .transpose()
    }
    /// Effects become the host worker's owned responsibility. Host bounds its
    /// output queues and applies committed entries before client success. A
    /// visit cannot be released while storage/snapshot dependencies remain.
    pub fn step_next(
        &mut self,
        ticket: VisitTicket,
        now: MonoTime,
    ) -> Result<Option<Stepped>, RuntimeError> {
        self.step_next_checked(ticket, now, |_, event| configuration_auth_required(event))
    }
    pub(super) fn step_next_checked(
        &mut self,
        ticket: VisitTicket,
        now: MonoTime,
        mut check: impl FnMut(&Raft, &Event) -> Result<(), RaftError>,
    ) -> Result<Option<Stepped>, RuntimeError> {
        self.observe(now)?;
        self.reserved_connection_peers()?;
        let limits = self.limits;
        let g = self.live(ticket)?;
        if g.core.has_pending_dependency() {
            return Err(RuntimeError::DependencyPending);
        }
        let Some((turn, class)) = g.next_queued(limits, now) else {
            return Ok(None);
        };
        g.turn = (turn + 1) % 4;
        let q = g.queues[class].pop_front().unwrap();
        g.visit.as_mut().unwrap().used[class].add(q.cost);
        let operation = match &q.event {
            Event::Propose { operation, .. } => Some(*operation),
            Event::Configure(proposal) => Some(proposal.record.operation),
            _ => None,
        };
        let read = match &q.event {
            Event::Read { request } => Some(*request),
            _ => None,
        };
        let timer = q.timer.filter(|token| g.timer == Some(*token));
        let stale_timer = q.timer.is_some() && timer.is_none();
        if timer.is_some() {
            g.timer = None;
        }
        let result = if stale_timer {
            Ok(Vec::new())
        } else {
            if matches!(q.event, Event::Propose { .. } | Event::Configure(_)) {
                check(&g.core, &q.event)
            } else {
                Ok(())
            }
            .and_then(|_| g.core.step(q.event))
        };
        let proposed = operation.and_then(|operation| result.as_ref().ok()?.iter().find_map(|e| {
            let Effect::Persist(update) = e else { return None; };
            let entry = update.suffix.as_ref()?.entries.first()?;
            (matches!(entry.payload, crate::log::EntryPayload::Command { operation: id, .. } if id == operation)
                || matches!(&entry.payload, crate::log::EntryPayload::Configuration(record) if record.operation == operation))
                .then_some(ProposalPosition { index: entry.index, term: entry.term })
        }));
        let stepped = Stepped {
            admission: q.admission,
            proposed,
            operation,
            read,
            timer,
            result,
        };
        if stepped.result.is_ok() {
            self.retain_connection_history(ticket.group)?;
        }
        Ok(Some(stepped))
    }
    /// Serialized owner access for the existing public durability, snapshot and
    /// read helpers. Do not block this call on I/O; submit work, retain the ticket
    /// and deliver the typed completion in a later call instead.
    pub fn with_core<R>(
        &mut self,
        ticket: VisitTicket,
        f: impl FnOnce(&mut Raft) -> R,
    ) -> Result<R, RuntimeError> {
        let result = f(&mut self.live(ticket)?.core);
        if let Err(error) = self
            .reserved_connection_peers()
            .and_then(|_| self.retain_connection_history(ticket.group))
        {
            let _ = self.stop_group(ticket.group);
            return Err(error);
        }
        Ok(result)
    }
    pub fn finish(&mut self, ticket: VisitTicket) -> Result<(), RuntimeError> {
        let g = self.live(ticket)?;
        if g.core.has_pending_dependency() {
            return Err(RuntimeError::DependencyPending);
        }
        // Readiness admission must succeed before releasing the scoped visit.
        if g.ready() {
            self.scheduler.enqueue(ticket.group)?;
        }
        let g = self.groups.get_mut(&ticket.group).unwrap();
        let used = g.visit.take().unwrap().used;
        for (i, usage) in used.into_iter().enumerate() {
            g.usage[i].sub(usage);
            self.usage[i].sub(usage);
        }
        Ok(())
    }
    /// Fences the group and invalidates its visit. Accepted external work is
    /// unresolved; callers must report unknown outcomes and recover explicitly.
    pub fn stop_group(&mut self, group: GroupIdentity) -> Result<Stopped, RuntimeError> {
        let g = self
            .groups
            .get_mut(&group)
            .ok_or(RuntimeError::UnknownGroup)?;
        self.scheduler.cancel(group);
        let had_active_visit = g.visit.take().is_some();
        g.core.storage_failed();
        g.fenced = true;
        g.timer = None;
        let mut admissions = Vec::new();
        let queued = g
            .queues
            .iter_mut()
            .flat_map(|q| q.drain(..))
            .map(|v| {
                if let Some(ticket) = v.admission {
                    admissions.push(ticket);
                }
                v.event
            })
            .collect();
        for i in 0..3 {
            self.usage[i].sub(g.usage[i]);
            g.usage[i] = Usage::default();
        }
        Ok(Stopped {
            admissions,
            queued,
            had_active_visit,
        })
    }
}

fn configuration_auth_required(event: &Event) -> Result<(), RaftError> {
    if matches!(event, Event::Configure(p) if !p.readiness.is_empty()) {
        Err(ConfigurationProposalError::AuthenticationRequired.into())
    } else {
        Ok(())
    }
}
fn event_cost(event: &Event, limit: usize) -> Result<(Class, usize), RuntimeError> {
    let (class, extra) = match event {
        Event::Configure(proposal) => {
            if proposal.readiness.len() > crate::quorum::Limits::default().max_voters {
                return Err(RuntimeError::EventTooLarge);
            }
            let extra = proposal
                .record
                .retained_bytes()
                .checked_add(size_of::<ConfigurationProposal>())
                .and_then(|n| {
                    n.checked_add(
                        proposal
                            .readiness
                            .capacity()
                            .checked_mul(size_of::<PromotionReadiness>())?,
                    )
                })
                .ok_or(RuntimeError::EventTooLarge)?;
            (Class::Control, extra)
        }
        Event::Checkpoint => (Class::Background, 0),
        Event::Read { .. } => (Class::Data, 0),
        Event::Propose { bytes, .. } => (Class::Data, bytes.capacity()),
        Event::Receive(message) => {
            let (class, cost) =
                message_cost(message, limit).map_err(|_| RuntimeError::EventTooLarge)?;
            let class = match class {
                MessageClass::Control => Class::Control,
                MessageClass::Data => Class::Data,
                MessageClass::Background => Class::Background,
            };
            (class, cost - size_of::<Message>())
        }
        _ => (Class::Control, 0),
    };
    let bytes = size_of::<Event>()
        .checked_add(extra)
        .filter(|v| *v <= limit)
        .ok_or(RuntimeError::EventTooLarge)?;
    Ok((class, bytes))
}
