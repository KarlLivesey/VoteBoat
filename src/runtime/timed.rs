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
use std::collections::BTreeSet;

#[derive(Clone, Copy, Debug)]
pub struct TimerConfig {
    pub heartbeat_ms: u64,
    pub election_min_ms: u64,
    pub election_spread_ms: u64,
    pub expirations_per_poll: usize,
    /// Longest a leader holds a ready follower's next Append under
    /// [`AppendHold::QuorumFirst`] (the next durable leader batch or heartbeat
    /// usually ends it sooner). Zero turns holds off. At most `heartbeat_ms`.
    /// A host must advance protocol time and poll. Expiry is admitted on the
    /// first poll at/after the deadline, subject to the timer budget and admission
    /// capacity; execution/output backpressure can delay delivery further.
    pub append_hold_ms: u64,
}
impl Default for TimerConfig {
    fn default() -> Self {
        Self {
            heartbeat_ms: 50,
            election_min_ms: 150,
            election_spread_ms: 150,
            expirations_per_poll: 32,
            append_hold_ms: Self::DEFAULT_APPEND_HOLD_MS,
        }
    }
}
impl TimerConfig {
    pub const DEFAULT_APPEND_HOLD_MS: u64 = 1;
    /// Pure timing validation; changing timers grants no consensus authority.
    pub fn validate(&self) -> Result<(), RuntimeError> {
        if self.heartbeat_ms == 0
            || self.election_min_ms <= self.heartbeat_ms
            || self.election_spread_ms == 0
            || self.expirations_per_poll == 0
            || self.expirations_per_poll > 65536
            || self.append_hold_ms > self.heartbeat_ms
        {
            return Err(RuntimeError::InvalidLimits);
        }
        Ok(())
    }
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TimerProgress {
    pub expired: usize,
    pub admitted: usize,
    pub blocked: usize,
    /// Maximum lateness observed in this poll, not a contiguous watermark.
    pub max_lateness_ms: u64,
    /// Held-Append releases admitted in this poll.
    pub released: usize,
}
#[derive(Clone, Copy, Eq, PartialEq)]
enum Phase {
    Armed,
    Pending,
    Queued,
}
struct Managed {
    token: TimerToken,
    phase: Phase,
    reset: u64,
}

/// Automatic election/heartbeat deadlines around the same single-owner shard.
/// Every core event and completion goes through this owner so role/contact
/// changes invalidate old timers, including expirations already in ingress.
/// Deadlines are liveness triggers; they do not provide quorum/read authority.
///
/// Construction consumes a quiescent shard and an empty timer service. Provider
/// failure latches an error: stop/drain groups and recover under a fresh owner.
/// Previously submitted effects may have unknown outcomes, never rollback.
pub struct TimedShard<Q: ReadyScheduler, T: TimerService, E: ElectionEntropy> {
    authorization: RuntimeAuthorization,
    shard: Shard<Q>,
    timers: T,
    entropy: E,
    config: TimerConfig,
    managed: BTreeMap<GroupIdentity, Managed>,
    pending: VecDeque<GroupIdentity>,
    // At most one hold deadline per group, with its ordered index.
    holds: BTreeMap<GroupIdentity, MonoTime>,
    hold_order: BTreeSet<(MonoTime, GroupIdentity)>,
    last_sequence: u64,
    failed: Option<RuntimeError>,
    closed: bool,
}
impl<Q: ReadyScheduler, T: TimerService, E: ElectionEntropy> TimedShard<Q, T, E> {
    pub fn new(
        shard: Shard<Q>,
        timers: T,
        entropy: E,
        config: TimerConfig,
        now: MonoTime,
    ) -> Result<Self, RuntimeError> {
        config.validate()?;
        if size_of::<Event>() + size_of::<TimerToken>() > shard.limits.max_event_bytes
            || size_of::<Event>() + size_of::<TimerToken>() > shard.limits.group_control_bytes
            || size_of::<Event>() + size_of::<TimerToken>() > shard.limits.control_bytes
        {
            return Err(RuntimeError::InvalidLimits);
        }
        if timers.owner() != shard.owner
            || timers.capacity() < shard.limits.max_groups
            || !timers.is_empty()
        {
            return Err(RuntimeError::SchedulerContract);
        }
        if !shard.is_drained() {
            return Err(RuntimeError::DependencyPending);
        }
        if shard.closed {
            return Err(RuntimeError::Closed);
        }
        let mut runtime = Self {
            authorization: RuntimeAuthorization::physical(shard.owner),
            shard,
            timers,
            entropy,
            config,
            managed: BTreeMap::new(),
            pending: VecDeque::new(),
            holds: BTreeMap::new(),
            hold_order: BTreeSet::new(),
            last_sequence: 0,
            failed: None,
            closed: false,
        };
        runtime.shard.observe(now)?;
        let hold = runtime.append_hold();
        for group in runtime.shard.groups.values_mut() {
            group.core.set_append_hold(hold);
        }
        let groups = runtime.shard.groups.keys().copied().collect::<Vec<_>>();
        for group in groups {
            runtime.refresh(group, now, false)?;
        }
        Ok(runtime)
    }
    fn check(&self) -> Result<(), RuntimeError> {
        self.failed.clone().map_or(Ok(()), Err)
    }
    fn latch<R>(&mut self, result: Result<R, RuntimeError>) -> Result<R, RuntimeError> {
        if let Err(e) = &result {
            self.failed = Some(e.clone());
        }
        result
    }
    fn cancel_managed(&mut self, group: GroupIdentity) -> Result<(), RuntimeError> {
        self.shard
            .groups
            .get_mut(&group)
            .ok_or(RuntimeError::UnknownGroup)?
            .timer = None;
        if let Some(old) = self.managed.remove(&group) {
            if old.phase == Phase::Pending {
                self.pending.retain(|g| *g != group);
            }
            if old.phase == Phase::Armed {
                self.timers.cancel(old.token)?;
            }
        }
        Ok(())
    }
    fn refresh_inner(
        &mut self,
        group: GroupIdentity,
        now: MonoTime,
        force: bool,
    ) -> Result<(), RuntimeError> {
        if self.closed {
            return Ok(());
        }
        let core = self.shard.core(group).ok_or(RuntimeError::UnknownGroup)?;
        if core.is_fenced()
            || (core.role() != Role::Leader && (!core.local_voter() || !core.campaigning_enabled()))
        {
            return self.cancel_managed(group);
        }
        let kind = if core.role() == Role::Leader {
            TimerKind::Heartbeat
        } else {
            TimerKind::Election
        };
        let reset = core.election_reset_sequence();
        if !force
            && self
                .managed
                .get(&group)
                .is_some_and(|m| m.token.kind == kind && m.reset == reset)
        {
            return Ok(());
        }
        self.cancel_managed(group)?;
        let wait = if kind == TimerKind::Heartbeat {
            self.config.heartbeat_ms
        } else {
            self.config
                .election_min_ms
                .checked_add(self.entropy.sample() % self.config.election_spread_ms)
                .ok_or(RuntimeError::Exhausted)?
        };
        let deadline = MonoTime(now.0.checked_add(wait).ok_or(RuntimeError::Exhausted)?);
        let token = self.timers.register(group, kind, deadline)?;
        if token.owner != self.shard.owner
            || token.group != group
            || token.kind != kind
            || token.deadline != deadline
            || token.sequence <= self.last_sequence
        {
            return Err(RuntimeError::SchedulerContract);
        }
        self.last_sequence = token.sequence;
        self.shard.groups.get_mut(&group).unwrap().timer = Some(token);
        self.managed.insert(
            group,
            Managed {
                token,
                phase: Phase::Armed,
                reset,
            },
        );
        Ok(())
    }
    fn refresh(
        &mut self,
        group: GroupIdentity,
        now: MonoTime,
        force: bool,
    ) -> Result<(), RuntimeError> {
        let result = self.refresh_inner(group, now, force);
        if result.is_ok() {
            self.sync_hold(group, now);
        }
        self.latch(result)
    }
    fn append_hold(&self) -> AppendHold {
        if self.config.append_hold_ms == 0 {
            AppendHold::Off
        } else {
            AppendHold::QuorumFirst
        }
    }
    /// Arm one hold deadline when a step leaves the core holding an Append;
    /// drop it when a step leaves none. An armed deadline is not moved, so no
    /// hold outlives `append_hold_ms` from the step that first armed it.
    fn sync_hold(&mut self, group: GroupIdentity, now: MonoTime) {
        if self.config.append_hold_ms == 0 {
            return;
        }
        let holding = !self.closed
            && self
                .shard
                .core(group)
                .is_some_and(|core| !core.is_fenced() && core.has_held_appends());
        match (holding, self.holds.get(&group).copied()) {
            (true, None) => {
                let deadline = MonoTime(now.0.saturating_add(self.config.append_hold_ms));
                self.holds.insert(group, deadline);
                self.hold_order.insert((deadline, group));
            }
            (false, Some(_)) => self.drop_hold(group),
            _ => {}
        }
    }
    fn drop_hold(&mut self, group: GroupIdentity) {
        if let Some(deadline) = self.holds.remove(&group) {
            self.hold_order.remove(&(deadline, group));
        }
    }
    /// Admit one release per expired hold deadline, earliest first.
    fn poll_holds(&mut self, now: MonoTime, limit: usize, progress: &mut TimerProgress) {
        for _ in 0..limit {
            let Some(&(deadline, group)) = self.hold_order.first() else {
                break;
            };
            if deadline > now {
                break;
            }
            match self.shard.admit(group, Event::ReleaseHeldAppends) {
                Ok(()) => progress.released += 1,
                // Keep the deadline: the next poll retries the admission.
                Err(rejected) if rejected.reason == RuntimeError::Overloaded => break,
                // A closed, fenced or unknown group has no hold left to bound.
                Err(_) => {}
            }
            self.drop_hold(group);
        }
    }
    pub fn register(&mut self, mut core: Raft, now: MonoTime) -> Result<(), RuntimeError> {
        self.check()?;
        self.shard.observe(now)?;
        if self.closed {
            return Err(RuntimeError::Closed);
        }
        core.set_append_hold(self.append_hold());
        let group = core.state().bootstrap.group;
        self.shard.register(core)?;
        self.refresh(group, now, false)
    }
    pub fn core(&self, group: GroupIdentity) -> Option<&Raft> {
        self.shard.core(group)
    }
    pub fn owner(&self) -> RuntimeOwner {
        self.shard.owner
    }
    pub fn authorization(&self) -> RuntimeAuthorization {
        self.authorization
    }
    pub(crate) fn bind_logical_authority(
        &mut self,
        authorization: RuntimeAuthorization,
        profile: Option<&crate::logical_voter::LogicalActivationControlProfile>,
    ) -> Result<(), RuntimeError> {
        self.validate_quiescent()?;
        let logical = authorization.logical().ok_or(RuntimeError::WrongOwner)?;
        if authorization.owner() != self.owner()
            || self.authorization.is_logical()
            || self.groups().count() != 1
            || self.core(logical.mapping.parent).is_none_or(|core| {
                core.local_node() != logical.mapping.voter
                    || core.storage_binding() != authorization.owner().store
            })
        {
            return Err(RuntimeError::WrongOwner);
        }
        if let Some(profile) = profile {
            self.shard.validate_control_profile(profile)?;
            self.shard.control_profile = Some(profile.clone());
        }
        self.authorization = authorization;
        Ok(())
    }
    pub fn limits(&self) -> ShardLimits {
        self.shard.limits
    }
    pub fn has_queued_input(&self) -> bool {
        !self.closed && self.failed.is_none() && self.shard.has_queued_input()
    }
    /// Earliest timer deadline of a live shard; see `TimerService::next_deadline`.
    pub fn next_deadline(&self) -> Option<MonoTime> {
        if self.closed || self.failed.is_some() {
            return None;
        }
        let hold = self.hold_order.first().map(|(deadline, _)| *deadline);
        match (self.timers.next_deadline(), hold) {
            (Some(timer), Some(hold)) => Some(timer.min(hold)),
            (timer, hold) => timer.or(hold),
        }
    }
    pub fn groups(&self) -> impl Iterator<Item = GroupIdentity> + '_ {
        self.shard.groups.keys().copied()
    }
    /// Ordered iteration strictly after a cursor, without scanning its prefix.
    pub fn groups_after(
        &self,
        after: Option<GroupIdentity>,
    ) -> impl Iterator<Item = GroupIdentity> + '_ {
        use std::ops::Bound::{Excluded, Unbounded};
        self.shard
            .groups
            .range((after.map_or(Unbounded, Excluded), Unbounded))
            .map(|(&g, _)| g)
    }
    pub fn validate_quiescent(&self) -> Result<(), RuntimeError> {
        self.check()?;
        if self.closed {
            return Err(RuntimeError::Closed);
        }
        if !self.is_drained() {
            return Err(RuntimeError::DependencyPending);
        }
        Ok(())
    }
    pub fn usage(&self) -> Usage {
        self.shard.usage()
    }
    pub fn set_connection_budget(&mut self, budget: ConnectionBudget) -> Result<(), RuntimeError> {
        self.check()?;
        self.shard.set_connection_budget(budget)
    }
    pub fn connection_budget(&self) -> Option<&ConnectionBudget> {
        self.shard.connection_budget()
    }
    pub(super) fn prepare_connection_budget(
        &self,
        budget: ConnectionBudget,
    ) -> Result<ConnectionBudget, RuntimeError> {
        self.check()?;
        self.shard.prepare_connection_budget(budget)
    }
    pub(super) fn install_connection_budget(&mut self, budget: ConnectionBudget) {
        self.shard.install_connection_budget(budget);
    }
    pub fn reserved_connection_peers(&self) -> Result<Option<usize>, RuntimeError> {
        self.shard.reserved_connection_peers()
    }
    pub fn group_usage(&self, group: GroupIdentity) -> Option<Usage> {
        self.shard.group_usage(group)
    }
    pub(super) fn has_pending_peer_ingress(&self, group: GroupIdentity, peer: NodeId) -> bool {
        self.shard.has_pending_peer_ingress(group, peer)
    }
    pub fn active_visits(&self) -> usize {
        self.shard.active_visits()
    }
    pub fn is_drained(&self) -> bool {
        self.shard.is_drained()
    }
    pub fn pending_expirations(&self) -> usize {
        self.pending.len()
    }
    pub fn deadline(&self, group: GroupIdentity) -> Option<TimerToken> {
        self.managed.get(&group).map(|m| m.token)
    }
    pub fn admit(&mut self, group: GroupIdentity, event: Event) -> Result<(), Rejected> {
        self.admit_input(group, event, false, None).map(|_| ())
    }
    pub fn admit_tracked(
        &mut self,
        group: GroupIdentity,
        event: Event,
    ) -> Result<AdmissionTicket, Rejected> {
        self.admit_input(group, event, true, None)
            .map(|t| t.unwrap())
    }
    pub(super) fn admit_logical_read(
        &mut self,
        group: GroupIdentity,
        event: Event,
        read: super::logical_intake::LogicalReadInput<'_>,
    ) -> Result<AdmissionTicket, Rejected> {
        self.admit_input(group, event, true, Some(read))
            .map(|t| t.unwrap())
    }
    fn admit_input(
        &mut self,
        group: GroupIdentity,
        event: Event,
        tracked: bool,
        read: Option<super::logical_intake::LogicalReadInput<'_>>,
    ) -> Result<Option<AdmissionTicket>, Rejected> {
        if let Err(reason) = self.admission_shape_scoped(group, &event, read) {
            return Err(Rejected {
                reason,
                event: Box::new(event),
            });
        }
        if read.is_some() {
            self.shard.admit_record(group, event, None, tracked, read)
        } else if tracked {
            self.shard.admit_tracked(group, event).map(Some)
        } else {
            self.shard.admit(group, event).map(|_| None)
        }
    }
    fn poll_timers_inner(&mut self, now: MonoTime) -> Result<TimerProgress, RuntimeError> {
        let mut progress = TimerProgress::default();
        if self.closed {
            return Ok(progress);
        }
        let limit = self
            .config
            .expirations_per_poll
            .min(self.shard.limits.max_groups);
        let expired = self.timers.poll(now, limit)?;
        if expired.len() > limit {
            return Err(RuntimeError::SchedulerContract);
        }
        for expiration in expired {
            let token = expiration.token;
            let Some(current) = self.managed.get_mut(&token.group) else {
                continue;
            };
            if current.token != token || current.phase != Phase::Armed {
                continue;
            }
            if now.0.checked_sub(token.deadline.0) != Some(expiration.late_ms) {
                return Err(RuntimeError::SchedulerContract);
            }
            progress.expired += 1;
            progress.max_lateness_ms = progress.max_lateness_ms.max(expiration.late_ms);
            current.phase = Phase::Pending;
            self.pending.push_back(token.group);
        }
        for _ in 0..limit.min(self.pending.len()) {
            let group = self.pending.pop_front().unwrap();
            let current = self
                .managed
                .get_mut(&group)
                .ok_or(RuntimeError::SchedulerContract)?;
            if current.phase != Phase::Pending {
                return Err(RuntimeError::SchedulerContract);
            }
            let token = current.token;
            progress.max_lateness_ms = progress.max_lateness_ms.max(now.0 - token.deadline.0);
            let event = match token.kind {
                TimerKind::Election => Event::Campaign,
                TimerKind::Heartbeat => Event::Heartbeat,
            };
            match self.shard.admit_tagged(group, event, Some(token)) {
                Ok(()) => {
                    current.phase = Phase::Queued;
                    progress.admitted += 1;
                }
                Err(rejected) if rejected.reason == RuntimeError::Overloaded => {
                    self.pending.push_back(group);
                    progress.blocked += 1;
                }
                Err(rejected) => return Err(rejected.reason),
            }
        }
        self.poll_holds(now, limit, &mut progress);
        Ok(progress)
    }
    pub fn poll_timers(&mut self, now: MonoTime) -> Result<TimerProgress, RuntimeError> {
        self.check()?;
        self.shard.observe(now)?;
        let result = self.poll_timers_inner(now);
        self.latch(result)
    }
    pub fn poll(&mut self, now: MonoTime) -> Result<Option<VisitTicket>, RuntimeError> {
        self.poll_timers(now)?;
        self.shard.poll(now)
    }
    pub fn step_next(
        &mut self,
        visit: VisitTicket,
        now: MonoTime,
    ) -> Result<Option<Stepped>, RuntimeError> {
        self.step_next_checked(visit, now, |_, event| configuration_auth_required(event))
    }
    pub(super) fn step_next_checked(
        &mut self,
        visit: VisitTicket,
        now: MonoTime,
        check: impl FnMut(&Raft, &Event) -> Result<(), RaftError>,
    ) -> Result<Option<Stepped>, RuntimeError> {
        self.check()?;
        let step = self.shard.step_next_checked(visit, now, check)?;
        if let Some(s) = &step {
            self.refresh(visit.group, now, s.timer.is_some())?;
        }
        Ok(step)
    }
    pub fn next_class(
        &mut self,
        visit: VisitTicket,
        now: MonoTime,
    ) -> Result<Option<MessageClass>, RuntimeError> {
        self.check()?;
        self.shard.next_class(visit, now)
    }
    pub(super) fn next_ready_effect_reservation(
        &mut self,
        visit: VisitTicket,
        now: MonoTime,
        max_items: usize,
    ) -> Result<Option<usize>, RuntimeError> {
        self.check()?;
        if self.authorization.is_logical() {
            return if max_items == 0 {
                Ok(None)
            } else {
                self.shard.next_effect_reservation(visit, now)
            };
        }
        self.shard
            .next_ready_effect_reservation(visit, now, max_items)
    }
    pub(super) fn step_ready_checked(
        &mut self,
        visit: VisitTicket,
        now: MonoTime,
        max_items: usize,
        mut check: impl FnMut(&Raft, &Event, &[ReadyProposal]) -> Result<(), RaftError>,
    ) -> Result<Option<Stepped>, RuntimeError> {
        self.check()?;
        if max_items == 0 {
            return Ok(None);
        }
        let step = if self.authorization.is_logical() {
            self.shard
                .step_next_checked(visit, now, |core, event| check(core, event, &[]))?
        } else {
            self.shard
                .step_ready_checked(visit, now, max_items, check)?
        };
        if let Some(stepped) = &step {
            self.refresh(visit.group, now, stepped.timer.is_some())?;
        }
        Ok(step)
    }
    pub fn next_effect_reservation(
        &mut self,
        visit: VisitTicket,
        now: MonoTime,
    ) -> Result<Option<usize>, RuntimeError> {
        self.check()?;
        self.shard.next_effect_reservation(visit, now)
    }
    pub(super) fn admission_shape(
        &self,
        group: GroupIdentity,
        event: &Event,
    ) -> Result<(Class, usize), RuntimeError> {
        self.admission_shape_scoped(group, event, None)
    }
    pub(super) fn admission_shape_scoped(
        &self,
        group: GroupIdentity,
        event: &Event,
        read: Option<super::logical_intake::LogicalReadInput<'_>>,
    ) -> Result<(Class, usize), RuntimeError> {
        self.check()?;
        if self.shard.core(group).is_some_and(Raft::is_fenced) {
            return Err(RuntimeError::Fenced);
        }
        if self.closed {
            return Err(RuntimeError::Closed);
        }
        if let Some(read) = read {
            if self
                .authorization
                .logical()
                .is_none_or(|a| a.mapping.parent != group || read.profile.parent() != group)
            {
                return Err(RuntimeError::WrongOwner);
            }
        }
        match read {
            Some(read) => self.shard.admission_shape_scoped(group, event, Some(read)),
            None => self.shard.admission_shape(group, event),
        }
    }
    pub fn with_core<R>(
        &mut self,
        visit: VisitTicket,
        now: MonoTime,
        f: impl FnOnce(&mut Raft) -> R,
    ) -> Result<R, RuntimeError> {
        self.check()?;
        self.shard.observe(now)?;
        let result = self.shard.with_core(visit, f)?;
        self.refresh(visit.group, now, false)?;
        Ok(result)
    }
    pub fn finish(&mut self, visit: VisitTicket) -> Result<(), RuntimeError> {
        self.check()?;
        self.shard.finish(visit)
    }
    pub fn stop_group(&mut self, group: GroupIdentity) -> Result<Stopped, RuntimeError> {
        // Cleanup remains available after a provider fault. Invalidate queued
        // tokens before returning unresolved work even if cancellation fails.
        let cancel = self.cancel_managed(group);
        self.drop_hold(group);
        let stopped = self.shard.stop_group(group)?;
        if let Err(e) = cancel {
            self.failed = Some(e);
        }
        Ok(stopped)
    }
    pub fn close_admission(&mut self) -> Result<(), RuntimeError> {
        self.closed = true;
        self.shard.close_admission();
        self.holds.clear();
        self.hold_order.clear();
        let groups = self.managed.keys().copied().collect::<Vec<_>>();
        let mut error = None;
        for group in groups {
            if let Err(e) = self.cancel_managed(group) {
                error.get_or_insert(e);
            }
        }
        if let Some(e) = error {
            self.failed = Some(e.clone());
            return Err(e);
        }
        Ok(())
    }
}
