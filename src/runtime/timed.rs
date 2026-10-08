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

#[derive(Clone, Copy, Debug)]
pub struct TimerConfig {
    pub heartbeat_ms: u64,
    pub election_min_ms: u64,
    pub election_spread_ms: u64,
    pub expirations_per_poll: usize,
}
impl Default for TimerConfig {
    fn default() -> Self {
        Self {
            heartbeat_ms: 50,
            election_min_ms: 150,
            election_spread_ms: 150,
            expirations_per_poll: 32,
        }
    }
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TimerProgress {
    pub expired: usize,
    pub admitted: usize,
    pub blocked: usize,
    /// Maximum lateness observed in this poll, not a contiguous watermark.
    pub max_lateness_ms: u64,
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
    shard: Shard<Q>,
    timers: T,
    entropy: E,
    config: TimerConfig,
    managed: BTreeMap<GroupIdentity, Managed>,
    pending: VecDeque<GroupIdentity>,
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
        if config.heartbeat_ms == 0
            || config.election_min_ms <= config.heartbeat_ms
            || config.election_spread_ms == 0
            || config.expirations_per_poll == 0
            || config.expirations_per_poll > 65536
            || size_of::<Event>() + size_of::<TimerToken>() > shard.limits.max_event_bytes
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
            shard,
            timers,
            entropy,
            config,
            managed: BTreeMap::new(),
            pending: VecDeque::new(),
            last_sequence: 0,
            failed: None,
            closed: false,
        };
        runtime.shard.observe(now)?;
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
        if core.is_fenced() || (core.role() != Role::Leader && !core.local_voter()) {
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
        self.latch(result)
    }
    pub fn register(&mut self, core: Raft, now: MonoTime) -> Result<(), RuntimeError> {
        self.check()?;
        self.shard.observe(now)?;
        if self.closed {
            return Err(RuntimeError::Closed);
        }
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
    pub fn limits(&self) -> ShardLimits {
        self.shard.limits
    }
    pub fn groups(&self) -> impl Iterator<Item = GroupIdentity> + '_ {
        self.shard.groups.keys().copied()
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
        self.admit_input(group, event, false).map(|_| ())
    }
    pub fn admit_tracked(
        &mut self,
        group: GroupIdentity,
        event: Event,
    ) -> Result<AdmissionTicket, Rejected> {
        self.admit_input(group, event, true).map(|t| t.unwrap())
    }
    fn admit_input(
        &mut self,
        group: GroupIdentity,
        event: Event,
        tracked: bool,
    ) -> Result<Option<AdmissionTicket>, Rejected> {
        let error = self.check().err().or_else(|| {
            self.shard
                .core(group)
                .filter(|c| c.is_fenced())
                .map(|_| RuntimeError::Fenced)
        });
        if let Some(reason) = error {
            return Err(Rejected {
                reason,
                event: Box::new(event),
            });
        }
        if tracked {
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
        self.check()?;
        if self.shard.core(group).is_some_and(Raft::is_fenced) {
            return Err(RuntimeError::Fenced);
        }
        if self.closed {
            return Err(RuntimeError::Closed);
        }
        self.shard.admission_shape(group, event)
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
        let stopped = self.shard.stop_group(group)?;
        if let Err(e) = cancel {
            self.failed = Some(e);
        }
        Ok(stopped)
    }
    pub fn close_admission(&mut self) -> Result<(), RuntimeError> {
        self.closed = true;
        self.shard.close_admission();
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
