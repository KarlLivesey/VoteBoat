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
//! Bounded original read invocation ownership, from admission through reply.
use super::*;
use crate::application::{ApplicationError, BoundedReadableStateMachine};
use std::ops::Bound::{Excluded, Unbounded};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReadInvocationBinding {
    pub owner: RuntimeOwner,
    pub generation: ReadInvocationGeneration,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReadInvocationTicket {
    pub binding: ReadInvocationBinding,
    pub sequence: u64,
    pub group: GroupIdentity,
    pub request: ReadRequestId,
}
#[derive(Clone, Copy, Debug)]
pub struct ReadInvocationLimits {
    pub requests: usize,
    pub group_requests: usize,
    pub bytes: usize,
    pub query_bytes: usize,
    pub result_bytes: usize,
}
impl Default for ReadInvocationLimits {
    fn default() -> Self {
        Self {
            requests: 4096,
            group_requests: 64,
            bytes: 32 * 1024 * 1024,
            query_bytes: 64 * 1024,
            result_bytes: 1024 * 1024,
        }
    }
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ReadInvocationUsage {
    pub requests: usize,
    pub bytes: usize,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReadInvocationError {
    InvalidLimits,
    WrongBinding,
    Closed,
    Draining,
    Fenced,
    UnknownGroup,
    Overloaded,
    InFlight,
    TooLarge,
    Exhausted,
    StaleTicket,
    ProviderViolation,
    Consensus(RaftError),
    Runtime(RuntimeError),
    Application(ApplicationError),
    Execution(ReadRouteError),
}
#[derive(Debug)]
pub struct ReadInvocationRejected<Q> {
    pub reason: ReadInvocationError,
    pub group: GroupIdentity,
    pub query: Q,
}
pub struct ReadInvocationConstructionRejected<R> {
    pub reason: ReadInvocationError,
    pub execution: Box<ReadRouter<R>>,
}
impl<R> std::fmt::Debug for ReadInvocationConstructionRejected<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReadInvocationConstructionRejected")
            .field("reason", &self.reason)
            .finish_non_exhaustive()
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadUnavailable {
    Cancelled,
    LeadershipChanged,
    OwnerFailed,
    Aborted,
}
#[derive(Debug)]
pub enum ReadOutcome<R> {
    Read {
        barrier: ReadBarrier,
        result: Result<R, ApplicationError>,
    },
    NotRead(RaftError),
    Unavailable(ReadUnavailable),
}
#[derive(Debug)]
pub struct ReadCompletion<R> {
    ticket: ReadInvocationTicket,
    outcome: ReadOutcome<R>,
}
impl<R> ReadCompletion<R> {
    pub fn ticket(&self) -> ReadInvocationTicket {
        self.ticket
    }
    pub fn outcome(&self) -> &ReadOutcome<R> {
        &self.outcome
    }
}
#[derive(Debug)]
pub struct ReadCompletionRejected<R> {
    pub reason: ReadInvocationError,
    pub completion: Box<ReadCompletion<R>>,
}
#[derive(Debug)]
pub enum ReadExecutionError {
    Rejected {
        reason: ReadInvocationError,
        lease: Box<EffectLease>,
    },
    Failed {
        reason: ReadInvocationError,
    },
}
struct Pending<Q, R> {
    ticket: ReadInvocationTicket,
    admission: AdmissionTicket,
    cancellation: Option<AdmissionTicket>,
    query: Option<Q>,
    query_bound: usize,
    result_bound: usize,
    charged: usize,
    term: u64,
    stepped: bool,
    terminal: bool,
    has_output: bool,
    exported: bool,
    released: bool,
    outcome: Option<ReadOutcome<R>>,
}
/// One selected service owner for each group's Read/CancelRead invocation stream.
/// Owner steps are supplied from the shared driver (including ClientRouter.advance)
/// before taking its effects. No hidden thread, clock, application or I/O exists.
pub struct ReadRequests<Q, R> {
    binding: ReadInvocationBinding,
    limits: ReadInvocationLimits,
    execution: ReadRouter<R>,
    pending: BTreeMap<u64, Pending<Q, R>>,
    admissions: BTreeMap<u64, (u64, bool)>,
    active: BTreeMap<GroupIdentity, u64>,
    groups: BTreeMap<GroupIdentity, usize>,
    ready: VecDeque<u64>,
    usage: ReadInvocationUsage,
    sequence: u64,
    cursor: u64,
    closed: bool,
}
impl<Q, R> ReadRequests<Q, R> {
    pub fn new(
        binding: ReadInvocationBinding,
        limits: ReadInvocationLimits,
        execution: ReadRouter<R>,
    ) -> Result<Self, ReadInvocationConstructionRejected<R>> {
        let reason = if execution.binding().owner != binding.owner {
            Some(ReadInvocationError::WrongBinding)
        } else if !execution.is_drained() {
            Some(ReadInvocationError::InFlight)
        } else if !execution.accepts_work() {
            Some(ReadInvocationError::Closed)
        } else if limits.requests == 0
            || limits.requests > 65536
            || limits.group_requests == 0
            || limits.group_requests > limits.requests
            || limits.bytes == 0
            || limits.bytes > 1024 * 1024 * 1024
            || limits.query_bytes > 1024 * 1024
            || limits.result_bytes < size_of::<R>()
            || limits.result_bytes > 16 * 1024 * 1024
            || limits
                .result_bytes
                .checked_add(size_of::<Pending<Q, R>>())
                .is_none_or(|n| n > limits.bytes)
            || !execution.fits_single::<Q>(limits.query_bytes, limits.result_bytes)
        {
            Some(ReadInvocationError::InvalidLimits)
        } else {
            None
        };
        if let Some(reason) = reason {
            return Err(ReadInvocationConstructionRejected {
                reason,
                execution: Box::new(execution),
            });
        }
        Ok(Self {
            binding,
            limits,
            execution,
            pending: BTreeMap::new(),
            admissions: BTreeMap::new(),
            active: BTreeMap::new(),
            groups: BTreeMap::new(),
            ready: VecDeque::with_capacity(limits.requests),
            usage: ReadInvocationUsage::default(),
            sequence: 0,
            cursor: 0,
            closed: false,
        })
    }
    pub fn binding(&self) -> ReadInvocationBinding {
        self.binding
    }
    pub fn limits(&self) -> ReadInvocationLimits {
        self.limits
    }
    pub fn usage(&self) -> ReadInvocationUsage {
        self.usage
    }
    pub fn close(&mut self) {
        self.closed = true;
    }
    pub fn is_drained(&self) -> bool {
        self.pending.is_empty() && self.execution.is_drained()
    }
    fn check_submit<
        A: BoundedReadableStateMachine<Query = Q, ReadResult = R>,
        S: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
    >(
        &self,
        owner: &EffectOwner<S, T, E>,
        application: &A,
        group: GroupIdentity,
        query: &Q,
    ) -> Result<(usize, usize, usize, u64, ReadRequestId, u64), ReadInvocationError> {
        if self.closed {
            return Err(ReadInvocationError::Closed);
        }
        if owner.identity() != self.binding.owner {
            return Err(ReadInvocationError::WrongBinding);
        }
        if owner.is_failed() {
            return Err(ReadInvocationError::Fenced);
        }
        let core = owner.core(group).ok_or(ReadInvocationError::UnknownGroup)?;
        if core.role() != Role::Leader {
            return Err(ReadInvocationError::Consensus(RaftError::NotLeader));
        }
        if self.active.contains_key(&group) {
            return Err(ReadInvocationError::InFlight);
        }
        if self.usage.requests == self.limits.requests
            || self.groups.get(&group).copied().unwrap_or(0) >= self.limits.group_requests
        {
            return Err(ReadInvocationError::Overloaded);
        }
        if application.applied_index() > core.state().commit_index {
            return Err(ReadInvocationError::Application(
                ApplicationError::NotApplied,
            ));
        }
        let nested = application
            .query_bytes(query, self.limits.query_bytes)
            .map_err(ReadInvocationError::Application)?;
        let bound = application
            .read_result_bound(query)
            .map_err(ReadInvocationError::Application)?;
        if nested > self.limits.query_bytes
            || bound < size_of::<R>()
            || bound > self.limits.result_bytes
        {
            return Err(ReadInvocationError::TooLarge);
        }
        let charged = nested
            .checked_add(bound)
            .and_then(|n| n.checked_add(size_of::<Pending<Q, R>>()))
            .ok_or(ReadInvocationError::TooLarge)?;
        if charged > self.limits.bytes - self.usage.bytes {
            return Err(ReadInvocationError::Overloaded);
        }
        let sequence = self
            .sequence
            .checked_add(1)
            .ok_or(ReadInvocationError::Exhausted)?;
        let request = core
            .read_request_floor()
            .checked_add(1)
            .map(|n| n.max(sequence))
            .and_then(ReadRequestId::new)
            .ok_or(ReadInvocationError::Exhausted)?;
        Ok((
            nested,
            bound,
            charged,
            sequence,
            request,
            core.state().hard_state.term,
        ))
    }
    pub fn submit<
        A: BoundedReadableStateMachine<Query = Q, ReadResult = R>,
        S: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
    >(
        &mut self,
        owner: &mut EffectOwner<S, T, E>,
        application: &A,
        group: GroupIdentity,
        query: Q,
    ) -> Result<ReadInvocationTicket, ReadInvocationRejected<Q>> {
        let checked = self.check_submit(owner, application, group, &query);
        let (query_bound, result_bound, charged, sequence, request, term) = match checked {
            Ok(v) => v,
            Err(reason) => {
                return Err(ReadInvocationRejected {
                    reason,
                    group,
                    query,
                })
            }
        };
        let admission = match owner.admit_tracked(group, Event::Read { request }) {
            Ok(t) => t,
            Err(rejected) => {
                return Err(ReadInvocationRejected {
                    reason: ReadInvocationError::Runtime(rejected.reason),
                    group,
                    query,
                })
            }
        };
        self.sequence = sequence;
        let ticket = ReadInvocationTicket {
            binding: self.binding,
            sequence,
            group,
            request,
        };
        self.pending.insert(
            sequence,
            Pending {
                ticket,
                admission,
                cancellation: None,
                query: Some(query),
                query_bound,
                result_bound,
                charged,
                term,
                stepped: false,
                terminal: false,
                has_output: false,
                exported: false,
                released: false,
                outcome: None,
            },
        );
        self.admissions
            .insert(admission.sequence, (sequence, false));
        self.active.insert(group, sequence);
        *self.groups.entry(group).or_default() += 1;
        self.usage.requests += 1;
        self.usage.bytes += charged;
        Ok(ticket)
    }
    fn publish(&mut self, sequence: u64, outcome: ReadOutcome<R>) {
        let p = self.pending.get_mut(&sequence).unwrap();
        if !p.has_output {
            p.has_output = true;
            p.outcome = Some(outcome);
            self.ready.push_back(sequence);
        }
    }
    fn terminal(&mut self, sequence: u64) {
        let p = self.pending.get_mut(&sequence).unwrap();
        p.terminal = true;
        p.query = None;
        if self.active.get(&p.ticket.group) == Some(&sequence) {
            self.active.remove(&p.ticket.group);
        }
    }
    fn cleanup(&mut self, sequence: u64) {
        if !self
            .pending
            .get(&sequence)
            .is_some_and(|p| p.terminal && p.released && p.cancellation.is_none())
        {
            return;
        }
        let p = self.pending.remove(&sequence).unwrap();
        self.admissions.remove(&p.admission.sequence);
        self.usage.requests -= 1;
        self.usage.bytes -= p.charged;
        let n = self.groups.get_mut(&p.ticket.group).unwrap();
        *n -= 1;
        if *n == 0 {
            self.groups.remove(&p.ticket.group);
        }
    }
    /// Feed each shared owner step once, before dispatching its effect leases.
    /// Read success still requires the original live ReadReady and actual callback.
    pub fn observe_steps<S: ReadyScheduler, T: TimerService, E: ElectionEntropy>(
        &mut self,
        owner: &mut EffectOwner<S, T, E>,
        steps: &[OwnerStep],
    ) -> Result<(), ReadInvocationError> {
        if owner.identity() != self.binding.owner {
            return Err(ReadInvocationError::WrongBinding);
        }
        if owner.is_failed() {
            return Err(ReadInvocationError::Fenced);
        }
        for step in steps {
            let Some(admission) = step.admission else {
                continue;
            };
            let Some((sequence, cancel)) = self.admissions.get(&admission.sequence).copied() else {
                continue;
            };
            let p = &self.pending[&sequence];
            let exact = if cancel {
                p.cancellation == Some(admission)
            } else {
                p.admission == admission
            };
            if !exact
                || step.visit.owner != self.binding.owner
                || step.visit.group != p.ticket.group
                || (!cancel && step.read != Some(p.ticket.request))
            {
                let _ = owner.fail::<()>(EffectOwnerError::ProviderContract);
                return Err(ReadInvocationError::ProviderViolation);
            }
            self.admissions.remove(&admission.sequence);
            if cancel {
                if step
                    .error
                    .as_ref()
                    .is_some_and(|e| *e != RaftError::StaleRead)
                {
                    let _ = owner.fail::<()>(EffectOwnerError::ProviderContract);
                    return Err(ReadInvocationError::ProviderViolation);
                }
                self.pending.get_mut(&sequence).unwrap().cancellation = None;
                self.terminal(sequence);
            } else {
                self.pending.get_mut(&sequence).unwrap().stepped = true;
                if let Some(error) = &step.error {
                    self.terminal(sequence);
                    self.publish(sequence, ReadOutcome::NotRead(error.clone()));
                }
            }
            self.cleanup(sequence);
        }
        Ok(())
    }
    pub fn cancel_wait(&mut self, ticket: ReadInvocationTicket) -> Result<(), ReadInvocationError> {
        let p = self
            .pending
            .get(&ticket.sequence)
            .filter(|p| p.ticket == ticket)
            .ok_or(ReadInvocationError::StaleTicket)?;
        if p.has_output {
            return Err(ReadInvocationError::StaleTicket);
        }
        self.publish(
            ticket.sequence,
            ReadOutcome::Unavailable(ReadUnavailable::Cancelled),
        );
        Ok(())
    }
    fn check_execution<S: ReadyScheduler, T: TimerService, E: ElectionEntropy>(
        &self,
        owner: &EffectOwner<S, T, E>,
        lease: &EffectLease,
    ) -> Result<(u64, ReadBarrier), ReadInvocationError> {
        if owner.identity() != self.binding.owner {
            return Err(ReadInvocationError::WrongBinding);
        }
        owner
            .validate_lease(lease)
            .map_err(|e| ReadInvocationError::Execution(ReadRouteError::Owner(e)))?;
        let Effect::ReadReady(barrier) = lease.effect else {
            return Err(ReadInvocationError::Execution(ReadRouteError::WrongEffect));
        };
        let sequence = self
            .active
            .get(&barrier.group())
            .copied()
            .ok_or(ReadInvocationError::StaleTicket)?;
        let p = &self.pending[&sequence];
        if !p.stepped || p.ticket.request != barrier.request() || p.terminal {
            return Err(ReadInvocationError::StaleTicket);
        }
        Ok((sequence, barrier))
    }
    /// Execute only the query owned by the original read invocation. A cancelled
    /// wait consumes its ready lease without requiring application catchup.
    pub fn execute<
        A: BoundedReadableStateMachine<Query = Q, ReadResult = R>,
        S: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
    >(
        &mut self,
        owner: &mut EffectOwner<S, T, E>,
        lease: EffectLease,
        application: &A,
        now: MonoTime,
    ) -> Result<(), ReadExecutionError> {
        let checked = self.check_execution(owner, &lease);
        let (sequence, barrier) = match checked {
            Ok(v) => v,
            Err(reason) => {
                return Err(ReadExecutionError::Rejected {
                    reason,
                    lease: Box::new(lease),
                })
            }
        };
        let p = &self.pending[&sequence];
        let changed = owner
            .core(barrier.group())
            .is_none_or(|c| c.role() != Role::Leader || c.state().hard_state.term != p.term);
        if changed {
            self.publish(
                sequence,
                ReadOutcome::Unavailable(ReadUnavailable::LeadershipChanged),
            );
        }
        let p = &self.pending[&sequence];
        let invalid = if p.has_output {
            None
        } else {
            let query = p.query.as_ref().unwrap();
            match (
                application.query_bytes(query, self.limits.query_bytes),
                application.read_result_bound(query),
            ) {
                (Ok(n), Ok(b))
                    if n <= p.query_bound && b >= size_of::<R>() && b <= p.result_bound =>
                {
                    None
                }
                (Err(e), _) | (_, Err(e)) => Some(e),
                _ => Some(ApplicationError::ReceiptBudget),
            }
        };
        if p.has_output || invalid.is_some() {
            if let Err(rejected) = owner.cancel_read(lease, now) {
                return Err(ReadExecutionError::Rejected {
                    reason: ReadInvocationError::Execution(ReadRouteError::Owner(rejected.reason)),
                    lease: rejected.lease,
                });
            }
            if let Some(error) = invalid {
                self.publish(sequence, ReadOutcome::NotRead(RaftError::Admission(error)));
            }
            self.terminal(sequence);
            self.cleanup(sequence);
            return Ok(());
        }
        let query = self
            .pending
            .get_mut(&sequence)
            .unwrap()
            .query
            .take()
            .unwrap();
        match self.execution.submit(owner, lease, application, query, now) {
            Ok(_) => {
                let output = self.execution.poll().unwrap();
                let barrier = *output.barrier();
                let result = self
                    .execution
                    .complete(output)
                    .unwrap_or_else(|_| unreachable!("original private read output"));
                self.terminal(sequence);
                self.publish(sequence, ReadOutcome::Read { barrier, result });
                self.cleanup(sequence);
                Ok(())
            }
            Err(ReadSubmitError::Rejected {
                reason,
                lease,
                query,
            }) => {
                self.pending.get_mut(&sequence).unwrap().query = Some(query);
                Err(ReadExecutionError::Rejected {
                    reason: ReadInvocationError::Execution(reason),
                    lease,
                })
            }
            Err(ReadSubmitError::Failed { reason }) => {
                self.closed = true;
                self.terminal(sequence);
                self.publish(
                    sequence,
                    ReadOutcome::Unavailable(ReadUnavailable::OwnerFailed),
                );
                self.cleanup(sequence);
                Err(ReadExecutionError::Failed {
                    reason: ReadInvocationError::Execution(reason),
                })
            }
        }
    }
    /// Fair bounded observation and cancellation admission. Cancellation is queued
    /// only after the original Read step, so control priority cannot overtake it.
    pub fn reconcile<S: ReadyScheduler, T: TimerService, E: ElectionEntropy>(
        &mut self,
        owner: &mut EffectOwner<S, T, E>,
        visits: usize,
    ) -> Result<usize, ReadInvocationError> {
        if owner.identity() != self.binding.owner {
            return Err(ReadInvocationError::WrongBinding);
        }
        if visits > 65536 {
            return Err(ReadInvocationError::InvalidLimits);
        }
        let mut visited = 0;
        for _ in 0..visits {
            let sequence = self
                .pending
                .range((Excluded(self.cursor), Unbounded))
                .next()
                .map(|(s, _)| *s)
                .or_else(|| self.pending.first_key_value().map(|(s, _)| *s));
            let Some(sequence) = sequence else {
                break;
            };
            self.cursor = sequence;
            visited += 1;
            let p = &self.pending[&sequence];
            if owner.is_failed() {
                if let Some(a) = self.pending.get_mut(&sequence).unwrap().cancellation.take() {
                    self.admissions.remove(&a.sequence);
                }
                self.terminal(sequence);
                self.publish(
                    sequence,
                    ReadOutcome::Unavailable(ReadUnavailable::OwnerFailed),
                );
            } else if !p.terminal {
                let changed = owner.core(p.ticket.group).is_none_or(|c| {
                    c.role() != Role::Leader || c.state().hard_state.term != p.term
                });
                if changed {
                    self.publish(
                        sequence,
                        ReadOutcome::Unavailable(ReadUnavailable::LeadershipChanged),
                    );
                }
                let p = &self.pending[&sequence];
                if p.stepped && p.has_output && p.cancellation.is_none() {
                    match owner.admit_tracked(
                        p.ticket.group,
                        Event::CancelRead {
                            request: p.ticket.request,
                        },
                    ) {
                        Ok(a) => {
                            self.pending.get_mut(&sequence).unwrap().cancellation = Some(a);
                            self.admissions.insert(a.sequence, (sequence, true));
                        }
                        Err(rejected)
                            if matches!(
                                rejected.reason,
                                RuntimeError::Overloaded | RuntimeError::Closed
                            ) => {}
                        Err(rejected) => return Err(ReadInvocationError::Runtime(rejected.reason)),
                    }
                }
            }
            self.cleanup(sequence);
        }
        Ok(visited)
    }
    pub fn poll(&mut self) -> Option<ReadCompletion<R>> {
        let sequence = self.ready.pop_front()?;
        let p = self.pending.get_mut(&sequence).unwrap();
        p.exported = true;
        Some(ReadCompletion {
            ticket: p.ticket,
            outcome: p.outcome.take().unwrap(),
        })
    }
    pub fn complete(
        &mut self,
        output: ReadCompletion<R>,
    ) -> Result<ReadOutcome<R>, ReadCompletionRejected<R>> {
        let sequence = output.ticket.sequence;
        if !self
            .pending
            .get(&sequence)
            .is_some_and(|p| p.ticket == output.ticket && p.exported && !p.released)
        {
            return Err(ReadCompletionRejected {
                reason: ReadInvocationError::StaleTicket,
                completion: Box::new(output),
            });
        }
        self.pending.get_mut(&sequence).unwrap().released = true;
        self.cleanup(sequence);
        Ok(output.outcome)
    }
    /// Fences the runtime; accepted external WAL still drains under its own owner.
    pub fn abort<S: ReadyScheduler, T: TimerService, E: ElectionEntropy>(
        &mut self,
        owner: &mut EffectOwner<S, T, E>,
    ) -> Result<(), ReadInvocationError> {
        if owner.identity() != self.binding.owner {
            return Err(ReadInvocationError::WrongBinding);
        }
        self.closed = true;
        let _ = owner.fail::<()>(EffectOwnerError::Runtime(RuntimeError::Fenced));
        let sequences = self.pending.keys().copied().collect::<Vec<_>>();
        for sequence in sequences {
            if let Some(a) = self.pending.get_mut(&sequence).unwrap().cancellation.take() {
                self.admissions.remove(&a.sequence);
            }
            self.terminal(sequence);
            self.publish(sequence, ReadOutcome::Unavailable(ReadUnavailable::Aborted));
            self.cleanup(sequence);
        }
        Ok(())
    }
}
