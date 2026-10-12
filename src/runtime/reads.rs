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
//! Bounded ownership of results from original one-use quorum read barriers.
use super::*;
use crate::logical_voter::LogicalActivationControlProfile;
mod control;
#[cfg(test)]
mod tests;
use crate::application::{ApplicationError, BoundedReadableStateMachine};
pub(super) use control::ReadCapacityClass;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReadRouterBinding {
    pub owner: RuntimeOwner,
    pub generation: ReadRouterGeneration,
    authorization: RuntimeAuthorization,
}
impl ReadRouterBinding {
    pub fn new(authorization: RuntimeAuthorization, generation: ReadRouterGeneration) -> Self {
        Self {
            owner: authorization.owner(),
            generation,
            authorization,
        }
    }
    pub fn authorization(self) -> RuntimeAuthorization {
        self.authorization
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReadResultTicket {
    pub binding: ReadRouterBinding,
    pub sequence: u64,
}
#[derive(Clone, Copy, Debug)]
pub struct ReadRouterLimits {
    pub results: usize,
    pub group_results: usize,
    pub bytes: usize,
    pub query_bytes: usize,
    pub result_bytes: usize,
    /// Whole selected activation windows; physical/unselected routers ignore these.
    pub control_results: usize,
    pub group_control_results: usize,
    pub control_bytes: usize,
}
impl Default for ReadRouterLimits {
    fn default() -> Self {
        Self {
            results: 4096,
            group_results: 64,
            bytes: 16 * 1024 * 1024,
            query_bytes: 64 * 1024,
            result_bytes: 1024 * 1024,
            control_results: 2,
            group_control_results: 2,
            control_bytes: 4 * 1024 * 1024,
        }
    }
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ReadResultUsage {
    pub results: usize,
    pub bytes: usize,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReadRouteError {
    LogicalReleaseRequired,
    InvalidLimits,
    WrongBinding,
    WrongEffect,
    Closed,
    Overloaded,
    ResultTooLarge,
    Exhausted,
    StaleResult,
    ProviderViolation,
    Application(ApplicationError),
    Owner(EffectOwnerError),
}
/// Rejected means the original query and effect remain available. Failed means
/// authority was consumed; a dishonest provider was fenced and must recover.
#[derive(Debug)]
pub enum ReadSubmitError<Q> {
    Rejected {
        reason: ReadRouteError,
        lease: Box<EffectLease>,
        query: Q,
    },
    Failed {
        reason: ReadRouteError,
    },
}
/// Opaque observation of one read, not reusable authority for another read.
#[derive(Debug)]
pub struct ReadResults<R> {
    ticket: ReadResultTicket,
    effect: EffectTicket,
    barrier: ReadBarrier,
    result: Result<R, ApplicationError>,
}
impl<R> ReadResults<R> {
    pub fn ticket(&self) -> ReadResultTicket {
        self.ticket
    }
    pub fn effect(&self) -> EffectTicket {
        self.effect
    }
    pub fn barrier(&self) -> &ReadBarrier {
        &self.barrier
    }
    pub fn result(&self) -> &Result<R, ApplicationError> {
        &self.result
    }
}
#[derive(Debug)]
pub struct ReadResultRejected<R> {
    pub reason: ReadRouteError,
    pub results: Box<ReadResults<R>>,
}
#[derive(Debug)]
pub(crate) struct CapturedRead<R> {
    pub(crate) lease: EffectLease,
    pub(crate) results: ReadResults<R>,
}
struct Held<R> {
    group: GroupIdentity,
    charged: usize,
    class: ReadCapacityClass,
    output: Option<ReadResults<R>>,
}
/// The host retains original query/request correlation until ReadReady arrives.
/// This owner consumes only that exact lease on the serialized group owner.
/// No thread, application, store, clock, or executor is constructed.
pub struct ReadRouter<R> {
    binding: ReadRouterBinding,
    limits: ReadRouterLimits,
    held: BTreeMap<u64, Held<R>>,
    groups: BTreeMap<GroupIdentity, usize>,
    ready: VecDeque<u64>,
    usage: ReadResultUsage,
    bulk: ReadResultUsage,
    bulk_groups: BTreeMap<GroupIdentity, usize>,
    control_profile: Option<LogicalActivationControlProfile>,
    sequence: u64,
    closed: bool,
}
impl<R> ReadRouter<R> {
    pub(super) fn accepts_work(&self) -> bool {
        !self.closed
    }
    pub(super) fn fits_single<Q>(&self, query: usize, result: usize) -> bool {
        query <= self.limits.query_bytes
            && result <= self.limits.result_bytes
            && query
                .checked_add(size_of::<Q>())
                .and_then(|n| n.checked_add(result))
                .and_then(|n| n.checked_add(size_of::<Held<R>>()))
                .is_some_and(|n| n <= self.limits.bytes)
    }
    pub fn new(
        binding: ReadRouterBinding,
        limits: ReadRouterLimits,
    ) -> Result<Self, ReadRouteError> {
        if binding.authorization.is_logical() {
            return Err(ReadRouteError::LogicalReleaseRequired);
        }
        Self::from_binding(binding, limits)
    }
    pub(crate) fn new_logical<Q>(
        binding: ReadRouterBinding,
        limits: ReadRouterLimits,
        profile: Option<&LogicalActivationControlProfile>,
    ) -> Result<Self, ReadRouteError> {
        if !binding.authorization.is_logical() {
            return Err(ReadRouteError::WrongBinding);
        }
        let router = Self::from_binding(binding, limits)?;
        let Some(profile) = profile else {
            return Ok(router);
        };
        if binding.authorization.logical().unwrap().mapping.parent != profile.parent() {
            return Err(ReadRouteError::WrongBinding);
        }
        router.select_activation::<Q>(profile.clone())
    }
    fn from_binding(
        binding: ReadRouterBinding,
        limits: ReadRouterLimits,
    ) -> Result<Self, ReadRouteError> {
        if limits.results == 0
            || limits.results > 65536
            || limits.group_results == 0
            || limits.group_results > limits.results
            || limits.bytes == 0
            || limits.bytes > 1024 * 1024 * 1024
            || limits.query_bytes > 1024 * 1024
            || limits.result_bytes < size_of::<R>()
            || limits.result_bytes > 16 * 1024 * 1024
            || limits
                .result_bytes
                .checked_add(size_of::<Held<R>>())
                .is_none_or(|n| n > limits.bytes)
        {
            return Err(ReadRouteError::InvalidLimits);
        }
        Ok(Self {
            binding,
            limits,
            held: BTreeMap::new(),
            groups: BTreeMap::new(),
            ready: VecDeque::with_capacity(limits.results),
            usage: ReadResultUsage::default(),
            bulk: ReadResultUsage::default(),
            bulk_groups: BTreeMap::new(),
            control_profile: None,
            sequence: 0,
            closed: false,
        })
    }
    pub fn binding(&self) -> ReadRouterBinding {
        self.binding
    }
    pub fn limits(&self) -> ReadRouterLimits {
        self.limits
    }
    pub fn usage(&self) -> ReadResultUsage {
        self.usage
    }
    pub fn close(&mut self) {
        self.closed = true;
    }
    pub fn is_drained(&self) -> bool {
        self.held.is_empty()
    }

    fn check_submit<
        A: BoundedReadableStateMachine<ReadResult = R>,
        Q: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
    >(
        &self,
        owner: &EffectOwner<Q, T, E>,
        lease: &EffectLease,
        application: &A,
        query: &A::Query,
        class: ReadCapacityClass,
    ) -> Result<(ReadBarrier, usize, usize, u64), ReadRouteError> {
        if self.closed {
            return Err(ReadRouteError::Closed);
        }
        if owner.identity() != self.binding.owner
            || owner.authorization() != self.binding.authorization
            || lease.ticket.visit.owner != self.binding.owner
        {
            return Err(ReadRouteError::WrongBinding);
        }
        owner.validate_lease(lease).map_err(ReadRouteError::Owner)?;
        let Effect::ReadReady(barrier) = lease.effect else {
            return Err(ReadRouteError::WrongEffect);
        };
        self.check_capacity(barrier.group(), 0, class)?;
        let nested = application
            .query_bytes(query, self.limits.query_bytes)
            .map_err(ReadRouteError::Application)?;
        let bound = application
            .read_result_bound(query)
            .map_err(ReadRouteError::Application)?;
        if nested > self.limits.query_bytes
            || bound < size_of::<R>()
            || bound > self.limits.result_bytes
        {
            return Err(ReadRouteError::ResultTooLarge);
        }
        let charged = nested
            .checked_add(size_of::<A::Query>())
            .and_then(|n| n.checked_add(bound))
            .and_then(|n| n.checked_add(size_of::<Held<R>>()))
            .ok_or(ReadRouteError::ResultTooLarge)?;
        self.check_capacity(barrier.group(), charged, class)?;
        let sequence = self
            .sequence
            .checked_add(1)
            .ok_or(ReadRouteError::Exhausted)?;
        let core = owner
            .core(barrier.group())
            .ok_or(ReadRouteError::WrongBinding)?;
        application
            .validate_group(barrier.group())
            .map_err(ReadRouteError::Application)?;
        if application.applied_index() > core.state().commit_index {
            return Err(ReadRouteError::Application(ApplicationError::NotApplied));
        }
        Ok((barrier, bound, charged, sequence))
    }
    pub fn submit<
        A: BoundedReadableStateMachine<ReadResult = R>,
        Q: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
    >(
        &mut self,
        owner: &mut EffectOwner<Q, T, E>,
        lease: EffectLease,
        application: &A,
        query: A::Query,
        now: MonoTime,
    ) -> Result<ReadResultTicket, ReadSubmitError<A::Query>> {
        if owner.authorization().is_logical() || self.binding.authorization.is_logical() {
            return Err(ReadSubmitError::Rejected {
                reason: ReadRouteError::LogicalReleaseRequired,
                lease: Box::new(lease),
                query,
            });
        }
        let CapturedRead { lease, results } =
            self.capture_owned(owner, lease, application, query, now)?;
        if let Err(rejected) = owner.complete_read_capture(lease) {
            let _ = self.complete_owned(results);
            self.closed = true;
            let _ = owner.fail::<()>(EffectOwnerError::ProviderContract);
            let _ = owner.discard_failed(*rejected.lease);
            return Err(ReadSubmitError::Failed {
                reason: ReadRouteError::Owner(rejected.reason),
            });
        }
        let ticket = results.ticket;
        self.held.get_mut(&ticket.sequence).unwrap().output = Some(results);
        self.ready.push_back(ticket.sequence);
        Ok(ticket)
    }
    pub(crate) fn capture_owned<
        A: BoundedReadableStateMachine<ReadResult = R>,
        Q: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
    >(
        &mut self,
        owner: &mut EffectOwner<Q, T, E>,
        lease: EffectLease,
        application: &A,
        query: A::Query,
        now: MonoTime,
    ) -> Result<CapturedRead<R>, ReadSubmitError<A::Query>> {
        self.capture_invocation(
            owner,
            lease,
            application,
            query,
            now,
            ReadCapacityClass::Ordinary,
        )
    }
    pub(super) fn capture_invocation<
        A: BoundedReadableStateMachine<ReadResult = R>,
        Q: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
    >(
        &mut self,
        owner: &mut EffectOwner<Q, T, E>,
        lease: EffectLease,
        application: &A,
        query: A::Query,
        now: MonoTime,
        class: ReadCapacityClass,
    ) -> Result<CapturedRead<R>, ReadSubmitError<A::Query>> {
        let checked = self.check_submit(owner, &lease, application, &query, class);
        let (barrier, bound, charged, sequence) = match checked {
            Ok(v) => v,
            Err(reason) => {
                return Err(ReadSubmitError::Rejected {
                    reason,
                    lease: Box::new(lease),
                    query,
                })
            }
        };
        let effect = lease.ticket;
        let mut query = Some(query);
        // No intervening event can change authority between finish_read and the
        // immutable application callback. Insufficient catchup leaves both intact.
        let output = owner.capture_read(&lease, application.applied_index(), now, || {
            application.read_at(barrier.index(), query.take().unwrap())
        });
        let result = match output {
            Ok(result) => result,
            Err(reason) => {
                return Err(ReadSubmitError::Rejected {
                    reason: ReadRouteError::Owner(reason),
                    lease: Box::new(lease),
                    query: query.unwrap(),
                });
            }
        };
        if let Ok(value) = &result {
            if application
                .read_result_bytes(value, bound - size_of::<R>())
                .ok()
                .filter(|n| *n <= bound - size_of::<R>())
                .is_none()
            {
                self.closed = true;
                let _ = owner.fail::<()>(EffectOwnerError::ProviderContract);
                let _ = owner.discard_failed(lease);
                return Err(ReadSubmitError::Failed {
                    reason: ReadRouteError::ProviderViolation,
                });
            }
        }
        self.sequence = sequence;
        let ticket = ReadResultTicket {
            binding: self.binding,
            sequence,
        };
        self.held.insert(
            sequence,
            Held {
                group: barrier.group(),
                charged,
                class,
                output: None,
            },
        );
        *self.groups.entry(barrier.group()).or_default() += 1;
        self.usage.results += 1;
        self.usage.bytes += charged;
        if class == ReadCapacityClass::Ordinary {
            self.bulk.results += 1;
            self.bulk.bytes += charged;
            *self.bulk_groups.entry(barrier.group()).or_default() += 1;
        }
        Ok(CapturedRead {
            lease,
            results: ReadResults {
                ticket,
                effect,
                barrier,
                result,
            },
        })
    }
    pub fn poll(&mut self) -> Option<ReadResults<R>> {
        if self.binding.authorization.is_logical() {
            return None;
        }
        let sequence = self.ready.pop_front()?;
        self.held.get_mut(&sequence)?.output.take()
    }
    pub fn complete(
        &mut self,
        output: ReadResults<R>,
    ) -> Result<Result<R, ApplicationError>, ReadResultRejected<R>> {
        if self.binding.authorization.is_logical() {
            return Err(ReadResultRejected {
                reason: ReadRouteError::LogicalReleaseRequired,
                results: Box::new(output),
            });
        }
        self.complete_owned(output)
    }
    pub(crate) fn validate_result(&self, output: &ReadResults<R>) -> Result<(), ReadRouteError> {
        if output.ticket.binding != self.binding
            || self
                .held
                .get(&output.ticket.sequence)
                .is_none_or(|h| h.output.is_some() || h.group != output.barrier.group())
        {
            return Err(ReadRouteError::StaleResult);
        }
        Ok(())
    }
    pub(crate) fn complete_owned(
        &mut self,
        output: ReadResults<R>,
    ) -> Result<Result<R, ApplicationError>, ReadResultRejected<R>> {
        if let Err(reason) = self.validate_result(&output) {
            return Err(ReadResultRejected {
                reason,
                results: Box::new(output),
            });
        }
        let held = self.held.remove(&output.ticket.sequence).unwrap();
        self.usage.results -= 1;
        self.usage.bytes -= held.charged;
        if held.class == ReadCapacityClass::Ordinary {
            self.bulk.results -= 1;
            self.bulk.bytes -= held.charged;
            let count = self.bulk_groups.get_mut(&held.group).unwrap();
            *count -= 1;
            if *count == 0 {
                self.bulk_groups.remove(&held.group);
            }
        }
        let count = self.groups.get_mut(&held.group).unwrap();
        *count -= 1;
        if *count == 0 {
            self.groups.remove(&held.group);
        }
        Ok(output.result)
    }
}
