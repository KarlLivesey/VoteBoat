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
use crate::application::{ApplicationError, BoundedReadableStateMachine};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReadRouterBinding {
    pub owner: RuntimeOwner,
    pub generation: ReadRouterGeneration,
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
}
impl Default for ReadRouterLimits {
    fn default() -> Self {
        Self {
            results: 4096,
            group_results: 64,
            bytes: 16 * 1024 * 1024,
            query_bytes: 64 * 1024,
            result_bytes: 1024 * 1024,
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
struct Held<R> {
    group: GroupIdentity,
    charged: usize,
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
        let checked = (|| {
            if self.closed {
                return Err(ReadRouteError::Closed);
            }
            if owner.identity() != self.binding.owner
                || lease.ticket.visit.owner != self.binding.owner
            {
                return Err(ReadRouteError::WrongBinding);
            }
            owner
                .validate_lease(&lease)
                .map_err(ReadRouteError::Owner)?;
            let Effect::ReadReady(barrier) = lease.effect else {
                return Err(ReadRouteError::WrongEffect);
            };
            if self.usage.results == self.limits.results
                || self.groups.get(&barrier.group()).copied().unwrap_or(0)
                    >= self.limits.group_results
            {
                return Err(ReadRouteError::Overloaded);
            }
            let nested = application
                .query_bytes(&query, self.limits.query_bytes)
                .map_err(ReadRouteError::Application)?;
            let bound = application
                .read_result_bound(&query)
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
            if charged > self.limits.bytes - self.usage.bytes {
                return Err(ReadRouteError::Overloaded);
            }
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
        })();
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
        let output = owner.complete_effect(lease, application.applied_index(), now, |_| {
            Ok((
                vec![],
                application.read_at(barrier.index(), query.take().unwrap()),
            ))
        });
        let result = match output {
            Ok(result) => result,
            Err(rejected) => {
                let Some(query) = query else {
                    self.closed = true;
                    let _ = owner.fail::<()>(EffectOwnerError::ProviderContract);
                    let _ = owner.discard_failed(*rejected.lease);
                    return Err(ReadSubmitError::Failed {
                        reason: ReadRouteError::Owner(rejected.reason),
                    });
                };
                return Err(ReadSubmitError::Rejected {
                    reason: ReadRouteError::Owner(rejected.reason),
                    lease: rejected.lease,
                    query,
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
                output: Some(ReadResults {
                    ticket,
                    effect,
                    barrier,
                    result,
                }),
            },
        );
        *self.groups.entry(barrier.group()).or_default() += 1;
        self.ready.push_back(sequence);
        self.usage.results += 1;
        self.usage.bytes += charged;
        Ok(ticket)
    }
    pub fn poll(&mut self) -> Option<ReadResults<R>> {
        let sequence = self.ready.pop_front()?;
        self.held.get_mut(&sequence)?.output.take()
    }
    pub fn complete(
        &mut self,
        output: ReadResults<R>,
    ) -> Result<Result<R, ApplicationError>, ReadResultRejected<R>> {
        if output.ticket.binding != self.binding
            || self
                .held
                .get(&output.ticket.sequence)
                .is_none_or(|h| h.output.is_some() || h.group != output.barrier.group())
        {
            return Err(ReadResultRejected {
                reason: ReadRouteError::StaleResult,
                results: Box::new(output),
            });
        }
        let held = self.held.remove(&output.ticket.sequence).unwrap();
        self.usage.results -= 1;
        self.usage.bytes -= held.charged;
        let count = self.groups.get_mut(&held.group).unwrap();
        *count -= 1;
        if *count == 0 {
            self.groups.remove(&held.group);
        }
        Ok(output.result)
    }
}
