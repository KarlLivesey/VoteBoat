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
//! Client proposal ownership, deterministic application admission and outcomes.
use super::*;
use crate::{application::*, log::EntryPayload};
use std::ops::Bound::{Excluded, Unbounded};
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClientRouterBinding {
    pub owner: RuntimeOwner,
    pub generation: ClientRouterGeneration,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClientTicket {
    pub binding: ClientRouterBinding,
    pub sequence: u64,
    pub group: GroupIdentity,
    pub operation: OperationId,
}
#[derive(Debug)]
pub struct ClientRequest {
    pub group: GroupIdentity,
    pub operation: OperationId,
    pub bytes: Vec<u8>,
}
#[derive(Clone, Copy, Debug)]
pub struct ClientRouterLimits {
    pub requests: usize,
    pub group_requests: usize,
    pub bytes: usize,
    pub command_bytes: usize,
    pub receipt_bytes: usize,
}
impl Default for ClientRouterLimits {
    fn default() -> Self {
        Self {
            requests: 4096,
            group_requests: 64,
            bytes: 32 * 1024 * 1024,
            command_bytes: 64 * 1024,
            receipt_bytes: 1024 * 1024,
        }
    }
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ClientUsage {
    pub requests: usize,
    pub bytes: usize,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClientError {
    InvalidLimits,
    WrongBinding,
    UnknownGroup,
    Closed,
    Fenced,
    Overloaded,
    RequestTooLarge,
    Exhausted,
    StaleTicket,
    ProviderViolation,
    Application(ApplicationError),
    Runtime(RuntimeError),
    Owner(EffectOwnerError),
    Consensus(RaftError),
}
#[derive(Debug)]
pub struct ClientRejected {
    pub reason: ClientError,
    pub request: ClientRequest,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClientUnknown {
    CancelledWait,
    LeadershipChanged,
    OwnerFailed,
    Aborted,
}
#[derive(Debug)]
pub enum ClientOutcome<R> {
    Applied {
        position: ProposalPosition,
        receipt: R,
    },
    /// Exact stepped invocation failed before generating a proposal.
    NotProposed(RaftError),
    /// Stops observation only. The operation may still commit/apply.
    Unknown(ClientUnknown),
}
#[derive(Debug)]
pub struct ClientCompletion<R> {
    ticket: ClientTicket,
    outcome: ClientOutcome<R>,
}
impl<R> ClientCompletion<R> {
    pub fn ticket(&self) -> ClientTicket {
        self.ticket
    }
    pub fn outcome(&self) -> &ClientOutcome<R> {
        &self.outcome
    }
}
#[derive(Debug)]
pub struct ClientCompletionRejected<R> {
    pub reason: ClientError,
    pub completion: Box<ClientCompletion<R>>,
}
#[derive(Debug)]
pub struct ClientDeliveryRejected<R> {
    pub reason: ClientError,
    pub results: Box<ApplicationResults<R>>,
}
struct Pending<R> {
    ticket: ClientTicket,
    admission: AdmissionTicket,
    bytes: Vec<u8>,
    term: u64,
    receipt_bound: usize,
    charged: usize,
    position: Option<ProposalPosition>,
    stepped: bool,
    terminal: bool,
    has_output: bool,
    exported: bool,
    released: bool,
    outcome: Option<ClientOutcome<R>>,
}
/// Fixed bounded ownership over public application/runtime providers. All service
/// proposals for a group must use one router while this owner is live; lower-level
/// direct Propose APIs remain available to explicitly budgeted embedding hosts.
/// Accepted tickets prove only volatile admission, never commit/application.
pub struct ClientRouter<R> {
    binding: ClientRouterBinding,
    limits: ClientRouterLimits,
    pending: BTreeMap<u64, Pending<R>>,
    admissions: BTreeMap<u64, u64>,
    positions: BTreeMap<(GroupIdentity, u64, u64), u64>,
    groups: BTreeMap<GroupIdentity, usize>,
    ready: VecDeque<u64>,
    usage: ClientUsage,
    sequence: u64,
    cursor: u64,
    closed: bool,
}
impl<R: ApplicationReceipt> ClientRouter<R> {
    pub fn new(
        binding: ClientRouterBinding,
        limits: ClientRouterLimits,
    ) -> Result<Self, ClientError> {
        if limits.requests == 0
            || limits.requests > 65536
            || limits.group_requests == 0
            || limits.group_requests > limits.requests
            || limits.bytes == 0
            || limits.bytes > 1024 * 1024 * 1024
            || limits.command_bytes == 0
            || limits.command_bytes > 1024 * 1024
            || limits.receipt_bytes < size_of::<R>()
            || limits.receipt_bytes > 16 * 1024 * 1024
            || limits
                .receipt_bytes
                .checked_add(size_of::<Pending<R>>())
                .is_none_or(|n| n > limits.bytes)
        {
            return Err(ClientError::InvalidLimits);
        }
        Ok(Self {
            binding,
            limits,
            pending: BTreeMap::new(),
            admissions: BTreeMap::new(),
            positions: BTreeMap::new(),
            groups: BTreeMap::new(),
            ready: VecDeque::with_capacity(limits.requests),
            usage: ClientUsage::default(),
            sequence: 0,
            cursor: 0,
            closed: false,
        })
    }
    pub fn binding(&self) -> ClientRouterBinding {
        self.binding
    }
    pub fn limits(&self) -> ClientRouterLimits {
        self.limits
    }
    pub fn usage(&self) -> ClientUsage {
        self.usage
    }
    pub fn close(&mut self) {
        self.closed = true;
    }
    pub fn is_drained(&self) -> bool {
        self.pending.is_empty()
    }
    pub fn submit<
        A: ProposalAdmission<Receipt = R>,
        Q: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
    >(
        &mut self,
        owner: &mut EffectOwner<Q, T, E>,
        application: &A,
        request: ClientRequest,
    ) -> Result<ClientTicket, ClientRejected> {
        let check = (|| {
            if self.closed {
                return Err(ClientError::Closed);
            }
            if owner.identity() != self.binding.owner {
                return Err(ClientError::WrongBinding);
            }
            if owner.is_failed() {
                return Err(ClientError::Fenced);
            }
            let core = owner.core(request.group).ok_or(ClientError::UnknownGroup)?;
            if core.role() != Role::Leader {
                return Err(ClientError::Consensus(RaftError::NotLeader));
            }
            if request.bytes.len() > self.limits.command_bytes
                || request.bytes.capacity() > self.limits.bytes
            {
                return Err(ClientError::RequestTooLarge);
            }
            if self.usage.requests >= self.limits.requests
                || self.groups.get(&request.group).copied().unwrap_or(0)
                    >= self.limits.group_requests
            {
                return Err(ClientError::Overloaded);
            }
            let state = core.state();
            if application.applied_index() < state.base_index()
                || application.applied_index() > state.commit_index
            {
                return Err(ClientError::Application(ApplicationError::NotApplied));
            }
            let log = state
                .entries
                .iter()
                .filter(|e| e.index > application.applied_index())
                .filter_map(|e| match &e.payload {
                    EntryPayload::Command { operation, bytes } => {
                        Some((*operation, bytes.as_slice()))
                    }
                    _ => None,
                });
            let queued = self
                .pending
                .values()
                .filter(|p| p.ticket.group == request.group)
                .map(|p| (p.ticket.operation, p.bytes.as_slice()));
            let receipt_bound = application
                .validate_proposal(request.operation, &request.bytes, log.chain(queued))
                .map_err(ClientError::Application)?;
            if receipt_bound < size_of::<R>() || receipt_bound > self.limits.receipt_bytes {
                return Err(ClientError::RequestTooLarge);
            }
            let charged = request
                .bytes
                .capacity()
                .checked_add(request.bytes.len())
                .and_then(|n| n.checked_add(receipt_bound))
                .and_then(|n| n.checked_add(size_of::<Pending<R>>()))
                .ok_or(ClientError::RequestTooLarge)?;
            if charged > self.limits.bytes - self.usage.bytes {
                return Err(ClientError::Overloaded);
            }
            let sequence = self.sequence.checked_add(1).ok_or(ClientError::Exhausted)?;
            Ok((receipt_bound, charged, sequence, state.hard_state.term))
        })();
        let (receipt_bound, charged, sequence, term) = match check {
            Ok(v) => v,
            Err(reason) => return Err(ClientRejected { reason, request }),
        };
        // The runtime owns an exact-length copy; retained original bytes preserve
        // future admission accounting and content correlation. Both are reserved.
        let event = Event::Propose {
            operation: request.operation,
            bytes: request.bytes.as_slice().to_vec(),
        };
        let admission = match owner.admit_tracked(request.group, event) {
            Ok(t) => t,
            Err(rejected) => {
                return Err(ClientRejected {
                    reason: ClientError::Runtime(rejected.reason),
                    request,
                })
            }
        };
        self.sequence = sequence;
        let ticket = ClientTicket {
            binding: self.binding,
            sequence,
            group: request.group,
            operation: request.operation,
        };
        self.pending.insert(
            sequence,
            Pending {
                ticket,
                admission,
                bytes: request.bytes,
                term,
                receipt_bound,
                charged,
                position: None,
                stepped: false,
                terminal: false,
                has_output: false,
                exported: false,
                released: false,
                outcome: None,
            },
        );
        self.admissions.insert(admission.sequence, sequence);
        *self.groups.entry(ticket.group).or_default() += 1;
        self.usage.requests += 1;
        self.usage.bytes += charged;
        Ok(ticket)
    }
    fn publish(&mut self, sequence: u64, outcome: ClientOutcome<R>) {
        let p = self.pending.get_mut(&sequence).unwrap();
        if !p.has_output {
            p.has_output = true;
            p.outcome = Some(outcome);
            self.ready.push_back(sequence);
        }
    }
    fn cleanup(&mut self, sequence: u64) {
        if !self
            .pending
            .get(&sequence)
            .is_some_and(|p| p.released && p.terminal)
        {
            return;
        }
        let p = self.pending.remove(&sequence).unwrap();
        self.admissions.remove(&p.admission.sequence);
        if let Some(pos) = p.position {
            self.positions
                .remove(&(p.ticket.group, pos.index, pos.term));
        }
        self.usage.requests -= 1;
        self.usage.bytes -= p.charged;
        let n = self.groups.get_mut(&p.ticket.group).unwrap();
        *n -= 1;
        if *n == 0 {
            self.groups.remove(&p.ticket.group);
        }
    }
    /// Drive through this method so proposal steps cannot be confused with another
    /// invocation of the same operation. Other events/steps remain visible to host.
    pub fn advance<
        'a,
        A: ProposalAdmission<Receipt = R> + 'a,
        Q: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
    >(
        &mut self,
        owner: &mut EffectOwner<Q, T, E>,
        now: MonoTime,
        limit: usize,
        application: impl Fn(GroupIdentity) -> Option<&'a A>,
    ) -> Result<Vec<OwnerStep>, ClientError> {
        self.advance_authorized(owner, now, limit, application, |_, event| {
            configuration_auth_required(event)
        })
    }
    pub(super) fn advance_authorized<
        'a,
        A: ProposalAdmission<Receipt = R> + 'a,
        Q: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
    >(
        &mut self,
        owner: &mut EffectOwner<Q, T, E>,
        now: MonoTime,
        limit: usize,
        application: impl Fn(GroupIdentity) -> Option<&'a A>,
        mut configuration: impl FnMut(&Raft, &Event) -> Result<(), RaftError>,
    ) -> Result<Vec<OwnerStep>, ClientError> {
        if owner.identity() != self.binding.owner {
            return Err(ClientError::WrongBinding);
        }
        let steps = owner
            .advance_authorized(now, limit, |core, event| {
                if let Event::Configure(proposal) = event {
                    let app = application(core.state().bootstrap.group)
                        .ok_or(RaftError::Admission(ApplicationError::NotApplied))?;
                    app.validate_deployment_requirements(proposal.requirements)
                        .map_err(RaftError::Admission)?;
                }
                configuration(core, event)?;
                let Event::Propose { operation, bytes } = event else {
                    return Ok(());
                };
                if core.role() != Role::Leader {
                    return Err(RaftError::NotLeader);
                }
                let group = core.state().bootstrap.group;
                let app =
                    application(group).ok_or(RaftError::Admission(ApplicationError::NotApplied))?;
                if app.applied_index() < core.state().base_index()
                    || app.applied_index() > core.state().commit_index
                {
                    return Err(RaftError::Admission(ApplicationError::NotApplied));
                }
                let log = core
                    .state()
                    .entries
                    .iter()
                    .filter(|e| e.index > app.applied_index())
                    .filter_map(|e| match &e.payload {
                        EntryPayload::Command { operation, bytes } => {
                            Some((*operation, bytes.as_slice()))
                        }
                        _ => None,
                    });
                let pending = self
                    .pending
                    .values()
                    .filter(|p| p.ticket.group == group)
                    .map(|p| (p.ticket.operation, p.bytes.as_slice()));
                let bound = app
                    .validate_proposal(*operation, bytes, log.chain(pending))
                    .map_err(RaftError::Admission)?;
                let reserved = self
                    .pending
                    .values()
                    .filter(|p| {
                        p.ticket.group == group
                            && p.ticket.operation == *operation
                            && p.bytes == *bytes
                    })
                    .map(|p| p.receipt_bound)
                    .min()
                    .unwrap_or(self.limits.receipt_bytes);
                if bound < size_of::<R>() || bound > reserved || bound > self.limits.receipt_bytes {
                    return Err(RaftError::Admission(ApplicationError::ReceiptBudget));
                }
                Ok(())
            })
            .map_err(ClientError::Owner)?;
        for step in &steps {
            let Some(admission) = step.admission else {
                continue;
            };
            let Some(sequence) = self.admissions.remove(&admission.sequence) else {
                continue;
            };
            let p = self.pending.get_mut(&sequence).unwrap();
            if admission != p.admission || step.operation != Some(p.ticket.operation) {
                let _ = owner.fail::<()>(EffectOwnerError::ProviderContract);
                return Err(ClientError::ProviderViolation);
            }
            p.stepped = true;
            if let Some(error) = &step.error {
                p.terminal = true;
                self.publish(sequence, ClientOutcome::NotProposed(error.clone()));
            } else if let Some(position) = step.proposed {
                p.position = Some(position);
                self.positions
                    .insert((p.ticket.group, position.index, position.term), sequence);
            } else {
                let _ = owner.fail::<()>(EffectOwnerError::ProviderContract);
                return Err(ClientError::ProviderViolation);
            }
            self.cleanup(sequence);
        }
        Ok(steps)
    }
    /// Exact applied result routing; unmatched follower/replay receipts are dropped.
    /// The application's result queue remains charged on preflight rejection.
    pub fn deliver<Q: ReadyScheduler, T: TimerService, E: ElectionEntropy>(
        &mut self,
        owner: &mut EffectOwner<Q, T, E>,
        applications: &mut ApplicationRouter<R>,
        output: ApplicationResults<R>,
    ) -> Result<usize, ClientDeliveryRejected<R>> {
        let preflight = (|| {
            if owner.identity() != self.binding.owner
                || output.effect().visit.owner != self.binding.owner
                || applications.binding().owner != self.binding.owner
            {
                return Err(ClientError::WrongBinding);
            }
            if owner.is_failed() {
                return Err(ClientError::Fenced);
            }
            let group = output.effect().visit.group;
            let state = owner.core(group).ok_or(ClientError::UnknownGroup)?.state();
            let mut matches = BTreeMap::new();
            for (i, receipt) in output.receipts().iter().enumerate() {
                let position = output.positions()[i];
                let key = (group, position.index, position.term);
                let Some(sequence) = self.positions.get(&key).copied() else {
                    continue;
                };
                let p = &self.pending[&sequence];
                // Opaque verified applied positions survive prefix compaction.
                // While the entry is retained, cross-check its exact content too.
                let same = if let Some(entry) = state.entry_at(position.index) {
                    entry.term == position.term
                        && matches!(&entry.payload, EntryPayload::Command { operation, bytes }
                        if *operation == p.ticket.operation && bytes == &p.bytes)
                } else {
                    position.index <= state.base_index()
                };
                if !same
                    || position.index != receipt.index()
                    || receipt.operation() != p.ticket.operation
                    || receipt
                        .nested_bytes(p.receipt_bound - size_of::<R>())
                        .ok()
                        .and_then(|n| n.checked_add(size_of::<R>()))
                        .is_none_or(|n| n > p.receipt_bound)
                {
                    return Err(ClientError::ProviderViolation);
                }
                matches.insert(i, sequence);
            }
            Ok(matches)
        })();
        let matches = match preflight {
            Ok(v) => v,
            Err(reason) => {
                if reason == ClientError::ProviderViolation {
                    let _ = owner.fail::<()>(EffectOwnerError::ProviderContract);
                    self.closed = true;
                }
                return Err(ClientDeliveryRejected {
                    reason,
                    results: Box::new(output),
                });
            }
        };
        let receipts = match applications.complete(output) {
            Ok(v) => v,
            Err(rejected) => {
                return Err(ClientDeliveryRejected {
                    reason: ClientError::StaleTicket,
                    results: rejected.results,
                })
            }
        };
        let mut delivered = 0;
        for (i, receipt) in receipts.into_iter().enumerate() {
            if let Some(sequence) = matches.get(&i).copied() {
                let p = self.pending.get_mut(&sequence).unwrap();
                p.terminal = true;
                let pos = p.position.unwrap();
                self.positions
                    .remove(&(p.ticket.group, pos.index, pos.term));
                if !p.has_output {
                    delivered += 1;
                    self.publish(
                        sequence,
                        ClientOutcome::Applied {
                            position: pos,
                            receipt,
                        },
                    );
                }
                self.cleanup(sequence);
            }
        }
        Ok(delivered)
    }
    /// Cancel only the wait. Reservation survives until exact step/log/application
    /// evidence transfers capacity or a failed-owner abort makes it obsolete.
    pub fn cancel_wait(&mut self, ticket: ClientTicket) -> Result<(), ClientError> {
        let p = self
            .pending
            .get(&ticket.sequence)
            .filter(|p| p.ticket == ticket)
            .ok_or(ClientError::StaleTicket)?;
        if p.has_output {
            return Err(ClientError::StaleTicket);
        }
        self.publish(
            ticket.sequence,
            ClientOutcome::Unknown(ClientUnknown::CancelledWait),
        );
        Ok(())
    }
    /// Bounded fair observation. Durable log takeover or a newer term with no
    /// pending dependency permits release of abandoned command reservations.
    pub fn reconcile<Q: ReadyScheduler, T: TimerService, E: ElectionEntropy>(
        &mut self,
        owner: &EffectOwner<Q, T, E>,
        visits: usize,
    ) -> Result<usize, ClientError> {
        if owner.identity() != self.binding.owner {
            return Err(ClientError::WrongBinding);
        }
        if visits > 65536 {
            return Err(ClientError::InvalidLimits);
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
            let core = owner.core(p.ticket.group).unwrap();
            let unknown = if owner.is_failed() {
                Some(ClientUnknown::OwnerFailed)
            } else if core.role() != Role::Leader || core.state().hard_state.term != p.term {
                Some(ClientUnknown::LeadershipChanged)
            } else {
                None
            };
            let takeover = p.stepped
                && p.position.is_some_and(|pos| {
                    !core.has_pending_dependency()
                        && (core
                            .state()
                            .entry_at(pos.index)
                            .is_some_and(|e| e.term == pos.term)
                            || core.state().hard_state.term > pos.term)
                });
            if owner.is_failed() || (p.has_output && takeover) {
                self.pending.get_mut(&sequence).unwrap().terminal = true;
            }
            if let Some(reason) = unknown {
                self.publish(sequence, ClientOutcome::Unknown(reason));
            }
            self.cleanup(sequence);
        }
        Ok(visited)
    }
    pub fn poll(&mut self) -> Option<ClientCompletion<R>> {
        let sequence = self.ready.pop_front()?;
        let p = self.pending.get_mut(&sequence).unwrap();
        p.exported = true;
        Some(ClientCompletion {
            ticket: p.ticket,
            outcome: p.outcome.take().unwrap(),
        })
    }
    pub fn complete(
        &mut self,
        completion: ClientCompletion<R>,
    ) -> Result<ClientOutcome<R>, ClientCompletionRejected<R>> {
        let sequence = completion.ticket.sequence;
        let valid = self
            .pending
            .get(&sequence)
            .is_some_and(|p| p.ticket == completion.ticket && p.exported && !p.released);
        if !valid {
            return Err(ClientCompletionRejected {
                reason: ClientError::StaleTicket,
                completion: Box::new(completion),
            });
        }
        self.pending.get_mut(&sequence).unwrap().released = true;
        self.cleanup(sequence);
        Ok(completion.outcome)
    }
    /// Abandon all waits by fencing the owner, not by rolling back accepted WAL.
    /// Its external workers must still drain and recovery uses fresh identities.
    pub fn abort<Q: ReadyScheduler, T: TimerService, E: ElectionEntropy>(
        &mut self,
        owner: &mut EffectOwner<Q, T, E>,
    ) -> Result<(), ClientError> {
        if owner.identity() != self.binding.owner {
            return Err(ClientError::WrongBinding);
        }
        self.closed = true;
        let _ = owner.fail::<()>(EffectOwnerError::Runtime(RuntimeError::Fenced));
        let sequences = self.pending.keys().copied().collect::<Vec<_>>();
        for sequence in sequences {
            self.pending.get_mut(&sequence).unwrap().terminal = true;
            self.publish(sequence, ClientOutcome::Unknown(ClientUnknown::Aborted));
            self.cleanup(sequence);
        }
        Ok(())
    }
}
