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
//! Bounded caller-driven local replica assembly over selected public providers.
use super::*;
use crate::{application::*, outbound::*, snapshot_worker::*, worker::*};

pub struct ReplicaSnapshots<'a> {
    pub router: &'a mut SnapshotRouter,
    pub worker: &'a mut dyn SnapshotWorker,
}
/// Borrowed components remain owned by the embedding host. Application keys and
/// selected provider identities stay fixed for the driver lifetime.
pub struct ReplicaParts<
    'a,
    S: ReadyScheduler,
    T: TimerService,
    E: ElectionEntropy,
    A: StateMachine + ReadableStateMachine,
    W: PersistenceWorker,
    O: OutboundQueue,
> where
    A::Receipt: ApplicationReceipt,
{
    pub owner: &'a mut EffectOwner<S, T, E>,
    pub persistence: &'a mut W,
    pub applications: &'a mut BTreeMap<GroupIdentity, A>,
    pub results: &'a mut ApplicationRouter<A::Receipt>,
    pub clients: &'a mut ClientRouter<A::Receipt>,
    pub reads: &'a mut ReadRequests<A::Query, A::ReadResult>,
    pub outbound: &'a mut O,
    pub snapshots: Option<ReplicaSnapshots<'a>>,
}
#[derive(Clone, Copy, Debug)]
pub struct ReplicaDriverLimits {
    pub leases: usize,
    pub metadata_bytes: usize,
    pub persistence_batch_units: usize,
}
impl Default for ReplicaDriverLimits {
    fn default() -> Self {
        Self {
            leases: 256,
            metadata_bytes: 1024 * 1024,
            persistence_batch_units: 128,
        }
    }
}
#[derive(Clone, Copy, Debug)]
pub struct ReplicaPollBudget {
    pub worker_events: usize,
    pub snapshot_events: usize,
    pub steps: usize,
    pub effects: usize,
    pub retries: usize,
    pub reconcile: usize,
}
impl Default for ReplicaPollBudget {
    fn default() -> Self {
        Self {
            worker_events: 32,
            snapshot_events: 16,
            steps: 128,
            effects: 256,
            retries: 256,
            reconcile: 128,
        }
    }
}
impl ReplicaPollBudget {
    pub(super) fn valid(self) -> bool {
        [
            self.worker_events,
            self.snapshot_events,
            self.steps,
            self.effects,
            self.retries,
        ]
        .iter()
        .all(|n| *n > 0 && *n <= 4096)
            && self.reconcile > 0
            && self.reconcile <= 65536
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReplicaError {
    InvalidLimits,
    WrongBinding,
    NotQuiescent,
    MissingApplication,
    MissingSnapshots,
    ClockRegressed,
    Fenced,
    ProviderContract,
    Owner(EffectOwnerError),
    Worker(WorkerError),
    Outbound(OutboundError),
    Application(ApplicationRouteError),
    Client(ClientError),
    Read(ReadInvocationError),
    Snapshot(SnapshotRouteError),
    Maintenance(crate::contracts::StorageError),
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ReplicaDriverUsage {
    pub control_persists: usize,
    pub data_persists: usize,
    pub retries: usize,
    pub failed_results: usize,
    pub reclaims: usize,
}
impl ReplicaDriverUsage {
    pub fn leases(self) -> usize {
        self.control_persists + self.data_persists + self.retries
    }
}
#[derive(Debug, Default)]
pub struct ReplicaProgress {
    pub steps: Vec<OwnerStep>,
    pub worker_events: usize,
    pub snapshot_events: usize,
    pub snapshot_installs: usize,
    pub snapshot_supplies: usize,
    pub effects: usize,
    pub retries: usize,
    pub persistence_batches: usize,
    pub sends: usize,
    pub applications: usize,
    pub reads: usize,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReplicaBindings {
    pub owner: RuntimeOwner,
    pub persistence: WorkerBinding,
    pub outbound: OutboundBinding,
    pub application: ApplicationRouterBinding,
    pub clients: ClientRouterBinding,
    pub reads: ReadInvocationBinding,
    pub snapshots: Option<SnapshotWorkerBinding>,
}
/// Fixed assembly guard. Queued effect payloads retain the EffectOwner's byte
/// reservation; this driver separately bounds retry metadata and transient work.
/// Peer connection/ingress polling remains an explicitly composed outer driver.
pub struct ReplicaDriver<R> {
    binding: ReplicaBindings,
    limits: ReplicaDriverLimits,
    control: VecDeque<EffectLease>,
    data: VecDeque<EffectLease>,
    retries: VecDeque<EffectLease>,
    failed_result: Option<ApplicationResults<R>>,
    now: MonoTime,
    failed: Option<ReplicaError>,
    reclaim: Option<(ReclaimTicket, usize)>,
    reclaim_result: Option<ReclaimEvent>,
    failed_reclaim: Option<ReclaimEvent>,
    reclaim_sequence: u64,
}
impl<R: ApplicationReceipt> ReplicaDriver<R> {
    fn bindings<
        S: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
        A: StateMachine<Receipt = R> + ReadableStateMachine,
        W: PersistenceWorker,
        O: OutboundQueue,
    >(
        p: &ReplicaParts<'_, S, T, E, A, W, O>,
    ) -> Result<ReplicaBindings, ReplicaError> {
        let owner = p.owner.identity();
        if p.persistence.binding() != p.owner.worker_binding()
            || p.outbound.binding().store != owner.store
            || p.results.binding().owner != owner
            || p.clients.binding().owner != owner
            || p.reads.binding().owner != owner
        {
            return Err(ReplicaError::WrongBinding);
        }
        let snapshots = if let Some(s) = &p.snapshots {
            if s.router.owner() != owner
                || s.router.worker_binding() != s.worker.binding()
                || s.worker.binding().store != owner.store
            {
                return Err(ReplicaError::WrongBinding);
            }
            Some(s.worker.binding())
        } else {
            None
        };
        Ok(ReplicaBindings {
            owner,
            persistence: p.persistence.binding(),
            outbound: p.outbound.binding(),
            application: p.results.binding(),
            clients: p.clients.binding(),
            reads: p.reads.binding(),
            snapshots,
        })
    }
    pub fn new<
        S: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
        A: StateMachine<Receipt = R> + ReadableStateMachine,
        W: PersistenceWorker,
        O: OutboundQueue,
    >(
        parts: &ReplicaParts<'_, S, T, E, A, W, O>,
        limits: ReplicaDriverLimits,
        now: MonoTime,
    ) -> Result<Self, ReplicaError> {
        let binding = Self::bindings(parts)?;
        let metadata = limits
            .leases
            .checked_mul(3)
            .and_then(|n| n.checked_add(limits.persistence_batch_units))
            .and_then(|n| n.checked_mul(size_of::<EffectLease>()))
            .and_then(|n| n.checked_add(size_of::<ApplicationResults<R>>()))
            .and_then(|n| {
                n.checked_add(2 * size_of::<ReclaimEvent>() + size_of::<(ReclaimTicket, usize)>())
            });
        if limits.leases == 0
            || limits.leases > 65536
            || limits.leases < parts.owner.limits().active_visits
            || limits.persistence_batch_units == 0
            || limits.persistence_batch_units > 4096
            || limits.persistence_batch_units > limits.leases
            || limits.metadata_bytes > 64 * 1024 * 1024
            || metadata.is_none_or(|n| n > limits.metadata_bytes)
        {
            return Err(ReplicaError::InvalidLimits);
        }
        if !parts.owner.is_drained()
            || !parts.persistence.is_drained()
            || !parts.outbound.is_drained()
            || !parts.results.is_drained()
            || !parts.clients.is_drained()
            || !parts.reads.is_drained()
            || parts
                .snapshots
                .as_ref()
                .is_some_and(|s| !s.router.is_drained() || !s.worker.is_drained())
        {
            return Err(ReplicaError::NotQuiescent);
        }
        let mut groups = 0;
        for group in parts.owner.groups() {
            groups += 1;
            let core = parts.owner.core(group).unwrap();
            if core.local_node() != binding.outbound.node {
                return Err(ReplicaError::WrongBinding);
            }
            let app = parts
                .applications
                .get(&group)
                .ok_or(ReplicaError::MissingApplication)?;
            app.validate_group(group)
                .map_err(|_| ReplicaError::WrongBinding)?;
            if app.applied_index() != core.state().commit_index {
                return Err(ReplicaError::MissingApplication);
            }
        }
        if groups != parts.applications.len() {
            return Err(ReplicaError::MissingApplication);
        }
        parts
            .persistence
            .limits()
            .validate()
            .map_err(ReplicaError::Worker)?;
        parts
            .outbound
            .limits()
            .validate()
            .map_err(ReplicaError::Outbound)?;
        Ok(Self {
            binding,
            limits,
            control: VecDeque::with_capacity(limits.leases),
            data: VecDeque::with_capacity(limits.leases),
            retries: VecDeque::with_capacity(limits.leases),
            failed_result: None,
            now,
            failed: None,
            reclaim: None,
            reclaim_result: None,
            failed_reclaim: None,
            reclaim_sequence: 0,
        })
    }
    pub fn binding(&self) -> ReplicaBindings {
        self.binding
    }
    pub fn limits(&self) -> ReplicaDriverLimits {
        self.limits
    }
    pub fn usage(&self) -> ReplicaDriverUsage {
        ReplicaDriverUsage {
            control_persists: self.control.len(),
            data_persists: self.data.len(),
            retries: self.retries.len(),
            failed_results: usize::from(self.failed_result.is_some()),
            reclaims: usize::from(self.reclaim.is_some())
                + usize::from(self.reclaim_result.is_some())
                + usize::from(self.failed_reclaim.is_some()),
        }
    }
    /// Only local retained effects, not quorum/client/network quiescence.
    pub fn is_drained(&self) -> bool {
        self.usage().leases() == 0 && self.failed_result.is_none() && self.usage().reclaims == 0
    }
    pub fn is_failed(&self) -> bool {
        self.failed.is_some()
    }
    pub fn poll_reclaim(&mut self) -> Option<ReclaimEvent> {
        self.reclaim_result.take()
    }
    pub fn failed_reclaim(&self) -> Option<&ReclaimEvent> {
        self.failed_reclaim.as_ref()
    }
    pub fn request_reclaim<
        S: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
        A: StateMachine<Receipt = R> + ReadableStateMachine,
        W: PersistenceWorker,
        O: OutboundQueue,
    >(
        &mut self,
        p: &mut ReplicaParts<'_, S, T, E, A, W, O>,
        max_bytes: usize,
    ) -> Result<ReclaimTicket, ReplicaError> {
        if Self::bindings(p)? != self.binding {
            return Err(ReplicaError::WrongBinding);
        }
        if self.failed.is_some() {
            return Err(ReplicaError::Fenced);
        }
        if self.usage().reclaims != 0 {
            return Err(ReplicaError::Worker(WorkerError::Overloaded));
        }
        if max_bytes == 0 || max_bytes > u32::MAX as usize {
            return Err(ReplicaError::InvalidLimits);
        }
        let ticket = match p.persistence.submit_reclaim(max_bytes) {
            Ok(ticket) => ticket,
            Err(reason) => {
                let error = ReplicaError::Worker(reason.clone());
                if matches!(reason, WorkerError::Fenced | WorkerError::Closed) {
                    let _ = p.owner.fail::<()>(EffectOwnerError::ProviderContract);
                    self.failed = Some(error.clone());
                }
                return Err(error);
            }
        };
        self.reclaim = Some((ticket, max_bytes));
        if ticket.binding != self.binding.persistence || ticket.sequence <= self.reclaim_sequence {
            let _ = p.owner.fail::<()>(EffectOwnerError::ProviderContract);
            self.failed = Some(ReplicaError::ProviderContract);
            return Err(ReplicaError::ProviderContract);
        }
        self.reclaim_sequence = ticket.sequence;
        Ok(ticket)
    }

    pub fn poll<
        S: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
        A: ProposalAdmission<Receipt = R> + BoundedReadableStateMachine + CheckpointStateMachine,
        W: PersistenceWorker,
        O: OutboundQueue,
    >(
        &mut self,
        parts: &mut ReplicaParts<'_, S, T, E, A, W, O>,
        now: MonoTime,
        budget: ReplicaPollBudget,
    ) -> Result<ReplicaProgress, ReplicaError> {
        self.poll_authorized(parts, now, budget, |_, event| {
            configuration_auth_required(event)
        })
    }
    pub(super) fn poll_authorized<
        S: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
        A: ProposalAdmission<Receipt = R> + BoundedReadableStateMachine + CheckpointStateMachine,
        W: PersistenceWorker,
        O: OutboundQueue,
    >(
        &mut self,
        parts: &mut ReplicaParts<'_, S, T, E, A, W, O>,
        now: MonoTime,
        budget: ReplicaPollBudget,
        configuration: impl FnMut(&Raft, &Event) -> Result<(), RaftError>,
    ) -> Result<ReplicaProgress, ReplicaError> {
        if Self::bindings(parts)? != self.binding {
            return Err(ReplicaError::WrongBinding);
        }
        if !budget.valid() {
            return Err(ReplicaError::InvalidLimits);
        }
        if now < self.now {
            return Err(ReplicaError::ClockRegressed);
        }
        if self.failed.is_some() {
            return Err(ReplicaError::Fenced);
        }
        self.now = now;
        let result = self.poll_inner(parts, now, budget, configuration);
        if let Err(reason) = &result {
            let _ = parts.owner.fail::<()>(EffectOwnerError::ProviderContract);
            self.failed = Some(reason.clone());
        }
        result
    }
    fn poll_inner<
        S: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
        A: ProposalAdmission<Receipt = R> + BoundedReadableStateMachine + CheckpointStateMachine,
        W: PersistenceWorker,
        O: OutboundQueue,
    >(
        &mut self,
        p: &mut ReplicaParts<'_, S, T, E, A, W, O>,
        now: MonoTime,
        b: ReplicaPollBudget,
        configuration: impl FnMut(&Raft, &Event) -> Result<(), RaftError>,
    ) -> Result<ReplicaProgress, ReplicaError> {
        let mut out = ReplicaProgress::default();
        self.poll_reclaims(p.persistence)?;
        self.poll_snapshots(p, now, b.snapshot_events, &mut out)?;
        let events = p.persistence.poll(b.worker_events);
        if events.len() > b.worker_events {
            return Err(ReplicaError::ProviderContract);
        }
        for event in events {
            out.worker_events += 1;
            p.owner
                .deliver_worker(event, now)
                .map_err(ReplicaError::Owner)?;
        }
        out.steps = p
            .clients
            .advance_authorized(
                p.owner,
                now,
                b.steps,
                |g| p.applications.get(&g),
                configuration,
            )
            .map_err(ReplicaError::Client)?;
        p.reads
            .observe_steps(p.owner, &out.steps)
            .map_err(ReplicaError::Read)?;
        self.collect_effects(p.owner, b.effects, &mut out)?;
        self.retry_effects(p, now, b.retries, &mut out)?;
        self.submit_batch(p.owner, p.persistence, now, true, &mut out)?;
        self.submit_batch(p.owner, p.persistence, now, false, &mut out)?;
        p.clients
            .reconcile(p.owner, b.reconcile)
            .map_err(ReplicaError::Client)?;
        p.reads
            .reconcile(p.owner, b.reconcile)
            .map_err(ReplicaError::Read)?;
        Ok(out)
    }
    fn poll_reclaims<W: PersistenceWorker>(&mut self, worker: &mut W) -> Result<(), ReplicaError> {
        let reclaims = worker.poll_reclaims(1);
        if reclaims.len() > 1 {
            return Err(ReplicaError::ProviderContract);
        }
        for event in reclaims {
            let valid = self.reclaim.is_some_and(|(ticket, max_bytes)| {
                ticket == event.request
                    && event.result.as_ref().map_or(true, |r| {
                        r.after_bytes <= r.before_bytes && r.after_bytes <= max_bytes
                    })
            });
            if !valid || self.reclaim_result.is_some() {
                self.failed_reclaim = Some(event);
                return Err(ReplicaError::ProviderContract);
            }
            self.reclaim = None;
            let fatal = match &event.result {
                Err(
                    e @ (crate::contracts::StorageError::Uncertain(_)
                    | crate::contracts::StorageError::Corrupt(_)
                    | crate::contracts::StorageError::Fenced),
                ) => Some(e.clone()),
                _ => None,
            };
            self.reclaim_result = Some(event);
            if let Some(e) = fatal {
                return Err(ReplicaError::Maintenance(e));
            }
        }
        Ok(())
    }
    fn poll_snapshots<
        S: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
        A: ProposalAdmission<Receipt = R> + BoundedReadableStateMachine + CheckpointStateMachine,
        W: PersistenceWorker,
        O: OutboundQueue,
    >(
        &mut self,
        p: &mut ReplicaParts<'_, S, T, E, A, W, O>,
        now: MonoTime,
        limit: usize,
        out: &mut ReplicaProgress,
    ) -> Result<(), ReplicaError> {
        if let Some(s) = &mut p.snapshots {
            let events = s.worker.poll(limit);
            if events.len() > limit {
                return Err(ReplicaError::ProviderContract);
            }
            for event in events {
                out.snapshot_events += 1;
                out.snapshot_installs += usize::from(matches!(
                    &event.result,
                    Ok(SnapshotOutput::Loaded {
                        reconciled: true,
                        ..
                    })
                ));
                out.snapshot_supplies += usize::from(matches!(
                    &event.result,
                    Ok(SnapshotOutput::Loaded {
                        reconciled: false,
                        ..
                    })
                ));
                let app = p
                    .applications
                    .get_mut(&event.visit.group)
                    .ok_or(ReplicaError::MissingApplication)?;
                s.router
                    .deliver(p.owner, app, event, now)
                    .map_err(ReplicaError::Snapshot)?;
            }
        }
        Ok(())
    }
    fn collect_effects<S: ReadyScheduler, T: TimerService, E: ElectionEntropy>(
        &mut self,
        owner: &mut EffectOwner<S, T, E>,
        limit: usize,
        out: &mut ReplicaProgress,
    ) -> Result<(), ReplicaError> {
        for _ in 0..limit {
            if self.usage().leases() == self.limits.leases {
                break;
            }
            let Some(lease) = owner.take_effect().map_err(ReplicaError::Owner)? else {
                break;
            };
            out.effects += 1;
            if let Effect::Persist(update) = &lease.effect {
                let (_, control) = match persist_payload_cost(update) {
                    Ok(cost) => cost,
                    Err(reason) => {
                        self.retries.push_back(lease);
                        return Err(ReplicaError::Worker(reason));
                    }
                };
                if control {
                    self.control.push_back(lease);
                } else {
                    self.data.push_back(lease);
                }
            } else {
                self.retries.push_back(lease);
            }
        }
        Ok(())
    }
    fn retry_send<
        S: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
        A: ProposalAdmission<Receipt = R> + BoundedReadableStateMachine + CheckpointStateMachine,
        W: PersistenceWorker,
        O: OutboundQueue,
    >(
        &mut self,
        p: &mut ReplicaParts<'_, S, T, E, A, W, O>,
        lease: EffectLease,
        out: &mut ReplicaProgress,
    ) -> Result<(), ReplicaError> {
        let EffectLease {
            ticket,
            effect: Effect::Send(message),
        } = lease
        else {
            unreachable!()
        };
        let peer = message.to;
        match p.outbound.submit(vec![message]) {
            Ok(accepted) => {
                if accepted.binding != self.binding.outbound
                    || accepted.peer != peer
                    || accepted.sequence == 0
                {
                    let _ = p.owner.fail::<()>(EffectOwnerError::ProviderContract);
                    p.owner
                        .discard_failed_transfer(ticket)
                        .map_err(ReplicaError::Owner)?;
                    return Err(ReplicaError::ProviderContract);
                }
                if let Err(reason) = p.owner.release_transferred_send(ticket) {
                    let _ = p.owner.fail::<()>(EffectOwnerError::ProviderContract);
                    let _ = p.owner.discard_failed_transfer(ticket);
                    return Err(ReplicaError::Owner(reason));
                }
                out.sends += 1;
            }
            Err(mut rejected) => {
                if rejected.messages.len() != 1 {
                    let _ = p.owner.fail::<()>(EffectOwnerError::ProviderContract);
                    p.owner
                        .discard_failed_transfer(ticket)
                        .map_err(ReplicaError::Owner)?;
                    return Err(ReplicaError::ProviderContract);
                }
                self.retries.push_back(EffectLease {
                    ticket,
                    effect: Effect::Send(rejected.messages.remove(0)),
                });
                if rejected.reason != OutboundError::Overloaded {
                    return Err(ReplicaError::Outbound(rejected.reason));
                }
            }
        }
        Ok(())
    }
    fn retry_committed<
        S: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
        A: ProposalAdmission<Receipt = R> + BoundedReadableStateMachine + CheckpointStateMachine,
        W: PersistenceWorker,
        O: OutboundQueue,
    >(
        &mut self,
        p: &mut ReplicaParts<'_, S, T, E, A, W, O>,
        lease: EffectLease,
        now: MonoTime,
        out: &mut ReplicaProgress,
    ) -> Result<(), ReplicaError> {
        let group = lease.ticket.visit.group;

        let Some(app) = p.applications.get_mut(&group) else {
            self.retries.push_back(lease);
            return Err(ReplicaError::MissingApplication);
        };
        match p.results.submit(p.owner, lease, app, now) {
            Ok(_) => {
                let output = p.results.poll().ok_or(ReplicaError::ProviderContract)?;
                if let Err(rejected) = p.clients.deliver(p.owner, p.results, output) {
                    self.failed_result = Some(*rejected.results);
                    return Err(ReplicaError::Client(rejected.reason));
                }
                out.applications += 1;
            }
            Err(rejected) => {
                self.retries.push_back(*rejected.lease);
                if rejected.reason != ApplicationRouteError::Overloaded {
                    return Err(ReplicaError::Application(rejected.reason));
                }
            }
        }
        Ok(())
    }
    fn retry_read<
        S: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
        A: ProposalAdmission<Receipt = R> + BoundedReadableStateMachine + CheckpointStateMachine,
        W: PersistenceWorker,
        O: OutboundQueue,
    >(
        &mut self,
        p: &mut ReplicaParts<'_, S, T, E, A, W, O>,
        lease: EffectLease,
        now: MonoTime,
        out: &mut ReplicaProgress,
    ) -> Result<(), ReplicaError> {
        let group = lease.ticket.visit.group;

        let Some(app) = p.applications.get(&group) else {
            self.retries.push_back(lease);
            return Err(ReplicaError::MissingApplication);
        };
        match p.reads.execute(p.owner, lease, app, now) {
            Ok(()) => out.reads += 1,
            Err(ReadExecutionError::Rejected { reason, lease }) => {
                self.retries.push_back(*lease);
                if !matches!(
                    reason,
                    ReadInvocationError::Execution(
                        ReadRouteError::Overloaded
                            | ReadRouteError::Owner(EffectOwnerError::Consensus(
                                RaftError::NotApplied
                            ))
                    )
                ) {
                    return Err(ReplicaError::Read(reason));
                }
            }
            Err(ReadExecutionError::Failed { reason }) => return Err(ReplicaError::Read(reason)),
        }
        Ok(())
    }
    fn retry_snapshot<
        S: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
        A: ProposalAdmission<Receipt = R> + BoundedReadableStateMachine + CheckpointStateMachine,
        W: PersistenceWorker,
        O: OutboundQueue,
    >(
        &mut self,
        p: &mut ReplicaParts<'_, S, T, E, A, W, O>,
        lease: EffectLease,
    ) -> Result<(), ReplicaError> {
        let group = lease.ticket.visit.group;

        let Some(s) = &mut p.snapshots else {
            self.retries.push_back(lease);
            return Err(ReplicaError::MissingSnapshots);
        };
        let Some(app) = p.applications.get(&group) else {
            self.retries.push_back(lease);
            return Err(ReplicaError::MissingApplication);
        };
        match s.router.submit(p.owner, s.worker, lease, app) {
            Ok(_) => (),
            Err(rejected) => {
                self.retries.push_back(*rejected.lease);
                if !matches!(
                    rejected.reason,
                    SnapshotRouteError::Overloaded
                        | SnapshotRouteError::Worker(SnapshotWorkError::Overloaded)
                        | SnapshotRouteError::Owner(EffectOwnerError::Overloaded)
                ) {
                    return Err(ReplicaError::Snapshot(rejected.reason));
                }
            }
        }
        Ok(())
    }
    fn retry_effects<
        S: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
        A: ProposalAdmission<Receipt = R> + BoundedReadableStateMachine + CheckpointStateMachine,
        W: PersistenceWorker,
        O: OutboundQueue,
    >(
        &mut self,
        p: &mut ReplicaParts<'_, S, T, E, A, W, O>,
        now: MonoTime,
        limit: usize,
        out: &mut ReplicaProgress,
    ) -> Result<(), ReplicaError> {
        for _ in 0..self.retries.len().min(limit) {
            let lease = self.retries.pop_front().unwrap();
            out.retries += 1;
            match &lease.effect {
                Effect::Send(_) => self.retry_send(p, lease, out)?,
                Effect::Committed(_) => self.retry_committed(p, lease, now, out)?,
                Effect::ReadReady(_) => self.retry_read(p, lease, now, out)?,
                _ => self.retry_snapshot(p, lease)?,
            }
        }
        Ok(())
    }
    fn submit_batch<
        S: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
        W: PersistenceWorker,
    >(
        &mut self,
        owner: &mut EffectOwner<S, T, E>,
        worker: &mut W,
        now: MonoTime,
        control: bool,
        out: &mut ReplicaProgress,
    ) -> Result<(), ReplicaError> {
        let queue = if control {
            &mut self.control
        } else {
            &mut self.data
        };
        let limits = worker.limits();
        let mut count = 0;
        let mut bytes = 0usize;
        for lease in queue
            .iter()
            .take(self.limits.persistence_batch_units.min(limits.batch_units))
        {
            let Effect::Persist(update) = &lease.effect else {
                unreachable!()
            };
            let (payload, _) = persist_payload_cost(update).map_err(ReplicaError::Worker)?;
            let next = bytes
                .checked_add(PERSIST_UNIT_METADATA)
                .and_then(|n| n.checked_add(payload));
            if next.is_none_or(|n| n > limits.batch_bytes) {
                break;
            }
            bytes = next.unwrap();
            count += 1;
        }
        if count == 0 {
            return if queue.is_empty() {
                Ok(())
            } else {
                Err(ReplicaError::Worker(WorkerError::BatchTooLarge))
            };
        }
        let mut leases = Vec::with_capacity(count);
        leases.extend(queue.drain(..count));
        match owner.submit_persists(worker, leases, now) {
            Ok(_) => {
                out.persistence_batches += 1;
                Ok(())
            }
            Err(rejected) => {
                queue.extend(rejected.leases);
                if rejected.reason == EffectOwnerError::Worker(WorkerError::Overloaded) {
                    Ok(())
                } else {
                    Err(ReplicaError::Owner(rejected.reason))
                }
            }
        }
    }
    /// Stops service intake; peer/control input and accepted work still drive.
    pub fn close_intake<
        S: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
        A: StateMachine<Receipt = R> + ReadableStateMachine,
        W: PersistenceWorker,
        O: OutboundQueue,
    >(
        &mut self,
        p: &mut ReplicaParts<'_, S, T, E, A, W, O>,
    ) -> Result<(), ReplicaError> {
        if Self::bindings(p)? != self.binding {
            return Err(ReplicaError::WrongBinding);
        }
        p.clients.close();
        p.reads.close();
        Ok(())
    }
    /// Explicit failed-domain cleanup of this driver's and snapshot router's
    /// leases only. Accepted external worker/transport work still drains or recovers
    /// under the host's provider lifetime and fresh authoritative store session.
    pub fn discard_failed<
        S: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
        A: StateMachine<Receipt = R> + ReadableStateMachine,
        W: PersistenceWorker,
        O: OutboundQueue,
    >(
        &mut self,
        p: &mut ReplicaParts<'_, S, T, E, A, W, O>,
    ) -> Result<(), ReplicaError> {
        if Self::bindings(p)? != self.binding {
            return Err(ReplicaError::WrongBinding);
        }
        if !p.owner.is_failed() {
            return Err(ReplicaError::NotQuiescent);
        }
        for queue in [&mut self.control, &mut self.data, &mut self.retries] {
            while let Some(lease) = queue.pop_front() {
                p.owner
                    .discard_failed(lease)
                    .map_err(|e| ReplicaError::Owner(e.reason))?;
            }
        }
        if let Some(output) = self.failed_result.take() {
            p.results
                .complete(output)
                .map_err(|e| ReplicaError::Application(e.reason))?;
        }
        if let Some(s) = &mut p.snapshots {
            s.router
                .discard_failed(p.owner)
                .map_err(ReplicaError::Snapshot)?;
        }
        p.clients
            .reconcile(p.owner, 65536)
            .map_err(ReplicaError::Client)?;
        p.reads
            .reconcile(p.owner, 65536)
            .map_err(ReplicaError::Read)?;
        Ok(())
    }
}
