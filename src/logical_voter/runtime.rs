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
//! Explicit logical-mode driver over the existing Raft/runtime providers.
use super::*;
use crate::{
    outbound::OutboundQueue,
    raft::{Event, Raft, RaftError},
    runtime::*,
    worker::PersistenceWorker,
};
pub struct LogicalRouterParts<R: ApplicationReceipt, Q, V> {
    pub results: ApplicationRouter<R>,
    pub clients: ClientRouter<R>,
    pub reads: ReadRequests<Q, V>,
}
#[derive(Clone, Copy, Debug, Default)]
pub struct LogicalRouterLimits {
    pub applications: ApplicationRouterLimits,
    pub clients: ClientRouterLimits,
    pub execution: ReadRouterLimits,
    pub invocations: ReadInvocationLimits,
}
impl VerifiedLogicalImage {
    pub fn routers<
        R: ApplicationReceipt,
        Q,
        V,
        S: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
    >(
        &self,
        owner: &EffectOwner<S, T, E>,
        limits: LogicalRouterLimits,
    ) -> Result<LogicalRouterParts<R, Q, V>, LogicalReleaseError> {
        let authorization = RuntimeAuthorization::from_verified_image(owner.identity(), self)
            .map_err(LogicalReleaseError::Owner)?;
        if authorization != owner.authorization() {
            return Err(LogicalReleaseError::WrongBinding);
        }
        let results = ApplicationRouter::new_logical(
            ApplicationRouterBinding::new(
                authorization,
                ApplicationRouterGeneration::new(1).unwrap(),
            ),
            limits.applications,
            self.control_profile.as_ref(),
            self.limits.parent_log.max_entries_per_group,
        )
        .map_err(LogicalReleaseError::ApplicationRoute)?;
        let clients = ClientRouter::new_logical(
            ClientRouterBinding::new(authorization, ClientRouterGeneration::new(1).unwrap()),
            limits.clients,
            self.control_profile.as_ref(),
        )
        .map_err(LogicalReleaseError::Client)?;
        let execution = ReadRouter::new_logical::<Q>(
            ReadRouterBinding::new(authorization, ReadRouterGeneration::new(1).unwrap()),
            limits.execution,
            self.control_profile.as_ref(),
        )
        .map_err(|e| LogicalReleaseError::Read(ReadInvocationError::Execution(e)))?;
        let reads = ReadRequests::new_logical(
            ReadInvocationBinding::new(authorization, ReadInvocationGeneration::new(1).unwrap()),
            limits.invocations,
            execution,
            self.control_profile.as_ref(),
        )
        .map_err(|e| LogicalReleaseError::Read(e.reason))?;
        Ok(LogicalRouterParts {
            results,
            clients,
            reads,
        })
    }
}
/// The same ReplicaDriver retains leases, persistence and snapshot work. This
/// facade adds the mandatory external child authorization coordinator; it does
/// not create consensus, transport, workers or an executor.
pub struct LogicalReplicaDriver<R, V> {
    driver: ReplicaDriver<R>,
    release: LogicalReleaseCoordinator<R, V>,
}
impl<R: ApplicationReceipt, V> LogicalReplicaDriver<R, V> {
    pub fn new<
        S: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
        A: ProposalAdmission<Receipt = R> + ReadableStateMachine<ReadResult = V>,
        W: PersistenceWorker,
        O: OutboundQueue,
    >(
        parts: &ReplicaParts<'_, S, T, E, A, W, O>,
        limits: ReplicaDriverLimits,
        release_limits: LogicalReleaseLimits,
        now: MonoTime,
    ) -> Result<Self, ReplicaError> {
        let release = LogicalReleaseCoordinator::new_for_results(
            parts.owner.authorization(),
            release_limits,
            parts.results,
        )
        .map_err(ReplicaError::Logical)?;
        let driver = ReplicaDriver::new_logical(parts, limits, now)?;
        Ok(Self { driver, release })
    }
    pub fn poll<
        S: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
        A: ProposalAdmission<Receipt = R>
            + LogicalResponseCodec<ReadResult = V>
            + CheckpointStateMachine,
        W: PersistenceWorker,
        O: LogicalOutboundQueue,
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
    /// Only sealed in-crate activation may supply configuration authorization.
    /// The original core/event still passes the existing owner intake path.
    pub(crate) fn poll_authorized<
        S: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
        A: ProposalAdmission<Receipt = R>
            + LogicalResponseCodec<ReadResult = V>
            + CheckpointStateMachine,
        W: PersistenceWorker,
        O: LogicalOutboundQueue,
    >(
        &mut self,
        parts: &mut ReplicaParts<'_, S, T, E, A, W, O>,
        now: MonoTime,
        budget: ReplicaPollBudget,
        configuration: impl FnMut(&Raft, &Event) -> Result<(), RaftError>,
    ) -> Result<ReplicaProgress, ReplicaError> {
        self.driver
            .poll_logical(parts, &mut self.release, now, budget, configuration)
    }
    pub fn releases(&mut self) -> &mut LogicalReleaseCoordinator<R, V> {
        &mut self.release
    }
    pub fn usage(&self) -> ReplicaDriverUsage {
        self.driver.usage()
    }
    pub fn release_usage(&self) -> LogicalReleaseUsage {
        self.release.usage()
    }
    pub fn is_drained(&self) -> bool {
        self.driver.is_drained() && self.release.is_drained()
    }
    pub fn close_intake<
        S: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
        A: StateMachine<Receipt = R> + ReadableStateMachine<ReadResult = V>,
        W: PersistenceWorker,
        O: OutboundQueue,
    >(
        &mut self,
        parts: &mut ReplicaParts<'_, S, T, E, A, W, O>,
    ) -> Result<(), ReplicaError> {
        self.driver.close_intake(parts)
    }
    pub fn discard_failed<
        S: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
        A: StateMachine<Receipt = R> + ReadableStateMachine<ReadResult = V>,
        W: PersistenceWorker,
        O: OutboundQueue,
    >(
        &mut self,
        parts: &mut ReplicaParts<'_, S, T, E, A, W, O>,
    ) -> Result<(), ReplicaError> {
        self.release
            .discard_failed(parts)
            .map_err(ReplicaError::Logical)?;
        self.driver.discard_failed(parts)
    }
}
