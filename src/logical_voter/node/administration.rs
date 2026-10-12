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
//! Original configuration observations over this logical node's selected owner.
use super::*;
impl<
        S: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
        A: ProposalAdmission + LogicalResponseCodec + CheckpointStateMachine,
        U: SecureSession,
        C: LogCodec + Clone,
        V: SnapshotCodec + Clone,
        B: WireCodec + Clone,
    > LogicalNode<S, T, E, A, U, C, V, B>
where
    A::Receipt: ApplicationReceipt,
{
    /// Admit an original administrative event; admission is not commitment.
    /// Polling retains the existing application and promotion authorization checks.
    pub fn configure(
        &mut self,
        request: ConfigurationRequest,
    ) -> Result<ConfigurationTicket, ConfigurationRejected> {
        if self.state != NodeState::Running {
            return Err(ConfigurationRejected {
                reason: ConfigurationRequestError::Closed,
                request: Box::new(request),
            });
        }
        self.configuration.submit(&mut self.local.owner, request)
    }
    /// Transfer fixed-size original metadata and release its observer credits.
    /// Queued owner or persistence work may outlive this observation.
    pub fn poll_configuration(&mut self) -> Option<ConfigurationCompletion> {
        self.configuration.poll()
    }
    /// Stop waiting without cancelling the original queued or persisted event.
    pub fn cancel_configuration(
        &mut self,
        ticket: ConfigurationTicket,
    ) -> Result<(), ConfigurationRequestError> {
        self.configuration.cancel_wait(ticket)
    }
    /// Observer emptiness alone establishes neither commitment nor node quiescence.
    pub fn configuration_requests_are_drained(&self) -> bool {
        self.configuration.is_drained()
    }
    /// Historical local durable status without a fresh cluster read barrier.
    /// Local absence grants no authority to reuse an operation identity.
    pub fn configuration_status(
        &self,
        group: GroupIdentity,
        operation: OperationId,
    ) -> Result<crate::membership::ConfigurationOperationStatus, LogicalNodeError> {
        if self.state == NodeState::RecoveryRequired {
            return Err(LogicalNodeError::RecoveryRequired);
        }
        self.local
            .owner
            .core(group)
            .ok_or(LogicalNodeError::Owner(EffectOwnerError::Runtime(
                RuntimeError::UnknownGroup,
            )))?
            .configuration_status(operation)
            .map_err(|error| LogicalNodeError::Owner(EffectOwnerError::Consensus(error)))
    }
}
