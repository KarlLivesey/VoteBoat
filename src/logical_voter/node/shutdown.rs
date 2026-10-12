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
mod network;
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
    /// Poll original work after close_intake, retaining local delivery metadata.
    /// Ordinary poll remains strict about unexpected peer failure.
    pub fn poll_shutdown(
        &mut self,
        now: MonoTime,
        budget: LogicalNodePollBudget,
    ) -> Result<LogicalNodeShutdownProgress, LogicalNodeShutdownRejected> {
        let mut progress = LogicalNodeShutdownProgress::default();
        if let Err(reason) = self.validate_shutdown(now, budget) {
            return Err(LogicalNodeShutdownRejected {
                reason,
                progress: Box::new(progress),
            });
        }
        self.now = now;
        if self.state == NodeState::Drained {
            return Ok(progress);
        }
        if let Err(reason) = self.poll_shutdown_inner(now, budget, &mut progress) {
            self.enter_recovery(reason.clone());
            return Err(LogicalNodeShutdownRejected {
                reason,
                progress: Box::new(progress),
            });
        }
        Ok(progress)
    }
    fn validate_shutdown(
        &self,
        now: MonoTime,
        b: LogicalNodePollBudget,
    ) -> Result<(), LogicalNodeError> {
        if b.sessions > 65536
            || b.dispatch > 65536
            || !b.replica.valid()
            || b.transport.validate().is_err()
        {
            return Err(LogicalNodeError::InvalidLimits);
        }
        if now < self.now {
            return Err(LogicalNodeError::TimeWentBack);
        }
        match self.state {
            NodeState::Running => Err(LogicalNodeError::Closed),
            NodeState::RecoveryRequired => Err(LogicalNodeError::RecoveryRequired),
            _ => Ok(()),
        }
    }
    fn poll_shutdown_inner(
        &mut self,
        now: MonoTime,
        budget: LogicalNodePollBudget,
        progress: &mut LogicalNodeShutdownProgress,
    ) -> Result<(), LogicalNodeError> {
        self.prepare_shutdown()?;
        if self.state == NodeState::Draining {
            self.poll_shutdown_network(now, budget, progress)?;
        } else {
            self.poll_network_record(now, budget, Some(&mut progress.completions))?;
        }
        progress.replica = self
            .replica
            .poll(&mut self.local.as_parts(), now, budget.replica)
            .map_err(LogicalNodeError::Replica)?;
        self.configuration
            .observe(&self.local.owner, &progress.replica.steps)
            .map_err(LogicalNodeError::Configuration)?;
        self.prepare_shutdown()?;
        self.finish_draining();
        Ok(())
    }
    fn prepare_shutdown(&mut self) -> Result<(), LogicalNodeError> {
        if self.state == NodeState::Quiescing
            && self.local.clients.is_drained()
            && self.local.reads.is_drained()
            && self.local.results.is_drained()
            && self.configuration.is_drained()
        {
            self.local
                .owner
                .close_admission()
                .map_err(LogicalNodeError::Owner)?;
            self.state = NodeState::Draining;
            for slot in self.sessions.values_mut() {
                slot.incoming = None;
                slot.session.close();
            }
        }
        Ok(())
    }
}
