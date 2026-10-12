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
    /// Read-only export of the selected live worker's exact durable anchor.
    /// This data is not a learner-readiness or quorum receipt.
    pub fn export_checkpoint(
        &mut self,
        group: GroupIdentity,
    ) -> Result<SnapshotExportTicket, SnapshotRouteError> {
        if self.state != NodeState::Running {
            return Err(SnapshotRouteError::Closed);
        }
        let snapshots = self
            .local
            .snapshots
            .as_mut()
            .ok_or(SnapshotRouteError::NotSnapshot)?;
        let app = self
            .local
            .applications
            .get(&group)
            .ok_or(SnapshotRouteError::UnknownGroup)?;
        let result = snapshots.router.export_checkpoint(
            &mut self.local.owner,
            &mut snapshots.worker,
            app,
            group,
        );
        if self.local.owner.is_failed() {
            self.enter_recovery(LogicalNodeError::Owner(EffectOwnerError::ProviderContract));
        }
        result
    }
    pub fn poll_checkpoint_export(&mut self) -> Option<SnapshotExportCompletion> {
        self.local.snapshots.as_mut()?.router.poll_export()
    }
    pub fn complete_checkpoint_export(
        &mut self,
        completion: SnapshotExportCompletion,
    ) -> Result<SnapshotExportOutcome, SnapshotExportRejected> {
        let Some(snapshots) = self.local.snapshots.as_mut() else {
            return Err(SnapshotExportRejected {
                reason: SnapshotRouteError::NotSnapshot,
                completion: Box::new(completion),
            });
        };
        snapshots.router.complete_export(completion)
    }
    pub fn cancel_checkpoint_export(
        &mut self,
        ticket: SnapshotExportTicket,
    ) -> Result<(), SnapshotRouteError> {
        self.local
            .snapshots
            .as_mut()
            .ok_or(SnapshotRouteError::NotSnapshot)?
            .router
            .cancel_export(ticket)
    }
    /// Only after failure, replace the ordinary snapshot consumer with bounded
    /// terminal cleanup. Held export results still need original completion.
    pub fn drain_failed_checkpoint_exports(
        &mut self,
        limit: usize,
    ) -> Result<usize, SnapshotRouteError> {
        if self.state != NodeState::RecoveryRequired {
            return Err(SnapshotRouteError::ProviderContract);
        }
        let snapshots = self
            .local
            .snapshots
            .as_mut()
            .ok_or(SnapshotRouteError::NotSnapshot)?;
        snapshots
            .router
            .drain_failed_exports(&mut self.local.owner, &mut snapshots.worker, limit)
    }
}
