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
impl SnapshotRouter {
    /// Admit one immutable read-only Load of the current durable core anchor.
    /// Current application progress may advance; the original reference does not.
    pub fn export_checkpoint<
        Q: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
        W: SnapshotWorker + ?Sized,
        A: CheckpointStateMachine,
    >(
        &mut self,
        owner: &mut EffectOwner<Q, T, E>,
        worker: &mut W,
        application: &A,
        group: GroupIdentity,
    ) -> Result<SnapshotExportTicket, SnapshotRouteError> {
        let (allowance, charge) = self.check_export(owner, worker, group)?;
        let core = owner.core(group).ok_or(SnapshotRouteError::UnknownGroup)?;
        application
            .validate_group(group)
            .map_err(|e| SnapshotRouteError::Checkpoint(CheckpointError::Application(e)))?;
        let reference = core
            .state()
            .snapshot
            .ok_or(SnapshotRouteError::MissingAnchor)?;
        if reference.group != group
            || reference.store != self.owner.store.identity
            || reference.application_schema != application.schema_version()
        {
            return Err(SnapshotRouteError::ProviderContract);
        }
        let expected = SnapshotMetadata {
            bootstrap: core.state().bootstrap.clone(),
            membership: core.state().snapshot_membership.clone(),
            index: reference.index,
            term: reference.term,
            application_schema: reference.application_schema,
        };
        let metadata = Snapshot {
            metadata: expected,
            application: Vec::new(),
        };
        if metadata.metadata.validate().is_err()
            || !reference.matches(&metadata)
            || snapshot_image_bytes(&metadata).is_none_or(|n| n > allowance)
        {
            return Err(SnapshotRouteError::TooLarge);
        }
        let sequence = self
            .exports
            .sequence
            .checked_add(1)
            .ok_or(SnapshotRouteError::Exhausted)?;
        let visit = VisitTicket {
            owner: self.owner,
            group,
            sequence,
        };
        self.exports.sequence = sequence;
        let request = worker
            .submit(SnapshotWork {
                visit,
                job: SnapshotJob::Load {
                    reference,
                    install: false,
                },
            })
            .map_err(|r| SnapshotRouteError::Worker(r.reason))?;
        let ticket = SnapshotExportTicket {
            owner: self.owner,
            request,
            visit,
            reference,
        };
        let invalid = request.binding != self.worker || request.sequence <= self.last_admission;
        self.exports.pending.insert(
            sequence,
            Export {
                ticket,
                expected: metadata.metadata,
                allowance,
                charge,
                cancelled: invalid,
                exported: false,
                outcome: None,
            },
        );
        if invalid {
            let _ = owner.fail::<()>(EffectOwnerError::ProviderContract);
            return Err(SnapshotRouteError::ProviderContract);
        }
        self.last_admission = request.sequence;
        Ok(ticket)
    }
    fn check_export<
        Q: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
        W: SnapshotWorker + ?Sized,
    >(
        &self,
        owner: &EffectOwner<Q, T, E>,
        worker: &W,
        group: GroupIdentity,
    ) -> Result<(usize, usize), SnapshotRouteError> {
        if self.exports.closed || owner.is_failed() {
            return Err(SnapshotRouteError::Closed);
        }
        if owner.identity() != self.owner {
            return Err(SnapshotRouteError::WrongOwner);
        }
        if worker.binding() != self.worker {
            return Err(SnapshotRouteError::WrongWorker);
        }
        if owner.core(group).is_none() {
            return Err(SnapshotRouteError::UnknownGroup);
        }
        if self.usage().requests >= self.limits.requests
            || self.exports.has_group(group)
            || self
                .pending
                .values()
                .any(|p| p.lease.ticket.visit.group == group)
        {
            return Err(SnapshotRouteError::Overloaded);
        }
        let allowance = worker
            .load_reservation(group)
            .ok_or(SnapshotRouteError::WrongWorker)?;
        if allowance == 0 || allowance > self.limits.max_image_bytes {
            return Err(SnapshotRouteError::TooLarge);
        }
        let charge = allowance
            .checked_mul(2)
            .and_then(|n| n.checked_add(size_of::<Export>()))
            .and_then(|n| n.checked_add(size_of::<SnapshotExportCompletion>() + 256))
            .ok_or(SnapshotRouteError::TooLarge)?;
        let usage = self.recovery_usage();
        if charge > self.recovery.image_bytes {
            return Err(SnapshotRouteError::TooLarge);
        }
        if usage.requests >= self.recovery.requests
            || charge > self.recovery.image_bytes.saturating_sub(usage.image_bytes)
        {
            return Err(SnapshotRouteError::Overloaded);
        }
        Ok((allowance, charge))
    }
}
