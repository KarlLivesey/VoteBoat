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
impl ApplicationReceipt for LogicalReceipt {
    fn index(&self) -> u64 {
        self.index
    }
    fn operation(&self) -> OperationId {
        self.operation
    }
    fn nested_bytes(&self, _: usize) -> Result<usize, ApplicationError> {
        Ok(0)
    }
}
impl<C: LogCodec + Clone, S: SnapshotCodec + Clone, W: WireCodec + Clone> StateMachine
    for LogicalApplication<C, S, W>
{
    type Receipt = LogicalReceipt;
    fn validate_group(&self, group: GroupIdentity) -> Result<(), ApplicationError> {
        if group == self.mapping.child {
            Ok(())
        } else {
            Err(ApplicationError::InvalidCommand)
        }
    }
    /// Configuration entries kept for life until AI3 (audit F5); actions retire by the LV2 watermark.
    fn retention(&self) -> crate::application::Retention {
        crate::application::Retention::LifetimeBounded
    }
    fn deployment_requirements(&self) -> Option<crate::raft::ReadinessRequirements> {
        let l = self.limits;
        Some(crate::raft::ReadinessRequirements {
            application_schema: self.application_schema(),
            // Words: the action limits, which decide whether a committed
            // action is recorded (`DedupCapacity`).
            identity: crate::raft::LayerIdentity::new(
                crate::raft::ApplicationFamily::LOGICAL_VOTER,
                [
                    l.max_actions as u64,
                    l.max_retained_action_bytes as u64,
                    l.control_actions as u64,
                    l.control_action_bytes as u64,
                ],
            ),
            inner_profile: crate::raft::InnerProfile::LEAF.planned(&self.plan_words()),
            command_bytes: self.limits.max_command_bytes,
            snapshot_bytes: self.limits.max_child_checkpoint_bytes,
        })
    }
    /// Whether the binding entry of the parent bootstrap has applied: a
    /// bootstrap voter's first successful claim (which carried the exact
    /// bootstrap) or a candidate's enrollment (whose snapshot did). Restore
    /// compares the bootstrap through the configuration.
    fn deployment_bound(&self) -> bool {
        match self.initialization {
            LogicalInitialization::BootstrapVoter => self.state.parent.is_some(),
            LogicalInitialization::EnrollmentCandidate(_) => self.state.enrollment.is_some(),
        }
    }
    fn applied_index(&self) -> u64 {
        self.state.applied
    }
    fn apply_batch(
        &mut self,
        entries: &[LogEntry],
    ) -> Result<Vec<LogicalReceipt>, ApplicationError> {
        // The test journal stays out of the working copy.
        #[cfg(test)]
        let mut journal = self.journal.take();
        let mut next = self.clone();
        let result = next.apply_entries(entries);
        #[cfg(test)]
        {
            if let (Ok(_), Some(journal)) = (&result, &mut journal) {
                journal.extend_from_slice(entries);
            }
            self.journal = journal;
        }
        let receipts = result?;
        self.state = next.state;
        Ok(receipts)
    }
}
impl<C: LogCodec + Clone, S: SnapshotCodec + Clone, W: WireCodec + Clone> BoundedStateMachine
    for LogicalApplication<C, S, W>
{
    fn receipt_bytes_bound(&self, entries: &[LogEntry]) -> Result<usize, ApplicationError> {
        entries
            .iter()
            .filter(|e| matches!(e.payload, EntryPayload::Command { .. }))
            .count()
            .checked_mul(std::mem::size_of::<LogicalReceipt>())
            .ok_or(ApplicationError::ReceiptBudget)
    }
}

impl<C: LogCodec + Clone, S: SnapshotCodec + Clone, W: WireCodec + Clone>
    LogicalApplication<C, S, W>
{
    /// Readiness plan words, the numeric part of the configuration that
    /// restore compares (`configuration_bytes`): the mapping (parent 3, voter,
    /// store 3, child 3); the parent log, parent snapshot and remaining
    /// application limits; the three codec format versions and the wire
    /// limits; an enrollment flag and, for a candidate, its plan (12); a
    /// control-profile flag and, if present, the profile (at most 121). At
    /// most 10 + 16 + 3 + 9 + 13 + 122 = 173 words. The child and parent
    /// bootstraps are not carried (unbounded); see
    /// docs/APPLICATION_ENVELOPES.md.
    fn plan_words(&self) -> Vec<u64> {
        use crate::raft::DeploymentPlan;
        let (m, l) = (self.mapping, self.limits);
        let (p, s, w) = (l.parent_log, l.parent_snapshots, self.wire_codec.limits());
        let mut out = Vec::with_capacity(173);
        out.extend(DeploymentPlan::group_words(m.parent));
        out.push(m.voter.get());
        out.extend(DeploymentPlan::store_words(m.store));
        out.extend(DeploymentPlan::group_words(m.child));
        for n in [
            p.max_groups,
            p.max_entries_per_group,
            p.max_command_bytes,
            p.max_snapshot_bytes,
            p.max_batch_bytes,
            p.max_batch_units,
            p.max_pending_units,
            p.max_wal_bytes,
            p.control_reserve_bytes,
            s.max_application_bytes,
            s.max_metadata_bytes,
            s.max_chunk_bytes,
            l.max_command_bytes,
            l.max_capsule_bytes,
            l.max_parent_checkpoint_bytes,
            l.max_child_checkpoint_bytes,
        ] {
            out.push(n as u64);
        }
        out.extend([
            u64::from(self.log_codec.format_version()),
            u64::from(self.snapshot_codec.format_version()),
            u64::from(self.wire_codec.format_version()),
        ]);
        for n in [
            w.max_frame_bytes,
            w.max_messages,
            w.max_entries_per_message,
            w.max_command_bytes,
            w.max_snapshot_bytes,
            w.max_decoded_bytes,
            w.policy.max_depth,
            w.policy.max_voters,
            w.policy.max_tree_nodes,
        ] {
            out.push(n as u64);
        }
        match self.initialization {
            LogicalInitialization::BootstrapVoter => out.push(0),
            LogicalInitialization::EnrollmentCandidate(plan) => {
                out.push(1);
                out.extend(DeploymentPlan::group_words(plan.parent));
                out.push(plan.target.node.get());
                out.extend(DeploymentPlan::store_words(plan.target.store));
                let operation = plan.operation.get();
                out.extend([
                    (operation >> 64) as u64,
                    operation as u64,
                    plan.expected.get(),
                    plan.configuration.get(),
                    plan.source_application_schema,
                ]);
            }
        }
        match &self.control_profile {
            None => out.push(0),
            Some(profile) => {
                out.push(1);
                profile.plan_words(&mut out);
            }
        }
        out
    }
}
