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
use codec::Envelope;
use std::mem::size_of;
impl<R: ApplicationReceipt> ApplicationReceipt for ActivationReceipt<R> {
    fn index(&self) -> u64 {
        match self {
            Self::Business(r) => r.index(),
            Self::Control(r) => r.index,
        }
    }
    fn operation(&self) -> OperationId {
        match self {
            Self::Business(r) => r.operation(),
            Self::Control(r) => r.operation,
        }
    }
    fn nested_bytes(&self, limit: usize) -> Result<usize, ApplicationError> {
        match self {
            Self::Business(r) => r.nested_bytes(limit),
            Self::Control(_) => Ok(0),
        }
    }
}
impl<A: ProposalAdmission + CheckpointStateMachine> StateMachine for LogicalActivationApplication<A>
where
    A::Receipt: ApplicationReceipt,
{
    type Receipt = ActivationReceipt<A::Receipt>;
    fn validate_group(&self, group: GroupIdentity) -> Result<(), ApplicationError> {
        if group != self.parent {
            return Err(ApplicationError::InvalidCommand);
        }
        self.inner.validate_group(group)
    }
    /// Activation intents kept for life until AI3 (audit L2). An undeclared
    /// inner application stays undeclared.
    fn retention(&self) -> crate::application::Retention {
        crate::application::Retention::wrapping(
            crate::application::Retention::LifetimeBounded,
            self.inner.retention(),
        )
    }
    fn deployment_requirements(&self) -> Option<crate::raft::ReadinessRequirements> {
        let inner = self.inner.deployment_requirements()?;
        if inner.application_schema != self.inner.schema_version()
            || inner.command_bytes.checked_add(BUSINESS_HEADER)? > self.limits.command_bytes
            || inner
                .snapshot_bytes
                .checked_add(self.limits.control_checkpoint_bound().ok()?)?
                > self.limits.checkpoint_bytes
        {
            return None;
        }
        Some(crate::raft::ReadinessRequirements {
            application_schema: self.schema,
            // Configuration words: intent capacity, command decode bound and
            // checkpoint bound; each decides committed entries or the
            // checkpoint encoding.
            identity: crate::raft::LayerIdentity::new(
                crate::raft::ApplicationFamily::LOGICAL_ACTIVATION,
                [
                    self.limits.intents as u64,
                    self.limits.command_bytes as u64,
                    self.limits.checkpoint_bytes as u64,
                    0,
                ],
            ),
            // The parent and every store mapping decide whether a committed
            // `Begin` is accepted (`check_intent`), and no committed entry
            // binds them, so they are this layer's deployment plan.
            inner_profile: crate::raft::InnerProfile::wrapping(inner).planned(&self.plan_words()),
            command_bytes: self.limits.command_bytes,
            snapshot_bytes: self.limits.checkpoint_bytes,
        })
    }
    fn applied_index(&self) -> u64 {
        self.inner.applied_index()
    }
    fn apply_batch(
        &mut self,
        entries: &[LogEntry],
    ) -> Result<Vec<Self::Receipt>, ApplicationError> {
        let mut next = self.clone();
        let mut out = Vec::with_capacity(
            entries
                .iter()
                .filter(|e| matches!(e.payload, EntryPayload::Command { .. }))
                .count(),
        );
        for entry in entries {
            if next.applied_index().checked_add(1) != Some(entry.index) {
                return Err(ApplicationError::IndexGap);
            }
            if entry.term == 0 {
                return Err(ApplicationError::InvalidCommand);
            }
            match &entry.payload {
                EntryPayload::Command { operation, bytes } => {
                    match codec::decode(bytes, next.limits.command_bytes)? {
                        Envelope::Business(raw) => {
                            Self::check_business(*operation)?;
                            let inner = LogEntry {
                                index: entry.index,
                                term: entry.term,
                                payload: EntryPayload::Command {
                                    operation: *operation,
                                    bytes: raw.into(),
                                },
                            };
                            let mut receipts = next.inner.apply_batch(&[inner])?;
                            if receipts.len() != 1
                                || receipts[0].index() != entry.index
                                || receipts[0].operation() != *operation
                            {
                                return Err(ApplicationError::InvalidCommand);
                            }
                            out.push(ActivationReceipt::Business(receipts.remove(0)));
                        }
                        Envelope::Control(c) => {
                            let receipt =
                                next.execute_control(*operation, c, entry.index, entry.term)?;
                            let noop = LogEntry {
                                index: entry.index,
                                term: entry.term,
                                payload: EntryPayload::Noop,
                            };
                            if !next.inner.apply_batch(&[noop])?.is_empty() {
                                return Err(ApplicationError::InvalidCommand);
                            }
                            out.push(ActivationReceipt::Control(receipt));
                        }
                    }
                }
                _ => {
                    if !next
                        .inner
                        .apply_batch(std::slice::from_ref(entry))?
                        .is_empty()
                    {
                        return Err(ApplicationError::InvalidCommand);
                    }
                }
            }
            if next.inner.applied_index() != entry.index {
                return Err(ApplicationError::IndexGap);
            }
        }
        *self = next;
        Ok(out)
    }
}
impl<A: ProposalAdmission + CheckpointStateMachine> BoundedStateMachine
    for LogicalActivationApplication<A>
where
    A::Receipt: ApplicationReceipt,
{
    fn receipt_bytes_bound(&self, entries: &[LogEntry]) -> Result<usize, ApplicationError> {
        let transformed = self.inner_entries(entries)?;
        let count = entries
            .iter()
            .filter(|e| matches!(e.payload, EntryPayload::Command { .. }))
            .count();
        count
            .checked_mul(size_of::<Self::Receipt>())
            .and_then(|v| v.checked_add(self.inner.receipt_bytes_bound(&transformed).ok()?))
            .ok_or(ApplicationError::ReceiptBudget)
    }
}
impl<A: ProposalAdmission + CheckpointStateMachine> LogicalActivationApplication<A>
where
    A::Receipt: ApplicationReceipt,
{
    /// Deployment plan words: the parent group (3 words), then each mapping
    /// in the constructor's voter order: voter, store (3), child (3). Every
    /// mapping's parent is the parent (constructor check). At most
    /// 3 + 16 × 7 = 115 words.
    fn plan_words(&self) -> Vec<u64> {
        let mut words = Vec::with_capacity(3 + 7 * self.mappings.len());
        words.extend(crate::raft::DeploymentPlan::group_words(self.parent));
        for m in &self.mappings {
            words.push(m.voter.get());
            words.extend(crate::raft::DeploymentPlan::store_words(m.store));
            words.extend(crate::raft::DeploymentPlan::group_words(m.child));
        }
        words
    }
    pub(super) fn inner_entries(
        &self,
        entries: &[LogEntry],
    ) -> Result<Vec<LogEntry>, ApplicationError> {
        entries
            .iter()
            .map(|e| {
                Ok(LogEntry {
                    index: e.index,
                    term: e.term,
                    payload: match &e.payload {
                        EntryPayload::Command { operation, bytes } => {
                            match codec::decode(bytes, self.limits.command_bytes)? {
                                Envelope::Business(raw) => EntryPayload::Command {
                                    operation: *operation,
                                    bytes: raw.into(),
                                },
                                Envelope::Control(_) => EntryPayload::Noop,
                            }
                        }
                        other => other.clone(),
                    },
                })
            })
            .collect()
    }
}
