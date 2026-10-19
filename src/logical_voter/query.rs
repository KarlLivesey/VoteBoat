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
use std::mem::size_of;
mod configurations;
impl<C: LogCodec + Clone, S: SnapshotCodec + Clone, W: WireCodec + Clone>
    LogicalApplication<C, S, W>
{
    /// Tri-state lookup (architect §2.4): retained, retired, or unknown.
    pub(crate) fn lookup(&self, operation: OperationId) -> LogicalOutcomeLookup {
        if let Some(status) = self.state.records.get(&operation) {
            LogicalOutcomeLookup::Retained(Box::new(status.clone()))
        } else if self.retired(operation) {
            LogicalOutcomeLookup::Expired
        } else {
            LogicalOutcomeLookup::Unknown
        }
    }
    pub fn metadata(&self) -> LogicalMetadata {
        LogicalMetadata {
            mapping: self.mapping,
            representative: self.state.representative,
            child_applied: self.state.applied,
            retained_actions: self.state.records.len(),
            retained_action_bytes: self.state.bytes,
            parent_revision: self.state.parent.as_ref().map(|p| p.revision),
            parent_generation: self.state.parent.as_ref().map(|p| p.generation),
            retired_below: self.state.retired_below,
        }
    }
}
impl<C: LogCodec + Clone, S: SnapshotCodec + Clone, W: WireCodec + Clone> ReadableStateMachine
    for LogicalApplication<C, S, W>
{
    type Query = LogicalQuery;
    type ReadResult = LogicalRead;
    fn read_at(&self, index: u64, query: LogicalQuery) -> Result<LogicalRead, ApplicationError> {
        if index > self.state.applied {
            return Err(ApplicationError::NotApplied);
        }
        Ok(match query {
            LogicalQuery::Enrollment => {
                let LogicalInitialization::EnrollmentCandidate(plan) = self.initialization else {
                    return Err(ApplicationError::UnsupportedSchema);
                };
                LogicalRead::Enrollment(Box::new(LogicalEnrollmentView {
                    plan,
                    record: self.state.enrollment.clone(),
                }))
            }
            LogicalQuery::ConfigurationOperation(operation) => LogicalRead::ConfigurationOperation(
                Box::new(self.configuration_operation(operation)?),
            ),
            LogicalQuery::Metadata => LogicalRead::Metadata(Box::new(self.metadata())),
            LogicalQuery::Outcome(id) => LogicalRead::Outcome(self.lookup(id)),
            LogicalQuery::Snapshot(reference) => self
                .state
                .snapshots
                .iter()
                .find(|(r, _)| *r == reference)
                .map_or(LogicalRead::Uninitialized, |(_, s)| {
                    LogicalRead::Snapshot(Box::new(s.clone()))
                }),
            LogicalQuery::Image => match (&self.state.parent, self.state.representative) {
                (Some(p), Some(r)) => LogicalRead::Image(Box::new(LogicalImage {
                    mapping: self.mapping,
                    representative: r,
                    parent: p.clone(),
                    child_membership: self.state.membership.clone(),
                    child_applied: self.state.applied,
                    published: self.state.published,
                    pins: self.state.snapshots.iter().map(|(r, _)| *r).collect(),
                })),
                _ => LogicalRead::Uninitialized,
            },
        })
    }
}
fn bootstrap_bytes(b: &Bootstrap) -> usize {
    crate::membership::Configuration::new(
        b.configuration,
        b.policy.clone(),
        b.voter_stores.clone(),
        BTreeMap::new(),
    )
    .map_or(usize::MAX, |c| c.retained_bytes())
}
pub(super) fn snapshot_charge(s: &Snapshot) -> usize {
    size_of::<Snapshot>()
        .saturating_add(s.application.capacity())
        .saturating_add(bootstrap_bytes(&s.metadata.bootstrap))
        .saturating_add(
            s.metadata
                .membership
                .as_ref()
                .map_or(0, |m| m.retained_bytes()),
        )
}
fn parent_charge(p: &GroupLog) -> usize {
    bootstrap_bytes(&p.bootstrap)
        .saturating_add(p.entries.capacity().saturating_mul(size_of::<LogEntry>()))
        .saturating_add(
            p.entries
                .iter()
                .map(LogEntry::retained_payload_bytes)
                .fold(0usize, usize::saturating_add),
        )
        .saturating_add(
            p.snapshot_membership
                .as_ref()
                .map_or(0, |m| m.retained_bytes()),
        )
}
pub(super) fn image_charge(i: &LogicalImage) -> usize {
    size_of::<LogicalImage>()
        .saturating_add(parent_charge(&i.parent))
        .saturating_add(i.child_membership.retained_bytes())
        .saturating_add(i.pins.capacity().saturating_mul(size_of::<SnapshotRef>()))
}
impl<C: LogCodec + Clone, S: SnapshotCodec + Clone, W: WireCodec + Clone>
    BoundedReadableStateMachine for LogicalApplication<C, S, W>
{
    fn query_bytes(&self, _: &LogicalQuery, _: usize) -> Result<usize, ApplicationError> {
        Ok(0)
    }
    fn read_result_bound(&self, query: &LogicalQuery) -> Result<usize, ApplicationError> {
        let nested = match query {
            LogicalQuery::Image
            | LogicalQuery::Enrollment
            | LogicalQuery::Snapshot(_)
            | LogicalQuery::ConfigurationOperation(_) => self.limits.max_parent_checkpoint_bytes,
            LogicalQuery::Outcome(_) => size_of::<LogicalActionStatus>()
                .checked_add(self.limits.max_command_bytes)
                .ok_or(ApplicationError::ReceiptBudget)?,
            LogicalQuery::Metadata => size_of::<LogicalMetadata>(),
        };
        size_of::<LogicalRead>()
            .checked_add(nested)
            .ok_or(ApplicationError::ReceiptBudget)
    }
    fn read_result_bytes(
        &self,
        result: &LogicalRead,
        limit: usize,
    ) -> Result<usize, ApplicationError> {
        let charge = match result {
            LogicalRead::ConfigurationOperation(h) => {
                self.validate_configuration_operation(h)?;
                configurations::charge(h)
            }
            LogicalRead::Image(i) => image_charge(i),
            LogicalRead::Enrollment(view) => size_of::<LogicalEnrollmentView>().saturating_add(
                view.record.as_ref().map_or(0, |r| {
                    size_of::<LogicalEnrollmentRecord>()
                        .saturating_add(r.learners.retained_payload_bytes())
                }),
            ),
            LogicalRead::Snapshot(s) => snapshot_charge(s),
            LogicalRead::Metadata(_) => size_of::<LogicalMetadata>(),
            LogicalRead::Outcome(LogicalOutcomeLookup::Retained(s)) => {
                size_of::<LogicalActionStatus>().saturating_add(s.command.capacity())
            }
            _ => 0,
        };
        if charge > limit {
            Err(ApplicationError::ReceiptBudget)
        } else {
            Ok(charge)
        }
    }
}

impl<C: LogCodec + Clone, S: SnapshotCodec + Clone, W: WireCodec + Clone>
    LogicalApplication<C, S, W>
{
    pub(super) fn validate_initial_parent(
        &self,
        parent: &GroupLog,
    ) -> Result<(), ApplicationError> {
        let charge = size_of::<LogicalImage>()
            .saturating_add(parent_charge(parent))
            .saturating_add(self.state.membership.retained_bytes())
            .saturating_add(
                self.state
                    .snapshots
                    .len()
                    .saturating_mul(size_of::<SnapshotRef>()),
            );
        if charge > self.limits.max_parent_checkpoint_bytes {
            return Err(ApplicationError::ReceiptBudget);
        }
        self.validate_response_initial(parent)
    }
    pub(super) fn validate_read_envelopes(&self) -> Result<(), ApplicationError> {
        let limit = self.limits.max_parent_checkpoint_bytes;
        if let Some(parent) = &self.state.parent {
            self.validate_initial_parent(parent)?;
        }
        for (_, snapshot) in &self.state.snapshots {
            if snapshot_charge(snapshot) > limit {
                return Err(ApplicationError::ReceiptBudget);
            }
        }
        self.validate_configuration_envelopes()?;
        self.validate_response_envelopes()
    }
}
