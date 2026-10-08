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
//! Local durable operation observations, separate from volatile proposals.
use super::*;
use crate::log::GroupLog;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConfigurationProgress {
    /// Absent from this local prefix. This cannot prove cluster-wide absence,
    /// failed execution, rollback, or permission to reuse another operation ID.
    NotFoundLocally,
    Learners {
        configuration: ConfigurationId,
        index: u64,
        term: u64,
    },
    Joint {
        configuration: ConfigurationId,
        target: ConfigurationId,
        index: u64,
        /// None when compaction discarded the original joint entry's term.
        term: Option<u64>,
    },
    Final {
        configuration: ConfigurationId,
        index: u64,
        term: u64,
    },
    /// Retained identity in a committed snapshot base, outside its active
    /// joint operation. Journal grammar proves that operation finished, but
    /// the original phase, payload and exact position were not retained.
    CompactedCompleted { through: u64 },
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConfigurationOperationStatus {
    pub group: GroupIdentity,
    pub operation: OperationId,
    /// Existing locally durable committed contiguous prefix, not a new receipt.
    pub committed_index: u64,
    /// Existing locally durable log end; accepted phases need not be committed.
    pub durable_last_index: u64,
    pub committed: ConfigurationProgress,
    pub accepted: ConfigurationProgress,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConfigurationResumeAction {
    /// Completed original operation, not a comparison to a new request payload.
    Completed,
    /// Joint/final work is locally durable but not yet committed.
    WaitForCommit,
    /// Submit through normal authorization, journal and durability checks.
    Finalize(ConfigurationRecord),
    NotFoundLocally,
}
impl ConfigurationOperationStatus {
    /// A local planning hint, never authority to append or change membership.
    /// Recheck the generated record through normal proposal admission.
    pub fn resume_action(&self) -> ConfigurationResumeAction {
        use ConfigurationProgress::*;
        match (self.committed, self.accepted) {
            (Learners { .. } | Final { .. } | CompactedCompleted { .. }, _) => {
                ConfigurationResumeAction::Completed
            }
            (
                Joint {
                    configuration,
                    target,
                    ..
                },
                Joint {
                    configuration: accepted,
                    target: accepted_target,
                    ..
                },
            ) if configuration == accepted && target == accepted_target => {
                ConfigurationResumeAction::Finalize(ConfigurationRecord {
                    operation: self.operation,
                    expected: configuration,
                    change: ConfigurationChange::Final { id: target },
                })
            }
            (NotFoundLocally, NotFoundLocally) => ConfigurationResumeAction::NotFoundLocally,
            _ => ConfigurationResumeAction::WaitForCommit,
        }
    }
}
impl GroupLog {
    /// Observe one operation in this provider's authoritative durable state.
    /// Positive committed progress is historical evidence, not a fresh read
    /// barrier or remote certificate. Negative observations are inconclusive.
    /// Pending/Written mutations must not be supplied as durable state.
    pub fn configuration_status(
        &self,
        operation: OperationId,
    ) -> Result<ConfigurationOperationStatus, MembershipError> {
        let committed = self.membership_at(self.commit_index)?;
        let accepted = self.membership()?;
        let progress = |view: &Membership, boundary: u64| {
            if !view.operations().contains(&operation) {
                return ConfigurationProgress::NotFoundLocally;
            }
            if let Some(entry) = self.entries.iter().rev().find(|entry| {
                entry.index <= boundary
                    && matches!(&entry.payload, EntryPayload::Configuration(record) if record.operation == operation)
            }) {
                let EntryPayload::Configuration(record) = &entry.payload else {
                    unreachable!()
                };
                return match &record.change {
                    ConfigurationChange::Learners(next) => ConfigurationProgress::Learners {
                        configuration: next.id(),
                        index: entry.index,
                        term: entry.term,
                    },
                    ConfigurationChange::Joint { id, next } => ConfigurationProgress::Joint {
                        configuration: *id,
                        target: next.id(),
                        index: entry.index,
                        term: Some(entry.term),
                    },
                    ConfigurationChange::Final { id } => ConfigurationProgress::Final {
                        configuration: *id,
                        index: entry.index,
                        term: entry.term,
                    },
                };
            }
            if let Some(joint) = view.joint().filter(|j| j.operation == operation) {
                return ConfigurationProgress::Joint {
                    configuration: joint.id,
                    target: joint.next.id(),
                    index: joint.index,
                    term: self.term_at(joint.index),
                };
            }
            // No retained record: the ID came from the validated committed base.
            // Another joint operation cannot start before this one's final,
            // and learner operations finish at their single committed record.
            ConfigurationProgress::CompactedCompleted {
                through: self.base_index(),
            }
        };
        Ok(ConfigurationOperationStatus {
            group: self.bootstrap.group,
            operation,
            committed_index: self.commit_index,
            durable_last_index: self.last_index(),
            committed: progress(&committed, self.commit_index),
            accepted: progress(&accepted, self.last_index()),
        })
    }
}
