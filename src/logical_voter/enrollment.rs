// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Immutable provisioning and retained import lineage; none grants authority.
use super::*;
mod capability;
pub use capability::{LogicalEnrollmentRejected, LogicalLearnerEnrollment};

pub const LOGICAL_ENROLLMENT_APPLICATION_SCHEMA: u64 = 6;
// Conservative space for the fixed command header, plan, export/owner/visit lineage,
// two blob lengths and selected native journal framing. Actual encoding remains bounded
// and checked; this reserve grants neither provider capacity nor import authority.
pub(super) const ENROLLMENT_FIXED_ENVELOPE_ALLOWANCE: usize = 2048;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LogicalEnrollmentPlan {
    pub parent: GroupIdentity,
    pub target: crate::secure::PeerIdentity,
    pub operation: OperationId,
    pub expected: ConfigurationId,
    pub configuration: ConfigurationId,
    pub source_application_schema: u64,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LogicalInitialization {
    BootstrapVoter,
    EnrollmentCandidate(LogicalEnrollmentPlan),
}
impl LogicalInitialization {
    pub(super) fn matches(self, mapping: LogicalStoreMapping, bootstrap: &Bootstrap) -> bool {
        match self {
            Self::BootstrapVoter => {
                bootstrap.voter_stores.get(&mapping.voter) == Some(&mapping.store)
            }
            Self::EnrollmentCandidate(plan) => {
                plan.source_application_schema != 0
                    && plan.parent == mapping.parent
                    && plan.target.node == mapping.voter
                    && plan.target.store == mapping.store
                    && plan.expected >= bootstrap.configuration
                    && plan.configuration > plan.expected
                    && !bootstrap.voter_stores.contains_key(&mapping.voter)
                    && !bootstrap
                        .voter_stores
                        .values()
                        .any(|store| *store == mapping.store)
            }
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LogicalEnrollmentOrigin {
    pub mapping: LogicalStoreMapping,
    pub representative: RepresentativeBinding,
    /// Original scalar correlation only, not a consumed export or runtime permit.
    pub export: crate::runtime::SnapshotExportTicket,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicalEnrollmentRecord {
    pub plan: LogicalEnrollmentPlan,
    pub origin: LogicalEnrollmentOrigin,
    pub learners: LogEntry,
    pub target_reference: SnapshotRef,
    pub operation: OperationId,
    pub first_index: u64,
    pub first_term: u64,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicalEnrollmentView {
    pub plan: LogicalEnrollmentPlan,
    pub record: Option<Box<LogicalEnrollmentRecord>>,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LogicalLearnerImport {
    pub plan: LogicalEnrollmentPlan,
    pub origin: LogicalEnrollmentOrigin,
    pub learners: LogEntry,
    pub snapshot: Snapshot,
}
impl<C: LogCodec + Clone, S: SnapshotCodec + Clone, W: WireCodec + Clone>
    LogicalApplication<C, S, W>
{
    /// Provision a schema2 candidate. This materializes no imported state or authority.
    pub fn new_enrollment_candidate(
        mapping: LogicalStoreMapping,
        child_bootstrap: Bootstrap,
        parent_bootstrap: Bootstrap,
        limits: LogicalLimits,
        codecs: (C, S, W),
        plan: LogicalEnrollmentPlan,
    ) -> Result<Self, ApplicationError> {
        Self::construct(
            mapping,
            child_bootstrap,
            parent_bootstrap,
            limits,
            codecs,
            LogicalInitialization::EnrollmentCandidate(plan),
        )
    }
    pub fn initialization(&self) -> LogicalInitialization {
        self.initialization
    }
    pub(super) fn candidate(&self) -> bool {
        matches!(
            self.initialization,
            LogicalInitialization::EnrollmentCandidate(_)
        )
    }
    pub(super) fn application_schema(&self) -> u64 {
        if self.candidate() {
            LOGICAL_ENROLLMENT_APPLICATION_SCHEMA
        } else {
            LOGICAL_APPLICATION_SCHEMA
        }
    }
    pub(super) fn validate_enrollment_limits(&self) -> Result<(), ApplicationError> {
        if !self.candidate() {
            return Ok(());
        }
        let command = self
            .limits
            .parent_snapshots
            .max_file_bytes()
            .checked_add(self.limits.parent_log.max_command_bytes)
            .and_then(|n| n.checked_add(ENROLLMENT_FIXED_ENVELOPE_ALLOWANCE))
            .ok_or(ApplicationError::InvalidCommand)?;
        let retained = self
            .limits
            .max_command_bytes
            .checked_mul(2)
            .ok_or(ApplicationError::InvalidCommand)?;
        if command > self.limits.max_command_bytes
            || self.limits.control_actions < 2
            || retained > self.limits.control_action_bytes
            || retained > self.limits.max_retained_action_bytes
            || self.limits.max_parent_checkpoint_bytes
                < self
                    .limits
                    .parent_log
                    .max_command_bytes
                    .saturating_add(ENROLLMENT_FIXED_ENVELOPE_ALLOWANCE)
        {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(())
    }
}
