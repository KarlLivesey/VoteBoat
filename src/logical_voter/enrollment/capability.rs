// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Owned source data granted only by the original activation driver.
use super::*;

/// One consumed source export under its original activation intent. Public
/// provisioning and parsed lineage cannot construct or clone this capability.
#[derive(Debug)]
pub struct LogicalLearnerEnrollment {
    pub(in crate::logical_voter) mapping: LogicalStoreMapping,
    pub(in crate::logical_voter) import: Box<LogicalLearnerImport>,
}
impl LogicalLearnerEnrollment {
    pub(in crate::logical_voter) fn mint(
        mapping: LogicalStoreMapping,
        import: LogicalLearnerImport,
    ) -> Self {
        Self {
            mapping,
            import: Box::new(import),
        }
    }
}
/// Failed target intake returns the entire original source capability.
#[derive(Debug)]
pub struct LogicalEnrollmentRejected {
    pub reason: LogicalBridgeError,
    pub enrollment: LogicalLearnerEnrollment,
}
