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
pub(super) fn receipt(e: &mut Encoder, r: &LogicalReceipt) -> Result<(), ApplicationError> {
    e.u64(r.index)?;
    e.u128(r.operation.get())?;
    e.u64(r.first_index)?;
    outcome(e, r.outcome)?;
    e.u64(r.parent_revision.map_or(0, LogRevision::get))?;
    e.u64(r.parent_generation.map_or(0, LogGeneration::get))?;
    e.u64(r.epoch.map_or(0, RepresentativeEpoch::get))?;
    e.u8(u8::from(r.duplicate))
}
pub(super) fn outcome(e: &mut Encoder, r: LogicalOutcome) -> Result<(), ApplicationError> {
    match r {
        LogicalOutcome::Claimed => e.u8(0),
        LogicalOutcome::Mutated => e.u8(1),
        LogicalOutcome::SnapshotPublished(r) => {
            e.u8(2)?;
            e.reference(r)
        }
        LogicalOutcome::SnapshotReconciled => e.u8(3),
        LogicalOutcome::CapsuleAuthorized => e.u8(4),
        LogicalOutcome::Refused(r) => {
            e.u8(5)?;
            e.u8(match r {
                LogicalRefusal::StaleRepresentative => 0,
                LogicalRefusal::StaleImage => 1,
                LogicalRefusal::Ineligible => 2,
                LogicalRefusal::MissingSnapshot => 3,
                LogicalRefusal::SnapshotConflict => 4,
                LogicalRefusal::InvalidResponse => 5,
                LogicalRefusal::OperationConflict => 6,
                LogicalRefusal::Expired => 7,
            })
        }
    }
}
pub(super) fn query(e: &mut Encoder, q: &LogicalQuery) -> Result<(), ApplicationError> {
    match q {
        LogicalQuery::Image => e.u8(0),
        LogicalQuery::Enrollment => e.u8(5),
        LogicalQuery::Outcome(id) => {
            e.u8(1)?;
            e.u128(id.get())
        }
        LogicalQuery::Snapshot(r) => {
            e.u8(2)?;
            e.reference(*r)
        }
        LogicalQuery::Metadata => e.u8(3),
        LogicalQuery::ConfigurationOperation(operation) => {
            e.u8(4)?;
            e.u128(operation.get())
        }
    }
}
pub(super) fn metadata(e: &mut Encoder, m: &LogicalMetadata) -> Result<(), ApplicationError> {
    e.mapping(m.mapping)?;
    e.u8(u8::from(m.representative.is_some()))?;
    if let Some(r) = m.representative {
        e.representative(r)?;
    }
    for n in [
        m.child_applied,
        u64::try_from(m.retained_actions).map_err(|_| ApplicationError::ReceiptBudget)?,
        u64::try_from(m.retained_action_bytes).map_err(|_| ApplicationError::ReceiptBudget)?,
        m.parent_revision.map_or(0, LogRevision::get),
        m.parent_generation.map_or(0, LogGeneration::get),
    ] {
        e.u64(n)?;
    }
    e.u128(m.retired_below)
}
