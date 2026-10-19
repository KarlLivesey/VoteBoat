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
//! Trusted bridge protocol commands, never business intake or certificates.
use super::*;
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Command {
    pub operation: OperationId,
    pub origin_configuration: ConfigurationId,
    pub representative: RepresentativeBinding,
    /// The bridge's highest consumed sequence in this logical session: every
    /// earlier session-space outcome of this representative may be retired.
    /// Always 0 for claim bodies.
    pub consumed_through: u64,
    pub body: CommandBody,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum CommandBody {
    Claim {
        expected_epoch: Option<RepresentativeEpoch>,
    },
    Mutate {
        expected_generation: LogGeneration,
        update: LogUpdate,
        origin: PersistOrigin,
    },
    Publish {
        expected_revision: LogRevision,
        durable: Option<SnapshotRef>,
        snapshot: Snapshot,
        origin: SnapshotOrigin,
    },
    Reconcile {
        expected_revision: LogRevision,
        reference: SnapshotRef,
        origin: SnapshotOrigin,
    },
    Authorize(LogicalCapsule),
    EnrollmentClaim(Box<LogicalLearnerImport>),
}
/// Claim-space operation IDs are below this bound; session-space IDs are
/// `(logical_session << 64) | sequence` and therefore at or above it.
pub(crate) const SESSION_SPACE: u128 = 1 << 64;
/// Session-space ID of `sequence` in `session`.
pub(crate) fn session_operation(session: u64, sequence: u64) -> u128 {
    (u128::from(session) << 64) | u128::from(sequence)
}
impl Command {
    /// Whether the body is a claim, which lives in claim space.
    pub(super) fn is_claim(&self) -> bool {
        matches!(
            self.body,
            CommandBody::Claim { .. } | CommandBody::EnrollmentClaim(_)
        )
    }
    /// The operation-ID partition and acknowledgement rule (architect §2.2).
    pub(super) fn partitioned(&self) -> bool {
        let op = self.operation.get();
        if self.is_claim() {
            op < SESSION_SPACE && self.consumed_through == 0
        } else {
            let sequence = op as u64;
            op >> 64 == u128::from(self.representative.logical_session.get())
                && sequence >= 1
                && self.consumed_through < sequence
        }
    }
    /// The epoch a claim body expects; enrollment claims predate every
    /// representative. `None` for session bodies.
    pub(super) fn claim_expectation(&self) -> Option<Option<RepresentativeEpoch>> {
        match self.body {
            CommandBody::Claim { expected_epoch } => Some(expected_epoch),
            CommandBody::EnrollmentClaim(_) => Some(None),
            _ => None,
        }
    }
    pub(super) fn control(&self, profile: Option<&LogicalActivationControlProfile>) -> bool {
        match &self.body {
            CommandBody::Mutate { update, .. } => mutation_control(update, profile),
            CommandBody::Authorize(c) => capsule_control(c, profile),
            _ => true,
        }
    }
}
fn mutation_control(update: &LogUpdate, profile: Option<&LogicalActivationControlProfile>) -> bool {
    let Some(suffix) = &update.suffix else {
        return true;
    };
    match profile {
        Some(profile) => profile.is_control_batch(update.group, &suffix.entries.contiguous()),
        None => !suffix
            .entries
            .iter()
            .any(|e| matches!(e.payload, EntryPayload::Command { .. })),
    }
}
fn capsule_control(
    capsule: &LogicalCapsule,
    profile: Option<&LogicalActivationControlProfile>,
) -> bool {
    if matches!(capsule.body, LogicalCapsuleBody::Protocol(_)) {
        return true;
    }
    let Some(profile) = profile else {
        return false;
    };
    match (&capsule.origin, &capsule.body) {
        (
            LogicalCapsuleOrigin::Client { ticket, .. },
            LogicalCapsuleBody::ClientApplied {
                command, schema, ..
            },
        ) => {
            ticket.group == profile.parent()
                && *schema == profile.response_schema()
                && profile.classify_command(ticket.operation, command)
                    == Ok(LogicalActivationCommandClass::Control)
        }
        (
            LogicalCapsuleOrigin::Read {
                ticket, barrier, ..
            },
            LogicalCapsuleBody::ReadResult { schema, query, .. },
        ) => {
            ticket.group == profile.parent()
                && barrier.group == profile.parent()
                && *schema == profile.response_schema()
                && profile.is_control_query(query)
        }
        _ => false,
    }
}
