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
impl Command {
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
        Some(profile) => profile.is_control_batch(update.group, &suffix.entries),
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
