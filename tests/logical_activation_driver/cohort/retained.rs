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
//! Retained child action history by kind, checked against the preflight budget.
//!
//! Records are never removed, so the count at the end of a workflow is its
//! peak. The kind comes from the encoded command; each decoded kind is
//! cross-checked against the record's public receipt outcome and operation,
//! so a format change fails here instead of miscounting.
use super::*;
use voteboat::{application::ReadableStateMachine, identity::OperationId};

/// Protocol, election and heartbeat actions the lifetime preflight budgets
/// within the 128 retained actions (`preflight/lifetime.rs`).
pub const PROTOCOL_ACTIONS: usize = 64;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Retained {
    pub claim: usize,
    pub mutate: usize,
    pub publish: usize,
    pub reconcile: usize,
    pub protocol: usize,
    pub client: usize,
    pub read: usize,
    pub enrollment: usize,
    pub refused: usize,
    /// Records that are not addressed by a bridge session sequence (claims
    /// carry their own operation id).
    pub unscanned: usize,
    pub total: usize,
}

// Fixed command header: magic, mapping (parent group, voter, store, child
// group), operation, origin configuration, representative binding.
const OPERATION: usize = 8 + 24 + 8 + 24 + 24;
const BODY: usize = OPERATION + 16 + 8 + 8 + 8 + 24 + 8 + 8;
// Authorize: expected revision, expected generation, local configuration.
const CAPSULE_ORIGIN: usize = BODY + 1 + 24;

pub fn count(app: &LogicalApplication) -> Retained {
    let metadata = app.metadata();
    let mut retained = Retained {
        total: metadata.retained_actions,
        ..Retained::default()
    };
    let mut found = 0;
    // Bridge operation ids are `logical_session << 64 | sequence`.
    'sessions: for session in 1..=32u128 {
        for sequence in 1..=1024u128 {
            if found == retained.total {
                break 'sessions;
            }
            let id = OperationId::new(session << 64 | sequence).unwrap();
            let Ok(LogicalRead::Outcome(Some(status))) =
                app.read_at(metadata.child_applied, LogicalQuery::Outcome(id))
            else {
                continue;
            };
            found += 1;
            classify(&status, &mut retained);
        }
    }
    retained.unscanned = retained.total - found;
    retained
}

fn classify(status: &LogicalActionStatus, retained: &mut Retained) {
    let bytes = &status.command;
    assert!(matches!(&bytes[..8], b"VBLOGA01" | b"VBLOGA02"));
    assert_eq!(
        u128::from_le_bytes(bytes[OPERATION..OPERATION + 16].try_into().unwrap()),
        status.operation.get(),
        "retained command header layout"
    );
    let outcome = status.receipt.outcome;
    let refused = matches!(outcome, LogicalOutcome::Refused(_));
    let expected = |ok: bool| assert!(ok || refused, "retained kind/outcome {outcome:?}");
    match bytes[BODY] {
        0 => {
            expected(outcome == LogicalOutcome::Claimed);
            retained.claim += 1;
        }
        1 => {
            expected(outcome == LogicalOutcome::Mutated);
            retained.mutate += 1;
        }
        2 => {
            expected(matches!(outcome, LogicalOutcome::SnapshotPublished(_)));
            retained.publish += 1;
        }
        3 => {
            expected(outcome == LogicalOutcome::SnapshotReconciled);
            retained.reconcile += 1;
        }
        4 => {
            expected(outcome == LogicalOutcome::CapsuleAuthorized);
            match bytes[CAPSULE_ORIGIN] {
                0 => retained.protocol += 1,
                1 => retained.client += 1,
                2 => retained.read += 1,
                tag => panic!("retained capsule origin {tag}"),
            }
        }
        5 => retained.enrollment += 1,
        tag => panic!("retained command body {tag}"),
    }
    if refused {
        retained.refused += 1;
    }
}

/// The most advanced replica's history for this child group.
pub fn group(nodes: &[cluster::ChildNode], slot: usize) -> Retained {
    let child = groups::mapping(slot).child;
    let app = nodes
        .iter()
        .filter_map(|n| n.local().applications.get(&child))
        .max_by_key(|a| a.metadata().child_applied)
        .unwrap();
    count(app)
}
