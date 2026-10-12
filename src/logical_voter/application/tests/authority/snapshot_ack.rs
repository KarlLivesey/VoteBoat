// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Actual Raft Ack plus synthetic malformed-capsule guards; no release permits.
use super::*;
use crate::snapshot::{Snapshot, SnapshotMetadata};
fn prepared() -> LogicalApplication {
    let mut a = fixture();
    let change = update(
        &a,
        2,
        Some(Suffix {
            from: 1,
            entries: (1..=3)
                .map(|index| LogEntry {
                    index,
                    term: 1,
                    payload: EntryPayload::Noop,
                })
                .collect(),
        }),
    );
    let generation = a.state.parent.as_ref().unwrap().generation;
    assert_eq!(
        apply(
            &mut a,
            &command(
                2,
                1,
                CommandBody::Mutate {
                    expected_generation: generation,
                    update: change,
                    origin: origin()
                }
            )
        )
        .outcome,
        LogicalOutcome::Mutated
    );
    a
}
fn capsule(a: &LogicalApplication) -> LogicalCapsule {
    let parent = a.state.parent.as_ref().unwrap();
    let mut incoming = incoming(2);
    incoming.rpc = Rpc::Snapshot {
        snapshot: Box::new(Snapshot {
            metadata: SnapshotMetadata {
                bootstrap: parent.bootstrap.clone(),
                membership: parent.checkpoint_membership(1).unwrap(),
                index: 1,
                term: 1,
                application_schema: 1,
            },
            application: vec![1],
        }),
    };
    let message = emitted(a, Event::Receive(incoming));
    assert_eq!(message.rpc, Rpc::SnapshotAck { index: 1 });
    let CommandBody::Authorize(capsule) = authorize(a, 3, message).body else {
        unreachable!()
    };
    capsule
}
#[test]
fn original_raft_snapshot_ack_above_base_requires_committed_known_prefix() {
    let a = prepared();
    let original = capsule(&a);
    assert_eq!(
        a.authorize(&original, representative(1)).unwrap(),
        LogicalOutcome::CapsuleAuthorized
    );
    let mut uncommitted = original.clone();
    let LogicalCapsuleBody::Protocol(m) = &mut uncommitted.body else {
        unreachable!()
    };
    m.rpc = Rpc::SnapshotAck { index: 3 };
    assert_eq!(
        a.authorize(&uncommitted, representative(1)).unwrap(),
        LogicalOutcome::Refused(LogicalRefusal::InvalidResponse)
    );
    let mut hole = a.clone();
    hole.state.parent.as_mut().unwrap().entries.remove(0);
    assert_eq!(
        hole.authorize(&original, representative(1)),
        Err(ApplicationError::InvalidCommand)
    );
}
#[test]
fn snapshot_ack_keeps_original_term_configuration_and_origin_guards() {
    let a = prepared();
    let original = capsule(&a);
    let mut wrong_term = original.clone();
    let LogicalCapsuleBody::Protocol(m) = &mut wrong_term.body else {
        unreachable!()
    };
    m.term = 2;
    assert_eq!(
        a.authorize(&wrong_term, representative(1)).unwrap(),
        LogicalOutcome::Refused(LogicalRefusal::InvalidResponse)
    );
    let mut wrong_configuration = original.clone();
    wrong_configuration.local_configuration = cid(2);
    assert_eq!(
        a.authorize(&wrong_configuration, representative(1))
            .unwrap(),
        LogicalOutcome::Refused(LogicalRefusal::StaleImage)
    );
    let mut wrong_origin = original;
    let LogicalCapsuleOrigin::Protocol(effect) = &mut wrong_origin.origin else {
        unreachable!()
    };
    effect.visit.owner.store.identity = store(3);
    assert_eq!(
        a.authorize(&wrong_origin, representative(1)).unwrap(),
        LogicalOutcome::Refused(LogicalRefusal::InvalidResponse)
    );
}
