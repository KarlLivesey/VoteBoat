// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
#[test]
fn compact_pending_and_durable_keep_exact_terms_pointers_membership_and_charge() {
    let mut core = leader();
    core.propose_batch((1..=4).map(proposal).collect()).unwrap();
    complete_exact(&mut core);
    core.persist(core.durable.hard_state, 5, None, After::Commit, None)
        .unwrap();
    complete_exact(&mut core);
    let pointers = payload_pointers(&core);
    let peers = core.connection_replicas(3).unwrap();
    let membership = core.membership() as *const Membership;
    let before = core.state().clone();
    let reference = SnapshotRef {
        store: core.binding.identity,
        group: before.bootstrap.group,
        configuration: before.bootstrap.configuration,
        generation: SnapshotGeneration::new(1).unwrap(),
        index: 3,
        term: 1,
        application_schema: 1,
        file_bytes: 100,
        checksum: 7,
    };
    // This internal fixture supplies the host's already verified/pinned reference.
    core.begin_compact(reference).unwrap();
    assert!(matches!(
        core.pending.as_ref().unwrap().next,
        NextLog::Compact(_)
    ));
    assert_eq!(core.state(), &before);
    assert_eq!(core.connection_replicas(3).unwrap(), peers);
    let pending = core.pending.as_ref().unwrap();
    assert_eq!(pending.next.term_at(core.state(), &pending.update, 2), None);
    assert_eq!(
        pending.next.term_at(core.state(), &pending.update, 3),
        Some(1)
    );
    assert_eq!(
        pending.next.term_at(core.state(), &pending.update, 4),
        Some(1)
    );
    complete_exact(&mut core);
    assert_eq!(payload_pointers(&core), pointers[2..]);
    assert_eq!(core.membership() as *const Membership, membership);
    assert_eq!(core.connection_replicas(3).unwrap(), peers);
    assert!(core.log_summary.matches(core.state()));
    assert_eq!(
        core.retained_log_bytes(),
        LogSummary::from_validated(core.state()).retained_bytes()
    );
    for index in 0..=6 {
        assert_eq!(
            core.checkpoint_membership(index),
            core.state().checkpoint_membership(index)
        );
    }
    assert!(core.begin_compact(reference).is_err());
    let final_reference = SnapshotRef {
        index: 5,
        generation: SnapshotGeneration::new(2).unwrap(),
        ..reference
    };
    core.begin_compact(final_reference).unwrap();
    complete_exact(&mut core);
    assert!(core.state().entries.is_empty());
    assert_eq!(core.retained_log_bytes(), Some(0));
}
