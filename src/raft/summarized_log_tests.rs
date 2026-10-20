// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! The core's summary and install version follow every change of its log.
use super::*;
use crate::membership::ConfigurationChange;
use membership_tests::{
    accept, cid, committed_fixture, configuration, core, durable, follower, node, one_reply,
    staged, store,
};
use volatile_tests::volatile_cluster;

/// What one core call may change.
struct Before {
    state: GroupLog,
    version: u64,
}
fn before(core: &Raft) -> Before {
    Before {
        state: core.state().clone(),
        version: core.durable.version(),
    }
}
/// After one core call: the cached summary is exactly a fresh summary of the
/// current log, and the version moved exactly when the log changed (a missed
/// version change would let a plan made before the change skip its
/// predecessor check). Returns whether the log changed.
fn after(core: &Raft, before: &Before) -> bool {
    assert_eq!(
        *core.durable.summary(),
        LogSummary::from_validated(core.state()),
        "cached summary is the log's"
    );
    let changed = core.state() != &before.state;
    assert_eq!(
        core.durable.version() != before.version,
        changed,
        "version moves exactly when the log changes"
    );
    changed
}
/// Run one core call and check it; counts calls that changed the log.
fn tracked<T>(core: &mut Raft, changes: &mut usize, call: impl FnOnce(&mut Raft) -> T) -> T {
    let start = before(core);
    let result = call(core);
    *changes += usize::from(after(core, &start));
    result
}

/// Deliver every effect and message to quiescence, checking each core call
/// (receive steps and volatile completions). Sends from `silent` are lost.
fn settle(
    cluster: &mut [(Raft, VolatileLog)],
    slot: usize,
    effects: Vec<Effect>,
    silent: Option<usize>,
    changes: &mut usize,
) {
    let mut inbox: Vec<VecDeque<Message>> = vec![VecDeque::new(); cluster.len()];
    let mut work = vec![(slot, effects)];
    loop {
        while let Some((slot, effects)) = work.pop() {
            let mut queue: VecDeque<Effect> = effects.into();
            while let Some(effect) = queue.pop_front() {
                match effect {
                    Effect::PersistVolatile(token) => {
                        let (core, log) = &mut cluster[slot];
                        let start = before(core);
                        let (_, following) =
                            persist_volatile(core, log, token).unwrap().into_parts();
                        *changes += usize::from(after(core, &start));
                        queue.extend(following);
                    }
                    Effect::Send(message) if silent != Some(slot) => {
                        inbox[(message.to.get() - 1) as usize].push_back(message);
                    }
                    _ => {}
                }
            }
        }
        let Some(slot) = (0..cluster.len()).find(|slot| !inbox[*slot].is_empty()) else {
            return;
        };
        let message = inbox[slot].pop_front().unwrap();
        let effects = tracked(&mut cluster[slot].0, changes, |core| {
            core.step(Event::Receive(message)).unwrap()
        });
        work.push((slot, effects));
    }
}

#[test]
fn summary_and_version_follow_every_volatile_log_change_including_conflict_truncation() {
    let mut cluster = volatile_cluster();
    let mut changes = 0;
    // The three proposal forms, each replicated and committed: leader and
    // follower appends, volatile completions and commit-only rounds.
    let effects = tracked(&mut cluster[0].0, &mut changes, |core| {
        core.step(Event::Propose {
            operation: OperationId::new(1).unwrap(),
            bytes: vec![1],
        })
        .unwrap()
    });
    settle(&mut cluster, 0, effects, None, &mut changes);
    let effects = tracked(&mut cluster[0].0, &mut changes, |core| {
        core.propose_batch(
            (2..=3)
                .map(|n| Proposal {
                    operation: OperationId::new(n).unwrap(),
                    bytes: vec![n as u8],
                })
                .collect(),
        )
        .unwrap()
    });
    settle(&mut cluster, 0, effects, None, &mut changes);
    let effects = tracked(&mut cluster[0].0, &mut changes, |core| {
        let mut batch = core.proposal_batch(2).unwrap();
        for n in 4..=5u128 {
            batch
                .push(OperationId::new(n).unwrap(), vec![n as u8])
                .unwrap();
        }
        core.propose_entries(batch).unwrap()
    });
    settle(&mut cluster, 0, effects, None, &mut changes);
    let committed = cluster[0].0.state().commit_index;
    assert_eq!(committed, cluster[0].0.state().last_index());
    for (core, _) in &cluster[1..] {
        assert_eq!(core.state().entries, cluster[0].0.state().entries);
    }
    // An entry only the old leader holds: its appends are lost.
    let effects = tracked(&mut cluster[0].0, &mut changes, |core| {
        core.step(Event::Propose {
            operation: OperationId::new(6).unwrap(),
            bytes: vec![6],
        })
        .unwrap()
    });
    settle(&mut cluster, 0, effects, Some(0), &mut changes);
    let orphan = cluster[0].0.state().last_index();
    assert_eq!(orphan, committed + 1);
    // A new election (ballot changes on all three), then the new leader's
    // first append conflicts with the orphan and truncates it.
    let effects = tracked(&mut cluster[1].0, &mut changes, |core| {
        core.step(Event::Campaign).unwrap()
    });
    settle(&mut cluster, 1, effects, None, &mut changes);
    assert_eq!(cluster[1].0.role(), Role::Leader);
    let effects = tracked(&mut cluster[1].0, &mut changes, |core| {
        core.step(Event::Propose {
            operation: OperationId::new(7).unwrap(),
            bytes: vec![7],
        })
        .unwrap()
    });
    settle(&mut cluster, 1, effects, None, &mut changes);
    let new_term = cluster[1].0.state().hard_state.term;
    assert!(new_term > 1);
    assert_eq!(cluster[0].0.state().term_at(orphan), Some(new_term));
    assert_eq!(cluster[0].0.state().entries, cluster[1].0.state().entries);
    // Every proposal, append, completion, ballot and truncation above went
    // through the one install site.
    assert!(changes >= 30, "only {changes} log changes observed");
}

#[test]
fn summary_and_version_follow_durable_configuration_compaction_and_snapshot_install() {
    let mut changes = 0;
    let mut leader = core();
    // Configuration: accepted (pending only), durable completion, commit.
    let effects = tracked(&mut leader, &mut changes, |core| {
        accept(
            core,
            100,
            ConfigurationChange::Learners(configuration(2, &[1, 2, 3], &[4, 5])),
        )
    });
    tracked(&mut leader, &mut changes, |core| durable(core, effects));
    tracked(&mut leader, &mut changes, |core| committed_fixture(core, 2));
    // Durable proposals and their completion, then a commit-only round.
    let effects = tracked(&mut leader, &mut changes, |core| {
        core.propose_batch(
            (1..=2)
                .map(|n| Proposal {
                    operation: OperationId::new(n).unwrap(),
                    bytes: vec![n as u8],
                })
                .collect(),
        )
        .unwrap()
    });
    tracked(&mut leader, &mut changes, |core| durable(core, effects));
    tracked(&mut leader, &mut changes, |core| committed_fixture(core, 4));
    // Compaction through index 4.
    let reference = SnapshotRef {
        store: store(1),
        group: leader.state().bootstrap.group,
        generation: SnapshotGeneration::new(1).unwrap(),
        configuration: cid(2),
        index: 4,
        term: 1,
        application_schema: 1,
        file_bytes: 128,
        checksum: 7,
    };
    let snapshot = Snapshot {
        metadata: crate::snapshot::SnapshotMetadata {
            bootstrap: leader.state().bootstrap.clone(),
            membership: leader.state().checkpoint_membership(4).unwrap(),
            index: 4,
            term: 1,
            application_schema: 1,
        },
        application: vec![7],
    };
    let effects = tracked(&mut leader, &mut changes, |core| {
        core.begin_compact(reference).unwrap()
    });
    tracked(&mut leader, &mut changes, |core| durable(core, effects));
    assert_eq!(leader.state().base_index(), 4);
    // A lagging follower installs the snapshot (a full replacement).
    let mut lagging = follower(staged(), 2);
    leader.requests.remove(&node(2));
    leader.next_index.insert(node(2), 3);
    let Effect::SnapshotRequired { to, context, .. } = leader.append_for(node(2)).unwrap() else {
        panic!("expected snapshot dependency")
    };
    let message = one_reply(
        leader
            .snapshot_send(to, context, reference, snapshot)
            .unwrap(),
    );
    tracked(&mut lagging, &mut changes, |core| {
        core.receive_inner(message).unwrap()
    });
    let local = SnapshotRef {
        store: store(2),
        ..reference
    };
    let effects = tracked(&mut lagging, &mut changes, |core| {
        core.snapshot_stored(local).unwrap()
    });
    let effects = tracked(&mut lagging, &mut changes, |core| durable(core, effects));
    assert!(matches!(&effects[..], [Effect::SnapshotInstalled(r)] if *r == local));
    assert_eq!(lagging.state().base_index(), 4);
    tracked(&mut lagging, &mut changes, |core| {
        core.snapshot_applied(local, 4).unwrap()
    });
    // Configuration, two commits, proposals, compaction and the install.
    assert!(changes >= 6, "only {changes} log changes observed");
}

#[test]
fn plain_log_reservation_counts_the_bootstrap_voters_and_a_pending_configuration() {
    // A plain log's membership is the bootstrap electorate, counted without
    // walking it; a pending full plan that adds replicas still widens the
    // reservation before it is durable.
    let mut leader = core();
    assert!(leader.durable.summary().is_plain());
    assert_eq!(leader.rollback_replicas(), 3);
    assert_eq!(
        leader.effect_reservation(100),
        leader.reserve_effects(100, 3)
    );
    let effects = accept(
        &mut leader,
        100,
        ConfigurationChange::Learners(configuration(2, &[1, 2, 3], &[4, 5])),
    );
    assert!(leader.durable.summary().is_plain());
    assert!(leader.pending.as_ref().unwrap().membership.is_some());
    assert_eq!(leader.membership().replicas().count(), 5);
    assert_eq!(leader.rollback_replicas(), 5);
    assert_eq!(
        leader.effect_reservation(100),
        leader.reserve_effects(100, 5)
    );
    durable(&mut leader, effects);
    assert!(!leader.durable.summary().is_plain());
    assert_eq!(leader.rollback_replicas(), 5);
}
