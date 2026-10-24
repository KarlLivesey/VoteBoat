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
    stamp: Option<LogStamp>,
}
fn before(core: &Raft) -> Before {
    Before {
        state: core.state().clone(),
        stamp: core.durable.stamp(),
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
        core.durable.stamp() != before.stamp,
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
        core.begin_compact(reference).unwrap().into_owned()
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
            .unwrap()
            .into_owned(),
    );
    tracked(&mut lagging, &mut changes, |core| {
        core.receive_inner(message).unwrap().into_owned()
    });
    let local = SnapshotRef {
        store: store(2),
        ..reference
    };
    let effects = tracked(&mut lagging, &mut changes, |core| {
        core.snapshot_stored(local).unwrap().into_owned()
    });
    let effects = tracked(&mut lagging, &mut changes, |core| durable(core, effects));
    assert!(matches!(&effects[..], [Effect::SnapshotInstalled(r)] if *r == local));
    assert_eq!(lagging.state().base_index(), 4);
    tracked(&mut lagging, &mut changes, |core| {
        core.snapshot_applied(local, 4).unwrap().into_owned()
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

/// A volatile follower whose log holds only the bootstrap entry.
fn volatile_follower() -> (Raft, VolatileLog) {
    let mut core = core();
    core.volatile = true;
    core.incarnation = Raft::next_volatile_incarnation().unwrap();
    core.role = Role::Follower;
    let log = VolatileLog::new(core.binding, core.limits).unwrap();
    (core, log)
}
fn command(index: u64) -> LogEntry {
    LogEntry {
        index,
        term: 1,
        payload: EntryPayload::Command {
            operation: OperationId::new(u128::from(index)).unwrap(),
            bytes: vec![index as u8].into(),
        },
    }
}
fn token(effects: &[Effect]) -> VolatilePersist {
    let [Effect::PersistVolatile(token)] = effects else {
        panic!("one payload-free persistence token, got {effects:?}")
    };
    *token
}
/// Stage a follower append of `from..=to` (commit unchanged).
fn stage_append(core: &mut Raft, from: u64, to: u64) -> VolatilePersist {
    let commit = core.state().commit_index;
    let effects = core
        .persist(
            core.state().hard_state,
            commit,
            Some(Suffix {
                from,
                entries: (from..=to).map(command).collect::<Vec<_>>().into(),
            }),
            After::Reply,
            None,
        )
        .unwrap()
        .into_owned();
    assert!(!core.pending.as_ref().unwrap().next.is_commit());
    token(&effects)
}

#[test]
fn ordinary_appends_and_commit_rounds_install_without_a_predecessor_comparison() {
    use crate::log::transition::checked_installs::count;
    // Volatile: a follower append, then a commit-only round.
    let (mut replica, log) = volatile_follower();
    let start = count();
    let persist = stage_append(&mut replica, 2, 3);
    persist_volatile(&mut replica, &log, persist).unwrap();
    let effects = replica
        .persist(replica.state().hard_state, 3, None, After::Commit, None)
        .unwrap()
        .into_owned();
    assert!(replica.pending.as_ref().unwrap().next.is_commit());
    persist_volatile(&mut replica, &log, token(&effects)).unwrap();
    assert_eq!(
        (replica.state().last_index(), replica.state().commit_index),
        (3, 3)
    );
    // Durable: proposals, their completion and a commit round.
    let mut leader = core();
    let effects = leader
        .propose_batch(
            (1..=2)
                .map(|n| Proposal {
                    operation: OperationId::new(n).unwrap(),
                    bytes: vec![n as u8],
                })
                .collect(),
        )
        .unwrap();
    durable(&mut leader, effects);
    committed_fixture(&mut leader, 3);
    assert_eq!(leader.state().commit_index, 3);
    assert_eq!(count(), start, "an unchanged log was compared again");
}

#[test]
fn a_plan_whose_log_version_moved_takes_the_checked_install_and_installs() {
    use crate::log::transition::checked_installs::count;
    let (mut replica, log) = volatile_follower();
    let persist = stage_append(&mut replica, 2, 3);
    // Moves the version without changing the log: the plan still fits, but
    // the install can no longer know that without comparing.
    let stamp = replica.durable.stamp();
    replica.durable.edit(|_| ());
    assert_ne!(replica.durable.stamp(), stamp);
    let start = count();
    persist_volatile(&mut replica, &log, persist).unwrap();
    assert_eq!(count(), start + 1);
    assert!(!replica.is_fenced());
    assert_eq!(replica.state().last_index(), 3);
    assert_eq!(replica.state().entries[1], command(2));
    assert_eq!(replica.state().entries[2], command(3));
    assert_eq!(
        *replica.durable.summary(),
        LogSummary::from_validated(replica.state())
    );
}

/// The core cannot move a plan to another core; if one ever did, the install
/// must check it in every build, not trust the equal version (here the check
/// refuses it: the other log is longer).
#[test]
fn a_plan_moved_to_another_core_at_the_same_version_is_refused() {
    use crate::log::transition::checked_installs::count;
    let (mut short, _) = volatile_follower();
    stage_append(&mut short, 2, 3);
    // Another core at the same version (0, freshly recovered) with a longer log.
    let (mut grown, grown_log) = volatile_follower();
    let persist = stage_append(&mut grown, 2, 4);
    persist_volatile(&mut grown, &grown_log, persist).unwrap();
    let mut other = Raft::recover_volatile(grown.node, &grown_log, grown.state().clone()).unwrap();
    let version = |core: &Raft| core.durable.stamp().map(LogStamp::version);
    assert_eq!((version(&short), version(&other)), (Some(0), Some(0)));
    let before = other.state().clone();
    other.pending = short.pending.take();
    let start = count();
    let mut out = EffectQueue::new();
    assert!(matches!(
        other.install_pending_into(&mut out),
        Err(RaftError::Storage(StorageError::Rejected(
            "transition plan predecessor or update mismatch"
        )))
    ));
    assert_eq!(count(), start + 1);
    assert!(other.is_fenced());
    assert!(!other.has_pending_dependency());
    assert_eq!(other.state(), &before);
    assert_eq!(
        *other.durable.summary(),
        LogSummary::from_validated(other.state())
    );
}

/// Commit `commit` (a commit-only round) and install it.
fn commit_round(core: &mut Raft, log: &VolatileLog, commit: u64) {
    let effects = core
        .persist(core.state().hard_state, commit, None, After::Commit, None)
        .unwrap()
        .into_owned();
    assert!(core.pending.as_ref().unwrap().next.is_commit());
    persist_volatile(core, log, token(&effects)).unwrap();
    assert_eq!(core.state().commit_index, commit);
}
/// Stage and install `from..=to`.
fn append(core: &mut Raft, log: &VolatileLog, from: u64, to: u64) {
    let persist = stage_append(core, from, to);
    persist_volatile(core, log, persist).unwrap();
    assert_eq!(core.state().last_index(), to);
}
/// Move `from`'s staged plan into `into`, whose log it does not fit: the
/// install compares it, refuses it and fences `into`, changing nothing.
fn refuse_moved_plan(from: &mut Raft, into: &mut Raft) {
    use crate::log::transition::checked_installs::count;
    let before = into.state().clone();
    into.pending = from.pending.take();
    let start = count();
    let mut out = EffectQueue::new();
    assert!(matches!(
        into.install_pending_into(&mut out),
        Err(RaftError::Storage(StorageError::Rejected(
            "transition plan predecessor or update mismatch"
        )))
    ));
    assert_eq!(count(), start + 1);
    assert!(into.is_fenced());
    assert!(!into.has_pending_dependency());
    assert_eq!(into.state(), &before);
    assert_eq!(
        *into.durable.summary(),
        LogSummary::from_validated(into.state())
    );
}

/// Round 2's identities wrapped, so two consecutively constructed cores could
/// share a stamp. At the identity bound the last identities are still
/// distinct, later cores get none, and a moved plan is refused whether it
/// comes from the last identified core or from another unidentified one; an
/// unidentified core installs its own plans through the checked path.
#[test]
fn a_plan_moved_at_the_identity_bound_is_refused() {
    use crate::log::transition::{checked_installs::count, test_identities::with_log_identities};
    let ((), refused) = with_log_identities(u64::MAX - 2, || {
        let (mut short, short_log) = volatile_follower();
        let (mut grown, grown_log) = volatile_follower();
        let (first, last) = (short.durable.stamp(), grown.durable.stamp());
        assert_eq!(first.map(LogStamp::log), Some(u64::MAX - 2));
        assert_eq!(last.map(LogStamp::log), Some(u64::MAX - 1));
        let start = count();
        append(&mut grown, &grown_log, 2, 4);
        assert_eq!(count(), start, "an identified core keeps the fast path");
        // Every identity is issued: these cores have no stamp.
        let mut beyond =
            Raft::recover_volatile(grown.node, &grown_log, grown.state().clone()).unwrap();
        let mut unidentified_short =
            Raft::recover_volatile(short.node, &short_log, short.state().clone()).unwrap();
        let mut unidentified_grown =
            Raft::recover_volatile(grown.node, &grown_log, grown.state().clone()).unwrap();
        for core in [&beyond, &unidentified_short, &unidentified_grown] {
            assert_eq!(core.durable.stamp(), None);
        }
        // The last identified core's plan, moved past the bound.
        stage_append(&mut short, 2, 3);
        refuse_moved_plan(&mut short, &mut beyond);
        // Between two unidentified cores (both stamps `None`).
        stage_append(&mut unidentified_grown, 5, 6);
        refuse_moved_plan(&mut unidentified_grown, &mut unidentified_short);
        // An unidentified core's own appends and commit round: checked, and
        // they install.
        let (mut own, own_log) = volatile_follower();
        assert_eq!(own.durable.stamp(), None);
        let start = count();
        append(&mut own, &own_log, 2, 3);
        commit_round(&mut own, &own_log, 3);
        assert_eq!(count(), start + 2);
        assert!(!own.is_fenced());
        assert_eq!(
            *own.durable.summary(),
            LogSummary::from_validated(own.state())
        );
    });
    assert_eq!(refused, 4);
}

/// The version never wraps: at its bound the log takes a never-issued
/// identity and keeps the fast path, a plan staged before the bound was
/// crossed takes the checked install, and with no identity left the log
/// installs everything through the checked path.
#[test]
fn the_version_bound_keeps_the_fast_path_or_fails_closed() {
    use crate::log::transition::{checked_installs::count, test_identities::with_log_identities};
    let version = |core: &Raft| core.durable.stamp().map(LogStamp::version);
    let (mut replica, log) = volatile_follower();
    let identity = replica.durable.stamp().unwrap().log();
    replica.durable.set_version(u64::MAX - 1);
    let start = count();
    append(&mut replica, &log, 2, 3);
    assert_eq!(version(&replica), Some(u64::MAX));
    commit_round(&mut replica, &log, 3);
    let renewed = replica.durable.stamp().unwrap();
    assert_eq!(renewed.version(), 0);
    assert_ne!(renewed.log(), identity, "a fresh identity past the bound");
    append(&mut replica, &log, 4, 4);
    assert_eq!(
        count(),
        start,
        "both installs at the bound and the next were fast"
    );
    // A plan staged at `u64::MAX`, then the log moves across the bound.
    replica.durable.set_version(u64::MAX);
    let persist = stage_append(&mut replica, 5, 5);
    replica.durable.edit(|_| ());
    assert_eq!(version(&replica), Some(0));
    persist_volatile(&mut replica, &log, persist).unwrap();
    assert_eq!(count(), start + 1);
    assert_eq!(replica.state().last_index(), 5);
    // No identity left when the version reaches its bound: fail closed.
    let ((), refused) = with_log_identities(u64::MAX, || {
        replica.durable.set_version(u64::MAX);
        let start = count();
        append(&mut replica, &log, 6, 6);
        assert_eq!(count(), start, "the install at the bound was fast");
        assert_eq!(replica.durable.stamp(), None);
        append(&mut replica, &log, 7, 7);
        commit_round(&mut replica, &log, 7);
        assert_eq!(count(), start + 2);
    });
    assert_eq!(refused, 1);
    assert!(!replica.is_fenced());
    assert_eq!(replica.state().last_index(), 7);
    assert_eq!(
        *replica.durable.summary(),
        LogSummary::from_validated(replica.state())
    );
}
