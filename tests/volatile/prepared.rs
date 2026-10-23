// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Public Counter/provider histories; no native runtime or authority bypass.
use super::*;

fn identity(ordinal: usize) -> u128 {
    (1u128 << 127) + ordinal as u128 * 17
}

fn apply_group(
    core: &mut Raft,
    log: &VolatileLog,
    app: &mut Counter,
    effects: Vec<Effect>,
) -> Vec<CounterReceipt> {
    let mut receipts = Vec::new();
    for effect in super::settle(core, log, effects) {
        match effect {
            Effect::Committed(entries) => receipts.extend(app.apply_batch(&entries).unwrap()),
            _ => panic!("single-group fixture has no foreign output: {effect:?}"),
        }
    }
    receipts
}

#[test]
fn fleet_ship_department_groups_keep_independent_pending_writes_ids_and_drop() {
    let mut groups: Vec<_> = [101, 202, 303]
        .into_iter()
        .map(|group| {
            let (mut core, log) = super::solo(group, group);
            let mut app = Counter::new(16).unwrap();
            let effects = core.step(Event::Campaign).unwrap();
            assert!(apply_group(&mut core, &log, &mut app, effects).is_empty());
            Some((core, log, app))
        })
        .collect();
    // Fleet's admitted original remains pending. Ship and department require
    // no ancestor completion and use the same full original IDs independently.
    let fleet = groups[0].as_mut().unwrap();
    let held = fleet
        .0
        .step(Event::Propose {
            operation: OperationId::new(identity(1)).unwrap(),
            bytes: 7i64.to_le_bytes().to_vec(),
        })
        .unwrap();
    let fleet_prefix = fleet.0.state().commit_index;
    for group in &mut groups[1..] {
        let (core, log, app) = group.as_mut().unwrap();
        for ordinal in 1..=3 {
            let effects = core
                .step(Event::Propose {
                    operation: OperationId::new(identity(ordinal)).unwrap(),
                    bytes: 1i64.to_le_bytes().to_vec(),
                })
                .unwrap();
            let receipts = apply_group(core, log, app, effects);
            assert_eq!(receipts[0].outcome, CounterOutcome::Value(ordinal as i64));
            assert_eq!(receipts[0].operation.get(), identity(ordinal));
        }
    }
    let fleet = groups[0].as_mut().unwrap();
    assert_eq!(fleet.0.state().commit_index, fleet_prefix);
    assert_eq!(fleet.2.read_applied(fleet_prefix), Ok(0));
    assert_eq!(
        apply_group(&mut fleet.0, &fleet.1, &mut fleet.2, held)[0].outcome,
        CounterOutcome::Value(7)
    );
    drop(groups[1].take());
    let (core, log, app) = groups[2].as_mut().unwrap();
    let effects = core
        .step(Event::Propose {
            operation: OperationId::new(identity(4)).unwrap(),
            bytes: 2i64.to_le_bytes().to_vec(),
        })
        .unwrap();
    assert_eq!(
        apply_group(core, log, app, effects)[0].outcome,
        CounterOutcome::Value(5)
    );
    assert_eq!(groups[0].as_ref().unwrap().2.read_applied(2), Ok(7));
}

fn assert_receipts(cluster: &mut Cluster, first: usize, count: usize, active: &[u64]) {
    let receipts = std::mem::take(&mut cluster.receipts);
    assert_eq!(receipts.len(), count * active.len());
    for &replica in active {
        let actual: Vec<_> = receipts
            .iter()
            .filter(|(id, _)| *id == replica)
            .map(|(_, r)| *r)
            .collect();
        let expected: Vec<_> = (first..first + count)
            .map(|ordinal| CounterReceipt {
                index: ordinal as u64 + 1,
                operation: OperationId::new(identity(ordinal)).unwrap(),
                outcome: CounterOutcome::Value(ordinal as i64),
                duplicate: false,
            })
            .collect();
        assert_eq!(actual, expected);
    }
}

fn writes(
    cluster: &mut Cluster,
    width: usize,
    first: usize,
    rounds: usize,
    active: &[u64],
    checkpoints: &mut BTreeMap<u64, u64>,
) {
    for round in 0..rounds {
        let start = first + round * width;
        if width == 1 {
            cluster.propose(1, identity(start), 1);
        } else {
            let originals: Vec<_> = (start..start + width)
                .map(|ordinal| (identity(ordinal), 1))
                .collect();
            cluster.batch(1, &originals);
        }
        cluster.act(1, Event::Heartbeat);
        cluster.pump();
        assert_receipts(cluster, start, width, active);
        for &id in active {
            let r = &cluster.replicas[&id];
            let applied = r.application.applied_index();
            assert_eq!(applied, (start + width) as u64);
            assert_eq!(r.core.state().commit_index, applied);
            assert_eq!(
                r.application.read_applied(applied),
                Ok((start + width - 1) as i64)
            );
            if applied - checkpoints[&id] >= 5000 {
                cluster.compact(id);
                checkpoints.insert(id, applied);
            }
        }
        cluster.pump();
        // Inspect and release the finite observation; it must not pin every
        // historical message/backing through later checkpoint compaction.
        cluster.trace.clear();
    }
}

#[test]
fn prepared_public_counter_b1_b231_b515_two_checkpoints_snapshot_and_continued_writes() {
    for volatile in [true, false] {
        for width in [1usize, 231, 515] {
            let mut cluster = Cluster::new(volatile);
            cluster.api = Api::Entries;
            for r in cluster.replicas.values_mut() {
                r.application = Counter::new(32_000).unwrap();
                let mut limits = r.core.batch_limits();
                limits.max_proposal_entries = 1024;
                r.core.configure_batch_limits(limits).unwrap();
            }
            cluster.act(1, Event::Campaign);
            cluster.pump();
            let rounds = 10_500usize.div_ceil(width);
            let first_total = rounds * width;
            let mut checkpoints = BTreeMap::from([(1, 0), (2, 0), (3, 0)]);
            writes(&mut cluster, width, 1, rounds, &[1, 2, 3], &mut checkpoints);
            assert!(checkpoints.values().all(|&index| index >= 10_000));
            cluster.isolate(3);
            writes(
                &mut cluster,
                width,
                first_total + 1,
                rounds,
                &[1, 2],
                &mut checkpoints,
            );
            let expected = 2 * first_total + 1;
            cluster.blocked.clear();
            cluster.act(1, Event::Heartbeat);
            cluster.pump();
            assert!(
                cluster.installs > 0,
                "lagging follower must consume an actual snapshot"
            );
            assert_eq!(
                cluster.replicas[&3].application.applied_index(),
                expected as u64
            );
            assert_eq!(
                cluster.replicas[&3]
                    .application
                    .read_applied(expected as u64),
                Ok((expected - 1) as i64)
            );
            cluster.receipts.clear();
            writes(
                &mut cluster,
                width,
                2 * first_total + 1,
                2,
                &[1, 2, 3],
                &mut checkpoints,
            );
            for r in cluster.replicas.values() {
                assert!(!r.core.is_fenced());
                assert_eq!(r.core.state().commit_index, r.application.applied_index());
                assert!(r.queue.is_empty());
            }
            assert!(cluster.messages.is_empty());
            println!("CPU6_PUBLIC volatile={volatile} width={width} checkpoints={checkpoints:?} snapshot_installs={} completed={}", cluster.installs, 2 * first_total + 2 * width);
        }
    }
}
