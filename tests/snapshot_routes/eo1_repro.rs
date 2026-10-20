// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! A loaded snapshot's Send keeps its place ahead of the same step's Persist,
//! driven through a real core, WAL worker and snapshot router.
use super::*;
use voteboat::membership::*;

fn learner_sender() -> StoreBinding {
    StoreBinding {
        identity: identity(2),
        session: StoreSession::new(1).unwrap(),
    }
}

/// Single voter (node 1) with learner node 2, log compacted through index 2.
fn compacted_single_voter_with_learner() -> (Raft, HostLogStore, Counter, Snapshot) {
    let mut log = HostLogStore::new(1);
    append(&mut log, vec![LogMutation::Create(bootstrap(1, 1))]);
    let s = log.state(group(1)).unwrap();
    let assign = LogEntry {
        index: 1,
        term: 1,
        payload: EntryPayload::Configuration(Box::new(ConfigurationRecord {
            operation: OperationId::new(900).unwrap(),
            expected: s.membership().unwrap().id(),
            change: ConfigurationChange::Learners(
                Configuration::new(
                    ConfigurationId::new(2).unwrap(),
                    s.bootstrap.policy.clone(),
                    s.bootstrap.voter_stores.clone(),
                    [(node(2), identity(2))].into_iter().collect(),
                )
                .unwrap(),
            ),
        })),
    };
    let command = LogEntry {
        index: 2,
        term: 1,
        payload: EntryPayload::Command {
            operation: OperationId::new(901).unwrap(),
            bytes: 7i64.to_le_bytes().to_vec().into(),
        },
    };
    append(
        &mut log,
        vec![update(
            &s,
            1,
            2,
            Some(Suffix {
                from: 1,
                entries: vec![assign.clone(), command.clone()].into(),
            }),
        )],
    );
    let mut core = Raft::recover_member(
        node(1),
        log.binding(),
        log.state(group(1)).unwrap(),
        log.limits(),
    )
    .unwrap();
    let mut app = Counter::new(20).unwrap();
    app.apply_batch(&[assign, command]).unwrap();
    let mut snapshots = support::snapshot::HostSnapshots::new();
    let receipt = checkpoint_application(&core, &app, &mut snapshots).unwrap();
    compact_replica(
        &mut core,
        &mut log,
        &mut snapshots,
        &app,
        receipt.reference(),
    )
    .unwrap();
    assert_eq!(core.state().base_index(), 2);
    let image = snapshots.current.clone().unwrap();
    (core, log, app, image)
}

/// Leader at term 2 with the learner in snapshot catch-up (outstanding request).
fn leader_with_lagging_learner() -> (Raft, HostLogStore, Counter, Snapshot) {
    let (mut core, mut log, mut app, image) = compacted_single_voter_with_learner();

    let persist_all = |core: &mut Raft, log: &mut HostLogStore, effects: Vec<Effect>| {
        let mut out = Vec::new();
        for e in effects {
            match e {
                Effect::Persist(u) => out.extend(persist_effect(core, log, u).unwrap()),
                other => out.push(other),
            }
        }
        out
    };
    // Campaign: single voter wins at once; become_leader appends the no-op.
    let effects = core.step(Event::Campaign).unwrap();
    let effects = persist_all(&mut core, &mut log, effects);
    let effects = persist_all(&mut core, &mut log, effects);
    let effects = persist_all(&mut core, &mut log, effects);
    assert_eq!(core.role(), Role::Leader);
    for e in &effects {
        if let Effect::Committed(entries) = e {
            app.apply_batch(entries).unwrap();
        }
    }
    let append_to_learner = effects
        .iter()
        .find_map(|e| match e {
            Effect::Send(m) if m.to == node(2) => Some(m.clone()),
            _ => None,
        })
        .expect("leader appends to the learner");
    // The learner has nothing: reject, so the leader falls back to the snapshot.
    let reply = Message {
        from: node(2),
        sender: learner_sender(),
        to: node(1),
        rpc: Rpc::Appended {
            success: false,
            matching_index: 0,
        },
        ..append_to_learner
    };
    let effects = core.step(Event::Receive(reply)).unwrap();
    assert!(
        matches!(&effects[..], [Effect::SnapshotRequired { to, .. }] if *to == node(2)),
        "{effects:?}"
    );
    assert_eq!(core.state().commit_index, 3);
    (core, log, app, image)
}

fn shape(effect: &Effect) -> &'static str {
    match effect {
        Effect::Send(_) => "Send",
        Effect::Persist(_) => "Persist",
        Effect::SnapshotRequired { .. } => "SnapshotRequired",
        Effect::Committed(_) => "Committed",
        _ => "Other",
    }
}

#[test]
fn leader_append_snapshot_retry_keeps_its_send_ahead_of_the_commit_persist() {
    let (core, log, mut app, image) = leader_with_lagging_learner();
    let identity = RuntimeOwner {
        store: log.binding(),
        lane: ExecutionLaneId::new(1).unwrap(),
        generation: RuntimeGeneration::new(1).unwrap(),
    };
    let mut shard = Shard::new(
        identity,
        ShardLimits {
            max_groups: 4,
            ..ShardLimits::default()
        },
        Ready(VecDeque::new()),
    )
    .unwrap();
    shard.register(core).unwrap();
    let runtime = TimedShard::new(
        shard,
        Timers {
            owner: identity,
            sequence: 0,
            entries: BTreeMap::new(),
        },
        Entropy(17),
        TimerConfig::default(),
        MonoTime(0),
    )
    .unwrap();
    let mut wal = HostWorker::new(log);
    let mut owner = EffectOwner::new(runtime, wal.binding(), EffectOwnerLimits::default()).unwrap();
    let (mut worker, mut router) = for_owner(&owner, 4);
    worker.images.insert(group(1), image);
    let now = MonoTime(0);

    // One client write under the lagging learner.
    owner
        .admit(
            group(1),
            Event::Propose {
                operation: OperationId::new(1).unwrap(),
                bytes: 5i64.to_le_bytes().to_vec(),
            },
        )
        .unwrap();
    owner.advance(now, 1).unwrap();
    let mut order = Vec::new();
    let mut submitted_snapshot = false;
    for _ in 0..100 {
        for event in wal.poll(1) {
            owner.deliver_worker(event, now).unwrap();
        }
        for event in worker.poll(1) {
            router.deliver(&mut owner, &mut app, event, now).unwrap();
        }
        owner.advance(now, 1).unwrap();
        while let Some(lease) = owner.take_effect().unwrap() {
            order.push(shape(&lease.effect));
            match &lease.effect {
                Effect::Send(_) => owner.release_transferred_send(lease.ticket).unwrap(),
                Effect::Committed(entries) => {
                    app.apply_batch(entries).unwrap();
                    owner.release(lease, app.applied_index(), now).unwrap();
                }
                Effect::SnapshotRequired { .. } => {
                    router.submit(&mut owner, &mut worker, lease, &app).unwrap();
                    submitted_snapshot = true;
                }
                Effect::Persist(_) => {
                    owner.submit_persists(&mut wal, vec![lease], now).unwrap();
                }
                other => panic!("unexpected {other:?}"),
            }
        }
        if submitted_snapshot && owner.is_drained() && wal.is_drained() && router.is_drained() {
            break;
        }
    }
    assert!(submitted_snapshot);
    assert!(!owner.is_failed());
    // The append's durable continuation is [SnapshotRequired, Persist(commit)]:
    // the loaded image's Send takes the SnapshotRequired's place.
    assert_eq!(
        order[..4],
        ["Persist", "SnapshotRequired", "Send", "Persist"]
    );
    assert_eq!(app.applied_index(), 4);
}
