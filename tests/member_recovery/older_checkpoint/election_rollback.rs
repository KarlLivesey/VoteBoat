// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Actual append rollback and durable elections over an older learner checkpoint.
use super::*;
use voteboat::native::{log_store::*, snapshot_store::*};

fn rollback_request() -> Message {
    let sender = StoreBinding {
        identity: identity(1),
        session: StoreSession::new(1).unwrap(),
    };
    Message {
        group: group(1),
        configuration: cid(3),
        from: node(1),
        to: node(4),
        sender,
        term: 2,
        context: RequestContext {
            origin: sender,
            sequence: 71,
        },
        rpc: Rpc::Append {
            previous_index: 4,
            previous_term: 1,
            entries: vec![LogEntry {
                index: 5,
                term: 2,
                payload: EntryPayload::Noop,
            }]
            .into(),
            leader_commit: 4,
        },
    }
}

fn replacement(core: &mut Raft) -> LogUpdate {
    assert_eq!(core.membership().id(), cid(4));
    let effects = core.step(Event::Receive(rollback_request())).unwrap();
    let [Effect::Persist(update)] = effects.as_slice() else {
        panic!("rollback must not release an append acknowledgement before persistence")
    };
    assert_eq!(core.membership().id(), cid(3));
    assert_eq!(core.state().membership().unwrap().id(), cid(4));
    assert_eq!(core.step(Event::Campaign), Err(RaftError::Busy));
    assert_eq!(core.state().commit_index, 4);
    assert_eq!(update.suffix.as_ref().unwrap().from, 5);
    update.clone()
}

fn recovered(
    log: &impl LogStore,
    snapshots: &mut impl SnapshotRetention,
    final_retained: bool,
) -> (Raft, Counter) {
    let mut app = Counter::new(16).unwrap();
    let (core, replay) =
        recover_member_replica(node(4), group(1), log, snapshots, &mut app).unwrap();
    assert_eq!(replay.checkpoint_index, 2);
    assert_eq!(core.state().commit_index, 4);
    assert_eq!(core.state().base_index(), 2);
    assert_eq!(core.state().last_index(), 5);
    assert_eq!(app.read_applied(4), Ok(10));
    assert!(core.local_voter());
    assert_eq!(
        core.membership().id(),
        cid(if final_retained { 4 } else { 3 })
    );
    assert_eq!(core.membership().joint().is_some(), !final_retained);
    assert_eq!(
        core.state()
            .configuration_status(OperationId::new(3).unwrap())
            .unwrap()
            .resume_action(),
        if final_retained {
            ConfigurationResumeAction::WaitForCommit
        } else {
            ConfigurationResumeAction::Finalize(ConfigurationRecord {
                operation: OperationId::new(3).unwrap(),
                expected: cid(3),
                change: ConfigurationChange::Final { id: cid(4) },
            })
        }
    );
    (core, app)
}

/// Matching committed history is a fixture premise; ballots below are actual
/// core transitions with native persistence, not fixture durability tokens.
fn ballot(request: Message, final_retained: bool) -> Message {
    let mut log = NativeLogStore::create(
        ModelIo::default(),
        identity(request.to.get() as u128),
        LogLimits::default(),
    )
    .unwrap();
    append(&mut log, vec![LogMutation::Create(bootstrap(1, 3))]);
    record(
        &mut log,
        2,
        ConfigurationChange::Learners(configuration(2, &[1, 2, 3], &[4, 5])),
        1,
    );
    command(&mut log, 900, 7);
    command(&mut log, 901, 3);
    advance(&mut log, Stage::FinalAccepted);
    let mut peer = Raft::recover_member(
        request.to,
        log.binding(),
        log.state(group(1)).unwrap(),
        log.limits(),
    )
    .unwrap();
    if !final_retained {
        let mut rollback = rollback_request();
        rollback.to = request.to;
        // Voter2 can receive voter1's request. For voter1 use the same exact
        // authorized prefix from voter2, avoiding a self-addressed ingress.
        if rollback.to == node(1) {
            rollback.from = node(2);
            rollback.sender.identity = identity(2);
            rollback.context.origin = rollback.sender;
        }
        let effects = peer.step(Event::Receive(rollback)).unwrap();
        persist(&mut peer, &mut log, effects);
    }
    let effects = peer.step(Event::Receive(request)).unwrap();
    assert_eq!(peer.state().hard_state.voted_for, None);
    let effects = persist(&mut peer, &mut log, effects);
    let [Effect::Send(reply)] = effects.as_slice() else {
        panic!("exact durable ballot")
    };
    assert!(matches!(reply.rpc, Rpc::Voted { granted: true }));
    assert_eq!(peer.state().hard_state.voted_for, Some(node(4)));
    assert_eq!(log.state(group(1)).unwrap(), *peer.state());
    reply.clone()
}

fn elect(core: &mut Raft, log: &mut impl LogStore, final_retained: bool) {
    let effects = core.step(Event::Campaign).unwrap();
    assert_eq!(core.role(), Role::Candidate);
    assert_eq!(core.state().hard_state.voted_for, None);
    assert_eq!(core.step(Event::Campaign), Err(RaftError::Busy));
    let requests = persist(core, log, effects);
    assert_eq!(core.role(), Role::Candidate);
    assert_eq!(
        core.state().ballot_origin.unwrap().configuration,
        cid(if final_retained { 4 } else { 3 })
    );
    let request = |id| {
        requests
            .iter()
            .find_map(|effect| match effect {
                Effect::Send(message)
                    if message.to == node(id) && matches!(message.rpc, Rpc::Vote { .. }) =>
                {
                    Some(message.clone())
                }
                _ => None,
            })
            .unwrap()
    };
    let first = ballot(request(1), final_retained);
    if !final_retained {
        let mut stale = first.clone();
        stale.configuration = cid(4);
        assert_eq!(
            core.step(Event::Receive(stale)),
            Err(RaftError::WrongIdentity)
        );
        assert_eq!(core.role(), Role::Candidate);
    }
    let mut effects = core.step(Event::Receive(first)).unwrap();
    if !final_retained {
        assert!(
            effects.is_empty(),
            "new-only majority cannot elect under Joint"
        );
        assert_eq!(core.role(), Role::Candidate);
        effects = core
            .step(Event::Receive(ballot(request(2), false)))
            .unwrap();
    }
    assert_eq!(core.role(), Role::Leader);
    assert_eq!(core.state().last_index(), 5);
    assert_eq!(core.step(Event::Campaign), Err(RaftError::Busy));
    let _replication = persist(core, log, effects);
    assert_eq!(core.role(), Role::Leader);
    assert_eq!(core.state().hard_state.voted_for, Some(node(4)));
    assert_eq!(core.state().last_index(), 6);
    assert_eq!(core.state().commit_index, 4);
}

#[test]
fn old_checkpoint_final_rollback_native_faults_restore_the_actual_election_policy() {
    let source = ModelIo::default();
    let mut baseline =
        NativeLogStore::create(source.clone(), identity(4), LogLimits::default()).unwrap();
    let mut images = snapshots();
    let original = old_checkpoint(&mut baseline, &mut images);
    advance(&mut baseline, Stage::FinalAccepted);
    let before = source.0.borrow().synced.clone();
    let manifest = source.0.borrow().manifest.clone();
    let (mut core, _) = recovered(&baseline, &mut images, true);
    let update = replacement(&mut core);
    let effects = persist_effect(&mut core, &mut baseline, update).unwrap();
    assert!(matches!(
        effects.as_slice(),
        [Effect::Send(Message {
            rpc: Rpc::Appended {
                success: true,
                matching_index: 5
            },
            ..
        })]
    ));
    let frame_bytes = source.0.borrow().synced.len() - before.len();
    eprintln!(
        "old-checkpoint rollback: frame_bytes={frame_bytes}, fault_schedules={}",
        frame_bytes + 4
    );
    for fault in (0..=frame_bytes).map(Fault::Append).chain([
        Fault::Sync,
        Fault::PublishBefore,
        Fault::PublishAfter,
    ]) {
        let io = ModelIo::default();
        {
            let mut device = io.0.borrow_mut();
            device.log = before.clone();
            device.synced = before.clone();
            device.manifest = manifest.clone();
        }
        let mut log =
            NativeLogStore::recover(io.clone(), identity(4), LogLimits::default()).unwrap();
        let mut snapshots = images.clone();
        let (mut core, _) = recovered(&log, &mut snapshots, true);
        let update = replacement(&mut core);
        io.0.borrow_mut().fault = fault;
        assert!(
            persist_effect(&mut core, &mut log, update).is_err(),
            "{fault:?}"
        );
        assert!(core.is_fenced());
        drop(core);
        drop(log);
        io.0.borrow_mut().power_loss();
        let mut log = NativeLogStore::recover(io, identity(4), LogLimits::default()).unwrap();
        assert_eq!(log.state(group(1)).unwrap().snapshot, Some(original));
        let final_retained = !matches!(fault, Fault::PublishBefore | Fault::PublishAfter);
        let (mut core, mut app) = recovered(&log, &mut snapshots, final_retained);
        elect(&mut core, &mut log, final_retained);
        retry(&mut app);
    }
}

#[test]
fn old_checkpoint_public_rollback_survives_file_reclaim_and_election_reopen() {
    let root = std::env::temp_dir().join(format!(
        "voteboat-old-election-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let snapshot_identity = SnapshotIdentity {
        store: identity(4),
        group: group(1),
    };
    let mut log = NativeLogStore::create(
        FileLogIo::create(root.join("wal")).unwrap(),
        identity(4),
        LogLimits::default(),
    )
    .unwrap();
    let mut images = NativeSnapshotStore::create(
        FileSnapshotIo::create(root.join("snapshots")).unwrap(),
        snapshot_identity,
        SnapshotLimits::default(),
    )
    .unwrap();
    let original = old_checkpoint(&mut log, &mut images);
    advance(&mut log, Stage::FinalAccepted);
    let (mut core, _) = recovered(&log, &mut images, true);
    let update = replacement(&mut core);
    persist_effect(&mut core, &mut log, update).unwrap();
    assert_eq!(core.membership().id(), cid(3));
    log.reclaim(log.limits().max_wal_bytes).unwrap();
    drop(log);
    drop(images);
    let mut log = NativeLogStore::recover(
        FileLogIo::open(root.join("wal")).unwrap(),
        identity(4),
        LogLimits::default(),
    )
    .unwrap();
    let mut images = NativeSnapshotStore::recover(
        FileSnapshotIo::open(root.join("snapshots")).unwrap(),
        snapshot_identity,
        SnapshotLimits::default(),
    )
    .unwrap();
    assert_eq!(log.state(group(1)).unwrap().snapshot, Some(original));
    let (mut core, mut app) = recovered(&log, &mut images, false);
    elect(&mut core, &mut log, false);
    retry(&mut app);
    drop(log);
    drop(images);
    std::fs::remove_dir_all(root).unwrap();
}
