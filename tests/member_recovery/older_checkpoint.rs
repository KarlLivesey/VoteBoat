// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Stage {
    Learner,
    JointAccepted,
    JointCommitted,
    FinalAccepted,
    FinalCommitted,
    JointRolledBack,
    FinalRolledBack,
    DemotionAccepted,
    Demoted,
    RemovalAccepted,
    Removed,
}
impl Stage {
    fn removed(self) -> bool {
        matches!(self, Self::RemovalAccepted | Self::Removed)
    }
}
const STAGES: [Stage; 11] = [
    Stage::Learner,
    Stage::JointAccepted,
    Stage::JointCommitted,
    Stage::FinalAccepted,
    Stage::FinalCommitted,
    Stage::JointRolledBack,
    Stage::FinalRolledBack,
    Stage::DemotionAccepted,
    Stage::Demoted,
    Stage::RemovalAccepted,
    Stage::Removed,
];

fn command(log: &mut impl LogStore, operation: u128, delta: i64) {
    let state = log.state(group(1)).unwrap();
    let index = state.last_index() + 1;
    append(
        log,
        vec![update(
            &state,
            1,
            index,
            Some(Suffix {
                from: index,
                entries: vec![LogEntry {
                    index,
                    term: 1,
                    payload: EntryPayload::Command {
                        operation: OperationId::new(operation).unwrap(),
                        bytes: delta.to_le_bytes().to_vec(),
                    },
                }],
            }),
        )],
    );
}

fn old_checkpoint(log: &mut impl LogStore, snapshots: &mut impl SnapshotRetention) -> SnapshotRef {
    append(log, vec![LogMutation::Create(bootstrap(1, 3))]);
    record(
        log,
        2,
        ConfigurationChange::Learners(configuration(2, &[1, 2, 3], &[4, 5])),
        1,
    );
    command(log, 900, 7);
    let mut core = recover(log);
    assert!(!core.local_voter());
    let mut app = Counter::new(16).unwrap();
    app.apply_batch(core.replay_committed()).unwrap();
    let receipt = checkpoint_application(&core, &app, snapshots).unwrap();
    compact_replica(&mut core, log, snapshots, &app, receipt.reference()).unwrap();
    command(log, 901, 3);
    receipt.reference()
}

fn rollback(log: &mut impl LogStore, from: u64) {
    let state = log.state(group(1)).unwrap();
    append(
        log,
        vec![update(
            &state,
            2,
            state.commit_index,
            Some(Suffix {
                from,
                entries: vec![],
            }),
        )],
    );
}

fn advance(log: &mut impl LogStore, stage: Stage) {
    if stage == Stage::Learner {
        return;
    }
    record(
        log,
        3,
        ConfigurationChange::Joint {
            id: cid(3),
            next: configuration(4, &[1, 2, 4], &[3, 5]),
        },
        3,
    );
    if stage == Stage::JointAccepted {
        return;
    }
    if stage == Stage::JointRolledBack {
        rollback(log, 4);
        return;
    }
    commit(log, 4);
    if stage == Stage::JointCommitted {
        return;
    }
    record(log, 3, ConfigurationChange::Final { id: cid(4) }, 4);
    if stage == Stage::FinalAccepted {
        return;
    }
    if stage == Stage::FinalRolledBack {
        rollback(log, 5);
        return;
    }
    commit(log, 5);
    if stage == Stage::FinalCommitted {
        return;
    }
    record(
        log,
        5,
        ConfigurationChange::Joint {
            id: cid(5),
            next: configuration(6, &[1, 2, 5], &[3, 4]),
        },
        5,
    );
    if stage == Stage::DemotionAccepted {
        return;
    }
    commit(log, 6);
    record(log, 5, ConfigurationChange::Final { id: cid(6) }, 7);
    if stage == Stage::Demoted {
        return;
    }
    record(
        log,
        7,
        ConfigurationChange::Learners(configuration(7, &[1, 2, 5], &[3])),
        if stage == Stage::RemovalAccepted {
            7
        } else {
            8
        },
    );
}

fn publish_uninstalled(log: &impl LogStore, snapshots: &mut impl SnapshotRetention) -> SnapshotRef {
    let state = log.state(group(1)).unwrap();
    let mut app = Counter::new(16).unwrap();
    let old = snapshots.load_pinned(state.snapshot.unwrap()).unwrap();
    app.restore_checkpoint(1, old.metadata.index, &old.application)
        .unwrap();
    app.apply_batch(&state.entries[..(state.commit_index - state.base_index()) as usize])
        .unwrap();
    let bytes = app.checkpoint(16384).unwrap();
    let ticket = snapshots
        .begin(
            SnapshotMetadata {
                bootstrap: state.bootstrap.clone(),
                membership: state.checkpoint_membership(state.commit_index).unwrap(),
                index: state.commit_index,
                term: state.term_at(state.commit_index).unwrap(),
                application_schema: 1,
            },
            bytes.len(),
        )
        .unwrap();
    for (i, chunk) in bytes.chunks(snapshots.limits().max_chunk_bytes).enumerate() {
        snapshots
            .write_chunk(ticket, i * snapshots.limits().max_chunk_bytes, chunk)
            .unwrap();
    }
    let sealed = snapshots.seal(ticket).unwrap();
    let reference = snapshots.publish(sealed).unwrap().reference();
    snapshots.pin_for_log(reference).unwrap();
    reference
}

fn expected(stage: Stage) -> (u64, bool, bool) {
    match stage {
        Stage::Learner | Stage::JointRolledBack => (2, false, false),
        Stage::JointAccepted | Stage::JointCommitted | Stage::FinalRolledBack => (3, true, true),
        Stage::FinalAccepted | Stage::FinalCommitted => (4, true, false),
        Stage::DemotionAccepted => (5, true, true),
        Stage::Demoted => (6, false, false),
        Stage::RemovalAccepted | Stage::Removed => (7, false, false),
    }
}

fn verify(
    log: &impl LogStore,
    snapshots: &mut impl SnapshotRetention,
    old: SnapshotRef,
    stage: Stage,
) {
    let state = log.state(group(1)).unwrap();
    assert_eq!(state.snapshot, Some(old));
    assert_eq!(state.base_index(), 2);
    verify_status(&state, stage);
    let mut app = Counter::new(16).unwrap();
    let recovered = recover_member_replica(node(4), group(1), log, snapshots, &mut app);
    if stage.removed() {
        assert!(
            recovered.is_err(),
            "old checkpoint cannot reauthorize removed member"
        );
        assert_eq!(app.applied_index(), 0);
        return;
    }
    let (mut core, replay) = recovered.unwrap();
    let (configuration, voter, joint) = expected(stage);
    assert_eq!(core.membership().id(), cid(configuration));
    assert_eq!(core.membership().joint().is_some(), joint);
    assert_eq!(core.local_voter(), voter);
    assert_eq!(replay.checkpoint_index, 2);
    assert_eq!(app.read_applied(state.commit_index), Ok(10));
    assert_eq!(snapshots.latest_reference().unwrap(), Some(old));
    let campaign = core.step(Event::Campaign);
    if voter {
        assert!(matches!(campaign.unwrap().as_slice(), [Effect::Persist(_)]));
    } else {
        assert_eq!(campaign, Err(RaftError::NotVoter));
    }
    check_predicate(&core, stage);
    retry(&mut app);
}

fn verify_status(state: &GroupLog, stage: Stage) {
    let action = |id| {
        state
            .configuration_status(OperationId::new(id).unwrap())
            .unwrap()
            .resume_action()
    };
    assert_eq!(action(2), ConfigurationResumeAction::Completed);
    match stage {
        Stage::Learner | Stage::JointRolledBack => {
            assert_eq!(action(3), ConfigurationResumeAction::NotFoundLocally)
        }
        Stage::JointAccepted | Stage::FinalAccepted => {
            assert_eq!(action(3), ConfigurationResumeAction::WaitForCommit)
        }
        Stage::JointCommitted | Stage::FinalRolledBack => {
            assert!(matches!(action(3), ConfigurationResumeAction::Finalize(_)))
        }
        _ => assert_eq!(action(3), ConfigurationResumeAction::Completed),
    }
    if stage == Stage::DemotionAccepted {
        assert_eq!(action(5), ConfigurationResumeAction::WaitForCommit);
    }
    if matches!(
        stage,
        Stage::Demoted | Stage::RemovalAccepted | Stage::Removed
    ) {
        assert_eq!(action(5), ConfigurationResumeAction::Completed);
    }
    if stage.removed() {
        assert_eq!(
            action(7),
            if stage == Stage::Removed {
                ConfigurationResumeAction::Completed
            } else {
                ConfigurationResumeAction::WaitForCommit
            }
        );
    }
}

fn check_predicate(core: &Raft, stage: Stage) {
    let set = |nodes: &[u64]| nodes.iter().map(|n| node(*n)).collect();
    assert!(core.membership().is_satisfied(&set(&[1, 2])));
    assert_eq!(
        core.membership().is_satisfied(&set(&[1, 5])),
        stage == Stage::Demoted
    );
    let one_old = core.membership().is_satisfied(&set(&[1, 3]));
    let promoted = core.membership().is_satisfied(&set(&[1, 4]));
    match stage {
        Stage::Learner | Stage::JointRolledBack => {
            assert!(one_old);
            assert!(!promoted);
        }
        Stage::FinalAccepted | Stage::FinalCommitted => {
            assert!(!one_old);
            assert!(promoted);
        }
        _ => {
            assert!(!one_old);
            assert!(!promoted);
        }
    }
}

fn retry(app: &mut Counter) {
    for (operation, delta, value) in [(900, 7i64, 7), (901, 3, 10)] {
        let receipts = app
            .apply_batch(&[LogEntry {
                index: app.applied_index() + 1,
                term: 3,
                payload: EntryPayload::Command {
                    operation: OperationId::new(operation).unwrap(),
                    bytes: delta.to_le_bytes().to_vec(),
                },
            }])
            .unwrap();
        assert!(receipts[0].duplicate);
        assert_eq!(receipts[0].outcome, CounterOutcome::Value(value));
    }
    assert_eq!(app.read_applied(app.applied_index()), Ok(10));
}

#[test]
fn host_old_learner_checkpoint_recovers_each_membership_transition() {
    for stage in STAGES {
        let mut log = HostLogStore::new(4);
        let mut snapshots = snapshots();
        let old = old_checkpoint(&mut log, &mut snapshots);
        advance(&mut log, stage);
        let uninstalled = publish_uninstalled(&log, &mut snapshots);
        verify(&log, &mut snapshots, old, stage);
        if !stage.removed() {
            assert!(snapshots.load_pinned(uninstalled).is_err());
        }
    }
}

#[test]
fn mismatched_old_pin_never_falls_back_to_newer_publication_or_changes_application() {
    for stage in [Stage::FinalAccepted, Stage::Demoted] {
        let mut log = HostLogStore::new(4);
        let mut snapshots = snapshots();
        let old = old_checkpoint(&mut log, &mut snapshots);
        advance(&mut log, stage);
        let latest = publish_uninstalled(&log, &mut snapshots);
        let state = log.state(group(1)).unwrap();
        for fault in 0..5 {
            let mut bad = snapshots.clone();
            let snapshot = &mut bad.pins.get_mut(&old.generation).unwrap().1;
            match fault {
                0 => snapshot.metadata.membership = None,
                1 => {
                    snapshot.metadata.membership =
                        state.checkpoint_membership(state.commit_index).unwrap()
                }
                2 => snapshot.metadata.index += 1,
                3 => snapshot.application[0] ^= 1,
                _ => {
                    bad.pins.remove(&old.generation);
                }
            }
            let mut app = Counter::new(16).unwrap();
            assert!(
                recover_member_replica(node(4), group(1), &log, &mut bad, &mut app).is_err(),
                "{stage:?} fault={fault}"
            );
            assert_eq!(app.applied_index(), 0);
            assert_eq!(bad.latest_reference().unwrap(), Some(latest));
            assert_eq!(log.state(group(1)).unwrap(), state);
        }
        verify(&log, &mut snapshots, old, stage);
    }
}

#[cfg(feature = "native")]
#[path = "older_checkpoint/native.rs"]
mod native;
