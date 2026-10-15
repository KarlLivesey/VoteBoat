// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use std::collections::BTreeMap;
use voteboat::native::{log_store::*, snapshot_store::*};

fn snapshot_identity() -> SnapshotIdentity {
    SnapshotIdentity {
        store: identity(4),
        group: group(1),
    }
}

#[test]
fn file_old_learner_checkpoint_recovers_voting_and_removal_after_reclaim() {
    let root = std::env::temp_dir().join(format!(
        "voteboat-old-member-checkpoint-{}",
        std::process::id()
    ));
    for stage in STAGES {
        let path = root.join(format!("{stage:?}"));
        std::fs::create_dir_all(&path).unwrap();
        let mut log = NativeLogStore::create(
            FileLogIo::create(path.join("wal")).unwrap(),
            identity(4),
            LogLimits::default(),
        )
        .unwrap();
        let mut snapshots = NativeSnapshotStore::create(
            FileSnapshotIo::create(path.join("snapshots")).unwrap(),
            snapshot_identity(),
            SnapshotLimits::default(),
        )
        .unwrap();
        let old = old_checkpoint(&mut log, &mut snapshots);
        advance(&mut log, stage);
        let uninstalled = publish_uninstalled(&log, &mut snapshots);
        let state = log.state(group(1)).unwrap();
        log.reclaim(log.limits().max_wal_bytes).unwrap();
        drop(log);
        drop(snapshots);
        let log = NativeLogStore::recover(
            FileLogIo::open(path.join("wal")).unwrap(),
            identity(4),
            LogLimits::default(),
        )
        .unwrap();
        let mut snapshots = NativeSnapshotStore::recover(
            FileSnapshotIo::open(path.join("snapshots")).unwrap(),
            snapshot_identity(),
            SnapshotLimits::default(),
        )
        .unwrap();
        assert_eq!(log.state(group(1)).unwrap(), state);
        assert_eq!(snapshots.latest_reference().unwrap(), Some(uninstalled));
        verify(&log, &mut snapshots, old, stage);
        if !stage.removed() {
            assert!(snapshots.load_pinned(uninstalled).is_err());
        }
        drop(log);
        drop(snapshots);
    }
    std::fs::remove_dir_all(root).unwrap();
}

fn mutation(state: &GroupLog, stage: Stage) -> (LogMutation, Stage) {
    if stage == Stage::FinalAccepted {
        return (update(state, 1, 5, None), Stage::FinalCommitted);
    }
    let (operation, change, committed, next) = match stage {
        Stage::FinalCommitted => (
            5,
            ConfigurationChange::Joint {
                id: cid(5),
                next: configuration(6, &[1, 2, 5], &[3, 4]),
            },
            5,
            Stage::DemotionAccepted,
        ),
        Stage::Demoted => (
            7,
            ConfigurationChange::Learners(configuration(7, &[1, 2, 5], &[3])),
            8,
            Stage::Removed,
        ),
        _ => panic!("unsupported fault transition"),
    };
    let index = state.last_index() + 1;
    (
        update(
            state,
            1,
            committed,
            Some(Suffix {
                from: index,
                entries: vec![LogEntry {
                    index,
                    term: 1,
                    payload: EntryPayload::Configuration(Box::new(ConfigurationRecord {
                        operation: OperationId::new(operation).unwrap(),
                        expected: state.membership().unwrap().id(),
                        change,
                    })),
                }],
            }),
        ),
        next,
    )
}

fn fault_transition(stage: Stage) {
    let io = ModelIo::default();
    let mut log = NativeLogStore::create(io.clone(), identity(4), LogLimits::default()).unwrap();
    let mut snapshots = snapshots();
    let old = old_checkpoint(&mut log, &mut snapshots);
    advance(&mut log, stage);
    publish_uninstalled(&log, &mut snapshots);
    let state = log.state(group(1)).unwrap();
    let prior = io.0.borrow().synced.clone();
    let manifest = io.0.borrow().manifest.clone();
    let (change, next) = mutation(&state, stage);
    log.append_batch(vec![change.clone()]).unwrap();
    let length = io.0.borrow().log.len() - prior.len();
    eprintln!(
        "old-checkpoint {stage:?}: frame_bytes={length}, fault_schedules={}",
        length + 4
    );
    drop(log);
    let mut complete = BTreeMap::from([(group(1), state.clone())]);
    apply_batch(
        &mut complete,
        std::slice::from_ref(&change),
        LogLimits::default(),
    )
    .unwrap();
    for fault in (0..=length).map(Fault::Append).chain([
        Fault::Sync,
        Fault::PublishBefore,
        Fault::PublishAfter,
    ]) {
        let io = ModelIo::default();
        {
            let mut device = io.0.borrow_mut();
            device.log = prior.clone();
            device.synced = prior.clone();
            device.manifest = manifest.clone();
        }
        let mut log =
            NativeLogStore::recover(io.clone(), identity(4), LogLimits::default()).unwrap();
        io.0.borrow_mut().fault = fault;
        let result = log
            .append_batch(vec![change.clone()])
            .and_then(|tickets| log.barrier(&tickets));
        assert!(result.is_err(), "{stage:?}: {fault:?}");
        drop(log);
        io.0.borrow_mut().power_loss();
        let log = NativeLogStore::recover(io, identity(4), LogLimits::default()).unwrap();
        let persisted = matches!(fault, Fault::PublishBefore | Fault::PublishAfter);
        assert_eq!(
            log.state(group(1)).unwrap(),
            if persisted {
                complete[&group(1)].clone()
            } else {
                state.clone()
            }
        );
        verify(
            &log,
            &mut snapshots.clone(),
            old,
            if persisted { next } else { stage },
        );
    }
}

#[test]
fn every_torn_transition_with_old_checkpoint_recovers_whole_membership_and_retries() {
    for stage in [Stage::FinalAccepted, Stage::FinalCommitted, Stage::Demoted] {
        fault_transition(stage);
    }
}
