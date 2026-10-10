// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
mod support;
use support::*;
use voteboat::{
    application::*, identity::*, log::*, maintenance::*, raft::*, secure::PeerIdentity,
};
fn intent(op: u128, target: u64) -> LeadershipIntent {
    LeadershipIntent {
        request: LeadershipTransferRequest {
            operation: OperationId::new(op).unwrap(),
            target: PeerIdentity {
                node: node(target),
                store: identity(target.into()),
            },
            configuration: ConfigurationId::new(1).unwrap(),
        },
        source: PeerIdentity {
            node: node(1),
            store: identity(1),
        },
    }
}
fn app(capacity: usize) -> Maintenance<Counter> {
    Maintenance::new(group(1), 8001, capacity, Counter::new(1).unwrap()).unwrap()
}
fn command(index: u64, term: u64, c: LeadershipCommand) -> LogEntry {
    LogEntry {
        index,
        term,
        payload: EntryPayload::Command {
            operation: c.intent().request.operation,
            bytes: c.encode().unwrap(),
        },
    }
}
fn data(index: u64, op: u128, value: i64) -> LogEntry {
    LogEntry {
        index,
        term: 1,
        payload: EntryPayload::Command {
            operation: OperationId::new(op).unwrap(),
            bytes: Maintenance::<Counter>::data(&value.to_le_bytes(), 100).unwrap(),
        },
    }
}
fn recorded(app: &mut Maintenance<Counter>, entry: LogEntry) -> LeadershipRecord {
    match app.apply_batch(&[entry]).unwrap().remove(0) {
        MaintenanceReceipt::Administration {
            outcome: MaintenanceOutcome::Recorded(r),
            ..
        } => r,
        other => panic!("{other:?}"),
    }
}
#[test]
fn data_and_administration_retries_remain_independent_and_survive_checkpoint() {
    let mut a = app(2);
    a.apply_batch(&[data(1, 10, 7)]).unwrap();
    let original = recorded(
        &mut a,
        command(2, 1, LeadershipCommand::Begin(intent(10, 2))),
    );
    assert_eq!(a.inner().read_applied(2), Ok(7));
    assert_eq!(
        recorded(
            &mut a,
            command(3, 1, LeadershipCommand::Begin(intent(10, 2)))
        ),
        original
    );
    let complete = LeadershipCommand::Complete {
        intent: intent(10, 2),
        index: 2,
        term: 2,
    };
    let result = recorded(&mut a, command(4, 2, complete));
    assert_eq!(
        result.phase,
        LeadershipPhase::Completed { index: 4, term: 2 }
    );
    let bytes = a.checkpoint(10000).unwrap();
    let mut recovered = app(2);
    recovered.restore_checkpoint(8001, 4, &bytes).unwrap();
    assert_eq!(
        recovered.record(OperationId::new(10).unwrap()),
        Some(result)
    );
    assert_eq!(recorded(&mut recovered, command(5, 3, complete)), result);
    let receipt = recovered.apply_batch(&[data(6, 10, 7)]).unwrap().remove(0);
    assert!(matches!(
        receipt,
        MaintenanceReceipt::Data(CounterReceipt {
            duplicate: true,
            ..
        })
    ));
    assert_eq!(recovered.inner().read_applied(6), Ok(7));
}
#[test]
fn conflicting_intent_and_second_pending_request_cannot_replace_original() {
    let mut a = app(2);
    let original = recorded(
        &mut a,
        command(1, 1, LeadershipCommand::Begin(intent(10, 2))),
    );
    for (index, i, expected) in [
        (2, intent(10, 3), MaintenanceOutcome::Conflict),
        (3, intent(11, 3), MaintenanceOutcome::Busy),
    ] {
        assert!(
            matches!(a.apply_batch(&[command(index, 1, LeadershipCommand::Begin(i))]).unwrap().remove(0), MaintenanceReceipt::Administration { outcome, .. } if outcome == expected)
        );
        assert_eq!(a.pending(), Some(original));
    }
    let cancelled = recorded(
        &mut a,
        command(
            4,
            1,
            LeadershipCommand::Cancel {
                intent: intent(10, 2),
                index: 1,
            },
        ),
    );
    assert!(matches!(
        cancelled.phase,
        LeadershipPhase::Cancelled { index: 4, .. }
    ));
    assert_eq!(
        recorded(
            &mut a,
            command(5, 2, LeadershipCommand::Begin(intent(10, 2)))
        ),
        cancelled
    );
    recorded(
        &mut a,
        command(6, 2, LeadershipCommand::Begin(intent(11, 3))),
    );
    assert_eq!(a.pending().unwrap().intent.request.operation.get(), 11);
}
#[test]
fn capacity_reserves_terminal_updates_and_inner_exhaustion_cannot_block_them() {
    let mut a = app(1);
    a.apply_batch(&[data(1, 1, 7)]).unwrap();
    let begin = LeadershipCommand::Begin(intent(10, 2));
    assert!(a
        .validate_proposal(
            begin.intent().request.operation,
            &begin.encode().unwrap(),
            std::iter::empty()
        )
        .is_ok());
    recorded(&mut a, command(2, 1, begin));
    let next = LeadershipCommand::Begin(intent(11, 2));
    assert_eq!(
        a.validate_proposal(
            next.intent().request.operation,
            &next.encode().unwrap(),
            std::iter::empty()
        ),
        Err(ApplicationError::DedupCapacity)
    );
    let cancel = LeadershipCommand::Cancel {
        intent: intent(10, 2),
        index: 2,
    };
    assert!(a
        .validate_proposal(
            cancel.intent().request.operation,
            &cancel.encode().unwrap(),
            std::iter::empty()
        )
        .is_ok());
    recorded(&mut a, command(3, 1, cancel));
    assert!(a
        .validate_proposal(
            next.intent().request.operation,
            &next.encode().unwrap(),
            std::iter::empty()
        )
        .is_err());
    let fresh = app(1);
    let pending = begin.encode().unwrap();
    assert!(fresh
        .validate_proposal(
            next.intent().request.operation,
            &next.encode().unwrap(),
            [(begin.intent().request.operation, pending.as_slice())].into_iter()
        )
        .is_err());
}
#[test]
fn malformed_batches_and_checkpoint_refusals_leave_the_original_state_unchanged() {
    let mut a = app(2);
    recorded(
        &mut a,
        command(1, 1, LeadershipCommand::Begin(intent(10, 2))),
    );
    let original = a.checkpoint(10000).unwrap();
    let invalid = data(4, 1, 9);
    assert!(a.apply_batch(&[data(2, 1, 7), invalid]).is_err());
    assert_eq!(a.checkpoint(10000).unwrap(), original);
    for end in 0..original.len() {
        assert!(a.restore_checkpoint(8001, 1, &original[..end]).is_err());
        assert_eq!(a.checkpoint(10000).unwrap(), original);
    }
    let mut trailing = original.clone();
    trailing.push(0);
    assert!(a.restore_checkpoint(8001, 1, &trailing).is_err());
    assert!(a.restore_checkpoint(8002, 1, &original).is_err());
    assert!(app(1).restore_checkpoint(8001, 1, &original).is_err());
    assert!(
        Maintenance::new(group(2), 8001, 2, Counter::new(1).unwrap())
            .unwrap()
            .restore_checkpoint(8001, 1, &original)
            .is_err()
    );
    assert_eq!(a.checkpoint(10000).unwrap(), original);
}
#[test]
fn command_codec_rejects_truncation_and_operation_mismatch_and_old_profiles() {
    let c = command(1, 1, LeadershipCommand::Begin(intent(u128::MAX, 2)));
    let EntryPayload::Command { operation, bytes } = c.payload.clone() else {
        unreachable!()
    };
    for end in 0..bytes.len() {
        let e = LogEntry {
            payload: EntryPayload::Command {
                operation,
                bytes: bytes[..end].to_vec(),
            },
            ..c.clone()
        };
        assert!(app(1).apply_batch(&[e]).is_err());
    }
    let e = LogEntry {
        payload: EntryPayload::Command {
            operation: OperationId::new(1).unwrap(),
            bytes,
        },
        ..c.clone()
    };
    assert!(app(1).apply_batch(&[e]).is_err());
    assert!(Counter::new(1).unwrap().apply_batch(&[c]).is_err());
    assert!(Maintenance::new(group(1), 1, 1, Counter::new(1).unwrap()).is_err());
    let mut old = Counter::new(1).unwrap();
    old.apply_batch(&[LogEntry {
        index: 1,
        term: 1,
        payload: EntryPayload::Noop,
    }])
    .unwrap();
    assert!(Maintenance::new(group(1), 8001, 1, old).is_err());
}

#[cfg(feature = "native")]
#[test]
fn every_torn_native_record_recovers_old_or_complete_administrative_state() {
    use std::{cell::RefCell, rc::Rc};
    use voteboat::native::log_store::NativeLogStore;
    for completing in [false, true] {
        let io = ModelIo::default();
        let mut log =
            NativeLogStore::create(io.clone(), identity(1), LogLimits::default()).unwrap();
        append(&mut log, vec![LogMutation::Create(bootstrap(1, 3))]);
        let seed = if completing {
            vec![
                data(1, 1, 7),
                command(2, 1, LeadershipCommand::Begin(intent(10, 2))),
            ]
        } else {
            vec![data(1, 1, 7)]
        };
        let index = seed.len() as u64;
        let change = update(
            &log.state(group(1)).unwrap(),
            1,
            index,
            Some(Suffix {
                from: 1,
                entries: seed,
            }),
        );
        append(&mut log, vec![change]);
        let before = io.0.borrow().synced.len();
        let old_manifest = io.0.borrow().manifest.clone();
        let entry = if completing {
            command(
                3,
                2,
                LeadershipCommand::Complete {
                    intent: intent(10, 2),
                    index: 2,
                    term: 2,
                },
            )
        } else {
            command(2, 1, LeadershipCommand::Begin(intent(10, 2)))
        };
        let change = update(
            &log.state(group(1)).unwrap(),
            entry.term,
            entry.index,
            Some(Suffix {
                from: entry.index,
                entries: vec![entry],
            }),
        );
        append(&mut log, vec![change]);
        let full = io.0.borrow().synced.clone();
        let committed_manifest = io.0.borrow().manifest.clone();
        drop(log);
        let missing = full[..before].to_vec();
        let corrupt = ModelIo(Rc::new(RefCell::new(Device {
            log: missing.clone(),
            synced: missing,
            manifest: committed_manifest,
            fault: Fault::None,
        })));
        assert!(NativeLogStore::recover(corrupt, identity(1), LogLimits::default()).is_err());
        for cut in before..=full.len() {
            let bytes = full[..cut].to_vec();
            let torn = ModelIo(Rc::new(RefCell::new(Device {
                log: bytes.clone(),
                synced: bytes,
                manifest: old_manifest.clone(),
                fault: Fault::None,
            })));
            let recovered =
                NativeLogStore::recover(torn, identity(1), LogLimits::default()).unwrap();
            let state = recovered.state(group(1)).unwrap();
            let mut a = app(2);
            a.apply_batch(&state.entries[..state.commit_index as usize])
                .unwrap();
            assert_eq!(a.inner().read_applied(a.applied_index()), Ok(7));
            let record = a.record(OperationId::new(10).unwrap());
            match (completing, cut == full.len()) {
                (false, false) => assert!(record.is_none()),
                (false, true) | (true, false) => {
                    assert_eq!(record.unwrap().phase, LeadershipPhase::Pending)
                }
                (true, true) => assert!(matches!(
                    record.unwrap().phase,
                    LeadershipPhase::Completed { index: 3, term: 2 }
                )),
            }
        }
    }
}
