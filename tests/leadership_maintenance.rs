// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
mod support;
use support::*;
use voteboat::{
    application::*, identity::*, log::*, maintenance::*, raft::*, secure::PeerIdentity,
};
#[path = "leadership_maintenance/binding.rs"]
mod binding;
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
    Maintenance::new(group(1), 1, capacity, Counter::new(1).unwrap()).unwrap()
}
/// The schema of `app`: format 1 above the inner Counter's schema.
fn schema() -> u64 {
    maintenance_schema(1, Counter::new(1).unwrap().schema_version()).unwrap()
}
fn command(index: u64, term: u64, c: LeadershipCommand) -> LogEntry {
    LogEntry {
        index,
        term,
        payload: EntryPayload::Command {
            operation: c.intent().request.operation,
            bytes: c.encode().unwrap().into(),
        },
    }
}
fn data(index: u64, op: u128, value: i64) -> LogEntry {
    LogEntry {
        index,
        term: 1,
        payload: EntryPayload::Command {
            operation: Counter::operation(1, op as u64).unwrap(),
            bytes: Maintenance::<Counter>::data(&Counter::command(0, value), 100)
                .unwrap()
                .into(),
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
    // The data command (sequence 1 of the counter's built-in session) and the
    // administrative intent share one operation ID in separate namespaces.
    const SHARED: u128 = 1 << 64 | 1;
    let mut a = app(2);
    a.apply_batch(&[data(1, 1, 7)]).unwrap();
    let original = recorded(
        &mut a,
        command(2, 1, LeadershipCommand::Begin(intent(SHARED, 2))),
    );
    assert_eq!(a.inner().read_applied(2), Ok(7));
    assert_eq!(
        recorded(
            &mut a,
            command(3, 1, LeadershipCommand::Begin(intent(SHARED, 2)))
        ),
        original
    );
    let complete = LeadershipCommand::Complete {
        intent: intent(SHARED, 2),
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
    recovered.restore_checkpoint(schema(), 4, &bytes).unwrap();
    assert_eq!(
        recovered.record(OperationId::new(SHARED).unwrap()),
        Some(result)
    );
    assert_eq!(recorded(&mut recovered, command(5, 3, complete)), result);
    let receipt = recovered.apply_batch(&[data(6, 1, 7)]).unwrap().remove(0);
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
        assert!(a.restore_checkpoint(schema(), 1, &original[..end]).is_err());
        assert_eq!(a.checkpoint(10000).unwrap(), original);
    }
    let mut trailing = original.clone();
    trailing.push(0);
    assert!(a.restore_checkpoint(schema(), 1, &trailing).is_err());
    assert!(a.restore_checkpoint(8002, 1, &original).is_err());
    assert!(app(1).restore_checkpoint(schema(), 1, &original).is_err());
    assert!(Maintenance::new(group(2), 1, 2, Counter::new(1).unwrap())
        .unwrap()
        .restore_checkpoint(schema(), 1, &original)
        .is_err());
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
                bytes: bytes[..end].to_vec().into(),
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
    // The wrapper's schema never equals the inner one for any format: the
    // format sits above every bit of the inner schema (5 for
    // `Counter::new(1)`).
    let inner = Counter::new(1).unwrap().schema_version();
    for format in 1..=MAX_MAINTENANCE_FORMAT {
        let numbered = Maintenance::new(group(1), format, 1, Counter::new(1).unwrap()).unwrap();
        assert_eq!(numbered.schema_version(), format << 60 | inner);
        assert_ne!(numbered.schema_version(), inner);
    }
    let mut old = Counter::new(1).unwrap();
    old.apply_batch(&[LogEntry {
        index: 1,
        term: 1,
        payload: EntryPayload::Noop,
    }])
    .unwrap();
    assert!(Maintenance::new(group(1), 1, 1, old).is_err());
}

#[cfg(feature = "native")]
#[test]
fn every_torn_native_record_recovers_old_or_complete_administrative_state() {
    use std::{cell::RefCell, rc::Rc};
    use voteboat::native::log_store::NativeLogStore;
    for (completing, completion_term) in [(false, 2), (true, 2), (true, 1)] {
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
                completion_term,
                LeadershipCommand::Complete {
                    intent: intent(10, 2),
                    index: 2,
                    term: completion_term,
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
                    LeadershipPhase::Completed { index: 3, term } if term == completion_term
                )),
            }
        }
    }
}

#[test]
fn maintenance_around_an_undeclared_application_is_refused_at_readiness() {
    // A host application that reports an envelope but declares no retention
    // rule. Maintenance keeps a lifetime table of its own, but it must not
    // hide the inner application's missing declaration.
    #[derive(Clone)]
    struct Legacy;
    impl StateMachine for Legacy {
        type Receipt = ();
        fn deployment_requirements(&self) -> Option<ReadinessRequirements> {
            Some(Counter::new(1).unwrap().readiness_requirements())
        }
        fn applied_index(&self) -> u64 {
            0
        }
        fn apply_batch(&mut self, _: &[LogEntry]) -> Result<Vec<()>, ApplicationError> {
            Ok(vec![])
        }
    }
    impl CheckpointStateMachine for Legacy {
        fn schema_version(&self) -> u64 {
            7
        }
        fn checkpoint(&self, _: usize) -> Result<Vec<u8>, ApplicationError> {
            Ok(vec![])
        }
        fn restore_checkpoint(&mut self, _: u64, _: u64, _: &[u8]) -> Result<(), ApplicationError> {
            Ok(())
        }
    }
    let wrapped = Maintenance::new(group(1), 3, 64, Legacy).unwrap();
    let declared = wrapped.deployment_requirements().unwrap();
    assert_eq!(wrapped.retention(), Retention::Undeclared);
    assert_eq!(
        wrapped.validate_deployment_requirements(declared),
        Err(ApplicationError::RetentionUndeclared)
    );
    // A declared inner application keeps the wrapper's own rule.
    let counter = Maintenance::new(group(1), 3, 64, Counter::new(1).unwrap()).unwrap();
    assert_eq!(counter.retention(), Retention::LifetimeBounded);
    let declared = counter.deployment_requirements().unwrap();
    assert_eq!(counter.validate_deployment_requirements(declared), Ok(()));
    // The rule a wrapper reports is the weaker of its own and its inner one.
    let sessions = Retention::Sessions(voteboat::retention::SessionLimits::default());
    for (own, inner, reported) in [
        (
            Retention::LifetimeBounded,
            Retention::Undeclared,
            Retention::Undeclared,
        ),
        (
            Retention::Stateless,
            Retention::Undeclared,
            Retention::Undeclared,
        ),
        (
            Retention::LifetimeBounded,
            sessions,
            Retention::LifetimeBounded,
        ),
        (
            Retention::Stateless,
            Retention::LifetimeBounded,
            Retention::LifetimeBounded,
        ),
        (Retention::Fenced, sessions, sessions),
    ] {
        assert_eq!(Retention::wrapping(own, inner), reported);
    }
}

#[test]
fn an_old_schema_two_maintenance_checkpoint_is_refused_by_schema_before_replay() {
    // Before DR1 the leadership-maintenance profile was schema 2, and DR1's
    // first Counter was schema 2 as well. Counter schemas now carry format 5
    // in the low byte and maintenance schemas a format in the top four bits,
    // so neither equals 1, 2 or 3, and a checkpoint of another profile format
    // is refused by the exact schema check that snapshot install performs
    // before restore and replay.
    let old = Maintenance::new(group(1), 2, 64, Counter::new(4).unwrap()).unwrap();
    let image = old.checkpoint(usize::MAX).unwrap();
    for window in [1, 4, 32] {
        assert_eq!(
            Counter::new(window).unwrap().schema_version() & 0xff,
            u64::from(COUNTER_FORMAT)
        );
    }
    let mut plain = Counter::new(4).unwrap();
    assert_eq!(
        plain.restore_checkpoint(old.schema_version(), 0, &image),
        Err(ApplicationError::UnsupportedSchema)
    );
    // The current profile (format 3) refuses it too; snapshot install
    // compares the schema exactly before calling restore at all.
    let mut profile = Maintenance::new(group(1), 3, 64, Counter::new(4).unwrap()).unwrap();
    for schema in [old.schema_version(), profile.schema_version()] {
        assert!(schema >> 60 != 0 && ![1, 2, 3].contains(&schema));
    }
    assert_eq!(
        profile.restore_checkpoint(old.schema_version(), 0, &image),
        Err(ApplicationError::InvalidCheckpoint)
    );
    assert_ne!(profile.schema_version(), old.schema_version());
}

#[test]
fn wrapped_counters_with_different_session_limits_refuse_each_other_before_any_entry() {
    use voteboat::retention::SessionLimits;
    // Reviewer reproducer (round 2, P2): the same profile format and
    // maintenance capacity around Counters with 2 and 3 sessions. With one
    // schema for both, both accepted the larger declaration, and after
    // registrations 77, 88, 99 the command (2, 1, 0, +7) was `Expired`
    // (value 0) on one replica and executed (value 7) on the other.
    let wrap = |sessions| {
        let limits = SessionLimits::new(sessions, 4, 4).unwrap();
        Maintenance::new(group(1), 3, 64, Counter::with_limits(limits).unwrap()).unwrap()
    };
    let (small, large) = (wrap(2), wrap(3));
    assert_ne!(small.schema_version(), large.schema_version());
    for (replica, leader) in [(&small, &large), (&large, &small)] {
        let declared = leader.deployment_requirements().unwrap();
        assert_eq!(leader.validate_deployment_requirements(declared), Ok(()));
        assert_eq!(
            replica.validate_deployment_requirements(declared),
            Err(ApplicationError::UnsupportedSchema)
        );
        assert_eq!(replica.applied_index(), 0);
    }
    // The schema is the format above the inner identity, exactly.
    let inner = Counter::schema(SessionLimits::new(2, 4, 4).unwrap());
    assert_eq!(
        small.schema_version(),
        maintenance_schema(3, inner).unwrap()
    );
    assert_eq!(small.schema_version(), 3 << 60 | inner);
    // The service profile: format 3 around the service's Counter limits.
    let service = Maintenance::new(
        group(1),
        3,
        64,
        Counter::with_limits(SessionLimits::default()).unwrap(),
    )
    .unwrap();
    assert_eq!(
        service.schema_version(),
        maintenance_schema(3, Counter::schema(SessionLimits::default())).unwrap()
    );
    // Formats outside 1..=15 and inner schemas using the top four bits are
    // refused, so the map stays injective.
    for format in [0, MAX_MAINTENANCE_FORMAT + 1, 8001] {
        assert!(Maintenance::new(group(1), format, 64, Counter::new(1).unwrap()).is_err());
    }
    assert_eq!(maintenance_schema(1, 1 << 60), None);
    assert!(Maintenance::new(group(1), 1, 64, service).is_err());
}
