// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

#[cfg(feature = "native")]
#[test]
fn accepted_begin_rollback_replays_only_the_surviving_exact_binding() {
    use voteboat::native::log_store::NativeLogStore;
    let io = ModelIo::default();
    let mut log = NativeLogStore::create(io.clone(), identity(2), LogLimits::default()).unwrap();
    append(&mut log, vec![LogMutation::Create(bootstrap(1, 3))]);
    let i = intent(10, 2);
    let seed = update(
        &log.state(group(1)).unwrap(),
        1,
        1,
        Some(Suffix {
            from: 1,
            entries: vec![data(1, 1, 7), command(2, 1, LeadershipCommand::Begin(i))],
        }),
    );
    append(&mut log, vec![seed]);
    let accepted = log.state(group(1)).unwrap();
    assert_eq!(accepted.last_index(), 2);
    assert_eq!(accepted.commit_index, 1);
    let replacement = update(
        &accepted,
        2,
        2,
        Some(Suffix {
            from: 2,
            entries: vec![LogEntry {
                index: 2,
                term: 2,
                payload: EntryPayload::Noop,
            }],
        }),
    );
    append(&mut log, vec![replacement]);
    drop(log);
    let mut log = NativeLogStore::recover(io.clone(), identity(2), LogLimits::default()).unwrap();
    let surviving = log.state(group(1)).unwrap();
    let mut a = app(2);
    a.apply_batch(&surviving.entries[..surviving.commit_index as usize])
        .unwrap();
    assert!(a.record(i.request.operation).is_none());
    assert_eq!(a.inner().read_applied(a.applied_index()), Ok(7));
    let entries = vec![
        command(3, 2, LeadershipCommand::Begin(i)),
        command(
            4,
            2,
            LeadershipCommand::Complete {
                intent: i,
                index: 3,
                term: 2,
            },
        ),
    ];
    let finish = update(
        &surviving,
        2,
        4,
        Some(Suffix {
            from: 3,
            entries: entries.clone(),
        }),
    );
    append(&mut log, vec![finish]);
    drop(log);
    let recovered = NativeLogStore::recover(io, identity(2), LogLimits::default())
        .unwrap()
        .state(group(1))
        .unwrap();
    let mut restored = app(2);
    restored
        .apply_batch(&recovered.entries[..recovered.commit_index as usize])
        .unwrap();
    let record = restored.record(i.request.operation).unwrap();
    assert_eq!(record.intent, i);
    assert_eq!(record.index, 3);
    assert_eq!(
        record.phase,
        LeadershipPhase::Completed { index: 4, term: 2 }
    );
    assert_eq!(restored.inner().read_applied(4), Ok(7));
}

#[test]
fn pending_and_applied_bindings_refuse_changed_identity_before_admission() {
    let mut a = app(2);
    let i = intent(10, 2);
    let begin = LeadershipCommand::Begin(i).encode().unwrap();
    let mut changed = i;
    changed.source = PeerIdentity {
        node: node(3),
        store: identity(3),
    };
    let conflicting = LeadershipCommand::Begin(changed).encode().unwrap();
    let before = a.checkpoint(10000).unwrap();
    assert_eq!(
        a.validate_proposal(
            i.request.operation,
            &conflicting,
            [(i.request.operation, begin.as_slice())].into_iter()
        ),
        Err(ApplicationError::OperationConflict)
    );
    assert!(a
        .validate_proposal(
            i.request.operation,
            &begin,
            [(i.request.operation, begin.as_slice())].into_iter()
        )
        .is_ok());
    // Administrative and data IDs occupy separate framed namespaces.
    assert!(a
        .validate_proposal(
            i.request.operation,
            &Maintenance::<Counter>::data(&7i64.to_le_bytes(), 100).unwrap(),
            [(i.request.operation, begin.as_slice())].into_iter()
        )
        .is_ok());
    assert_eq!(a.checkpoint(10000).unwrap(), before);
    recorded(&mut a, command(1, 2, LeadershipCommand::Begin(i)));
    let before = a.checkpoint(10000).unwrap();
    assert_eq!(
        a.validate_proposal(i.request.operation, &conflicting, std::iter::empty()),
        Err(ApplicationError::OperationConflict)
    );
    assert_eq!(a.checkpoint(10000).unwrap(), before);
}

#[test]
fn same_term_completion_recovers_and_older_completion_cannot_escape() {
    let mut a = app(2);
    let i = intent(10, 2);
    recorded(&mut a, command(1, 2, LeadershipCommand::Begin(i)));
    let before = a.checkpoint(10000).unwrap();
    let stale = LeadershipCommand::Complete {
        intent: i,
        index: 1,
        term: 1,
    };
    assert!(a.apply_batch(&[command(2, 1, stale)]).is_err());
    assert_eq!(a.checkpoint(10000).unwrap(), before);
    let complete = LeadershipCommand::Complete {
        intent: i,
        index: 1,
        term: 2,
    };
    let done = recorded(&mut a, command(2, 2, complete));
    assert_eq!(done.phase, LeadershipPhase::Completed { index: 2, term: 2 });
    let bytes = a.checkpoint(10000).unwrap();
    let mut restored = app(2);
    restored.restore_checkpoint(8001, 2, &bytes).unwrap();
    assert_eq!(restored.record(i.request.operation), Some(done));
    assert_eq!(
        recorded(&mut restored, command(3, 3, LeadershipCommand::Begin(i))),
        done
    );
}
