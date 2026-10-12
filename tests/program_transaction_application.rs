// SPDX-License-Identifier: RPL-1.5
//! Downstream public application contracts; no private phase certificate intake.
use voteboat::{application::*, identity::*, log::*, program_transaction::*};
fn group(id: u128) -> GroupIdentity {
    GroupIdentity {
        id: GroupId::new(id).unwrap(),
        incarnation: GroupIncarnation::new(1).unwrap(),
    }
}
fn key(id: u64) -> KeyId {
    KeyId::new(id).unwrap()
}
fn profile() -> ProgramProfile {
    ProgramProfile::new(
        1,
        vec![group(1), group(2)],
        vec![group(3), group(4), group(5)],
    )
    .unwrap()
}
fn program(id: u128) -> Program {
    Program::new(
        TransactionId::new(id).unwrap(),
        profile(),
        vec![ParticipantProgram::new(
            group(3),
            vec![
                KeyAccess {
                    key: key(1),
                    expected_version: KeyVersion::default(),
                    write: Some(20),
                },
                KeyAccess {
                    key: key(2),
                    expected_version: KeyVersion::default(),
                    write: None,
                },
            ],
        )
        .unwrap()],
    )
    .unwrap()
}
fn coordinator(p: &Program) -> ProgramApplication {
    ProgramApplication::coordinator(
        p.coordinator(),
        profile(),
        ProgramLimits::new(2, 2).unwrap(),
    )
    .unwrap()
}
fn start(index: u64, p: Program) -> LogEntry {
    LogEntry {
        index,
        term: 1,
        payload: EntryPayload::Command {
            operation: p.operation(0).unwrap(),
            bytes: ProgramApplication::start(p).unwrap(),
        },
    }
}
#[test]
fn checked_program_canonical_footprints_and_phase_ids_are_immutable() {
    assert!(KeyId::new(0).is_none());
    assert!(ProgramProfile::new(0, vec![group(1)], vec![group(3)]).is_err());
    assert!(ProgramProfile::new(1, vec![group(2), group(1)], vec![group(3)]).is_err());
    assert!(ProgramProfile::new(1, vec![group(1)], vec![group(1)]).is_err());
    let mut stale = group(1);
    stale.incarnation = GroupIncarnation::new(2).unwrap();
    assert!(ProgramProfile::new(1, vec![group(1)], vec![stale]).is_err());
    let p = program(1);
    let q = program(2);
    assert_ne!(p.coordinator(), q.coordinator());
    for slot in 0..4 {
        assert_eq!(p.operation(slot).unwrap().get(), 4 + slot as u128);
        assert_ne!(p.operation(slot), q.operation(slot));
    }
    assert!(p.operation(4).is_err());
    assert_eq!(p.participants()[0].keys().len(), 2);
    assert!(Program::new(
        TransactionId::new(u128::MAX).unwrap(),
        profile(),
        p.participants().to_vec()
    )
    .is_err());
    assert!(ParticipantProgram::new(group(3), vec![p.participants()[0].keys()[0]; 2]).is_err());
    assert!(ParticipantProgram::new(
        group(3),
        vec![KeyAccess {
            key: key(1),
            expected_version: KeyVersion::default(),
            write: Some(-1)
        }]
    )
    .is_err());
}
#[test]
fn public_start_dedup_admission_read_bounds_and_checkpoint_roundtrip() {
    let p = program(1);
    let mut app = coordinator(&p);
    let entries = [start(1, p.clone()), start(2, p.clone())];
    let bound = app.receipt_bytes_bound(&entries).unwrap();
    let receipts = app.apply_batch(&entries).unwrap();
    assert_eq!(
        bound,
        receipts.capacity() * std::mem::size_of::<ProgramReceipt>()
    );
    assert_eq!(receipts[0].first_index, 1);
    assert_eq!(receipts[1].first_index, 1);
    assert!(receipts[1].duplicate);
    let query = ProgramQuery::Status(p.id());
    let result = app.read_at(2, query.clone()).unwrap();
    let nested = app.read_result_bytes(&result, usize::MAX).unwrap();
    assert!(nested > 0);
    assert!(nested + std::mem::size_of::<ProgramRead>() <= app.read_result_bound(&query).unwrap());
    assert!(app.read_result_bytes(&result, nested - 1).is_err());
    assert_eq!(app.read_at(3, query), Err(ApplicationError::NotApplied));
    let bytes = app
        .checkpoint(app.deployment_requirements().unwrap().snapshot_bytes)
        .unwrap();
    let mut fresh = coordinator(&p);
    fresh.restore_checkpoint(PROGRAM_SCHEMA, 2, &bytes).unwrap();
    assert_eq!(fresh, app);
}
#[test]
fn participant_public_reads_charge_original_box_and_preserve_opening_configuration() {
    let limits = ProgramLimits::new(2, 2).unwrap();
    let mut app =
        ProgramApplication::participant(group(3), profile(), &[(key(1), 10), (key(2), 20)], limits)
            .unwrap();
    assert_eq!(app.transaction_capacity(), 4);
    let q = ProgramQuery::Keys(vec![key(1), key(2)].into_boxed_slice());
    assert_eq!(app.query_bytes(&q, 16), Ok(16));
    assert!(app.query_bytes(&q, 15).is_err());
    let result = app.read_at(0, q.clone()).unwrap();
    let nested = app.read_result_bytes(&result, usize::MAX).unwrap();
    assert_eq!(
        nested + std::mem::size_of::<ProgramRead>(),
        app.read_result_bound(&q).unwrap()
    );
    assert_eq!(
        app.key(key(1)),
        KeyRead::Value {
            value: 10,
            version: KeyVersion::default()
        }
    );
    let bytes = app.checkpoint(usize::MAX).unwrap();
    let before = app.clone();
    assert!(app
        .restore_checkpoint(PROGRAM_SCHEMA + 1, 0, &bytes)
        .is_err());
    assert_eq!(app, before);
    let mut incompatible =
        ProgramApplication::participant(group(3), profile(), &[(key(1), 11), (key(2), 20)], limits)
            .unwrap();
    assert!(incompatible
        .restore_checkpoint(PROGRAM_SCHEMA, 0, &bytes)
        .is_err());
    assert!(app
        .read_at(
            0,
            ProgramQuery::Keys(vec![key(2), key(1)].into_boxed_slice())
        )
        .is_err());
}
