// SPDX-License-Identifier: RPL-1.5
use super::*;
fn restore(app: &ProgramApplication) -> ProgramApplication {
    let bytes = app
        .checkpoint(app.deployment_requirements().unwrap().snapshot_bytes)
        .unwrap();
    let mut fresh = match app.role {
        ProgramRole::Coordinator => coordinator(&program(1, 0), 4),
        ProgramRole::Participant => participant(app.group.id.get(), 4),
    };
    fresh
        .restore_checkpoint(PROGRAM_SCHEMA, app.applied, &bytes)
        .unwrap();
    assert_eq!(fresh, *app);
    fresh
}
#[test]
fn partial_prepare_and_every_decision_stage_reconstruct_from_own_checkpoint() {
    let p = program(1, 0);
    let mut f = Fixture::new(&p);
    f.start(&p);
    f.c = restore(&f.c);
    let facts = f.prepared(&p);
    for app in &mut f.p {
        *app = restore(app);
    }
    let d = f.decide(&p, Decision::Commit, facts);
    f.c = restore(&f.c);
    let r = apply(
        &mut f.p[0],
        command(
            &p,
            ProgramStep::ApplyDecision,
            Some(Decision::Commit),
            vec![d],
        ),
    );
    f.p[0] = restore(&f.p[0]);
    assert_eq!(r.first_index, 2);
    f.finish(&p, Decision::Commit, d);
    f.c = restore(&f.c);
    for app in &mut f.p {
        *app = restore(app);
    }
}
#[test]
fn checkpoint_changes_to_values_locks_events_or_suffix_are_refused_atomically() {
    let p = program(1, 0);
    let mut app = participant(3, 4);
    prepare(&mut app, &p);
    let bytes = app.checkpoint(usize::MAX).unwrap();
    let mut fresh = participant(3, 4);
    let header = 91 + (app.profile.coordinators().len() + app.profile.participants().len()) * 24;
    // Materialized keys and event positions must agree with reconstruction.
    // Valid rewritten command intent is not an integrity-proof obligation here.
    for i in [
        header + 16,
        header + 24,
        header + 32,
        header + 48 + 16,
        header + app.keys.len() * 48 + 8,
    ] {
        let mut bad = bytes.clone();
        bad[i] ^= 0x80;
        let before = fresh.clone();
        assert!(
            fresh
                .restore_checkpoint(PROGRAM_SCHEMA, app.applied, &bad)
                .is_err(),
            "byte {i}"
        );
        assert_eq!(fresh, before);
    }
    for end in 0..bytes.len() {
        assert!(fresh
            .restore_checkpoint(PROGRAM_SCHEMA, app.applied, &bytes[..end])
            .is_err());
    }
    assert!(fresh
        .restore_checkpoint(PROGRAM_SCHEMA + 1, app.applied, &bytes)
        .is_err());
    assert!(fresh
        .restore_checkpoint(PROGRAM_SCHEMA, app.applied + 1, &bytes)
        .is_err());
    let mut extra = bytes.clone();
    extra.push(0);
    assert!(fresh
        .restore_checkpoint(PROGRAM_SCHEMA, app.applied, &extra)
        .is_err());
    fresh
        .restore_checkpoint(PROGRAM_SCHEMA, app.applied, &bytes)
        .unwrap();
    assert_eq!(fresh, app);
}
#[test]
fn whole_batch_failure_and_failed_restore_preserve_original_state() {
    let p = program(1, 0);
    let c = Command::start(p.clone());
    let mut app = coordinator(&p, 4);
    let before = app.clone();
    let mut bad = entry(2, app.group, &c);
    if let EntryPayload::Command { bytes, .. } = &mut bad.payload {
        bytes.push(0);
    }
    assert!(app.apply_batch(&[entry(1, app.group, &c), bad]).is_err());
    assert_eq!(app, before);
    let entry = entry(2, app.group, &c);
    assert_eq!(app.apply_batch(&[entry]), Err(ApplicationError::IndexGap));
    assert_eq!(app, before);
}
