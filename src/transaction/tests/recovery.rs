// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
// Unless explicitly acquired and licensed from Licensor under another license,
// the contents of this file are subject to the Reciprocal Public License
// ("RPL") Version 1.5, or subsequent versions as allowed by the RPL, and You may
// not copy or use this file in either source code or executable form, except
// in compliance with the terms and conditions of the RPL.
//
// All software distributed under the RPL is provided strictly on an "AS IS"
// basis, WITHOUT WARRANTY OF ANY KIND, EITHER EXPRESS OR IMPLIED, AND LICENSOR
// HEREBY DISCLAIMS ALL SUCH WARRANTIES, INCLUDING WITHOUT LIMITATION, ANY
// WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, QUIET
// ENJOYMENT, OR NON-INFRINGEMENT. See the RPL for specific language governing
// rights and limitations under the RPL.
use super::*;
fn reopen(app: &TransferApplication) -> TransferApplication {
    let checkpoint = app.checkpoint(usize::MAX).unwrap();
    let mut restored = app.clone();
    restored
        .restore_checkpoint(TRANSFER_SCHEMA, app.applied_index(), &checkpoint)
        .unwrap();
    assert_eq!(&restored, app);
    restored
}
#[test]
fn active_lock_checkpoint_and_suffix_reconstruct_original_atomic_phases() {
    let mut f = Fixture::new(2);
    let s = spec(1);
    f.start(s);
    let prepared = f.prepared(s);
    for app in [&mut f.c, &mut f.p[0]] {
        *app = reopen(app);
    }
    let decided = apply(
        &mut f.c,
        command(
            s,
            TransferStep::Decide,
            Some(TransferDecision::Commit),
            prepared,
        ),
    );
    let decision = fact(&f.c, decided);
    let applied = apply(
        &mut f.p[0],
        command(
            s,
            TransferStep::ApplyDecision,
            Some(TransferDecision::Commit),
            [Some(decision), None],
        ),
    );
    f.p[0] = reopen(&f.p[0]);
    assert_eq!(f.p[0].account(aid(1)), AccountRead::Locked(s.id()));
    let duplicate = apply(
        &mut f.p[0],
        command(
            s,
            TransferStep::ApplyDecision,
            Some(TransferDecision::Commit),
            [Some(decision), None],
        ),
    );
    assert_eq!(duplicate.first_index, applied.first_index);
    assert!(duplicate.duplicate);
    f.finish(s, TransferDecision::Commit, decision);
    let second = spec(2);
    f.start(second);
    f.commit(second);
    for app in [&f.c, &f.p[0], &f.p[1]] {
        reopen(app);
    }
    assert_eq!(f.p[0].account(aid(1)), AccountRead::Balance(10));
    assert_eq!(f.p[1].account(aid(2)), AccountRead::Balance(10));
}
#[test]
fn malformed_batch_and_every_checkpoint_truncation_preserve_original_state() {
    let mut f = Fixture::new(1);
    let s = spec(1);
    let before = f.c.clone();
    let valid = LogEntry {
        index: 1,
        term: 1,
        payload: EntryPayload::Command {
            operation: s.phases().coordinator()[0],
            bytes: TransferApplication::start(s).unwrap(),
        },
    };
    let bad = LogEntry {
        index: 2,
        term: 1,
        payload: EntryPayload::Command {
            operation: s.phases().coordinator()[1],
            bytes: vec![0],
        },
    };
    assert_eq!(
        f.c.apply_batch(&[valid, bad]),
        Err(ApplicationError::InvalidCommand)
    );
    assert_eq!(f.c, before);
    f.start(s);
    f.prepared(s);
    let original = f.p[0].clone();
    let checkpoint = original.checkpoint(usize::MAX).unwrap();
    for n in 0..checkpoint.len() {
        assert!(f.p[0]
            .restore_checkpoint(TRANSFER_SCHEMA, original.applied_index(), &checkpoint[..n])
            .is_err());
        assert_eq!(f.p[0], original);
    }
    for offset in [
        0,
        16,
        153 + 8,
        153 + 16,
        153 + 24,
        153 + 40 + 320,
        153 + 40 + 320 + 2 + 35 + 1,
    ] {
        let mut bad = checkpoint.clone();
        bad[offset] ^= 1;
        assert!(
            f.p[0]
                .restore_checkpoint(TRANSFER_SCHEMA, original.applied_index(), &bad)
                .is_err(),
            "offset {offset}"
        );
        assert_eq!(f.p[0], original);
    }
}
#[test]
fn semantic_restore_rejects_unknown_version_capacity_incarnation_and_opening_accounts() {
    let mut f = Fixture::new(1);
    let s = spec(1);
    f.start(s);
    f.commit(s);
    let bytes = f.p[0].checkpoint(usize::MAX).unwrap();
    let index = f.p[0].applied_index();
    let configs = [
        TransferApplication::participant(
            gid(20),
            s.profile(),
            &[(aid(1), 21)],
            TransferLimits::new(1, 2).unwrap(),
        )
        .unwrap(),
        TransferApplication::participant(
            gid(20),
            s.profile(),
            &[(aid(1), 20)],
            TransferLimits::new(2, 2).unwrap(),
        )
        .unwrap(),
    ];
    for mut app in configs {
        let original = app.clone();
        assert!(app
            .restore_checkpoint(TRANSFER_SCHEMA, index, &bytes)
            .is_err());
        assert_eq!(app, original);
    }
    let original = f.p[0].clone();
    assert_eq!(
        f.p[0].restore_checkpoint(TRANSFER_SCHEMA + 1, index, &bytes),
        Err(ApplicationError::UnsupportedSchema)
    );
    assert_eq!(f.p[0], original);
    let mut bad = bytes.clone();
    bad[16 + 1 + 16] ^= 1;
    assert!(f.p[0]
        .restore_checkpoint(TRANSFER_SCHEMA, index, &bad)
        .is_err());
    assert_eq!(f.p[0], original);
}
