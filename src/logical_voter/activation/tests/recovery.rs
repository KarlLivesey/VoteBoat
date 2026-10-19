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
#[test]
fn own_checkpoint_and_original_suffix_restore_busy_and_active_state() {
    let mut a = app(3);
    let i = intent(1);
    begin(&mut a, &i);
    begin(&mut a, &intent(2));
    let bytes = a.business(&6i64.to_le_bytes()).unwrap();
    a.apply_batch(&[entry(3, op(100), bytes)]).unwrap();
    let checkpoint = a.checkpoint(usize::MAX).unwrap();
    let mut restored = app(3);
    restored
        .restore_checkpoint(LOGICAL_ACTIVATION_SCHEMA, 3, &checkpoint)
        .unwrap();
    assert_eq!(restored.checkpoint(usize::MAX).unwrap(), checkpoint);
    assert_eq!(restored.active(), Some(ops(1).begin));
    let outcome = entry(
        4,
        ops(1).outcome,
        codec::command(&final_command(&i), CONTROL_BYTES).unwrap(),
    );
    let release = entry(
        5,
        ops(1).release,
        codec::command(
            &ControlCommand::Release {
                begin: ops(1).begin,
                outcome: ActivationPosition { index: 4, term: 4 },
            },
            CONTROL_BYTES,
        )
        .unwrap(),
    );
    let retry = entry(6, op(100), a.business(&6i64.to_le_bytes()).unwrap());
    for e in [outcome, release, retry] {
        a.apply_batch(std::slice::from_ref(&e)).unwrap();
        restored.apply_batch(&[e]).unwrap();
    }
    assert_eq!(
        a.checkpoint(usize::MAX).unwrap(),
        restored.checkpoint(usize::MAX).unwrap()
    );
    assert_eq!(restored.inner.read_at(6, ()).unwrap(), 6);
    assert_eq!(restored.active(), None);
    assert_eq!(restored.status(ops(1).begin).unwrap().begin.first.index, 1);
    assert_eq!(
        restored.status(ops(2).begin).unwrap().begin.outcome,
        ChildActivationOutcome::Busy(ops(1).begin)
    );
}
#[test]
fn invalid_restore_is_atomic_and_rejects_history_materialization_and_profile() {
    let mut a = app(2);
    let i = intent(1);
    begin(&mut a, &i);
    finish(&mut a, &i);
    let checkpoint = a.checkpoint(usize::MAX).unwrap();
    let original = app(2).checkpoint(usize::MAX).unwrap();
    let mut restored = app(2);
    for end in 0..checkpoint.len() {
        assert!(restored
            .restore_checkpoint(LOGICAL_ACTIVATION_SCHEMA, 3, &checkpoint[..end])
            .is_err());
        assert_eq!(restored.checkpoint(usize::MAX).unwrap(), original);
    }
    let mut changed = checkpoint.clone();
    *changed.last_mut().unwrap() ^= 1;
    assert!(restored
        .restore_checkpoint(LOGICAL_ACTIVATION_SCHEMA, 3, &changed)
        .is_err());
    assert!(restored
        .restore_checkpoint(LOGICAL_ACTIVATION_SCHEMA, 2, &checkpoint)
        .is_err());
    assert_eq!(
        restored.restore_checkpoint(restored.inner.schema_version(), 3, &checkpoint),
        Err(ApplicationError::UnsupportedSchema)
    );
    assert!(app(3)
        .restore_checkpoint(LOGICAL_ACTIVATION_SCHEMA, 3, &checkpoint)
        .is_err());
    let mut changed = checkpoint.clone();
    changed[8 + 8 + 16] ^= 1;
    assert!(restored
        .restore_checkpoint(LOGICAL_ACTIVATION_SCHEMA, 3, &changed)
        .is_err());
    assert_eq!(restored.checkpoint(usize::MAX).unwrap(), original);
}
