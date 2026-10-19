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
//! Checkpoint v4 goldens (schema 5), legacy-format refusal, direct-decode
//! restore equivalence with full-history replay, and the snapshot-generation
//! high-water mark.
use super::*;
mod history;
mod refusals;
mod restore;
pub(super) use restore::{assert_restore_equivalent, assert_step_replays};
use restore::{genesis_replay, legacy_generation};

const V5_EMPTY: &[u8] = include_bytes!("formats/v5-empty.bin");
const V5_POPULATED: &[u8] = include_bytes!("formats/v5-populated.bin");
/// LV1 outputs (checkpoint v3, schema 3, command format v2), kept as legacy
/// fixtures: LV2 changed the embedded command format.
const V3_EMPTY: &[u8] = include_bytes!("formats/v3-empty.bin");
const V3_POPULATED: &[u8] = include_bytes!("formats/v3-populated.bin");
/// Generated on `origin/main` 30a1f6c5 by `history::populated` (applied 10).
const V1_POPULATED: &[u8] = include_bytes!("formats/v1-populated.bin");
const V1_EMPTY: &[u8] = include_bytes!("enrollment/v1-empty.bin");

fn restored(bytes: &[u8], applied: u64) -> LogicalApplication {
    let mut a = app();
    a.restore_checkpoint(LOGICAL_APPLICATION_SCHEMA, applied, bytes)
        .unwrap();
    a
}
#[test]
fn v5_goldens_round_trip_exactly() {
    assert_eq!(LOGICAL_APPLICATION_SCHEMA, 5);
    for (a, golden) in [(app(), V5_EMPTY), (history::populated(), V5_POPULATED)] {
        let bytes = a.checkpoint(usize::MAX).unwrap();
        assert_eq!(&bytes[..8], b"VBLOGC05");
        assert_eq!(bytes, golden);
        let b = restored(golden, a.applied_index());
        assert_eq!(b.state, a.state);
        assert_eq!(b.checkpoint(usize::MAX).unwrap(), golden);
    }
    let a = history::populated();
    assert_eq!(a.applied_index(), 10);
    // Retirement left the reconcile refusal and the final mutation.
    assert_eq!(
        a.state
            .records
            .keys()
            .map(|op| op.get())
            .collect::<Vec<_>>(),
        [
            command::session_operation(1, 7),
            command::session_operation(1, 8)
        ]
    );
    assert_eq!(a.state.snapshot_generation, 2);
    assert_eq!(a.state.retired_below, command::session_operation(1, 6));
}
#[test]
fn legacy_checkpoints_are_typed_refusals_and_leave_state_unchanged() {
    let mut a = history::populated();
    let before = a.checkpoint(usize::MAX).unwrap();
    for (bytes, applied, schema, magic) in [
        (V1_EMPTY, 0, 1, b"VBLOGC01"),
        (V1_POPULATED, 10, 1, b"VBLOGC01"),
        (V3_EMPTY, 0, 3, b"VBLOGC03"),
        (V3_POPULATED, 10, 3, b"VBLOGC03"),
    ] {
        assert_eq!(&bytes[..8], magic);
        assert_eq!(
            a.restore_checkpoint(schema, applied, bytes),
            Err(ApplicationError::UnsupportedSchema)
        );
        assert_eq!(
            a.restore_checkpoint(LOGICAL_APPLICATION_SCHEMA, applied, bytes),
            Err(ApplicationError::InvalidCheckpoint)
        );
        assert_eq!(a.checkpoint(usize::MAX).unwrap(), before);
    }
    // Current checkpoints presented under an old schema number are refused.
    for schema in [1, 3, 4] {
        assert_eq!(
            a.restore_checkpoint(schema, 10, &before),
            Err(ApplicationError::UnsupportedSchema)
        );
    }
    // LV1 bytes renamed to the new magic still fail: their commands use the
    // old command format.
    let mut renamed = V3_POPULATED.to_vec();
    renamed[..8].copy_from_slice(b"VBLOGC05");
    assert_eq!(
        a.restore_checkpoint(LOGICAL_APPLICATION_SCHEMA, 10, &renamed),
        Err(ApplicationError::InvalidCheckpoint)
    );
    assert_eq!(a.checkpoint(usize::MAX).unwrap(), before);
}
#[test]
fn direct_restore_matches_the_replay_restore_and_next_generation() {
    let a = history::populated();
    let bytes = a.checkpoint(usize::MAX).unwrap();
    let mut direct = restored(&bytes, a.applied_index());
    let mut replayed = a.clone();
    replayed.state = genesis_replay(&a).unwrap();
    assert_eq!(direct.state, a.state);
    assert_eq!(replayed.state, a.state);
    for (op, r) in &a.state.records {
        assert_eq!(direct.state.records[op].receipt, r.receipt);
    }
    // The next publication allocates the same, unused generation everywhere.
    let mut original = a.clone();
    let s = history::later_snapshot(&a);
    let expected = history::publish(&mut original, 9, s.clone());
    let LogicalOutcome::SnapshotPublished(r) = expected.outcome else {
        panic!("third publication");
    };
    assert_eq!(r.generation.get(), 3);
    assert_eq!(history::publish(&mut direct, 9, s.clone()), expected);
    assert_eq!(history::publish(&mut replayed, 9, s), expected);
    assert_eq!(direct.state, original.state);
}
#[test]
fn restore_does_not_replay_and_encodes_once_at_any_size() {
    for n in [0, 1, 64, 512] {
        let mut a = app();
        claim(&mut a);
        for id in 0..n {
            let c = command(
                10 + id,
                2,
                CommandBody::Claim {
                    expected_epoch: None,
                },
            );
            apply(&mut a, &c);
        }
        assert_eq!(a.state.records.len() as u128, n + 1);
        let bytes = a.checkpoint(usize::MAX).unwrap();
        let mut b = app();
        let (applies, encodes) = application::work();
        b.restore_checkpoint(LOGICAL_APPLICATION_SCHEMA, a.applied_index(), &bytes)
            .unwrap();
        let (after_applies, after_encodes) = application::work();
        assert_eq!(after_applies - applies, 0, "restore replayed records");
        assert_eq!(after_encodes - encodes, 1, "restore encoded more than once");
        assert_eq!(b.state, a.state);
    }
}
/// Offsets inside the populated v3 checkpoint, past magic and configuration.
fn header(a: &LogicalApplication) -> usize {
    8 + 8 + a.configuration.len() + 8 + 8
}
#[test]
fn every_corruption_class_is_refused_without_partial_restore() {
    let a = history::populated();
    let good = a.checkpoint(usize::MAX).unwrap();
    assert_eq!(good, V5_POPULATED);
    let h = header(&a);
    let mut cases: Vec<(&str, Vec<u8>)> = Vec::new();
    // The watermark is (1 << 64) | 6, set by the final mutation's
    // acknowledgement of sequence 5; the live keys are sequences 7 and 8.
    for (name, watermark) in [
        ("zero watermark", 0),
        ("lowered watermark", command::session_operation(1, 1)),
        // Raised by one: still at or below every live key, so only the
        // equality with the highest retained acknowledgement refuses it.
        ("raised watermark", command::session_operation(1, 7)),
        (
            "watermark below its acknowledgement",
            command::session_operation(1, 5),
        ),
        (
            "watermark above a live key",
            command::session_operation(1, 8),
        ),
        (
            "watermark in another session",
            command::session_operation(2, 6),
        ),
        ("watermark at sequence 0", command::session_operation(1, 0)),
    ] {
        let mut retired = good.clone();
        retired[h..h + 16].copy_from_slice(&watermark.to_le_bytes());
        cases.push((name, retired));
    }
    // A lower generation mark is refused; a higher one is history-dependent
    // once publication records are pruned (see `generation_mark_*`).
    let mut generation = good.clone();
    let n = u64::from_le_bytes(good[h + 16..h + 24].try_into().unwrap());
    generation[h + 16..h + 24].copy_from_slice(&(n - 1).to_le_bytes());
    cases.push(("generation mark below a retained generation", generation));
    let mut bytes_total = good.clone();
    let n = bytes_total.len();
    let total = u64::from_le_bytes(good[n - 8..].try_into().unwrap());
    bytes_total[n - 8..].copy_from_slice(&(total + 1).to_le_bytes());
    cases.push(("retained bytes", bytes_total));
    // First record: op (16), first index, first term, preceding configuration.
    let first = h + 24 + 8;
    let mut preceding = good.clone();
    preceding[first + 16 + 16] ^= 3;
    cases.push(("preceding configuration", preceding));
    let mut index = good.clone();
    index[first + 16..first + 24].copy_from_slice(&11u64.to_le_bytes());
    cases.push(("index past applied", index));
    let mut term = good.clone();
    term[first + 24..first + 32].copy_from_slice(&3u64.to_le_bytes());
    cases.push(("term past applied", term));
    let mut trailing = good.clone();
    trailing.push(0);
    cases.push(("trailing byte", trailing));
    for cut in [1, 8, h, first + 8, good.len() / 2, good.len() - 1] {
        cases.push(("truncated", good[..cut].to_vec()));
    }
    let mut b = history::populated();
    for (name, bytes) in cases {
        assert_eq!(
            b.restore_checkpoint(LOGICAL_APPLICATION_SCHEMA, 10, &bytes),
            Err(ApplicationError::InvalidCheckpoint),
            "{name}"
        );
        assert_eq!(b.checkpoint(usize::MAX).unwrap(), good, "{name}");
    }
    assert_eq!(
        b.restore_checkpoint(LOGICAL_APPLICATION_SCHEMA, 9, &good),
        Err(ApplicationError::InvalidCheckpoint)
    );
}
#[test]
fn raised_watermark_right_after_a_claim_is_refused() {
    let mut a = app();
    claim(&mut a);
    let session = a.state.representative.unwrap().logical_session.get();
    let floor = command::session_operation(session, 1);
    assert_eq!(a.state.retired_below, floor);
    let good = a.checkpoint(usize::MAX).unwrap();
    let h = header(&a);
    assert_eq!(good[h..h + 16], floor.to_le_bytes());
    // No session record exists, so no live key bounds a raised value.
    let mut raised = good.clone();
    raised[h..h + 16].copy_from_slice(&command::session_operation(session, 2).to_le_bytes());
    let mut b = a.clone();
    assert_eq!(
        b.restore_checkpoint(LOGICAL_APPLICATION_SCHEMA, a.applied_index(), &raised),
        Err(ApplicationError::InvalidCheckpoint)
    );
    assert_eq!(b.state, a.state);
    assert_eq!(b.checkpoint(usize::MAX).unwrap(), good);
}
#[test]
fn generation_mark_is_monotonic_and_independent_of_retained_records() {
    let mut a = history::populated();
    let mark = a.state.snapshot_generation;
    assert_eq!(mark, 2);
    // Retirement pruned both publication records: the former derivation
    // would fall back to 0 and reuse generation 1.
    assert!(!a
        .state
        .records
        .values()
        .any(|r| matches!(r.receipt.outcome, LogicalOutcome::SnapshotPublished(_))));
    assert_eq!(legacy_generation(&a.state), 0);
    let s = history::later_snapshot(&a);
    let LogicalOutcome::SnapshotPublished(r) = history::publish(&mut a, 9, s).outcome else {
        panic!("publication after record retirement");
    };
    assert_eq!(r.generation.get(), mark + 1);
    assert_eq!(a.state.snapshot_generation, mark + 1);
    // Reusing an existing identical snapshot allocates nothing.
    let s = a.state.snapshots.last().unwrap().1.clone();
    let reused = history::publish(&mut a, 10, s).outcome;
    assert_eq!(reused, LogicalOutcome::SnapshotPublished(r));
    assert_eq!(a.state.snapshot_generation, mark + 1);
}
/// Measurement only, never asserted: `cargo test --release --lib
/// restore_cost_report -- --ignored --nocapture`. Prints restore time for the
/// former replay restore and the direct decode at growing record counts.
#[test]
#[ignore = "timing report; run explicitly"]
fn restore_cost_report() {
    let mut a = app();
    claim(&mut a);
    let mut next = 1;
    for n in [256u128, 512, 1024, 2048, 4096 - 128] {
        while a.state.records.len() < n as usize {
            let c = command(
                10 + next,
                2,
                CommandBody::Claim {
                    expected_epoch: None,
                },
            );
            let e = entry(&a, &c, a.applied_index() + 1);
            a.apply_batch(&[e]).unwrap();
            next += 1;
        }
        let bytes = a.checkpoint(usize::MAX).unwrap();
        let started = std::time::Instant::now();
        let replayed = genesis_replay(&a).unwrap();
        let replay = started.elapsed();
        let mut b = app();
        let started = std::time::Instant::now();
        b.restore_checkpoint(LOGICAL_APPLICATION_SCHEMA, a.applied_index(), &bytes)
            .unwrap();
        let direct = started.elapsed();
        assert_eq!(b.state, replayed);
        eprintln!(
            "records {:5} checkpoint {:8} B replay {:10.3} ms direct {:8.3} ms",
            a.state.records.len(),
            bytes.len(),
            replay.as_secs_f64() * 1e3,
            direct.as_secs_f64() * 1e3
        );
    }
}
