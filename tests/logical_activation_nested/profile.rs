// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Finite selected canonical envelope, not a universal protocol bound.
use super::setup;
use voteboat::{
    application::CheckpointStateMachine,
    contracts::HardState,
    identity::*,
    log::*,
    log_format::{LogCodec, NativeLogCodec},
    membership::*,
    snapshot::SnapshotMetadata,
    snapshot_format::{NativeSnapshotCodec, SnapshotCodec},
};
#[test]
fn selected_complete_upper_canonical_history_fits_lower_snapshot_and_import() {
    let limits = setup::upper_limits();
    let mut entries = entries();
    entries.extend(second_entries(&entries));
    assert_eq!(entries.len(), 6);
    let initial = setup::upper_app()
        .checkpoint(limits.max_child_checkpoint_bytes)
        .unwrap()
        .len();
    let history = entries
        .iter()
        .map(|entry| {
            NativeLogCodec
                .encode_batch(
                    1,
                    &[LogMutation::Update(LogUpdate {
                        group: setup::middle().group,
                        expected_revision: LogRevision::new(1).unwrap(),
                        hard_state: HardState::default(),
                        commit_index: 0,
                        suffix: Some(Suffix {
                            from: entry.index,
                            entries: vec![entry.clone()],
                        }),
                        snapshot: None,
                        snapshot_membership: None,
                    })],
                    limits.parent_log,
                )
                .unwrap()
                .len()
                + 8
        })
        .sum::<usize>();
    let prefix = (0..=entries.len())
        .map(|n| {
            NativeSnapshotCodec
                .prefix(
                    &SnapshotMetadata {
                        bootstrap: setup::middle(),
                        membership: Some(Box::new(
                            Membership::replay(&setup::middle(), &entries[..n], n as u64).unwrap(),
                        )),
                        index: (n as u64).max(1),
                        term: 1,
                        application_schema: 1,
                    },
                    1,
                    limits.parent_snapshots,
                )
                .unwrap()
                .len()
        })
        .max()
        .unwrap();
    let fixed = initial + history + prefix + 56 + 104 + 8;
    let bound = limits.max_retained_action_bytes
        + limits.max_actions * 177
        + limits.max_parent_checkpoint_bytes
        + 2 * (limits.parent_snapshots.max_file_bytes() + 104 + 8)
        + fixed;
    assert!(
        bound <= limits.max_child_checkpoint_bytes,
        "selected bound={bound},fixed={fixed}"
    );
    let lower = setup::lower_limits();
    assert!(lower.parent_snapshots.max_application_bytes >= limits.max_child_checkpoint_bytes);
    assert!(lower.max_command_bytes > lower.parent_snapshots.max_file_bytes() + history + 1024);
    assert!(lower.control_action_bytes >= lower.max_command_bytes * 2);
    assert_eq!(setup::lower_app(3).schema_version(), 2);
    assert_eq!(setup::upper_app().schema_version(), 1);
    assert!(
        setup::lower_app(3)
            .checkpoint(lower.max_child_checkpoint_bytes)
            .unwrap()
            .len()
            < lower.max_child_checkpoint_bytes
    );
    eprintln!(
        "356 finite upper bound={bound} fixed={fixed} cap={} lower_import_cap={}",
        limits.max_child_checkpoint_bytes, lower.max_command_bytes
    );
}
fn entries() -> Vec<LogEntry> {
    let old = Membership::replay(&setup::middle(), &[], 0)
        .unwrap()
        .stable()
        .clone();
    let mut learners = old.learners().clone();
    learners.insert(super::support::node(204), super::support::identity(2004));
    let learners = Configuration::new(
        ConfigurationId::new(2).unwrap(),
        old.policy().clone(),
        old.voter_stores().clone(),
        learners,
    )
    .unwrap();
    let target = Configuration::new(
        ConfigurationId::new(4).unwrap(),
        voteboat::quorum::Policy::new(
            voteboat::quorum::Tree::Majority(
                [201, 202, 204]
                    .map(|n| voteboat::quorum::Tree::Voter(super::support::node(n)))
                    .to_vec(),
            ),
            voteboat::quorum::Limits::default(),
        )
        .unwrap(),
        [201, 202, 204]
            .into_iter()
            .map(|n| {
                (
                    super::support::node(n),
                    super::support::identity(1800 + u128::from(n)),
                )
            })
            .collect(),
        Default::default(),
    )
    .unwrap();
    let op = |n| OperationId::new(setup::BASE + n).unwrap();
    [
        ConfigurationRecord {
            operation: op(1),
            expected: old.id(),
            change: ConfigurationChange::Learners(learners),
        },
        ConfigurationRecord {
            operation: op(2),
            expected: ConfigurationId::new(2).unwrap(),
            change: ConfigurationChange::Joint {
                id: ConfigurationId::new(3).unwrap(),
                next: target,
            },
        },
        ConfigurationRecord {
            operation: op(2),
            expected: ConfigurationId::new(3).unwrap(),
            change: ConfigurationChange::Final {
                id: ConfigurationId::new(4).unwrap(),
            },
        },
    ]
    .into_iter()
    .enumerate()
    .map(|(i, r)| LogEntry {
        index: i as u64 + 1,
        term: 1,
        payload: EntryPayload::Configuration(Box::new(r)),
    })
    .collect()
}
fn second_entries(previous: &[LogEntry]) -> Vec<LogEntry> {
    let old = Membership::replay(&setup::middle(), previous, 3)
        .unwrap()
        .stable()
        .clone();
    let intent = setup::second_intent();
    let mut learners = old.learners().clone();
    learners.insert(super::support::node(205), super::support::identity(2005));
    let learners = Configuration::new(
        intent.learners_configuration(),
        old.policy().clone(),
        old.voter_stores().clone(),
        learners,
    )
    .unwrap();
    let target = Configuration::new(
        intent.target_configuration(),
        voteboat::quorum::Policy::new(
            voteboat::quorum::Tree::Majority(
                intent
                    .target_voters()
                    .iter()
                    .map(|v| voteboat::quorum::Tree::Voter(v.node))
                    .collect(),
            ),
            voteboat::quorum::Limits::default(),
        )
        .unwrap(),
        intent
            .target_voters()
            .iter()
            .map(|v| (v.node, v.store))
            .collect(),
        Default::default(),
    )
    .unwrap();
    [
        ConfigurationRecord {
            operation: intent.operations().learners,
            expected: intent.old_configuration(),
            change: ConfigurationChange::Learners(learners),
        },
        ConfigurationRecord {
            operation: intent.operations().membership,
            expected: intent.learners_configuration(),
            change: ConfigurationChange::Joint {
                id: intent.joint_configuration(),
                next: target,
            },
        },
        ConfigurationRecord {
            operation: intent.operations().membership,
            expected: intent.joint_configuration(),
            change: ConfigurationChange::Final {
                id: intent.target_configuration(),
            },
        },
    ]
    .into_iter()
    .enumerate()
    .map(|(i, r)| LogEntry {
        index: 4 + i as u64,
        term: 1,
        payload: EntryPayload::Configuration(Box::new(r)),
    })
    .collect()
}
#[test]
fn declared_full_middle_and_top_batches_fit_separate_release_windows() {
    use voteboat::{application::CounterReceipt, logical_voter::*};
    let lower = setup::lower_limits();
    let middle = super::parts::node_limits::<LogicalReceipt, LogicalRead>(lower).releases;
    let n = middle
        .capture_reservation_bytes::<LogicalReceipt, LogicalRead>(
            lower.parent_log.max_entries_per_group,
        )
        .unwrap();
    assert_eq!(middle.bytes - middle.control_bytes, n);
    assert_eq!(middle.control_bytes, n * middle.control_pending);
    let upper = setup::upper_limits();
    let top =
        super::parts::node_limits::<ActivationReceipt<CounterReceipt>, ActivationRead<i64>>(upper)
            .releases;
    let n = top
        .capture_reservation_bytes::<ActivationReceipt<CounterReceipt>, ActivationRead<i64>>(
            upper.parent_log.max_entries_per_group,
        )
        .unwrap();
    assert_eq!(top.bytes - top.control_bytes, n);
    assert_eq!(top.control_bytes, n * top.control_pending);
    eprintln!(
        "367 declared full batches middle={} control={} total={} top={} control={} total={}",
        middle.bytes - middle.control_bytes,
        middle.control_bytes,
        middle.bytes,
        top.bytes - top.control_bytes,
        top.control_bytes,
        top.bytes
    );
}
