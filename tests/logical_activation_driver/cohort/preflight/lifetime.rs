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
//! Constructor/bound preflight; these scalar checks mint no runtime authority.
//! Finite retained-history profile, independent of live bridge credits.
use super::*;
use voteboat::{
    contracts::HardState,
    log::{EntryPayload, LogEntry, LogMutation, LogUpdate, Suffix},
    membership::{Configuration, ConfigurationChange, ConfigurationRecord, Membership},
    quorum::{Limits, Policy, Tree},
    snapshot::SnapshotMetadata,
    snapshot_format::NativeSnapshotCodec,
};
#[test]
fn complete_retained_history_and_canonical_replacement_fit_child_checkpoint() {
    let limits = groups::storage_limits();
    // One Claim +64 protocol/election/heartbeat +24 business/retry/read
    // +24 activation/status/readiness +8 checkpoint +7 cleanup actions.
    assert_eq!(limits.max_actions, 1 + 64 + 24 + 24 + 8 + 7);
    assert_eq!(limits.control_actions, 4);
    let reference = 2 * (16 + 8) + 7 * 8;
    let record = 3 * 8 + 16 + 8 + 1 + reference + 3 * 8;
    assert_eq!(reference, 104);
    assert_eq!(record, 177);
    let fixed = (0..3).map(scope_bytes).max().unwrap();
    assert!(fixed <= 16 * 1024, "exact selected scope bytes {fixed}");
    let bound = limits.max_retained_action_bytes
        + limits.max_actions * record
        + limits.max_parent_checkpoint_bytes
        + 2 * (limits.parent_snapshots.max_file_bytes() + reference + 8)
        + 16 * 1024;
    assert_eq!(bound, 516_640);
    assert!(bound <= limits.max_child_checkpoint_bytes);
    eprintln!(
        "332 lifetime actions={} fixed_scope={fixed} complete_bound={bound} child_cap={}",
        limits.max_actions, limits.max_child_checkpoint_bytes
    );
}
fn scope_bytes(slot: usize) -> usize {
    use voteboat::{log_format::LogCodec, snapshot_format::SnapshotCodec};
    let limits = groups::storage_limits();
    let entries = if slot == 0 { entries(slot) } else { Vec::new() };
    let initial = groups::application(slot)
        .checkpoint(limits.max_child_checkpoint_bytes)
        .unwrap()
        .len();
    let history = entries
        .iter()
        .map(|entry| {
            let update = LogUpdate {
                group: groups::mapping(slot).child,
                expected_revision: LogRevision::new(1).unwrap(),
                hard_state: HardState::default(),
                commit_index: 0,
                suffix: Some(Suffix {
                    from: entry.index,
                    entries: vec![entry.clone()].into(),
                }),
                snapshot: None,
                snapshot_membership: None,
            };
            voteboat::log_format::NativeLogCodec
                .encode_batch(1, &[LogMutation::Update(update)], limits.parent_log)
                .unwrap()
                .len()
                + 8
        })
        .sum::<usize>();
    let bootstrap = groups::child(slot);
    let membership = (0..=entries.len())
        .map(|n| {
            let boundary = u64::try_from(n).unwrap();
            let m = Membership::replay(&bootstrap, &entries[..n], boundary).unwrap();
            let metadata = SnapshotMetadata {
                bootstrap: bootstrap.clone(),
                membership: Some(Box::new(m)),
                index: boundary.max(1),
                term: 1,
                application_schema: 1,
            };
            NativeSnapshotCodec
                .prefix(&metadata, 1, limits.parent_snapshots)
                .unwrap()
                .len()
        })
        .max()
        .unwrap();
    // Initial bytes include the exact immutable profile/catalog/bootstrap and
    // empty framing. Prefix conservatively includes encoded membership plus
    // bootstrap/framing. Add a live representative, published ref and parent
    // blob framing; complete parent/snapshot bodies are charged separately.
    initial + history + membership + 56 + 104 + 8
}
fn entries(slot: usize) -> Vec<LogEntry> {
    use std::collections::BTreeMap;
    let ids = groups::ids(slot);
    let config = |id, nodes: &[u64], learners: BTreeMap<NodeId, StoreIdentity>| {
        Configuration::new(
            ConfigurationId::new(id).unwrap(),
            Policy::new(
                Tree::Majority(
                    nodes
                        .iter()
                        .map(|&n| Tree::Voter(support::node(n)))
                        .collect(),
                ),
                Limits::default(),
            )
            .unwrap(),
            nodes
                .iter()
                .map(|&n| (support::node(n), support::identity(n.into())))
                .collect(),
            learners,
        )
        .unwrap()
    };
    let learners = config(
        2,
        &ids[..3],
        BTreeMap::from([(support::node(ids[3]), support::identity(ids[3].into()))]),
    );
    let target = config(4, &[ids[0], ids[1], ids[3]], BTreeMap::new());
    [
        ConfigurationRecord {
            operation: OperationId::new(33401).unwrap(),
            expected: ConfigurationId::new(1).unwrap(),
            change: ConfigurationChange::Learners(learners),
        },
        ConfigurationRecord {
            operation: OperationId::new(33402).unwrap(),
            expected: ConfigurationId::new(2).unwrap(),
            change: ConfigurationChange::Joint {
                id: ConfigurationId::new(3).unwrap(),
                next: target,
            },
        },
        ConfigurationRecord {
            operation: OperationId::new(33402).unwrap(),
            expected: ConfigurationId::new(3).unwrap(),
            change: ConfigurationChange::Final {
                id: ConfigurationId::new(4).unwrap(),
            },
        },
    ]
    .into_iter()
    .enumerate()
    .map(|(i, record)| LogEntry {
        index: u64::try_from(i + 1).unwrap(),
        term: 1,
        payload: EntryPayload::Configuration(Box::new(record)),
    })
    .collect()
}
