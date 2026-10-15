// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use voteboat::quorum::{Limits, Policy, Tree};
fn store<const TRACE: bool>() -> MemoryStore<TRACE> {
    let binding = StoreBinding {
        identity: StoreIdentity {
            id: StoreId::new(1).unwrap(),
            incarnation: StoreIncarnation::new(1).unwrap(),
        },
        session: StoreSession::new(1).unwrap(),
    };
    let bootstrap = Bootstrap {
        group: GroupIdentity {
            id: GroupId::new(1).unwrap(),
            incarnation: GroupIncarnation::new(1).unwrap(),
        },
        configuration: ConfigurationId::new(1).unwrap(),
        policy: Policy::new(Tree::Voter(NodeId::new(1).unwrap()), Limits::default()).unwrap(),
        voter_stores: [(NodeId::new(1).unwrap(), binding.identity)].into(),
    };
    MemoryStore::new(binding, LogLimits::default(), bootstrap).unwrap()
}
#[test]
fn completed_predicate_keeps_pending_before_missing_errors_and_exact_outcomes() {
    let mut store = store::<true>();
    let group = GroupIdentity {
        id: GroupId::new(1).unwrap(),
        incarnation: GroupIncarnation::new(1).unwrap(),
    };
    let before = store.state(group).unwrap();
    let tickets = store
        .append_batch(vec![LogMutation::Update(LogUpdate {
            group,
            expected_revision: before.revision,
            hard_state: voteboat::contracts::HardState {
                term: 1,
                voted_for: None,
            },
            commit_index: 0,
            suffix: Some(Suffix {
                from: 1,
                entries: vec![LogEntry {
                    index: 1,
                    term: 1,
                    payload: EntryPayload::Command {
                        operation: OperationId::new(1).unwrap(),
                        bytes: vec![7].into(),
                    },
                }],
            }),
            snapshot: None,
            snapshot_membership: None,
        })])
        .unwrap();
    let mut missing = group;
    missing.id = GroupId::new(99).unwrap();
    for group in [group, missing] {
        let error = StorageError::Rejected("volatile transition not completed");
        assert_eq!(store.state(group), Err(error.clone()));
        assert_eq!(store.matches_state(group, &before), Err(error));
    }
    store.barrier(&tickets).unwrap();
    let actual = store.state(group).unwrap();
    assert_eq!(store.matches_state(group, &actual), Ok(true));
    assert_eq!(store.matches_state(group, &before), Ok(false));
    let mut bytes = actual.clone();
    let EntryPayload::Command { bytes: payload, .. } = &mut bytes.entries[0].payload else {
        unreachable!()
    };
    let mut replacement = payload.to_vec();
    replacement[0] ^= 1;
    *payload = replacement.into();
    assert_eq!(store.matches_state(group, &bytes), Ok(false));
    assert_eq!(
        store.matches_state(missing, &actual),
        Err(StorageError::Rejected("unknown group"))
    );
    let report = serde_json::to_value(
        store
            .attribution
            .report(0, &store.attribution, &Recorder::default())
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    let phases = report["phases"].as_array().unwrap();
    assert_eq!(phases.len(), 29);
    let comparison = phases
        .iter()
        .find(|p| p["phase"] == "store_matches_state_exact_nested")
        .unwrap();
    assert_eq!(comparison["calls"], 6);
    assert_eq!(comparison["failures"], 3);
    assert_eq!(comparison["items"], 1);
    assert!(report["checkpoint_provider_scopes"]
        .as_array()
        .unwrap()
        .iter()
        .all(|s| s["methods"].as_array().unwrap().len() == 15));
}
#[test]
fn disabled_comparison_preserves_equality_without_touching_observer_cells() {
    let store = store::<false>();
    let group = GroupIdentity {
        id: GroupId::new(1).unwrap(),
        incarnation: GroupIncarnation::new(1).unwrap(),
    };
    let actual = store.state(group).unwrap();
    assert_eq!(store.matches_state(group, &actual), Ok(true));
    assert!(store
        .attribution
        .report(0, &store.attribution, &Recorder::default())
        .unwrap()
        .is_none());
}

#[test]
fn protected_checkpoint_comparison_keeps_completed_gate_and_distinct_phase() {
    let mut store = store::<true>();
    let group = GroupIdentity {
        id: GroupId::new(1).unwrap(),
        incarnation: GroupIncarnation::new(1).unwrap(),
    };
    let core = voteboat::raft::Raft::recover(
        NodeId::new(1).unwrap(),
        store.binding(),
        store.state(group).unwrap(),
        store.limits(),
    )
    .unwrap();
    assert_eq!(
        store.matches_checkpoint_state(core.checkpoint_log()),
        Ok(true)
    );
    assert_eq!(
        store.matches_checkpoint_state(core.checkpoint_log()),
        Ok(true)
    );
    let before = store.state(group).unwrap();
    let tickets = store
        .append_batch(vec![LogMutation::Update(LogUpdate {
            group,
            expected_revision: before.revision,
            hard_state: voteboat::contracts::HardState {
                term: 1,
                voted_for: None,
            },
            commit_index: 0,
            suffix: None,
            snapshot: None,
            snapshot_membership: None,
        })])
        .unwrap();
    assert_eq!(
        store.matches_checkpoint_state(core.checkpoint_log()),
        Err(StorageError::Rejected("volatile transition not completed"))
    );
    let mut missing_state = before.clone();
    missing_state.bootstrap.group.id = GroupId::new(99).unwrap();
    let missing_core = voteboat::raft::Raft::recover(
        NodeId::new(1).unwrap(),
        store.binding(),
        missing_state,
        store.limits(),
    )
    .unwrap();
    assert_eq!(
        store.matches_checkpoint_state(missing_core.checkpoint_log()),
        Err(StorageError::Rejected("volatile transition not completed"))
    );
    store.barrier(&tickets).unwrap();
    assert_eq!(
        store.matches_checkpoint_state(core.checkpoint_log()),
        Ok(false)
    );
    assert_eq!(
        store.matches_checkpoint_state(missing_core.checkpoint_log()),
        Err(StorageError::Rejected("unknown group"))
    );
    let report = serde_json::to_value(
        store
            .attribution
            .report(0, &store.attribution, &Recorder::default())
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    let phase = report["phases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|p| p["phase"] == "store_checkpoint_state_protected_inclusive_nested")
        .unwrap();
    assert_eq!(phase["calls"], 6);
    assert_eq!(phase["failures"], 3);
    assert_eq!(phase["items"], 0);
    assert!(report["item_units"]
        .as_str()
        .unwrap()
        .contains("not_actual_compared_or_skipped_entries"));
}
