// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use voteboat::{contracts::StorageError, membership::*, snapshot::SnapshotRef};

pub(super) fn exact(store: &impl LogStore, state: &GroupLog) {
    let actual_group = state.bootstrap.group;
    assert_eq!(store.matches_state(actual_group, state), Ok(true));
    let mut capacity = state.clone();
    capacity.entries.edit(|entries| {
        entries.reserve(100);
        for entry in entries {
            if let EntryPayload::Command { bytes, .. } = &mut entry.payload {
                let charge = bytes.retained_bytes();
                let mut replacement = bytes.to_vec();
                replacement.reserve(100);
                *bytes = replacement.into();
                assert_eq!(bytes.retained_bytes(), charge);
            }
        }
    });
    assert_eq!(store.matches_state(actual_group, &capacity), Ok(true));
    let changes: &[fn(&mut GroupLog)] = &[
        |s| s.bootstrap.group.id = GroupId::new(99).unwrap(),
        |s| s.bootstrap.group.incarnation = GroupIncarnation::new(99).unwrap(),
        |s| s.bootstrap.configuration = ConfigurationId::new(99).unwrap(),
        |s| s.bootstrap.policy = bootstrap(1, 1).policy,
        |s| {
            s.bootstrap.voter_stores.insert(node(1), identity(99));
        },
        |s| s.revision = LogRevision::new(99).unwrap(),
        |s| s.generation = LogGeneration::new(99).unwrap(),
        |s| s.hard_state.term += 1,
        |s| s.hard_state.voted_for = Some(node(2)),
        |s| {
            s.ballot_origin = Some(BallotOrigin {
                configuration: ConfigurationId::new(99).unwrap(),
                candidate_store: identity(2),
            })
        },
        |s| s.commit_index += 1,
        |s| s.snapshot = Some(reference(s)),
        |s| {
            s.snapshot_membership =
                Some(Box::new(Membership::replay(&s.bootstrap, &[], 0).unwrap()))
        },
        |s| s.entries.pop().map(|_| ()).unwrap(),
        |s| s.entries.edit(|entries| entries[0].index += 1),
        |s| s.entries.edit(|entries| entries[0].term += 1),
        |s| {
            s.entries
                .edit(|entries| entries[0].payload = EntryPayload::Noop)
        },
        |s| {
            s.entries.edit(|entries| {
                if let EntryPayload::Command { operation, .. } = &mut entries[0].payload {
                    *operation = OperationId::new(99).unwrap();
                }
            })
        },
        |s| {
            s.entries.edit(|entries| {
                if let EntryPayload::Command { bytes, .. } = &mut entries[0].payload {
                    let mut replacement = bytes.to_vec();
                    replacement[0] ^= 1;
                    *bytes = replacement.into();
                }
            })
        },
        |s| s.entries.edit(|entries| entries.swap(0, 1)),
    ];
    for (index, change) in changes.iter().enumerate() {
        let mut different = state.clone();
        change(&mut different);
        assert_ne!(different, *state, "invalid test mutation {index}");
        assert_eq!(
            store.matches_state(actual_group, &different),
            Ok(false),
            "mutation {index}"
        );
    }
    let missing = group(99);
    assert_eq!(
        store.matches_state(missing, state),
        store.state(missing).map(|s| s == *state)
    );
}
fn reference(state: &GroupLog) -> SnapshotRef {
    SnapshotRef {
        store: identity(1),
        group: state.bootstrap.group,
        generation: SnapshotGeneration::new(1).unwrap(),
        configuration: state.bootstrap.configuration,
        index: 1,
        term: 1,
        application_schema: 1,
        file_bytes: 100,
        checksum: 7,
    }
}

#[test]
fn default_is_object_safe_and_preserves_complete_snapshot_and_configuration_equality() {
    let mut store = HostLogStore::new(1);
    append(&mut store, vec![LogMutation::Create(bootstrap(1, 3))]);
    let mut state = store.state(group(1)).unwrap();
    state.entries = vec![entry(1, 1, 7), entry(2, 1, 8)].into();
    state.snapshot = Some(reference(&state));
    state.snapshot_membership = Some(Box::new(
        Membership::replay(&state.bootstrap, &[], 0).unwrap(),
    ));
    state.entries.push(LogEntry {
        index: 3,
        term: 1,
        payload: EntryPayload::Configuration(Box::new(ConfigurationRecord {
            operation: OperationId::new(3).unwrap(),
            expected: ConfigurationId::new(1).unwrap(),
            change: ConfigurationChange::Final {
                id: ConfigurationId::new(2).unwrap(),
            },
        })),
    });
    store.durable.insert(group(1), state.clone());
    let object: &dyn LogStore = &store;
    assert_eq!(object.matches_state(group(1), &state), Ok(true));
    snapshot_fields(object, &state);
    configuration_fields(object, &state);
}
fn snapshot_fields(store: &dyn LogStore, state: &GroupLog) {
    let changes: &[fn(&mut SnapshotRef)] = &[
        |r| r.store.id = StoreId::new(99).unwrap(),
        |r| r.store.incarnation = StoreIncarnation::new(99).unwrap(),
        |r| r.group.id = GroupId::new(99).unwrap(),
        |r| r.group.incarnation = GroupIncarnation::new(99).unwrap(),
        |r| r.generation = SnapshotGeneration::new(99).unwrap(),
        |r| r.configuration = ConfigurationId::new(99).unwrap(),
        |r| r.index += 1,
        |r| r.term += 1,
        |r| r.application_schema += 1,
        |r| r.file_bytes += 1,
        |r| r.checksum ^= 1,
    ];
    for change in changes {
        let mut altered = state.clone();
        change(altered.snapshot.as_mut().unwrap());
        assert_eq!(store.matches_state(group(1), &altered), Ok(false));
    }
    let mut altered = state.clone();
    altered.snapshot_membership = Some(Box::new(
        Membership::replay(&bootstrap(1, 1), &[], 0).unwrap(),
    ));
    assert_eq!(store.matches_state(group(1), &altered), Ok(false));
}
fn configuration_fields(store: &dyn LogStore, state: &GroupLog) {
    let changes: &[fn(&mut ConfigurationRecord)] = &[
        |r| r.operation = OperationId::new(99).unwrap(),
        |r| r.expected = ConfigurationId::new(99).unwrap(),
        |r| {
            r.change = ConfigurationChange::Final {
                id: ConfigurationId::new(99).unwrap(),
            }
        },
    ];
    for change in changes {
        let mut altered = state.clone();
        altered.entries.edit(|entries| {
            let EntryPayload::Configuration(record) = &mut entries[2].payload else {
                unreachable!()
            };
            change(record);
        });
        assert_eq!(store.matches_state(group(1), &altered), Ok(false));
    }
    assert_eq!(
        store.matches_state(group(99), state),
        Err(StorageError::Rejected("unknown group"))
    );
}
