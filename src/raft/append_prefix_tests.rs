// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Prefix differential controls use real core transitions and fixture tickets,
//! not a filesystem durability assertion or a new replication protocol.
use super::*;
use membership_tests::{core, durable};

fn peer() -> NodeId {
    NodeId::new(2).unwrap()
}

fn fixture(payloads: Vec<EntryPayload>, count: usize, budget: usize) -> Raft {
    let mut core = core();
    core.configure_batch_limits(BatchLimits {
        max_replication_entries: count,
        ..BatchLimits::default()
    })
    .unwrap();
    if !payloads.is_empty() {
        let entries = payloads
            .into_iter()
            .enumerate()
            .map(|(offset, payload)| LogEntry {
                index: offset as u64 + 2,
                term: 1,
                payload,
            })
            .collect();
        let effects = core
            .persist(
                core.state().hard_state,
                1,
                Some(Suffix { from: 2, entries }),
                After::Reply,
                None,
            )
            .unwrap()
            .into_owned();
        durable(&mut core, effects);
    }
    core.requests.clear();
    core.next_index.insert(peer(), 2);
    core.limits.max_batch_bytes = budget;
    core.limits.max_command_bytes = budget.min(LogLimits::default().max_command_bytes);
    core
}

/// Preserved pre450 push/clone loop, independent of the new prefix selection.
fn old_prefix(core: &Raft, next: u64) -> Result<Vec<LogEntry>, RaftError> {
    let mut entries = Vec::new();
    let mut bytes = 0usize;
    for entry in core
        .state()
        .entries
        .iter()
        .skip((next - core.state().base_index() - 1) as usize)
        .take(core.batch_limits.max_replication_entries)
    {
        let size = 37 + entry.payload_bytes();
        if size > core.limits.max_batch_bytes.saturating_sub(bytes + 256) {
            break;
        }
        entries.push(entry.clone());
        bytes += size;
    }
    if entries.is_empty() && next <= core.state().last_index() {
        return Err(StorageError::Rejected("entry exceeds replication batch budget").into());
    }
    Ok(entries)
}

fn differential(core: &mut Raft) -> EntryBatch {
    let next = core.next_index[&peer()]
        .min(core.state().last_index() + 1)
        .max(1);
    let old = old_prefix(core, next);
    let sequence = core.request_sequence;
    let state = core.state().clone();
    let result = core.append_for(peer());
    if let Err(expected) = old {
        assert_eq!(result, Err(expected));
        assert_eq!(core.request_sequence, sequence);
        assert!(!core.requests.contains_key(&peer()));
        assert_eq!(core.state(), &state);
        return EntryBatch::default();
    }
    let old = old.unwrap();
    let Effect::Send(message) = result.unwrap() else {
        panic!("ordinary replication effect required")
    };
    let Rpc::Append {
        previous_index,
        previous_term,
        entries,
        leader_commit,
    } = message.rpc
    else {
        panic!("ordinary append required")
    };
    assert_eq!(entries, old);
    // A wide selection shares the canonical entries unless its backing would
    // pin too much; a small one, or one that pins too much, is a copy (a wide
    // copy in a backing of its own).
    if let Some(first) = entries.first() {
        let ordinal = (next - state.base_index() - 1) as usize;
        let canonical = &core.state().entries[ordinal];
        let backing = core
            .state()
            .entries
            .shared_range(ordinal..ordinal + 1)
            .unwrap()
            .first_backing_ptr();
        assert_eq!(
            std::ptr::eq(first, canonical),
            entries.is_shared() && entries.first_backing_ptr() == backing
        );
        if !std::ptr::eq(first, canonical) {
            assert_eq!(entries.retained_bytes(), entries.log_charge());
        }
    }
    assert_eq!(previous_index, next - 1);
    assert_eq!(previous_term, state.term_at(next - 1).unwrap());
    assert_eq!(leader_commit, state.commit_index);
    assert_eq!(message.group, state.bootstrap.group);
    assert_eq!(message.configuration, core.membership().id());
    assert_eq!(
        (message.from, message.sender, message.to),
        (core.node, core.binding, peer())
    );
    assert_eq!(message.term, state.hard_state.term);
    assert_eq!(
        message.context,
        RequestContext {
            origin: core.binding,
            sequence: sequence + 1
        }
    );
    let request = core.requests[&peer()];
    assert_eq!(request.context, message.context);
    assert_eq!(request.configuration, message.configuration);
    assert_eq!(
        (request.start, request.end),
        (next, entries.last().map_or(next - 1, |e| e.index))
    );
    assert_eq!(request.snapshot, None);
    assert_eq!(request.commit_at_start, state.commit_index);
    assert_eq!(core.state(), &state);
    entries
}

#[test]
fn old_prefix_matches_empty_byte_boundaries_counts_and_non_power_of_two_ranges() {
    for count in [1, 64, 1024] {
        for length in [0, 1, 3, 37, 65, 257, 1025] {
            for budget in [256, 292, 293, 329, 330, 367, 1024 * 1024] {
                let mut core = fixture(vec![EntryPayload::Noop; length], count, budget);
                let entries = differential(&mut core);
                let expected = length.min(count).min(budget.saturating_sub(256) / 37);
                assert_eq!(entries.len(), expected);
            }
        }
    }
}

#[test]
fn command_byte_prefix_keeps_full_ids_and_first_oversize_error_before_context() {
    let payloads = [u128::MAX, 7, (1 << 127) | 11]
        .into_iter()
        .map(|id| EntryPayload::Command {
            operation: OperationId::new(id).unwrap(),
            bytes: vec![0, 255, 7].into(),
        })
        .collect::<Vec<_>>();
    for count in [1, 64, 1024] {
        for budget in [256, 295, 296, 335, 336, 376] {
            let mut core = fixture(payloads.clone(), count, budget);
            let entries = differential(&mut core);
            assert_eq!(
                entries.len(),
                3.min(count).min(budget.saturating_sub(256) / 40)
            );
        }
    }
    let mut core = fixture(vec![EntryPayload::Noop], 64, 292);
    core.request_sequence = u64::MAX;
    assert_eq!(
        core.append_for(peer()),
        Err(StorageError::Rejected("entry exceeds replication batch budget").into())
    );
    assert_eq!(core.request_sequence, u64::MAX);
    assert!(core.requests.is_empty());
    core.limits.max_batch_bytes = 293;
    assert_eq!(core.append_for(peer()), Err(RaftError::Exhausted));
    assert!(core.requests.is_empty());
}

#[test]
fn full_configuration_and_nonempty_payload_copy_owns_independent_storage() {
    use crate::membership::{Configuration, ConfigurationChange, ConfigurationRecord};
    let initial = core();
    let configuration = Configuration::new(
        ConfigurationId::new(2).unwrap(),
        initial.state().bootstrap.policy.clone(),
        initial.state().bootstrap.voter_stores.clone(),
        BTreeMap::new(),
    )
    .unwrap();
    let mut core = fixture(
        vec![
            EntryPayload::Command {
                operation: OperationId::new(u128::MAX).unwrap(),
                bytes: Vec::new().into(),
            },
            EntryPayload::Command {
                operation: OperationId::new((1 << 127) | 9).unwrap(),
                bytes: vec![1, 2, 3].into(),
            },
            EntryPayload::Configuration(Box::new(ConfigurationRecord {
                operation: OperationId::new(17).unwrap(),
                expected: ConfigurationId::new(1).unwrap(),
                change: ConfigurationChange::Learners(configuration),
            })),
            EntryPayload::Noop,
        ],
        64,
        LogLimits::default().max_batch_bytes,
    );
    core.durable.edit(|log| {
        log.entries.edit(|entries| {
            if let EntryPayload::Command { bytes, .. } = &mut entries[2].payload {
                let mut raw = bytes.to_vec();
                raw.reserve(257);
                *bytes = raw.into();
            }
        })
    });
    let state = core.state().clone();
    let shared = differential(&mut core);
    // Four entries are below the sharing width: the message owns a copy.
    assert!(!shared.is_shared());
    assert!(!std::ptr::eq(&shared[0], &core.state().entries[1]));
    // An edit is an explicit copy; the message and the canonical log keep
    // their contents.
    let sent = shared.clone();
    let mut entries = shared.to_vec();
    let EntryPayload::Command { bytes, .. } = &mut entries[1].payload else {
        unreachable!()
    };
    let EntryPayload::Command {
        bytes: original, ..
    } = &core.state().entries[2].payload
    else {
        unreachable!()
    };
    assert_eq!(bytes.as_ptr(), original.as_ptr());
    assert_eq!(bytes.retained_bytes(), original.retained_bytes());
    let mut replacement = bytes.to_vec();
    replacement[0] = 99;
    *bytes = replacement.into();
    let EntryPayload::Configuration(copy) = &mut entries[2].payload else {
        unreachable!()
    };
    let EntryPayload::Configuration(original) = &core.state().entries[3].payload else {
        unreachable!()
    };
    assert_ne!(&**copy as *const _, &**original as *const _);
    copy.expected = ConfigurationId::new(99).unwrap();
    assert_eq!(core.state(), &state);
    assert_eq!(shared, sent);
    assert_ne!(shared, entries);
}
