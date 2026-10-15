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
//! Consensus log transitions and the host-replaceable storage contract.
use super::*;
use voteboat::{
    contracts::{HardState, StorageError},
    identity::*,
    log::*,
    quorum::*,
};

fn fixture() -> (store::MemoryStore, GroupIdentity) {
    let group = GroupIdentity {
        id: GroupId::new(1).unwrap(),
        incarnation: GroupIncarnation::new(1).unwrap(),
    };
    let identity = StoreIdentity {
        id: StoreId::new(1).unwrap(),
        incarnation: StoreIncarnation::new(1).unwrap(),
    };
    let binding = StoreBinding {
        identity,
        session: StoreSession::new(1).unwrap(),
    };
    let node = NodeId::new(1).unwrap();
    let bootstrap = Bootstrap {
        group,
        configuration: ConfigurationId::new(1).unwrap(),
        policy: Policy::new(Tree::Voter(node), Limits::default()).unwrap(),
        voter_stores: std::collections::BTreeMap::from([(node, identity)]),
    };
    (
        store::MemoryStore::new(binding, LogLimits::default(), bootstrap).unwrap(),
        group,
    )
}
fn update(state: &GroupLog, term: u64) -> LogMutation {
    LogMutation::Update(LogUpdate {
        group: state.bootstrap.group,
        expected_revision: state.revision,
        hard_state: HardState {
            term,
            voted_for: None,
        },
        commit_index: state.commit_index,
        suffix: None,
        snapshot: None,
        snapshot_membership: None,
    })
}

#[test]
fn later_invalid_model_unit_does_not_install_state_or_consume_sequence() {
    let (mut store, group) = fixture();
    let before = store.state(group).unwrap();
    let mut unknown = before.clone();
    unknown.bootstrap.group.id = GroupId::new(2).unwrap();
    assert!(store
        .append_batch(vec![update(&before, 1), update(&unknown, 1)])
        .is_err());
    assert_eq!(store.state(group).unwrap(), before);
    let tickets = store.append_batch(vec![update(&before, 1)]).unwrap();
    assert_eq!(tickets[0].batch, 1);
    assert!(store.state(group).is_err());
    let mut wrong = tickets.clone();
    wrong[0].revision = before.revision;
    assert_eq!(store.barrier(&wrong), Err(StorageError::StaleTicket));
    assert!(store.state(group).is_err());
    assert_eq!(store.barrier(&tickets).unwrap().tickets, tickets);
    assert_eq!(store.state(group).unwrap().hard_state.term, 1);
}

#[test]
fn modeled_suffix_generation_and_range_still_require_completed_exact_batch() {
    let (mut store, group) = fixture();
    let state = store.state(group).unwrap();
    let command = |term| LogEntry {
        index: 1,
        term,
        payload: EntryPayload::Noop,
    };
    let suffix = |state: &GroupLog, term| {
        LogMutation::Update(LogUpdate {
            group,
            expected_revision: state.revision,
            hard_state: HardState {
                term,
                voted_for: None,
            },
            commit_index: 0,
            suffix: Some(Suffix {
                from: 1,
                entries: vec![command(term)],
            }),
            snapshot: None,
            snapshot_membership: None,
        })
    };
    let first = store.append_batch(vec![suffix(&state, 1)]).unwrap();
    assert!(store
        .fetch_range(group, first[0].generation, 1, 1, 100)
        .is_err());
    assert!(store.append_batch(vec![update(&state, 2)]).is_err());
    store.barrier(&first).unwrap();
    let state = store.state(group).unwrap();
    let replacement = store.append_batch(vec![suffix(&state, 2)]).unwrap();
    assert!(replacement[0].generation > first[0].generation);
    assert_eq!(store.barrier(&first), Err(StorageError::StaleTicket));
    store.barrier(&replacement).unwrap();
    assert_eq!(
        store.fetch_range(group, first[0].generation, 1, 1, 100),
        Err(StorageError::StaleTicket)
    );
    assert_eq!(
        store
            .fetch_range(group, replacement[0].generation, 1, 1, 100)
            .unwrap(),
        vec![command(2)]
    );
}
