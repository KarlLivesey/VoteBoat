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
mod compact;
mod incoming;
use crate::{
    contracts::HardState,
    quorum::{Limits, Tree},
};

fn setup() -> OwnedLogState {
    let node = NodeId::new(1).unwrap();
    let store = StoreIdentity {
        id: StoreId::new(1).unwrap(),
        incarnation: StoreIncarnation::new(1).unwrap(),
    };
    let mut owned = OwnedLogState::new(LogLimits::default()).unwrap();
    let creates = (1..=2)
        .map(|id| {
            LogMutation::Create(Bootstrap {
                group: GroupIdentity {
                    id: GroupId::new(id).unwrap(),
                    incarnation: GroupIncarnation::new(1).unwrap(),
                },
                configuration: ConfigurationId::new(1).unwrap(),
                policy: Policy::new(Tree::Voter(node), Limits::default()).unwrap(),
                voter_stores: BTreeMap::from([(node, store)]),
            })
        })
        .collect();
    owned.apply_batch(creates).unwrap();
    owned
}
fn states(owned: &OwnedLogState) -> BTreeMap<GroupIdentity, GroupLog> {
    owned
        .groups
        .iter()
        .map(|(&id, group)| (id, group.state.clone()))
        .collect()
}
fn update(state: &GroupLog) -> LogMutation {
    LogMutation::Update(LogUpdate {
        group: state.bootstrap.group,
        expected_revision: state.revision,
        hard_state: HardState {
            term: 1,
            voted_for: None,
        },
        commit_index: 0,
        suffix: None,
        snapshot: None,
        snapshot_membership: None,
    })
}

#[test]
fn stale_later_private_summary_is_refused_before_any_canonical_install() {
    let mut owned = setup();
    let second = owned.groups.values_mut().next_back().unwrap();
    let mut other = second.state.clone();
    other.revision = LogRevision::new(2).unwrap();
    second.summary = LogSummary::from_validated(&other);
    let before = states(&owned);
    let batch = before.values().map(update).collect();
    assert_eq!(
        owned.apply_batch(batch),
        Err(StorageError::Rejected("stale log summary"))
    );
    assert_eq!(owned.into_states(), before);
}

#[test]
fn revision_exhaustion_after_an_earlier_valid_unit_has_no_partial_progress() {
    let mut owned = setup();
    let first = owned.groups.values_mut().next().unwrap();
    first.state.revision = LogRevision::new(u64::MAX).unwrap();
    first.summary = LogSummary::from_validated(&first.state);
    let before = states(&owned);
    let batch = before.values().rev().map(update).collect::<Vec<_>>();
    let mut oracle = before.clone();
    let expected = apply_batch(&mut oracle, &batch, owned.limits());
    assert_eq!(owned.apply_batch(batch), expected);
    assert_eq!(oracle, before);
    assert_eq!(owned.into_states(), before);
}
