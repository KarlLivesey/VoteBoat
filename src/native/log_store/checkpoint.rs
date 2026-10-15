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

/// Mandatory logical validation also runs on host codec results. Encoding is
/// replaceable; bootstrap, vote, commit and suffix invariants are not.
pub(super) fn validate_state(
    sequence: u64,
    state: &BTreeMap<GroupIdentity, GroupLog>,
    limits: LogLimits,
) -> Result<(), StorageError> {
    if state.len() > limits.max_groups || (sequence == 0) != state.is_empty() {
        return Err(StorageError::Corrupt("checkpoint state/sequence budget"));
    }
    let mut ids = std::collections::BTreeSet::new();
    for (group, s) in state {
        if *group != s.bootstrap.group
            || !ids.insert(group.id)
            || s.generation.get() > s.revision.get()
            || s.revision.get() > sequence
            || (s.snapshot.is_some() && s.generation.get() < 2)
        {
            return Err(StorageError::Corrupt("checkpoint identity/counters"));
        }
        s.validate_ballot()
            .map_err(|_| StorageError::Corrupt("checkpoint ballot origin invariant"))?;
        let mut one = BTreeMap::new();
        apply_batch(
            &mut one,
            &[LogMutation::Create(s.bootstrap.clone())],
            limits,
        )
        .map_err(|_| StorageError::Corrupt("checkpoint bootstrap invariant"))?;
        let initial = one[group].clone();
        if let Some(reference) = s.snapshot {
            apply_batch(
                &mut one,
                &[LogMutation::Update(LogUpdate {
                    snapshot_membership: s.snapshot_membership.clone(),
                    group: *group,
                    expected_revision: initial.revision,
                    hard_state: crate::contracts::HardState {
                        term: s.hard_state.term,
                        voted_for: None,
                    },
                    commit_index: reference.index,
                    suffix: None,
                    snapshot: Some(reference),
                })],
                limits,
            )
            .map_err(|_| StorageError::Corrupt("checkpoint snapshot invariant"))?;
        }
        let revision = one[group].revision;
        let from = one[group]
            .base_index()
            .checked_add(1)
            .ok_or(StorageError::Corrupt("checkpoint index exhausted"))?;
        apply_batch(
            &mut one,
            &[LogMutation::Update(LogUpdate {
                snapshot_membership: None,
                group: *group,
                expected_revision: revision,
                hard_state: crate::contracts::HardState {
                    term: s.hard_state.term,
                    voted_for: None,
                },
                commit_index: s.commit_index,
                suffix: Some(Suffix {
                    from,
                    entries: s.entries.to_vec().into(),
                }),
                snapshot: None,
            })],
            limits,
        )
        .map_err(|_| StorageError::Corrupt("checkpoint suffix/ballot/commit invariant"))?;
        let mut checked = one.remove(group).unwrap();
        checked.hard_state = s.hard_state;
        checked.ballot_origin = s.ballot_origin;
        checked.revision = s.revision;
        checked.generation = s.generation;
        if checked != *s || (s.revision.get() == 1 && *s != initial) {
            return Err(StorageError::Corrupt("checkpoint state invariant"));
        }
    }
    Ok(())
}
