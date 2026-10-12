// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Classify only the original automatic request already covered by a snapshot.
use super::*;
use voteboat::raft::RaftError;

pub(super) fn covered_checkpoint<L: LogStore + Send + 'static>(
    replica: &Replica<L>,
    scheduled: &BTreeMap<GroupIdentity, ScheduledCheckpoint>,
    step: &OwnerStep,
) -> bool {
    if step.error != Some(RaftError::NotApplied) {
        return false;
    }
    let Some(admission) = step.admission else {
        return false;
    };
    let Some(original) = scheduled.get(&admission.group) else {
        return false;
    };
    if original.admission != admission || original.executed {
        return false;
    }
    replica
        .local()
        .owner
        .core(admission.group)
        .is_some_and(|core| {
            original.target_index > 0 && core.state().base_index() >= original.target_index
        })
}
