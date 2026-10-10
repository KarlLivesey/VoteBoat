// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use crate::runtime::DrainGroup;

/// An unchanged stable assignment where the draining source is already a
/// learner. This preserves replication until local shutdown; it does not
/// remove the learner, transfer leadership or grant a voting right.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DrainRetainedGroup {
    pub group: GroupIdentity,
    pub original: Configuration,
}
impl DrainRetainedGroup {
    pub(super) fn validate(&self, owner: PeerIdentity) -> Result<(), DrainPlanError> {
        if self.original.learners().get(&owner.node) != Some(&owner.store)
            || self.original.voter_stores().contains_key(&owner.node)
        {
            return Err(DrainPlanError::InvalidPlan);
        }
        Ok(())
    }
    pub(super) fn completed(&self, core: &Raft) -> Result<bool, DrainPlanError> {
        let membership = core.membership();
        if core.state().bootstrap.group != self.group {
            return Err(DrainPlanError::UnknownGroup);
        }
        if membership.joint().is_some() || membership.stable() != &self.original {
            return Err(DrainPlanError::StateChanged);
        }
        Ok(membership.last_configuration_index() <= core.state().commit_index)
    }
}
pub(super) fn retained_bytes(
    base: Option<usize>,
    retained: &Vec<DrainRetainedGroup>,
    count: usize,
) -> Option<usize> {
    let base = base?
        .checked_add(
            retained
                .capacity()
                .checked_mul(size_of::<DrainRetainedGroup>())?,
        )?
        .checked_add(count.checked_mul(size_of::<DrainGroup>())?)?;
    retained.iter().try_fold(base, |sum, entry| {
        sum.checked_add(entry.original.retained_bytes())
    })
}
pub(super) fn assignments(
    groups: &[DrainMembershipGroup],
    retained: &[DrainRetainedGroup],
) -> Result<Vec<DrainGroup>, DrainPlanError> {
    let mut all = Vec::with_capacity(groups.len() + retained.len());
    all.extend(groups.iter().map(|entry| DrainGroup {
        group: entry.group,
        configuration: entry.original.id(),
    }));
    all.extend(retained.iter().map(|entry| DrainGroup {
        group: entry.group,
        configuration: entry.original.id(),
    }));
    all.sort_unstable_by_key(|entry| entry.group);
    if all.windows(2).any(|pair| pair[0].group == pair[1].group) {
        return Err(DrainPlanError::InvalidPlan);
    }
    Ok(all)
}
