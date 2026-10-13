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
//! Opaque exclusive ownership for reusable atomic log validation.
use super::{
    transition::{LogSummary, LogView, OwnedNextLog, PreparedOwnedLog},
    *,
};

#[cfg(test)]
pub(super) mod tests;

struct Group {
    state: GroupLog,
    summary: LogSummary,
    comparison: CheckpointComparison,
}
enum Plan {
    Create(Group),
    Update(PreparedOwnedLog),
}

/// Bounded transactional log state for host-supplied storage implementations.
///
/// This helper owns its canonical groups and private versioned validation
/// summaries. It starts empty: groups enter through explicit validated Create
/// mutations. Read-only lookup cannot invalidate its caches. Plain appends and
/// metadata changes and eligible plain matching-prefix compaction retain unchanged
/// entry allocations. Configuration history, other snapshots and replacement use
/// the same full validator as [`apply_batch`].
///
/// This is logical validation, not a storage provider or durability token.
/// The host still owns store identity, snapshot pins, I/O, uncertain-write
/// fencing, accepted/durable visibility and exact-ticket barriers. All units
/// validate before any state changes. Failed admission leaves every group intact.
/// Process allocation failure has the same standard allocation behavior as the
/// existing owned-map validator. Limits remain immutable throughout its lifetime.
pub struct OwnedLogState {
    limits: LogLimits,
    groups: BTreeMap<GroupIdentity, Group>,
}
impl OwnedLogState {
    pub fn new(limits: LogLimits) -> Result<Self, StorageError> {
        Ok(Self {
            limits: limits.validate()?,
            groups: BTreeMap::new(),
        })
    }
    pub fn limits(&self) -> LogLimits {
        self.limits
    }
    /// Immutable canonical state. Visibility and durability remain host-owned.
    pub fn get(&self, group: GroupIdentity) -> Option<&GroupLog> {
        self.groups.get(&group).map(|group| &group.state)
    }
    /// Exact checkpoint predicate over this owner's protected canonical history.
    /// Visibility/durability gates remain the host provider's responsibility.
    pub fn matches_checkpoint_state(
        &mut self,
        expected: CheckpointLog<'_>,
    ) -> Result<bool, StorageError> {
        let group = self
            .groups
            .get_mut(&expected.group())
            .ok_or(StorageError::Rejected("group not bootstrapped"))?;
        Ok(group.comparison.compare(&group.state, &expected))
    }
    /// Transfer every canonical state to the caller, consuming its cache owner.
    pub fn into_states(self) -> BTreeMap<GroupIdentity, GroupLog> {
        self.groups
            .into_iter()
            .map(|(id, group)| (id, group.state))
            .collect()
    }
    /// One atomic unit per group. Consume original mutations, plan and correlate
    /// the whole batch, then install under this exclusive owner without callbacks.
    /// Exact error conditions and full-fallback rules follow [`apply_batch`].
    pub fn apply_batch(&mut self, mutations: Vec<LogMutation>) -> Result<(), StorageError> {
        let plans = self.plan(mutations)?;
        // Preflight includes every fallible validation/correlation. Unique group
        // units cannot change another unit's predecessor during this commit.
        for (id, plan) in plans {
            match plan {
                Plan::Create(group) => {
                    self.groups.insert(id, group);
                }
                Plan::Update(prepared) => {
                    let group = self.groups.get_mut(&id).expect("preflighted owned group");
                    let preserves = prepared.preserves_checkpoint_prefix();
                    group.summary = prepared.install(&mut group.state);
                    if preserves {
                        group.comparison.preserve(&group.state);
                    } else {
                        group.comparison.clear();
                    }
                }
            }
        }
        Ok(())
    }
    fn plan(
        &self,
        mutations: Vec<LogMutation>,
    ) -> Result<BTreeMap<GroupIdentity, Plan>, StorageError> {
        if mutations.is_empty() || mutations.len() > self.limits.max_batch_units {
            return Err(StorageError::Rejected("batch unit limit"));
        }
        let mut plans = BTreeMap::new();
        let mut created = 0;
        for mutation in mutations {
            let id = match &mutation {
                LogMutation::Create(bootstrap) => bootstrap.group,
                LogMutation::Update(update) => update.group,
            };
            if plans.contains_key(&id) {
                return Err(StorageError::Rejected(
                    "one atomic unit per group per batch",
                ));
            }
            let plan = match mutation {
                LogMutation::Create(bootstrap) => {
                    if self.groups.len() + created >= self.limits.max_groups
                        || self
                            .groups
                            .keys()
                            .chain(plans.keys())
                            .any(|group| group.id == id.id)
                    {
                        return Err(StorageError::Rejected(
                            "existing group identity or group limit",
                        ));
                    }
                    let state = stage_create(&bootstrap)?;
                    created += 1;
                    let summary = LogSummary::from_validated(&state);
                    Plan::Create(Group {
                        state,
                        summary,
                        comparison: CheckpointComparison::default(),
                    })
                }
                LogMutation::Update(update) => {
                    let group = self
                        .groups
                        .get(&id)
                        .ok_or(StorageError::Rejected("group not bootstrapped"))?;
                    let next = OwnedNextLog::plan_owned(
                        LogView::new(&group.state),
                        &group.summary,
                        update,
                        self.limits,
                    )?;
                    Plan::Update(next.prepare_install(&group.state)?)
                }
            };
            plans.insert(id, plan);
        }
        Ok(plans)
    }
}
