// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MembershipDrainAction {
    Wait,
    Transfer(LeadershipTransferRequest),
    /// Submit via Node::configure with normal readiness and execution-time
    /// placement authorization. The plan is not an authorization provider.
    Configure(ConfigurationRecord),
    Completed,
}
impl MembershipDrainPlan {
    pub fn next(
        &self,
        journal: &impl DrainJournal,
        core: &Raft,
    ) -> Result<MembershipDrainAction, DrainPlanError> {
        self.verify(journal)?;
        if let Some(entry) = self
            .retained
            .iter()
            .find(|entry| entry.group == core.state().bootstrap.group)
        {
            return entry.completed(core).map(|complete| {
                if complete {
                    MembershipDrainAction::Completed
                } else {
                    MembershipDrainAction::Wait
                }
            });
        }
        let entry = self
            .groups
            .iter()
            .find(|entry| entry.group == core.state().bootstrap.group)
            .ok_or(DrainPlanError::UnknownGroup)?;
        if entry.completed(core)? {
            return Ok(MembershipDrainAction::Completed);
        }
        let membership = core.membership();
        if membership.joint().is_some_and(|joint| {
            joint.operation != entry.change.joint.operation || joint.next != *entry.target()
        }) {
            return Err(DrainPlanError::StateChanged);
        }
        let original = membership.joint().is_none() && membership.stable() == &entry.original;
        let joint = membership.joint().is_some_and(|joint| {
            membership.stable() == &entry.original
                && matches!(&entry.change.joint.change, ConfigurationChange::Joint { id, .. } if *id == joint.id)
        });
        if !original && !joint && membership.stable() != entry.target() {
            return Err(DrainPlanError::StateChanged);
        }
        let state = core.state();
        if core.role() != Role::Leader
            || core.has_pending_dependency()
            || core.is_fenced()
            || state.term_at(state.commit_index) != Some(state.hard_state.term)
        {
            return Ok(MembershipDrainAction::Wait);
        }
        if original {
            return Ok(if core.local_node() != entry.handoff.node {
                MembershipDrainAction::Transfer(LeadershipTransferRequest {
                    operation: self.operation,
                    configuration: entry.original.id(),
                    target: entry.handoff,
                })
            } else {
                MembershipDrainAction::Configure(entry.change.joint.clone())
            });
        }
        if joint && membership.last_configuration_index() <= state.commit_index {
            Ok(MembershipDrainAction::Configure(
                entry.change.finalize.clone(),
            ))
        } else {
            Ok(MembershipDrainAction::Wait)
        }
    }
    pub(crate) fn completed(&self, core: &Raft) -> Result<bool, DrainPlanError> {
        let group = core.state().bootstrap.group;
        if let Some(entry) = self.groups.iter().find(|entry| entry.group == group) {
            entry.completed(core)
        } else {
            self.retained
                .iter()
                .find(|entry| entry.group == group)
                .ok_or(DrainPlanError::UnknownGroup)?
                .completed(core)
        }
    }
    pub(crate) fn target_configuration(
        &self,
        group: GroupIdentity,
    ) -> Result<ConfigurationId, DrainPlanError> {
        if let Some(entry) = self.groups.iter().find(|entry| entry.group == group) {
            Ok(entry.target_configuration())
        } else {
            self.retained
                .iter()
                .find(|entry| entry.group == group)
                .map(|entry| entry.original.id())
                .ok_or(DrainPlanError::UnknownGroup)
        }
    }
}
impl DrainMembershipGroup {
    pub(crate) fn completed(&self, core: &Raft) -> Result<bool, DrainPlanError> {
        if core.state().bootstrap.group != self.group {
            return Err(DrainPlanError::UnknownGroup);
        }
        let membership = core.membership();
        if membership.joint().is_some()
            || membership.stable() != self.target()
            || membership.last_configuration_index() > core.state().commit_index
        {
            return Ok(false);
        }
        let status = core
            .configuration_status(self.change.joint.operation)
            .map_err(|_| DrainPlanError::StateChanged)?;
        Ok(
            matches!(status.committed, ConfigurationProgress::Final { configuration, .. } if configuration == self.target().id())
                || matches!(
                    status.committed,
                    ConfigurationProgress::CompactedCompleted { .. }
                ),
        )
    }
    pub(crate) fn target_configuration(&self) -> ConfigurationId {
        self.target().id()
    }
}
