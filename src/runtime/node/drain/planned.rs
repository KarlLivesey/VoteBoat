// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use crate::drain::{DrainJournal, DrainPlanError, MembershipDrainPlan};
impl<
        S: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
        A: ProposalAdmission + BoundedReadableStateMachine + CheckpointStateMachine,
        W: PersistenceWorker,
        O: OutboundQueue,
        H: SnapshotWorker,
        C: PeerConnector,
        F: PeerTransportFactory<C::Session>,
    > Node<S, T, E, A, W, O, H, C, F>
where
    A::Receipt: ApplicationReceipt,
    C::Endpoint: Clone,
{
    /// Local durable final-membership evidence plus owned-work quiescence.
    /// Does not certify current remote availability or remove the retained
    /// learner. The verified journal must have been restored before polling.
    pub fn membership_drain_ready(
        &self,
        plan: &MembershipDrainPlan,
        journal: &impl DrainJournal,
    ) -> Result<bool, DrainPlanError> {
        let record = plan.verify(journal)?;
        if self.drain_history.as_ref() != Some(&record) {
            return Err(DrainPlanError::Unrestored);
        }
        let drain = self.drain.as_ref().ok_or(DrainPlanError::Unrestored)?;
        if self.state != NodeState::Running
            || drain.resuming
            || !plan
                .assignments()
                .iter()
                .map(|g| g.group)
                .eq(self.local.owner.groups())
            || self.local.clients.usage().requests != 0
            || self.local.reads.usage().requests != 0
            || !self.configuration.is_drained()
            || !self.local.results.is_drained()
        {
            return Ok(false);
        }
        for entry in plan.assignments() {
            let core = self
                .local
                .owner
                .core(entry.group)
                .ok_or(DrainPlanError::UnknownGroup)?;
            if core.local_node() != record.owner.node
                || core.storage_binding().identity != record.owner.store
            {
                return Err(DrainPlanError::WrongIntent);
            }
            if !plan.completed(core)?
                || !self
                    .local
                    .applications
                    .get(&entry.group)
                    .is_some_and(|app| app.applied_index() >= core.state().commit_index)
                || group_state(
                    core,
                    DrainGroup {
                        group: entry.group,
                        configuration: plan.target_configuration(entry.group)?,
                    },
                ) != DrainGroupState::LocallyQuiescent
            {
                return Ok(false);
            }
        }
        Ok(true)
    }
}
