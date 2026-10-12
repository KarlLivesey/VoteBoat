// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use crate::membership::ConfigurationChange;
impl<P: LogicalActivationParent> ChildActivationDriver<'_, P> {
    pub(super) fn enrollment_source<
        S: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
        A: ProposalAdmission + LogicalResponseCodec + CheckpointStateMachine,
        U: SecureSession,
        C: LogCodec + Clone,
        V: SnapshotCodec + Clone,
        B: WireCodec + Clone,
    >(
        &self,
        source: &LogicalNode<S, T, E, A, U, C, V, B>,
        target: &LogicalApplication<C, V, B>,
        output: &SnapshotExportCompletion,
    ) -> Result<(LogicalEnrollmentPlan, LogicalEnrollmentOrigin), ChildActivationDriverError>
    where
        A::Receipt: ApplicationReceipt,
    {
        let wrong = ChildActivationDriverError::WrongBinding;
        let auth = self.authorization.as_ref().ok_or(wrong.clone())?;
        let observed = self.child.learners.as_ref().ok_or(wrong.clone())?;
        let LogicalInitialization::EnrollmentCandidate(plan) = target.initialization() else {
            return Err(wrong);
        };
        let mapping = target.mapping();
        let local = source.local();
        let owner = local.owner.identity();
        let original = local.owner.authorization().logical().ok_or(wrong.clone())?;
        let group = self.intent.mapping.child;
        let core = local.owner.core(group).ok_or(wrong.clone())?;
        let app = local.applications.get(&group).ok_or(wrong.clone())?;
        source.endpoint_chain(group).map_err(|_| wrong.clone())?;
        let record = auth.record(0);
        let ConfigurationChange::Learners(configuration) = &record.change else {
            return Err(wrong);
        };
        let learner = auth.learner();
        let ticket = output.ticket();
        if self.stopped
            || self.failure.is_some()
            || source.state() != NodeState::Running
            || self.parent.bindings() != self.bindings
            || auth.parent_owner() != self.bindings.0
            || mapping.parent != group
            || mapping.voter != learner.node
            || mapping.store != learner.store
            || original.mapping.parent != group
            || original.mapping == mapping
            || original.mapping.child == mapping.child
            || original.mapping.store != owner.store.identity
            || core.local_node() != original.mapping.voter
            || core.storage_binding() != owner.store
            || original.representative.logical_session != owner.store.session
            || plan.parent != group
            || plan.target.node != learner.node
            || plan.target.store != learner.store
            || plan.operation != record.operation
            || plan.expected != record.expected
            || plan.configuration != configuration.id()
            || plan.source_application_schema != app.schema_version()
            || core.membership().joint().is_some()
            || core.membership().stable() != configuration
            || core.membership().voter_store(original.mapping.voter) != Some(original.mapping.store)
            || observed.history.entries.len() != 1
            || observed.history.entries[0].payload
                != EntryPayload::Configuration(Box::new(record.clone()))
            || observed.history.child != group
            || observed.ticket.group != group
            || observed.owner != observed.ticket.binding.owner
            || observed.ticket.request != observed.barrier.request()
            || observed.barrier.group() != group
            || ticket.owner != owner
            || ticket.visit.owner != owner
            || ticket.visit.group != group
            || ticket.request.binding.store != owner.store
            || ticket.reference.store != owner.store.identity
            || core.state().snapshot != Some(ticket.reference)
        {
            return Err(wrong);
        }
        let committed = core
            .state()
            .membership_at(core.state().commit_index)
            .map_err(|_| wrong.clone())?;
        if committed.joint().is_some() || committed.stable() != configuration {
            return Err(wrong);
        }
        let origin = LogicalEnrollmentOrigin {
            mapping: original.mapping,
            representative: original.representative,
            export: ticket,
        };
        let SnapshotExportOutcome::Loaded(snapshot) = output.outcome() else {
            return Err(ChildActivationDriverError::ProviderViolation);
        };
        self.enrollment_snapshot(target, plan, origin, snapshot)?;
        Ok((plan, origin))
    }
    pub(super) fn enrollment_snapshot<
        C: LogCodec + Clone,
        V: SnapshotCodec + Clone,
        B: WireCodec + Clone,
    >(
        &self,
        target: &LogicalApplication<C, V, B>,
        plan: LogicalEnrollmentPlan,
        origin: LogicalEnrollmentOrigin,
        snapshot: &Snapshot,
    ) -> Result<(), ChildActivationDriverError> {
        let wrong = ChildActivationDriverError::WrongBinding;
        let observed = self.child.learners.as_ref().ok_or(wrong.clone())?;
        let entry = &observed.history.entries[0];
        let EntryPayload::Configuration(record) = &entry.payload else {
            return Err(wrong);
        };
        let ConfigurationChange::Learners(configuration) = &record.change else {
            return Err(wrong);
        };
        let member = snapshot.metadata.membership.as_ref().ok_or(wrong.clone())?;
        if snapshot.metadata.validate().is_err()
            || snapshot.metadata.bootstrap != target.parent_bootstrap
            || snapshot.metadata.application_schema != plan.source_application_schema
            || snapshot.metadata.index < entry.index
            || snapshot.metadata.term < entry.term
            || entry.index > observed.barrier.index()
            || entry.term > observed.barrier.term()
            || entry.index == 0
            || entry.term == 0
            || member.joint().is_some()
            || member.stable() != configuration
            || member.last_configuration_index() != entry.index
            || !member.operations().contains(&record.operation)
        {
            return Err(wrong);
        }
        let expected = target
            .snapshot_reference(snapshot, origin.export.reference.generation)
            .map_err(ChildActivationDriverError::Application)?;
        if (crate::snapshot::SnapshotRef {
            store: origin.mapping.store,
            ..expected
        }) != origin.export.reference
        {
            return Err(wrong);
        }
        let extra = crate::logical_voter::query::snapshot_charge(snapshot)
            .checked_add(entry.retained_payload_bytes())
            .and_then(|n| n.checked_add(std::mem::size_of::<LogicalLearnerImport>()))
            .ok_or(ChildActivationDriverError::Overloaded)?;
        let auth = self.authorization.as_ref().ok_or(wrong)?;
        let authorization =
            LogicalActivationAuthorization::retained_bound(&self.intent, auth.profile())
                .map_err(ChildActivationDriverError::Application)?;
        if self
            .parent_reservation
            .checked_add(authorization)
            .and_then(|n| n.checked_add(self.child.learners_reservation.checked_mul(2)?))
            .and_then(|n| n.checked_add(extra))
            .is_none_or(|n| n > self.retained_limit)
        {
            return Err(ChildActivationDriverError::Overloaded);
        }
        Ok(())
    }
}
