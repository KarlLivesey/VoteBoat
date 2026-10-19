// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use crate::membership::ConfigurationChange;
impl<C: LogCodec + Clone, S: SnapshotCodec + Clone, W: WireCodec + Clone>
    LogicalApplication<C, S, W>
{
    pub(super) fn enroll(
        &mut self,
        c: &Command,
        index: u64,
        term: u64,
        import: &LogicalLearnerImport,
    ) -> Result<LogicalOutcome, ApplicationError> {
        if self.initialization != LogicalInitialization::EnrollmentCandidate(import.plan)
            || self.state.parent.is_some()
            || self.state.representative.is_some()
            || self.state.enrollment.is_some()
            || c.representative.epoch.get() != 1
            || c.representative.logical_session.get() != 1
            || !self.valid_import(import)?
        {
            return Ok(LogicalOutcome::Refused(LogicalRefusal::SnapshotConflict));
        }
        let reference =
            self.snapshot_reference(&import.snapshot, SnapshotGeneration::new(1).unwrap())?;
        let mut groups = BTreeMap::new();
        apply_batch(
            &mut groups,
            &[LogMutation::Create(self.parent_bootstrap.clone())],
            self.limits.parent_log,
        )
        .map_err(|_| ApplicationError::InvalidCommand)?;
        let revision = groups[&self.mapping.parent].revision;
        apply_batch(
            &mut groups,
            &[LogMutation::Update(LogUpdate {
                group: self.mapping.parent,
                expected_revision: revision,
                hard_state: crate::contracts::HardState {
                    term: import.snapshot.metadata.term,
                    voted_for: None,
                },
                commit_index: import.snapshot.metadata.index,
                suffix: None,
                snapshot: Some(reference),
                snapshot_membership: import.snapshot.metadata.membership.clone(),
            })],
            self.limits.parent_log,
        )
        .map_err(|_| ApplicationError::InvalidCommand)?;
        let parent = groups
            .remove(&self.mapping.parent)
            .ok_or(ApplicationError::InvalidCommand)?;
        crate::raft::Raft::recover_member_verified(
            self.mapping.voter,
            self.binding(c.representative),
            parent.clone(),
            self.limits.parent_log,
        )
        .map_err(|_| ApplicationError::InvalidCommand)?;
        self.state.parent = Some(parent);
        self.state
            .snapshots
            .push((reference, import.snapshot.clone()));
        self.state.published = Some(reference);
        self.state.snapshot_generation = self
            .state
            .snapshot_generation
            .max(reference.generation.get());
        self.state.representative = Some(c.representative);
        self.state.enrollment = Some(Box::new(LogicalEnrollmentRecord {
            plan: import.plan,
            origin: import.origin,
            learners: import.learners.clone(),
            target_reference: reference,
            operation: c.operation,
            first_index: index,
            first_term: term,
        }));
        Ok(LogicalOutcome::Claimed)
    }
    pub(in crate::logical_voter) fn valid_import(
        &self,
        import: &LogicalLearnerImport,
    ) -> Result<bool, ApplicationError> {
        let snapshot = &import.snapshot;
        let Some(membership) = &snapshot.metadata.membership else {
            return Ok(false);
        };
        let EntryPayload::Configuration(record) = &import.learners.payload else {
            return Ok(false);
        };
        let ConfigurationChange::Learners(configuration) = &record.change else {
            return Ok(false);
        };
        let p = import.plan;
        if snapshot.metadata.application_schema != p.source_application_schema
            || snapshot.metadata.bootstrap != self.parent_bootstrap
            || snapshot.metadata.validate().is_err()
            || snapshot.application.capacity() > self.limits.parent_snapshots.max_application_bytes
            || membership.joint().is_some()
            || membership.stable() != configuration
            || !membership.operations().contains(&p.operation)
            || membership.id() != p.configuration
            || membership.stable().learners().get(&self.mapping.voter) != Some(&self.mapping.store)
            || record.operation != p.operation
            || record.expected != p.expected
            || configuration.id() != p.configuration
            || import.learners.index == 0
            || import.learners.index > snapshot.metadata.index
            || import.learners.term == 0
            || import.learners.term > snapshot.metadata.term
            || membership.last_configuration_index() != import.learners.index
            || (import.learners.index == snapshot.metadata.index
                && import.learners.term != snapshot.metadata.term)
        {
            return Ok(false);
        }
        self.valid_origin(import)
    }
    fn valid_origin(&self, import: &LogicalLearnerImport) -> Result<bool, ApplicationError> {
        let o = import.origin;
        let e = o.export;
        let binding = StoreBinding {
            identity: o.mapping.store,
            session: o.representative.logical_session,
        };
        if o.mapping.parent != self.mapping.parent
            || o.mapping.store == self.mapping.store
            || o.mapping.voter == self.mapping.voter
            || o.mapping.child == self.mapping.child
            || e.owner.store != binding
            || e.request.binding.store != binding
            || e.request.sequence == 0
            || e.visit.owner != e.owner
            || e.visit.group != self.mapping.parent
            || e.visit.sequence == 0
            || e.reference.store != o.mapping.store
            || import
                .snapshot
                .metadata
                .membership
                .as_ref()
                .is_none_or(|m| m.voter_store(o.mapping.voter) != Some(o.mapping.store))
        {
            return Ok(false);
        }
        let expected = self.snapshot_reference(&import.snapshot, e.reference.generation)?;
        Ok(SnapshotRef {
            store: o.mapping.store,
            ..expected
        } == e.reference)
    }
}
