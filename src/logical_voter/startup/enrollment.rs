// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Lineage is accepted only alongside consumed original lower quorum reads.
use super::*;
pub(in crate::logical_voter) fn validate_enrollment(
    view: &LogicalEnrollmentView,
    initialization: LogicalInitialization,
    mapping: LogicalStoreMapping,
    barrier: ReadBarrier,
) -> Result<(), StorageError> {
    let invalid = || StorageError::Corrupt("logical enrollment observation scope");
    if initialization != LogicalInitialization::EnrollmentCandidate(view.plan) {
        return Err(invalid());
    }
    let r = view.record.as_ref().ok_or_else(invalid)?;
    let EntryPayload::Configuration(record) = &r.learners.payload else {
        return Err(invalid());
    };
    let crate::membership::ConfigurationChange::Learners(next) = &record.change else {
        return Err(invalid());
    };
    let o = r.origin;
    let binding = StoreBinding {
        identity: o.mapping.store,
        session: o.representative.logical_session,
    };
    if r.plan != view.plan
        || r.plan.parent != mapping.parent
        || r.plan.target.node != mapping.voter
        || r.plan.target.store != mapping.store
        || r.operation.get() == 0
        || r.first_index == 0
        || r.first_term == 0
        || barrier.group() != mapping.child
        || r.first_index > barrier.index()
        || r.first_term > barrier.term()
        || r.learners.index == 0
        || r.learners.term == 0
        || record.operation != r.plan.operation
        || record.expected != r.plan.expected
        || next.id() != r.plan.configuration
        || next.learners().get(&mapping.voter) != Some(&mapping.store)
        || r.target_reference.store != mapping.store
        || r.target_reference.generation.get() != 1
        || r.target_reference.group != mapping.parent
        || r.target_reference.configuration != r.plan.configuration
        || r.target_reference.application_schema != r.plan.source_application_schema
        || r.learners.index > r.target_reference.index
        || r.learners.term > r.target_reference.term
        || o.mapping.parent != mapping.parent
        || o.mapping.voter == mapping.voter
        || o.mapping.store == mapping.store
        || o.mapping.child == mapping.child
        || o.export.owner.store != binding
        || o.export.request.binding.store != binding
        || o.export.visit.owner != o.export.owner
        || o.export.visit.group != mapping.parent
        || o.export.request.sequence == 0
        || o.export.visit.sequence == 0
        || o.export.reference.store != o.mapping.store
        || o.export.reference
            != (SnapshotRef {
                store: o.mapping.store,
                generation: o.export.reference.generation,
                ..r.target_reference
            })
    {
        return Err(invalid());
    }
    Ok(())
}
pub(in crate::logical_voter) fn validate_initialization(
    image: &LogicalImage,
    barrier: ReadBarrier,
    context: (LogicalInitialization, Option<&ObservedLogicalEnrollment>),
) -> Result<(), StorageError> {
    let (initialization, observed) = context;
    if !initialization.matches(image.mapping, &image.parent.bootstrap) {
        return Err(StorageError::Corrupt("logical immutable initialization"));
    }
    match initialization {
        LogicalInitialization::BootstrapVoter if observed.is_none() => Ok(()),
        LogicalInitialization::EnrollmentCandidate(_) => {
            let o = observed.ok_or(StorageError::Corrupt(
                "logical enrollment provenance absent",
            ))?;
            validate_enrollment(&o.view, initialization, image.mapping, o.barrier)?;
            let accepted = image
                .parent
                .membership()
                .map_err(|_| StorageError::Corrupt("logical accepted membership"))?;
            let committed = image
                .parent
                .membership_at(image.parent.commit_index)
                .map_err(|_| StorageError::Corrupt("logical committed membership"))?;
            if accepted.replica_store(image.mapping.voter) != Some(image.mapping.store)
                || committed.replica_store(image.mapping.voter) != Some(image.mapping.store)
                || !committed.operations().contains(&o.view.plan.operation)
                || !accepted.operations().contains(&o.view.plan.operation)
                || image.parent.snapshot.is_none_or(|r| {
                    r.application_schema != o.view.plan.source_application_schema
                        || r.generation.get()
                            < o.view
                                .record
                                .as_ref()
                                .expect("checked record")
                                .target_reference
                                .generation
                                .get()
                })
                || o.ticket.group != image.mapping.child
                || o.ticket.binding.owner != o.owner
                || o.ticket.request != o.barrier.request()
                || o.barrier.index() > barrier.index()
                || o.barrier.term() > barrier.term()
            {
                return Err(StorageError::Corrupt(
                    "logical current candidate membership",
                ));
            }
            Ok(())
        }
        LogicalInitialization::BootstrapVoter => {
            Err(StorageError::Corrupt("unexpected enrollment provenance"))
        }
    }
}
