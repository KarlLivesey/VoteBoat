// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Applied deterministic candidate state, not native consumed enrollment authority.
use super::*;
use crate::{
    membership::{Configuration, ConfigurationChange, ConfigurationRecord},
    snapshot_worker::*,
};
mod empty;
pub(super) mod fixture;
mod formats;
mod recovery;
mod refusals;
use fixture::{candidate, enrollment, import};

#[test]
fn v5_empty_checkpoint_schema_and_unchanged_query_and_command_shapes() {
    let a = app();
    let bytes = a.checkpoint(usize::MAX).unwrap();
    assert_eq!(bytes, include_bytes!("formats/v5-empty.bin"));
    assert_eq!(
        app().restore_checkpoint(1, 0, include_bytes!("enrollment/v1-empty.bin")),
        Err(ApplicationError::UnsupportedSchema)
    );
    assert_eq!(a.schema_version(), 5);
    assert_eq!(a.deployment_requirements().unwrap().application_schema, 5);
    assert_eq!(a.initialization(), LogicalInitialization::BootstrapVoter);
    assert_eq!(a.logical_response_schema(), LOGICAL_RESPONSE_SCHEMA);
    assert_eq!(
        a.read_at(0, LogicalQuery::Enrollment),
        Err(ApplicationError::UnsupportedSchema)
    );
    let c = command(
        1,
        1,
        CommandBody::Claim {
            expected_epoch: None,
        },
    );
    assert_eq!(
        &a.encode_command(&c, a.limits.max_command_bytes).unwrap()[..8],
        b"VBLOGA03"
    );
}
#[test]
fn candidate_import_materializes_exact_learner_pin_lineage_and_no_source_ballot() {
    let mut a = candidate();
    assert_eq!(a.schema_version(), 6);
    assert_eq!(
        a.read_at(0, LogicalQuery::Image).unwrap(),
        LogicalRead::Uninitialized
    );
    let input = import(&a);
    let bytes = input.snapshot.application.clone();
    let c = enrollment(input.clone(), 11);
    let receipt = apply(&mut a, &c);
    assert_eq!(receipt.outcome, LogicalOutcome::Claimed);
    let parent = a.state.parent.as_ref().unwrap();
    assert_eq!(
        parent.hard_state,
        HardState {
            term: input.snapshot.metadata.term,
            voted_for: None
        }
    );
    assert_eq!(parent.commit_index, input.snapshot.metadata.index);
    assert!(parent.entries.is_empty());
    let reference = parent.snapshot.unwrap();
    assert_eq!(reference.store, store(3));
    assert_eq!(reference.generation.get(), 1);
    assert_eq!(input.origin.export.reference.generation.get(), 9);
    assert_eq!(a.state.snapshots[0], (reference, input.snapshot));
    assert_eq!(a.state.snapshots[0].1.application, bytes);
    let binding = StoreBinding {
        identity: store(3),
        session: StoreSession::new(1).unwrap(),
    };
    let core = crate::raft::Raft::recover_member_verified(
        NodeId::new(3).unwrap(),
        binding,
        parent.clone(),
        a.limits.parent_log,
    )
    .unwrap();
    assert_eq!(
        core.membership().replica_store(NodeId::new(3).unwrap()),
        Some(store(3))
    );
    assert_eq!(core.membership().voter_store(NodeId::new(3).unwrap()), None);
    let view = a
        .read_at(a.applied_index(), LogicalQuery::Enrollment)
        .unwrap();
    let LogicalRead::Enrollment(view) = view else {
        panic!("actual enrollment diagnostics");
    };
    assert_eq!(view.record.unwrap().target_reference, reference);
    assert!(apply(&mut a, &c).duplicate);
    assert_eq!(a.state.records[&c.operation].first_index, 1);
}
