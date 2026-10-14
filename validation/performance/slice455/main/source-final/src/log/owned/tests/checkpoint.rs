// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Pure transition controls; tickets below model the host's completed barrier.
use super::*;
use crate::{
    raft::{Effect, Event, Raft},
    snapshot::SnapshotRef,
};
use std::collections::VecDeque;

fn complete(
    core: &mut Raft,
    owned: &mut OwnedLogState,
    initial: Vec<Effect>,
    sequence: &mut u64,
    history: &mut Vec<LogMutation>,
) {
    let mut effects = VecDeque::from(initial);
    while let Some(effect) = effects.pop_front() {
        if let Effect::Persist(update) = effect {
            let mutation = LogMutation::Update(update.clone());
            owned.apply_batch(vec![mutation.clone()]).unwrap();
            history.push(mutation);
            *sequence += 1;
            let state = owned.get(update.group).unwrap();
            let ticket = LogTicket {
                binding: core.storage_binding(),
                batch: *sequence,
                group: update.group,
                revision: state.revision,
                generation: state.generation,
                last_index: state.last_index(),
                term: state.hard_state.term,
            };
            core.admit_effect(&update, ticket).unwrap();
            effects.extend(
                core.complete(&DurableLog {
                    tickets: vec![ticket],
                })
                .unwrap(),
            );
        }
    }
}
fn check(core: &Raft, owned: &mut OwnedLogState, compared: usize, reused: usize) {
    assert_eq!(
        owned.matches_checkpoint_state(core.checkpoint_log()),
        Ok(true)
    );
    let group = &owned.groups[&core.state().bootstrap.group];
    assert_eq!(group.state, *core.state());
    assert_eq!(group.comparison.work().compared, compared);
    assert_eq!(group.comparison.work().reused, reused);
}

#[test]
fn genuine_core_and_owned_plans_preserve_append_compact_and_metadata_but_reset_restart() {
    let mut owned = setup();
    let first = *owned.groups.keys().next().unwrap();
    let state = owned.get(first).unwrap().clone();
    let binding = StoreBinding {
        identity: *state.bootstrap.voter_stores.values().next().unwrap(),
        session: StoreSession::new(1).unwrap(),
    };
    let mut history = vec![LogMutation::Create(state.bootstrap.clone())];
    let mut core = Raft::recover(NodeId::new(1).unwrap(), binding, state, owned.limits()).unwrap();
    let initial = core.checkpoint_log().lineage().clone();
    let mut sequence = 0;
    let effects = core.step(Event::Campaign).unwrap();
    complete(&mut core, &mut owned, effects, &mut sequence, &mut history);
    assert_eq!(initial, *core.checkpoint_log().lineage());
    assert_eq!(core.state().hard_state.term, 1);
    assert_eq!(core.state().entries[0].payload, EntryPayload::Noop);
    check(&core, &mut owned, 1, 0);
    check(&core, &mut owned, 0, 1);
    let lineage = core.checkpoint_log().lineage().clone();
    for id in 1..=2 {
        let effects = core
            .step(Event::Propose {
                operation: OperationId::new((1 << 127) | id).unwrap(),
                bytes: vec![7; usize::try_from(id - 1).unwrap()],
            })
            .unwrap();
        complete(&mut core, &mut owned, effects, &mut sequence, &mut history);
        assert_eq!(lineage, *core.checkpoint_log().lineage());
        check(&core, &mut owned, 1, usize::try_from(id).unwrap());
    }
    let reference = SnapshotRef {
        store: binding.identity,
        group: first,
        configuration: core.state().bootstrap.configuration,
        generation: SnapshotGeneration::new(1).unwrap(),
        index: 2,
        term: 1,
        application_schema: 1,
        file_bytes: 100,
        checksum: 7,
    };
    let effects = core.begin_compact(reference).unwrap();
    complete(&mut core, &mut owned, effects, &mut sequence, &mut history);
    assert_eq!(lineage, *core.checkpoint_log().lineage());
    assert_eq!(
        owned.groups[&first].comparison.verified_interval(),
        Some((3, 3))
    );
    check(&core, &mut owned, 0, 1);
    let mut restored = OwnedLogState::new(owned.limits()).unwrap();
    for mutation in history {
        restored.apply_batch(vec![mutation]).unwrap();
    }
    check(&core, &mut restored, 1, 0);
    let recovered = Raft::recover_verified(
        NodeId::new(1).unwrap(),
        binding,
        core.state().clone(),
        owned.limits(),
    )
    .unwrap();
    assert_ne!(lineage, *recovered.checkpoint_log().lineage());
    check(&recovered, &mut owned, 1, 0);
    check(&core, &mut owned, 1, 0);
    let effects = core.step(Event::Campaign).unwrap();
    complete(
        &mut core,
        &mut owned,
        effects,
        &mut sequence,
        &mut Vec::new(),
    );
    assert_eq!(lineage, *core.checkpoint_log().lineage());
    assert_eq!(core.state().hard_state.term, 2);
    assert_eq!(core.state().entries.last().unwrap().term, 2);
    check(&core, &mut owned, 1, 1);
}

#[test]
fn failed_owner_changes_lineage_and_arbitrary_same_revision_mutations_remain_visible() {
    let mut owned = setup();
    let first = *owned.groups.keys().next().unwrap();
    let state = owned.get(first).unwrap().clone();
    let binding = StoreBinding {
        identity: *state.bootstrap.voter_stores.values().next().unwrap(),
        session: StoreSession::new(1).unwrap(),
    };
    let mut core = Raft::recover(NodeId::new(1).unwrap(), binding, state, owned.limits()).unwrap();
    let effects = core.step(Event::Campaign).unwrap();
    complete(&mut core, &mut owned, effects, &mut 0, &mut Vec::new());
    let lineage = core.checkpoint_log().lineage().clone();
    check(&core, &mut owned, 1, 0);
    core.storage_failed();
    assert_ne!(lineage, *core.checkpoint_log().lineage());
    check(&core, &mut owned, 1, 0);
    let mut arbitrary = core.state().clone();
    arbitrary.entries[0].payload = EntryPayload::Command {
        operation: OperationId::new(u128::MAX).unwrap(),
        bytes: Vec::new().into(),
    };
    assert_eq!(arbitrary.revision, core.state().revision);
    assert_ne!(arbitrary, *owned.get(first).unwrap());
    let other = Raft::recover(NodeId::new(1).unwrap(), binding, arbitrary, owned.limits()).unwrap();
    assert_eq!(
        owned.matches_checkpoint_state(other.checkpoint_log()),
        Ok(false)
    );
    assert_eq!(owned.groups[&first].comparison.verified_interval(), None);
    check(&core, &mut owned, 1, 0);
}
