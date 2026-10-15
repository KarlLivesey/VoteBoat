// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use crate::{log::*, worker::PersistUnitId};

fn selected() -> Shard<Ready> {
    selected_originals(8)
}
fn selected_originals(originals: usize) -> Shard<Ready> {
    selected_budget(originals, 1024 * 1024)
}
fn selected_budget(originals: usize, max_retained_bytes: usize) -> Shard<Ready> {
    shard_with_append_window(
        ShardLimits::default(),
        LogLimits::default(),
        BatchLimits::default(),
        Some(AppendWindowLimits {
            max_units: originals.min(4),
            max_originals: originals,
            max_retained_bytes,
        }),
    )
}
fn replaced_core(original: &Raft) -> Raft {
    use crate::quorum::{Limits, Policy, Tree};
    let mut state = original.state().clone();
    let voters = (1..=3).map(|n| NodeId::new(n).unwrap()).collect::<Vec<_>>();
    state.bootstrap.policy = Policy::new(
        Tree::Majority(voters.iter().copied().map(Tree::Voter).collect()),
        Limits::default(),
    )
    .unwrap();
    for (node, id) in [(voters[1], 3), (voters[2], 4)] {
        state.bootstrap.voter_stores.insert(
            node,
            StoreIdentity {
                id: StoreId::new(id).unwrap(),
                incarnation: StoreIncarnation::new(1).unwrap(),
            },
        );
    }
    Raft::recover(
        original.local_node(),
        original.storage_binding(),
        state,
        LogLimits::default(),
    )
    .unwrap()
}
fn stage(shard: &mut Shard<Ready>) -> (PersistUnitId, LogTicket) {
    let visit = shard.poll(MonoTime(0)).unwrap().unwrap();
    let (step, append) = shard
        .step_append_checked(visit, MonoTime(0), 1, |_, _, _| Ok(()))
        .unwrap()
        .unwrap();
    let effects = step.result.unwrap();
    let [Effect::Persist(update)] = effects.as_slice() else {
        panic!("missing append");
    };
    let core = shard.core(group()).unwrap();
    let revision = LogRevision::new(update.expected_revision.get() + 1).unwrap();
    let ticket = LogTicket {
        binding: core.storage_binding(),
        batch: revision.get() * 10,
        group: group(),
        revision,
        generation: core.state().generation,
        last_index: update
            .suffix
            .as_ref()
            .unwrap()
            .entries
            .last()
            .unwrap()
            .index,
        term: update.hard_state.term,
    };
    let id = PersistUnitId {
        effect: EffectTicket {
            visit,
            sequence: visit.sequence,
        },
        append: Some(append),
    };
    shard
        .with_persist_unit(id, |core| {
            core.validate_append_effect(append, update).unwrap();
            core.admitted_append(append, ticket).unwrap();
        })
        .unwrap();
    shard.park_append_visit(visit).unwrap();
    (id, ticket)
}
fn complete(shard: &mut Shard<Ready>, id: PersistUnitId, ticket: LogTicket) -> AppendCompletion {
    shard
        .with_persist_unit(id, |core| {
            core.complete_append_unit(
                id.append.unwrap(),
                &DurableLog {
                    tickets: vec![ticket],
                },
            )
        })
        .unwrap()
        .unwrap()
}
fn retire(shard: &mut Shard<Ready>, anchor: VisitTicket, completion: AppendCompletion) {
    let AppendCompletion::Retired { effects, .. } = completion else {
        panic!("not retired");
    };
    shard.activate_append_anchor(anchor).unwrap();
    shard
        .with_core(anchor, |core| finish(core, effects))
        .unwrap();
    shard.finish_append_visits(anchor).unwrap();
}

#[test]
fn parked_inputs_keep_exact_visits_and_wait_for_every_durable_unit() {
    let mut shard = selected();
    for id in 1..=3 {
        shard.admit_tracked(group(), command(id)).unwrap();
    }
    let before = shard.core(group()).unwrap().state().clone();
    let a = stage(&mut shard);
    let b = stage(&mut shard);
    let c = stage(&mut shard);
    assert_eq!(shard.groups[&group()].turn, 3);
    assert_eq!(shard.active_visits(), 3);
    assert_eq!(shard.usage().items, 3);
    assert!(shard.with_core(a.0.visit(), |_| ()).is_err());
    let mut foreign = a.0;
    foreign.effect.visit.sequence = c.0.visit().sequence;
    assert!(shard.with_persist_unit(foreign, |_| ()).is_err());
    assert!(matches!(
        complete(&mut shard, b.0, b.1),
        AppendCompletion::Waiting
    ));
    assert_eq!(shard.core(group()).unwrap().state(), &before);
    assert!(matches!(
        complete(&mut shard, a.0, a.1),
        AppendCompletion::Waiting
    ));
    let done = complete(&mut shard, c.0, c.1);
    retire(&mut shard, c.0.visit(), done);
    assert_eq!(shard.core(group()).unwrap().state().last_index(), 4);
    assert_eq!(shard.core(group()).unwrap().state().commit_index, 4);
    assert_eq!(shard.active_visits(), 0);
    assert_eq!(shard.usage().items, 0);
}

#[test]
fn failed_empty_successor_visit_seals_without_consuming_or_releasing_old_inputs() {
    let mut shard = selected_originals(2);
    for id in 1..=3 {
        shard.admit_tracked(group(), command(id)).unwrap();
    }
    let a = stage(&mut shard);
    let fresh = shard.poll(MonoTime(0)).unwrap().unwrap();
    assert!(shard
        .step_append_checked(fresh, MonoTime(0), 2, |_, _, _| Ok(()))
        .unwrap()
        .is_none());
    assert_eq!(shard.finish(fresh), Err(RuntimeError::DependencyPending));
    shard.cancel_append_candidate(fresh).unwrap();
    assert!(!shard.core(group()).unwrap().can_stage_append_window());
    assert_eq!(shard.usage().items, 3);
    assert_eq!(shard.groups[&group()].queues[1].len(), 2);
    assert_eq!(shard.active_visits(), 1);
    let done = complete(&mut shard, a.0, a.1);
    retire(&mut shard, a.0.visit(), done);
    assert_eq!(shard.usage().items, 2);
    let next = shard.poll(MonoTime(0)).unwrap().unwrap();
    assert_eq!(
        shard
            .step_next(next, MonoTime(0))
            .unwrap()
            .unwrap()
            .operation,
        Some(OperationId::new(2).unwrap())
    );
}

#[test]
fn queued_control_seals_and_remains_queued_until_the_window_drains() {
    let mut shard = selected();
    shard.admit_tracked(group(), command(1)).unwrap();
    let a = stage(&mut shard);
    shard.admit(group(), Event::Heartbeat).unwrap();
    assert!(!shard.core(group()).unwrap().can_stage_append_window());
    assert!(shard.poll(MonoTime(0)).unwrap().is_none());
    assert_eq!(shard.usage().items, 2);
    let done = complete(&mut shard, a.0, a.1);
    retire(&mut shard, a.0.visit(), done);
    let visit = shard.poll(MonoTime(0)).unwrap().unwrap();
    let step = shard.step_next(visit, MonoTime(0)).unwrap().unwrap();
    assert!(step.operation.is_none());
    assert_eq!(shard.groups[&group()].queues[0].len(), 0);
}

#[test]
fn parked_scoped_callback_checks_replaced_core_even_when_callback_returns_error() {
    // Direct scoped-access regression; the public owner's parked early-Send
    // callback invokes this same accessor. No physical worker claim is made.
    for conflict in [false, true] {
        let mut shard = selected();
        let local = crate::secure::LocalIdentity {
            node: shard.core(group()).unwrap().local_node(),
            store: shard.owner.store,
        };
        let history = if conflict {
            BTreeMap::from([(
                NodeId::new(2).unwrap(),
                StoreIdentity {
                    id: StoreId::new(2).unwrap(),
                    incarnation: StoreIncarnation::new(1).unwrap(),
                },
            )])
        } else {
            BTreeMap::new()
        };
        shard
            .set_connection_budget(
                ConnectionBudget::new(local, if conflict { 2 } else { 1 }, history).unwrap(),
            )
            .unwrap();
        let replacement = replaced_core(shard.core(group()).unwrap());
        shard.admit_tracked(group(), command(1)).unwrap();
        shard.admit_tracked(group(), command(2)).unwrap();
        let (id, _) = stage(&mut shard);
        assert_eq!(
            shard
                .with_persist_unit(id, |_| Err::<(), _>(RaftError::Busy))
                .unwrap(),
            Err(RaftError::Busy)
        );
        assert_eq!(shard.usage().items, 2);
        let result = shard.with_persist_unit(id, |core| {
            *core = replacement;
            Err::<(), _>(RaftError::Busy)
        });
        assert_eq!(
            result,
            Err(if conflict {
                RuntimeError::PeerStoreConflict
            } else {
                RuntimeError::PeerCapacity
            })
        );
        assert!(shard.core(group()).unwrap().is_fenced());
        assert_eq!(shard.usage(), Usage::default());
        assert_eq!(shard.active_visits(), 0);
        assert!(shard.groups[&group()].queues.iter().all(VecDeque::is_empty));
        assert!(shard.with_persist_unit(id, |_| ()).is_err());
    }
}

#[test]
fn parked_scoped_fence_preserves_callback_result_and_ownership_until_explicit_stop() {
    let mut shard = selected();
    let local = crate::secure::LocalIdentity {
        node: shard.core(group()).unwrap().local_node(),
        store: shard.owner.store,
    };
    shard
        .set_connection_budget(ConnectionBudget::new(local, 1, BTreeMap::new()).unwrap())
        .unwrap();
    shard.admit_tracked(group(), command(1)).unwrap();
    let queued = shard.admit_tracked(group(), command(2)).unwrap();
    let (id, _) = stage(&mut shard);
    let result = shard.with_persist_unit(id, |core| {
        core.storage_failed();
        Err::<(), _>(RaftError::Busy)
    });
    assert_eq!(result, Ok(Err(RaftError::Busy)));
    assert!(shard.core(group()).unwrap().is_fenced());
    assert_eq!(shard.usage().items, 2);
    assert_eq!(shard.active_visits(), 1);
    let stopped = shard.stop_group(group()).unwrap();
    assert!(stopped.had_active_visit);
    assert_eq!(stopped.admissions, vec![queued]);
    assert_eq!(stopped.queued.len(), 1);
    assert_eq!(shard.usage(), Usage::default());
    assert_eq!(shard.active_visits(), 0);
}

#[test]
fn caught_parked_callback_panic_keeps_reference_admission_and_original_credits() {
    use crate::membership::{ConfigurationChange, ConfigurationRecord};
    let mut shard = selected();
    let local = crate::secure::LocalIdentity {
        node: shard.core(group()).unwrap().local_node(),
        store: shard.owner.store,
    };
    shard
        .set_connection_budget(ConnectionBudget::new(local, 1, BTreeMap::new()).unwrap())
        .unwrap();
    let replacement = replaced_core(shard.core(group()).unwrap());
    shard.admit_tracked(group(), command(1)).unwrap();
    let queued = shard.admit_tracked(group(), command(2)).unwrap();
    let (id, _) = stage(&mut shard);
    let before = shard.usage();
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        shard.with_persist_unit(id, |current| {
            *current = replacement;
            panic!("parked host callback unwind after replacement");
        })
    }));
    assert!(panic.is_err());
    // Final advertises no new stores. Without the incomplete-callback guard,
    // known-input eligibility could overlook the replacement Core's new peers.
    // Resource refusal precedes any later configuration/protocol execution.
    let incoming = Event::Configure(Box::new(ConfigurationProposal {
        record: ConfigurationRecord {
            operation: OperationId::new(3).unwrap(),
            expected: ConfigurationId::new(1).unwrap(),
            change: ConfigurationChange::Final {
                id: ConfigurationId::new(2).unwrap(),
            },
        },
        readiness: Vec::new(),
        requirements: ReadinessRequirements {
            application_schema: 1,
            command_bytes: 16,
            snapshot_bytes: 1024,
        },
    }));
    let rejected = shard.admit(group(), incoming.clone()).unwrap_err();
    assert_eq!(rejected.reason, RuntimeError::PeerCapacity);
    assert_eq!(*rejected.event, incoming);
    assert_eq!(shard.usage(), before);
    assert_eq!(shard.active_visits(), 1);
    let stopped = shard.stop_group(group()).unwrap();
    assert!(stopped.had_active_visit);
    assert_eq!(stopped.admissions, vec![queued]);
    assert_eq!(stopped.queued.len(), 1);
    assert_eq!(shard.usage(), Usage::default());
    assert_eq!(shard.active_visits(), 0);
}

#[cfg(feature = "native")]
#[test]
fn opening_fallback_replays_the_full_checked_prefix_and_terminal_error_once() {
    use crate::native::runtime::{DeadlineQueue, JitterEntropy};
    let reference = selected_originals(2);
    let proposals = [1u128, 2].map(|id| Proposal {
        operation: OperationId::new(id).unwrap(),
        bytes: id.to_le_bytes().to_vec(),
    });
    let ceiling = reference
        .core(group())
        .unwrap()
        .append_window_reservation(&proposals, proposals.len())
        .unwrap()
        - 1;
    assert!(
        ceiling
            >= 3 * (size_of::<ReadyProposal>()
                + size_of::<Proposal>()
                + size_of::<(Option<AdmissionTicket>, OperationId)>()
                + size_of::<u128>())
    );
    for refused in [None, Some(1), Some(3)] {
        let shard = selected_budget(2, ceiling);
        let identity = shard.owner;
        let timed = TimedShard::new(
            shard,
            DeadlineQueue::new(identity, 1).unwrap(),
            JitterEntropy::new(1),
            TimerConfig::default(),
            MonoTime(0),
        )
        .unwrap();
        let mut owner = EffectOwner::new(
            timed,
            crate::worker::WorkerBinding {
                store: identity.store,
                generation: StorageWorkerGeneration::new(1).unwrap(),
            },
            EffectOwnerLimits::default(),
        )
        .unwrap();
        for id in 1..=3 {
            owner.admit_tracked(group(), command(id)).unwrap();
        }
        let mut calls = Vec::new();
        let steps = owner
            .advance_batch_authorized(
                MonoTime(0),
                3,
                |_| true,
                |_| true,
                |_, event, prefix| {
                    let Event::Propose { operation, .. } = event else {
                        panic!("not proposal");
                    };
                    calls.push((operation.get(), prefix.len()));
                    if Some(operation.get()) == refused {
                        Err(RaftError::Busy)
                    } else {
                        Ok(())
                    }
                },
            )
            .unwrap();
        let expected = if refused == Some(1) {
            vec![(1, 0), (2, 0), (3, 1)]
        } else {
            vec![(1, 0), (2, 1), (3, 2)]
        };
        assert_eq!(calls, expected);
        assert_eq!(
            steps.len(),
            match refused {
                None => 3,
                Some(1) => 3,
                _ => 2,
            }
        );
        if refused == Some(1) {
            assert_eq!(steps[0].error, Some(RaftError::Busy));
        }
        assert_eq!(owner.usage().active_visits, 1);
        let lease = owner.take_effect().unwrap().unwrap();
        let Effect::Persist(update) = lease.effect else {
            panic!("missing ordinary fallback");
        };
        assert_eq!(
            update.suffix.unwrap().entries.len(),
            if refused.is_none() { 3 } else { 2 }
        );
    }
}
