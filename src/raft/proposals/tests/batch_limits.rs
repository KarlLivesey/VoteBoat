// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Configured envelopes retain original order, exact tickets and peer scope.
use super::*;
use std::collections::VecDeque;

fn wide() -> BatchLimits {
    BatchLimits {
        max_proposal_entries: 4096,
        max_replication_entries: 1024,
    }
}

#[test]
fn startup_limits_reject_invalid_settings_and_lock_on_the_first_refused_event() {
    let mut core = leader();
    let before = core.state().clone();
    for limits in [
        BatchLimits {
            max_proposal_entries: 0,
            ..wide()
        },
        BatchLimits {
            max_proposal_entries: 4097,
            ..wide()
        },
        BatchLimits {
            max_replication_entries: 0,
            ..wide()
        },
        BatchLimits {
            max_replication_entries: 1025,
            ..wide()
        },
    ] {
        assert!(core.configure_batch_limits(limits).is_err());
        assert_eq!(core.batch_limits(), BatchLimits::default());
        assert_eq!(core.state(), &before);
    }
    core.configure_batch_limits(wide()).unwrap();
    core.role = Role::Follower;
    assert_eq!(
        core.propose_batch(vec![proposal(1)]),
        Err(RaftError::NotLeader)
    );
    assert_eq!(
        core.configure_batch_limits(BatchLimits::default()),
        Err(RaftError::Busy)
    );
    assert_eq!(core.batch_limits(), wide());
    assert_eq!(core.state(), &before);
    core.storage_failed();
    assert_eq!(core.configure_batch_limits(wide()), Err(RaftError::Fenced));
}

#[test]
fn configured_owned_capacity_bytes_and_retained_budget_reject_atomically() {
    for budget in 0..4 {
        let mut core = leader();
        core.configure_batch_limits(wide()).unwrap();
        let proposals = match budget {
            0 => (1..=4097).map(proposal).collect(),
            1 => {
                let mut proposals = Vec::with_capacity(4097);
                proposals.push(proposal(1));
                proposals
            }
            2 => (1..=4096)
                .map(|id| Proposal {
                    operation: OperationId::new(id).unwrap(),
                    bytes: vec![0; 1024],
                })
                .collect(),
            _ => {
                core.limits.max_entries_per_group = 4096;
                (1..=4096).map(proposal).collect()
            }
        };
        rejected(&mut core, proposals);
    }
}

fn replicas() -> BTreeMap<NodeId, Raft> {
    let mut source = leader();
    source.configure_batch_limits(wide()).unwrap();
    let mut cores = BTreeMap::new();
    for id in [2, 3] {
        let binding = StoreBinding {
            identity: source.state().bootstrap.voter_stores[&node(id)],
            session: StoreSession::new(1).unwrap(),
        };
        let follower =
            Raft::recover(node(id), binding, source.state().clone(), source.limits).unwrap();
        assert_eq!(follower.batch_limits(), BatchLimits::default());
        cores.insert(node(id), follower);
    }
    cores.insert(node(1), source);
    cores
}

fn drive(
    cores: &mut BTreeMap<NodeId, Raft>,
    effects: Vec<Effect>,
) -> (BTreeMap<NodeId, Vec<u128>>, Vec<usize>) {
    let mut queued: VecDeque<_> = effects
        .into_iter()
        .map(|effect| (node(1), effect))
        .collect();
    let mut applied: BTreeMap<NodeId, Vec<u128>> = BTreeMap::new();
    let mut widths = Vec::new();
    let mut steps = 0;
    while let Some((id, effect)) = queued.pop_front() {
        steps += 1;
        assert!(steps < 1000, "bounded history must drain");
        match effect {
            Effect::Persist(_) => {
                let core = cores.get_mut(&id).unwrap();
                let exact = ticket(core);
                core.admitted(exact).unwrap();
                let effects = core
                    .complete(&DurableLog {
                        tickets: vec![exact],
                    })
                    .unwrap();
                for effect in effects.into_iter().rev() {
                    queued.push_front((id, effect));
                }
            }
            Effect::Send(message) => {
                if let Rpc::Append { entries, .. } = &message.rpc {
                    assert!(entries.len() <= 1024);
                    if !entries.is_empty() {
                        widths.push(entries.len());
                    }
                }
                let target = message.to;
                let effects = cores
                    .get_mut(&target)
                    .unwrap()
                    .step(Event::Receive(message))
                    .unwrap();
                for effect in effects.into_iter().rev() {
                    queued.push_front((target, effect));
                }
            }
            Effect::Committed(entries) => {
                for entry in entries {
                    if let EntryPayload::Command { operation, .. } = entry.payload {
                        applied.entry(id).or_default().push(operation.get());
                    }
                }
            }
            other => panic!("unexpected effect {other:?}"),
        }
    }
    (applied, widths)
}

fn reject_oversized_append(cores: &mut BTreeMap<NodeId, Raft>, effects: &[Effect]) {
    let mut message = effects
        .iter()
        .find_map(|effect| {
            if let Effect::Send(message) = effect {
                Some(message.clone())
            } else {
                None
            }
        })
        .unwrap();
    let Rpc::Append { entries, .. } = &mut message.rpc else {
        panic!("ordinary append required");
    };
    let mut extra = entries.last().unwrap().clone();
    extra.index += 1;
    entries.push(extra);
    assert_eq!(entries.len(), 1025);
    let receiver = cores.get_mut(&message.to).unwrap();
    let before = receiver.state().clone();
    assert_eq!(
        receiver.step(Event::Receive(message)),
        Err(RaftError::InvalidMessage)
    );
    assert_eq!(receiver.state(), &before);
    assert!(!receiver.has_pending_dependency());
}

#[test]
fn larger_outbound_envelope_keeps_exact_final_short_chunk_and_original_results() {
    let mut cores = replicas();
    let effects = cores
        .get_mut(&node(1))
        .unwrap()
        .propose_batch((1..=1057).map(proposal).collect())
        .unwrap();
    let (applied, widths) = drive(&mut cores, effects);
    assert!(widths.contains(&1024));
    assert!(widths.contains(&33));
    for id in [1, 2, 3] {
        assert_eq!(cores[&node(id)].state().commit_index, 1058);
        assert_eq!(applied[&node(id)], (1..=1057).collect::<Vec<_>>());
    }
}

#[test]
fn large_original_batch_requires_exact_durability_then_replicates_in_bounded_chunks() {
    let mut cores = replicas();
    let source = cores.get_mut(&node(1)).unwrap();
    let effects = source
        .propose_batch((1..=4096).map(proposal).collect())
        .unwrap();
    assert_eq!(effects.len(), 1);
    assert!(
        matches!(&effects[0], Effect::Persist(update) if update.suffix.as_ref().unwrap().entries.len() == 4096)
    );
    let exact = ticket(source);
    assert_eq!(
        source.complete(&DurableLog {
            tickets: vec![exact]
        }),
        Err(RaftError::WrongCompletion)
    );
    assert!(source.replicate_admitted(exact).unwrap().is_empty());
    assert_eq!(source.state().last_index(), 1);
    assert_eq!(source.state().commit_index, 1);
    let mut wrong = exact;
    wrong.binding.session = StoreSession::new(2).unwrap();
    assert_eq!(
        source.complete(&DurableLog {
            tickets: vec![wrong]
        }),
        Err(RaftError::WrongCompletion)
    );
    let effects = source
        .complete(&DurableLog {
            tickets: vec![exact],
        })
        .unwrap();
    assert_eq!(source.state().last_index(), 4097);
    assert_eq!(source.state().commit_index, 1);
    reject_oversized_append(&mut cores, &effects);
    let (applied, widths) = drive(&mut cores, effects);
    assert!(widths.contains(&1024));
    let originals: Vec<_> = (1..=4096).collect();
    for id in [1, 2, 3] {
        let core = &cores[&node(id)];
        assert_eq!(core.state().commit_index, 4097);
        assert_eq!(applied[&node(id)], originals);
        Raft::recover(node(id), core.binding, core.state().clone(), core.limits).unwrap();
    }
    let state = cores[&node(1)].state().clone();
    let mut restarted = Raft::recover(
        node(1),
        cores[&node(1)].binding,
        state,
        LogLimits::default(),
    )
    .unwrap();
    assert_eq!(restarted.batch_limits(), BatchLimits::default());
    restarted.configure_batch_limits(wide()).unwrap();
    assert_eq!(restarted.replay_committed().len(), 4097);
    assert_eq!(
        restarted.complete(&DurableLog {
            tickets: vec![exact]
        }),
        Err(RaftError::WrongCompletion)
    );
}
