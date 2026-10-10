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
//! Internal state-transition fixtures exercise the real core helpers while the
//! public ingress/recovery gates remain closed. This does not model the complete
//! reconfiguration protocol or certify a storage provider's durability.
use super::*;
#[path = "leadership_membership_tests.rs"]
mod leadership;
use crate::{membership::*, quorum::*};

fn node(n: u64) -> NodeId {
    NodeId::new(n).unwrap()
}
fn cid(n: u64) -> ConfigurationId {
    ConfigurationId::new(n).unwrap()
}
fn operation(n: u128) -> OperationId {
    OperationId::new(n).unwrap()
}
fn store(n: u64) -> StoreIdentity {
    StoreIdentity {
        id: StoreId::new(n as u128).unwrap(),
        incarnation: StoreIncarnation::new(1).unwrap(),
    }
}
fn configuration(id: u64, voters: &[u64], learners: &[u64]) -> Configuration {
    Configuration::new(
        cid(id),
        Policy::new(
            Tree::Majority(voters.iter().map(|n| Tree::Voter(node(*n))).collect()),
            Limits::default(),
        )
        .unwrap(),
        voters.iter().map(|n| (node(*n), store(*n))).collect(),
        learners.iter().map(|n| (node(*n), store(*n))).collect(),
    )
    .unwrap()
}
fn core() -> Raft {
    let initial = configuration(1, &[1, 2, 3], &[]);
    let bootstrap = Bootstrap {
        group: GroupIdentity {
            id: GroupId::new(1).unwrap(),
            incarnation: GroupIncarnation::new(1).unwrap(),
        },
        configuration: initial.id(),
        policy: initial.policy().clone(),
        voter_stores: initial.voter_stores().clone(),
    };
    let mut states = BTreeMap::new();
    apply_batch(
        &mut states,
        &[LogMutation::Create(bootstrap.clone())],
        LogLimits::default(),
    )
    .unwrap();
    let state = &states[&bootstrap.group];
    let update = LogUpdate {
        group: bootstrap.group,
        expected_revision: state.revision,
        hard_state: HardState {
            term: 1,
            voted_for: None,
        },
        commit_index: 1,
        suffix: Some(Suffix {
            from: 1,
            entries: vec![LogEntry {
                index: 1,
                term: 1,
                payload: EntryPayload::Noop,
            }],
        }),
        snapshot: None,
        snapshot_membership: None,
    };
    apply_batch(
        &mut states,
        &[LogMutation::Update(update)],
        LogLimits::default(),
    )
    .unwrap();
    let binding = StoreBinding {
        identity: store(1),
        session: StoreSession::new(1).unwrap(),
    };
    let mut core = Raft::recover(
        node(1),
        binding,
        states.remove(&bootstrap.group).unwrap(),
        LogLimits::default(),
    )
    .unwrap();
    core.role = Role::Leader;
    core.initialize_replication().unwrap();
    core
}
/// Deliver one exact host-asserted completion to the actual core. No filesystem,
/// wire, application or quorum durability is inferred from these fixture tokens.
fn durable(core: &mut Raft, effects: Vec<Effect>) -> Vec<Effect> {
    assert_eq!(effects.len(), 1);
    let Effect::Persist(update) = &effects[0] else {
        panic!("expected one persistence dependency")
    };
    core.validate_persist_effect(update).unwrap();
    let next = &core.pending.as_ref().unwrap().next;
    let ticket = LogTicket {
        binding: core.binding,
        batch: core.last_batch + 1,
        group: next.bootstrap.group,
        revision: next.revision,
        generation: next.generation,
        last_index: next.last_index(),
        term: next.hard_state.term,
    };
    core.admitted(ticket).unwrap();
    core.complete(&DurableLog {
        tickets: vec![ticket],
    })
    .unwrap()
}
fn accept(core: &mut Raft, op: u128, change: ConfigurationChange) -> Vec<Effect> {
    let entry = LogEntry {
        index: core.durable.last_index() + 1,
        term: core.durable.hard_state.term,
        payload: EntryPayload::Configuration(Box::new(ConfigurationRecord {
            operation: operation(op),
            expected: core.membership().id(),
            change,
        })),
    };
    core.persist(
        core.durable.hard_state,
        core.durable.commit_index,
        Some(Suffix {
            from: entry.index,
            entries: vec![entry],
        }),
        After::LeaderAppend,
        None,
    )
    .unwrap()
}
/// Set up an already committed journal boundary, independently of the live
/// quorum test under examination. The core still validates the logical update.
fn committed_fixture(core: &mut Raft, index: u64) {
    let effects = core
        .persist(core.durable.hard_state, index, None, After::Commit, None)
        .unwrap();
    durable(core, effects);
}
fn staged() -> Raft {
    let mut core = core();
    let effects = accept(
        &mut core,
        100,
        ConfigurationChange::Learners(configuration(2, &[1, 2, 3], &[4, 5])),
    );
    durable(&mut core, effects);
    committed_fixture(&mut core, 2);
    core
}
fn joint(weighted: bool) -> Raft {
    let mut core = staged();
    let target = if weighted {
        Configuration::new(
            cid(4),
            Policy::new(
                Tree::Weighted(vec![
                    WeightedChild {
                        weight: 1,
                        node: Tree::Voter(node(1)),
                    },
                    WeightedChild {
                        weight: 1,
                        node: Tree::Voter(node(2)),
                    },
                    WeightedChild {
                        weight: 3,
                        node: Tree::Voter(node(3)),
                    },
                ]),
                Limits::default(),
            )
            .unwrap(),
            configuration(4, &[1, 2, 3], &[]).voter_stores().clone(),
            [(node(4), store(4)), (node(5), store(5))]
                .into_iter()
                .collect(),
        )
        .unwrap()
    } else {
        configuration(4, &[2, 3, 4], &[1, 5])
    };
    let effects = accept(
        &mut core,
        101,
        ConfigurationChange::Joint {
            id: cid(3),
            next: target,
        },
    );
    durable(&mut core, effects);
    core
}
fn reply(core: &Raft, from: u64, context: RequestContext, rpc: Rpc) -> Message {
    Message {
        group: core.durable.bootstrap.group,
        configuration: core.membership().id(),
        from: node(from),
        sender: StoreBinding {
            identity: store(from),
            session: StoreSession::new(1).unwrap(),
        },
        to: core.node,
        term: core.durable.hard_state.term,
        context,
        rpc,
    }
}
fn acknowledge(core: &mut Raft, from: u64) -> Vec<Effect> {
    let sent = core.requests[&node(from)];
    let message = reply(
        core,
        from,
        sent.context,
        Rpc::Appended {
            success: true,
            matching_index: sent.end,
        },
    );
    core.step(Event::Receive(message)).unwrap()
}

#[test]
fn partially_delivered_final_can_elect_new_leader_after_old_leader_loss() {
    let mut old_leader = joint(false);
    committed_fixture(&mut old_leader, 3);
    let state = old_leader.state().clone();
    let mut peers: BTreeMap<_, _> = [2, 3, 4]
        .into_iter()
        .map(|id| {
            let binding = StoreBinding {
                identity: store(id),
                session: StoreSession::new(1).unwrap(),
            };
            (
                id,
                Raft::recover_member(node(id), binding, state.clone(), LogLimits::default())
                    .unwrap(),
            )
        })
        .collect();
    let effects = accept(
        &mut old_leader,
        101,
        ConfigurationChange::Final { id: cid(4) },
    );
    let effects = durable(&mut old_leader, effects);
    let final_to_two = effects
        .into_iter()
        .find_map(|e| match e {
            Effect::Send(m) if m.to == node(2) => Some(m),
            _ => None,
        })
        .unwrap();
    let candidate = peers.get_mut(&2).unwrap();
    let effects = candidate.receive_inner(final_to_two).unwrap();
    let [Effect::Send(failed_probe)] = effects.as_slice() else {
        panic!("expected initial final-prefix probe")
    };
    assert!(matches!(
        failed_probe.rpc,
        Rpc::Appended { success: false, .. }
    ));
    let effects = old_leader
        .step(Event::Receive(failed_probe.clone()))
        .unwrap();
    let final_to_two = effects
        .into_iter()
        .find_map(|e| match e {
            Effect::Send(m) if m.to == node(2) => Some(m),
            _ => None,
        })
        .unwrap();
    let effects = candidate.receive_inner(final_to_two).unwrap();
    durable(candidate, effects);
    assert_eq!(candidate.membership().id(), cid(4));
    assert_eq!(candidate.state().commit_index, 3);
    // Node 1 is now unavailable. Nodes 2/3 are an old majority, and 2/3/4
    // contain a new majority, but only node 2 received the final record.
    let effects = candidate.step(Event::Campaign).unwrap();
    let effects = durable(candidate, effects);
    let vote_to_three = effects
        .into_iter()
        .find_map(|e| match e {
            Effect::Send(m) if m.to == node(3) => Some(m),
            _ => None,
        })
        .unwrap();
    let voter = peers.get_mut(&3).unwrap();
    let effects = voter.step(Event::Receive(vote_to_three)).unwrap();
    let effects = durable(voter, effects);
    // The ballot origin records the voter's local accepted joint view, not
    // the newer candidate scope echoed in its reply.
    assert_eq!(voter.state().ballot_origin.unwrap().configuration, cid(3));
    let reply = effects
        .into_iter()
        .find_map(|e| match e {
            Effect::Send(m) if matches!(m.rpc, Rpc::Voted { granted: true }) => Some(m),
            _ => None,
        })
        .unwrap();
    assert_eq!(reply.configuration, cid(4));
    let candidate = peers.get_mut(&2).unwrap();
    let effects = candidate.step(Event::Receive(reply)).unwrap();
    let effects = durable(candidate, effects);
    assert_eq!(candidate.role(), Role::Leader);
    let probe = message_to(effects, 3);
    let voter = peers.get_mut(&3).unwrap();
    let rejected = message_to(voter.receive_inner(probe).unwrap(), 2);
    let candidate = peers.get_mut(&2).unwrap();
    let suffix = message_to(candidate.step(Event::Receive(rejected)).unwrap(), 3);
    let voter = peers.get_mut(&3).unwrap();
    let effects = voter.receive_inner(suffix).unwrap();
    let ack = message_to(durable(voter, effects), 2);
    let candidate = peers.get_mut(&2).unwrap();
    let effects = candidate.step(Event::Receive(ack)).unwrap();
    let committed = durable(candidate, effects);
    assert_eq!(candidate.state().commit_index, 5);
    let announcement = message_to(committed, 3);
    let voter = peers.get_mut(&3).unwrap();
    let effects = voter.receive_inner(announcement).unwrap();
    durable(voter, effects);
    assert_eq!(voter.state().commit_index, 5);
    assert_eq!(voter.membership().id(), cid(4));
    assert_eq!(voter.state().ballot_origin.unwrap().configuration, cid(3));
    let recovered =
        Raft::recover_member(node(3), voter.binding, voter.state().clone(), voter.limits).unwrap();
    assert_eq!(recovered.membership().id(), cid(4));
}

/// Lost repair traffic reproduces the stall; eventual production repair fixes
/// the exact-prefix case without voting from a learner or treating acks as votes.
#[test]
fn partial_joint_repair_survives_lost_delivery_and_restart_without_learner_votes() {
    let mut survivors = partial_joint_survivors();
    let available = [node(2), node(3), node(5)].into_iter().collect();
    assert!(survivors[&2].membership().is_satisfied(&available));
    lose_joint_repair_rounds(&mut survivors);
    let candidate = survivors.get_mut(&2).unwrap();
    let effects = candidate.step(Event::Campaign).unwrap();
    let requests = durable(candidate, effects);
    let transfer = requests
        .iter()
        .find_map(|e| match e {
            Effect::Send(message)
                if message.to == node(5) && matches!(message.rpc, Rpc::Append { .. }) =>
            {
                Some(message.clone())
            }
            _ => None,
        })
        .unwrap();
    let request = requests
        .into_iter()
        .find_map(|e| match e {
            Effect::Send(message)
                if message.to == node(5) && matches!(message.rpc, Rpc::Vote { .. }) =>
            {
                Some(message)
            }
            _ => None,
        })
        .unwrap();
    assert_eq!(candidate.role(), Role::Candidate);
    assert_eq!(
        candidate.step(Event::Propose {
            operation: operation(999),
            bytes: vec![0; 8]
        }),
        Err(RaftError::NotLeader)
    );
    let learner = survivors.get_mut(&5).unwrap();
    // Network reordering can deliver the Vote before its repair. No vote
    // escapes from the old learner view, even in this campaign's term.
    let effects = learner.step(Event::Receive(request.clone())).unwrap();
    let denied = one_reply(durable(learner, effects));
    assert!(matches!(denied.rpc, Rpc::Voted { granted: false }));
    let candidate = survivors.get_mut(&2).unwrap();
    assert!(candidate.step(Event::Receive(denied)).unwrap().is_empty());
    let learner = survivors.get_mut(&5).unwrap();
    let effects = learner.step(Event::Receive(transfer.clone())).unwrap();
    assert!(learner.has_pending_dependency());
    assert_eq!(learner.step(Event::Campaign), Err(RaftError::Busy));
    assert_eq!(
        learner.step(Event::Receive(request.clone())),
        Err(RaftError::Busy)
    );
    assert_eq!(learner.state().last_index(), 2);
    let response = one_reply(durable(learner, effects));
    assert!(matches!(response.rpc, Rpc::Appended { success: true, .. }));
    assert!(learner.local_voter());
    assert_eq!(learner.state().last_index(), 3);
    assert_eq!(learner.state().commit_index, 2);
    let candidate = survivors.get_mut(&2).unwrap();
    assert!(candidate.step(Event::Receive(response)).unwrap().is_empty());
    assert_eq!(
        candidate.role(),
        Role::Candidate,
        "repair acknowledgements are not votes"
    );
    let voter = survivors.get_mut(&5).unwrap();
    let effects = voter.step(Event::Receive(request)).unwrap();
    assert_eq!(voter.state().hard_state.voted_for, None);
    let response = one_reply(durable(voter, effects));
    assert!(matches!(response.rpc, Rpc::Voted { granted: true }));
    let candidate = survivors.get_mut(&2).unwrap();
    let effects = candidate.step(Event::Receive(response)).unwrap();
    durable(candidate, effects);
    assert_eq!(candidate.role(), Role::Leader);
}

#[test]
fn recursive_joint_recovery_catches_up_old_voter_and_commits_after_election() {
    let mut peers = recursive_joint_peers();
    assert!(!peers[&2]
        .membership()
        .is_satisfied(&[node(2), node(5)].into()));
    assert!(!peers[&2]
        .membership()
        .is_satisfied(&[node(2), node(3)].into()));
    assert!(peers[&2]
        .membership()
        .is_satisfied(&[node(2), node(3), node(5)].into()));
    let source = peers.get_mut(&2).unwrap();
    let effects = source.step(Event::Campaign).unwrap();
    let mut queue: std::collections::VecDeque<_> = durable(source, effects).into();
    let mut ready = Vec::new();
    let mut delivered = 0;
    loop {
        while let Some(effect) = queue.pop_front() {
            delivered += 1;
            assert!(delivered < 1000);
            deliver_recursive_effect(&mut peers, effect, &mut queue, &mut ready);
        }
        if peers[&2].state().commit_index >= 4 {
            break;
        }
        assert!(delivered < 500, "no commitment after recursive election");
        queue.extend(peers.get_mut(&2).unwrap().step(Event::Heartbeat).unwrap());
    }
    assert_eq!(peers[&2].role(), Role::Leader);
    assert_eq!(peers[&3].membership().id(), cid(3));
    assert_eq!(peers[&5].membership().id(), cid(3));
    let leader = peers.get_mut(&2).unwrap();
    let effects = leader
        .step(Event::Propose {
            operation: operation(900),
            bytes: 7i64.to_le_bytes().to_vec(),
        })
        .unwrap();
    queue.extend(durable(leader, effects));
    // The same queue driver now checks a real write and a read barrier through
    // the joint recursive policies, including the lagging old voter.
    let mut reads_started = false;
    for _ in 0..1000 {
        if let Some(effect) = queue.pop_front() {
            deliver_recursive_effect(&mut peers, effect, &mut queue, &mut ready);
        } else if peers.values().all(|p| p.state().commit_index >= 5) && !reads_started {
            queue.extend(
                peers
                    .get_mut(&2)
                    .unwrap()
                    .step(Event::Read {
                        request: ReadRequestId::new(1).unwrap(),
                    })
                    .unwrap(),
            );
            reads_started = true;
        } else if !ready.is_empty() {
            break;
        } else {
            queue.extend(peers.get_mut(&2).unwrap().step(Event::Heartbeat).unwrap());
        }
    }
    assert_eq!(ready.len(), 1);
    assert_eq!(ready[0].index, 5);
    for peer in peers.values() {
        let mut app = crate::application::Counter::new(100).unwrap();
        use crate::application::StateMachine;
        app.apply_batch(peer.replay_committed()).unwrap();
        assert_eq!(app.read_applied(5), Ok(7));
    }
}

#[test]
fn joint_repair_rejects_overwrite_commit_claims_and_foreign_authority_before_mutation() {
    let mut source = follower(joint(false), 2);
    let effects = source.step(Event::Campaign).unwrap();
    assert!(matches!(effects.as_slice(), [Effect::Persist(_)]));
    let requests = durable(&mut source, effects);
    let valid = requests
        .into_iter()
        .find_map(|e| match e {
            Effect::Send(message)
                if message.to == node(4) && matches!(message.rpc, Rpc::Append { .. }) =>
            {
                Some(message)
            }
            _ => None,
        })
        .unwrap();
    for variant in 0..14 {
        let mut receiver = follower(staged(), 4);
        let mut message = valid.clone();
        match variant {
            0 => message.sender.identity = store(99),
            1 => message.context.origin.identity = store(99),
            2 => message.context.sequence = 0,
            3 => message.term = 0,
            4 => message.configuration = cid(4),
            5 => {
                if let Rpc::Append { leader_commit, .. } = &mut message.rpc {
                    *leader_commit = 2;
                }
            }
            6 => {
                if let Rpc::Append { previous_index, .. } = &mut message.rpc {
                    *previous_index = 1;
                }
            }
            7 => {
                if let Rpc::Append { previous_term, .. } = &mut message.rpc {
                    *previous_term = 99;
                }
            }
            8 => {
                if let Rpc::Append { entries, .. } = &mut message.rpc {
                    entries.push(LogEntry {
                        index: 4,
                        term: 1,
                        payload: EntryPayload::Noop,
                    });
                }
            }
            9 => {
                receiver = follower(staged(), 3);
                message.to = node(3);
            }
            10 => receiver = follower(joint(false), 4),
            11 => {
                receiver.durable.commit_index = 1;
            }
            12 => receiver.limits.max_command_bytes = 1,
            13 => {
                if let Rpc::Append { entries, .. } = &mut message.rpc {
                    let EntryPayload::Configuration(record) = &mut entries[0].payload else {
                        unreachable!()
                    };
                    record.expected = cid(1);
                }
            }
            _ => unreachable!(),
        }
        let state = receiver.state().clone();
        let role = receiver.role();
        let reset = receiver.election_reset_sequence();
        assert_eq!(
            receiver.step(Event::Receive(message)),
            Err(RaftError::InvalidMessage),
            "variant {variant}"
        );
        assert_eq!(receiver.state(), &state);
        assert_eq!(receiver.role(), role);
        assert_eq!(receiver.election_reset_sequence(), reset);
        assert!(!receiver.has_pending_dependency());
    }
    let mut receiver = follower(staged(), 4);
    let effects = receiver.step(Event::Receive(valid.clone())).unwrap();
    durable(&mut receiver, effects);
    // A lost repair ack is harmless: restart keeps the joint and its new voting
    // assignment; a duplicate cannot append again or reset its ballot/history.
    let mut restarted = Raft::recover_member(
        node(4),
        StoreBinding {
            identity: store(4),
            session: StoreSession::new(2).unwrap(),
        },
        receiver.state().clone(),
        LogLimits::default(),
    )
    .unwrap();
    let before = restarted.state().clone();
    assert_eq!(
        restarted.step(Event::Receive(valid)),
        Err(RaftError::InvalidMessage)
    );
    assert_eq!(restarted.state(), &before);
    assert!(restarted.local_voter());
}

fn retained_repair_pair(gap: u64, overlap: usize) -> (Raft, Raft) {
    let mut state = joint(false).state().clone();
    let mut joint = state.entries.pop().unwrap();
    let prefix = (3..3 + gap)
        .map(|index| LogEntry {
            index,
            term: 1,
            payload: EntryPayload::Command {
                operation: operation(u128::from(1000 + index)),
                bytes: vec![index as u8; 8],
            },
        })
        .collect::<Vec<_>>();
    state.entries.extend(prefix.clone());
    joint.index += gap;
    state.entries.push(joint);
    let recover = |id, state| {
        Raft::recover_member(
            node(id),
            StoreBinding {
                identity: store(id),
                session: StoreSession::new(1).unwrap(),
            },
            state,
            LogLimits::default(),
        )
        .unwrap()
    };
    let source = recover(2, state);
    let mut state = staged().state().clone();
    state.entries.extend_from_slice(&prefix[..overlap]);
    (source, recover(4, state))
}
fn emitted_repair(source: &mut Raft) -> Message {
    let effects = source.step(Event::Campaign).unwrap();
    durable(source, effects)
        .into_iter()
        .find_map(|effect| match effect {
            Effect::Send(m) if m.to == node(4) && matches!(m.rpc, Rpc::Append { .. }) => Some(m),
            _ => None,
        })
        .unwrap()
}
#[test]
fn joint_repair_extends_retained_prefix_with_identical_overlap_and_no_commit_claim() {
    for (gap, overlap) in [(3, 0), (3, 1), (63, 0), (100, 70), (100, 100)] {
        let (mut source, mut receiver) = retained_repair_pair(gap, overlap);
        let repair = emitted_repair(&mut source);
        let Rpc::Append { entries, .. } = &repair.rpc else {
            unreachable!()
        };
        assert!(entries.len() <= 64);
        let before = receiver.state().clone();
        let effects = receiver.step(Event::Receive(repair)).unwrap();
        assert_eq!(receiver.state(), &before);
        assert!(receiver.has_pending_dependency());
        assert_eq!(receiver.step(Event::Campaign), Err(RaftError::Busy));
        let ack = one_reply(durable(&mut receiver, effects));
        assert!(
            matches!(ack.rpc, Rpc::Appended { success: true, matching_index } if matching_index == gap + 3)
        );
        assert_eq!(receiver.state().entries, source.state().entries);
        assert_eq!(receiver.state().commit_index, before.commit_index);
        assert!(receiver.local_voter());
        assert!(source.step(Event::Receive(ack)).unwrap().is_empty());
        assert_eq!(source.role(), Role::Candidate);
        let recovered = Raft::recover_member(
            node(4),
            receiver.binding,
            receiver.state().clone(),
            receiver.limits,
        )
        .unwrap();
        assert!(recovered.local_voter());
        assert_eq!(recovered.state().commit_index, before.commit_index);
    }
}
#[test]
fn joint_repair_refuses_forked_overlap_bad_ranges_and_out_of_window_learners() {
    for variant in 0..7 {
        let (mut source, mut receiver) =
            retained_repair_pair(if variant == 6 { 100 } else { 3 }, 1);
        let mut repair = emitted_repair(&mut source);
        let Rpc::Append { entries, .. } = &mut repair.rpc else {
            unreachable!()
        };
        match variant {
            0 => {
                let EntryPayload::Command { bytes, .. } = &mut entries[0].payload else {
                    unreachable!()
                };
                bytes[0] ^= 1;
            }
            1 => entries[1].term = repair.term + 1,
            2 => entries[1].index += 1,
            3 => entries[1].term = 0,
            4 => entries[1].payload = source.state().entries[1].payload.clone(),
            5 => receiver.limits.max_batch_bytes = 256,
            6 => (),
            _ => unreachable!(),
        }
        let before = (
            receiver.state().clone(),
            receiver.role(),
            receiver.election_reset_sequence(),
        );
        assert_eq!(
            receiver.step(Event::Receive(repair)),
            Err(RaftError::InvalidMessage),
            "variant {variant}"
        );
        assert_eq!(
            (
                receiver.state().clone(),
                receiver.role(),
                receiver.election_reset_sequence()
            ),
            before
        );
        assert!(!receiver.has_pending_dependency());
    }
}

#[test]
fn joint_repair_tail_respects_byte_budget_and_omits_oversized_joint() {
    let (mut source, mut receiver) = retained_repair_pair(3, 2);
    let joint_bytes = source
        .state()
        .entries
        .last()
        .unwrap()
        .retained_payload_bytes();
    source.limits.max_command_bytes = joint_bytes;
    source.limits.max_batch_bytes = 256 + 37 + joint_bytes + 45;
    let message = emitted_repair(&mut source);
    let Rpc::Append {
        entries,
        previous_index,
        ..
    } = &message.rpc
    else {
        unreachable!()
    };
    assert_eq!(entries.len(), 2);
    assert_eq!(*previous_index, 4);
    let effects = receiver.step(Event::Receive(message)).unwrap();
    durable(&mut receiver, effects);
    assert_eq!(receiver.state().entries, source.state().entries);
    source.limits.max_batch_bytes = 256 + 37 + joint_bytes - 1;
    let effects = source.step(Event::Campaign).unwrap();
    let effects = durable(&mut source, effects);
    assert!(!effects.iter().any(|e| matches!(
        e,
        Effect::Send(Message {
            rpc: Rpc::Append { .. },
            ..
        })
    )));
}

#[test]
fn joint_repair_trims_only_a_verified_compacted_overlap() {
    for valid in [false, true] {
        let (mut source, mut receiver) = retained_repair_pair(3, 2);
        committed_fixture(&mut receiver, 4);
        // Host-verified checkpoint fixture, as in the other core compaction
        // histories; native snapshot publication is separately tested.
        let reference = SnapshotRef {
            store: store(4),
            group: receiver.state().bootstrap.group,
            generation: SnapshotGeneration::new(1).unwrap(),
            configuration: cid(2),
            index: 4,
            term: 1,
            application_schema: 1,
            file_bytes: 128,
            checksum: 7,
        };
        let effects = receiver.begin_compact(reference).unwrap();
        durable(&mut receiver, effects);
        let mut message = emitted_repair(&mut source);
        if !valid {
            let Rpc::Append { entries, .. } = &mut message.rpc else {
                unreachable!()
            };
            entries
                .iter_mut()
                .filter(|e| e.index >= 4)
                .for_each(|e| e.term = 2);
        }
        let before = receiver.state().clone();
        let effects = receiver.step(Event::Receive(message));
        if valid {
            let effects = effects.unwrap();
            assert_eq!(receiver.state(), &before);
            durable(&mut receiver, effects);
            assert!(receiver.local_voter());
            assert_eq!(receiver.state().commit_index, 4);
            assert_eq!(receiver.state().base_index(), 4);
            assert_eq!(receiver.state().entries, source.state().entries[4..]);
        } else {
            assert_eq!(effects, Err(RaftError::InvalidMessage));
            assert_eq!(receiver.state(), &before);
            assert!(!receiver.has_pending_dependency());
        }
    }
}

#[test]
fn batched_joint_repair_recovers_lost_cursor_and_rejects_delayed_traffic_after_promotion() {
    let (source, mut receiver) = retained_repair_pair(160, 0);
    let mut source = source.with_batched_joint_repair();
    let campaign = source.step(Event::Campaign).unwrap();
    let first = durable(&mut source, campaign)
        .into_iter()
        .find_map(|e| match e {
            Effect::Send(m) if matches!(m.rpc, Rpc::LearnerRepair { .. }) => Some(m),
            _ => None,
        })
        .unwrap();
    let effects = receiver.step(Event::Receive(first.clone())).unwrap();
    assert_eq!(receiver.state().last_index(), 2);
    assert_eq!(receiver.step(Event::Campaign), Err(RaftError::Busy));
    let lost_ack = one_reply(durable(&mut receiver, effects));
    assert!(!receiver.local_voter());
    assert_eq!(receiver.state().commit_index, 2);
    // Lose the cursor acknowledgement and both volatile owners. Replaying the
    // first range must compare overlap, not truncate or manufacture commitment.
    source = Raft::recover_member(
        node(2),
        source.binding,
        source.state().clone(),
        source.limits,
    )
    .unwrap()
    .with_batched_joint_repair();
    receiver = Raft::recover_member(
        node(4),
        receiver.binding,
        receiver.state().clone(),
        receiver.limits,
    )
    .unwrap();
    let effects = source.step(Event::Campaign).unwrap();
    let mut repair = durable(&mut source, effects)
        .into_iter()
        .find_map(|e| match e {
            Effect::Send(m) if matches!(m.rpc, Rpc::LearnerRepair { .. }) => Some(m),
            _ => None,
        })
        .unwrap();
    assert!(source.step(Event::Receive(lost_ack)).unwrap().is_empty());
    let mut batches = 0;
    loop {
        let before = receiver.state().clone();
        let effects = receiver.step(Event::Receive(repair)).unwrap();
        let effects = if receiver.has_pending_dependency() {
            assert_eq!(receiver.state(), &before);
            durable(&mut receiver, effects)
        } else {
            effects
        };
        let ack = one_reply(effects);
        let next = source.step(Event::Receive(ack.clone())).unwrap();
        assert_eq!(source.role(), Role::Candidate);
        assert_eq!(source.state().commit_index, 2);
        assert!(source.step(Event::Receive(ack)).unwrap().is_empty());
        batches += 1;
        let next = one_reply(next);
        if matches!(next.rpc, Rpc::Vote { .. }) {
            assert!(receiver.local_voter());
            assert_eq!(receiver.state().entries, source.state().entries);
            let before = receiver.state().clone();
            // The authenticated old-term replay is discarded without reviving
            // learner repair or changing the promoted replica's state.
            assert!(receiver.step(Event::Receive(first)).unwrap().is_empty());
            assert_eq!(receiver.state(), &before);
            let effects = receiver.step(Event::Receive(next)).unwrap();
            assert!(matches!(
                one_reply(durable(&mut receiver, effects)).rpc,
                Rpc::Voted { granted: true }
            ));
            break;
        }
        repair = next;
        assert!(batches < 5);
    }
    assert_eq!(batches, 3);
}

#[test]
fn batched_repair_checks_authority_ranges_and_ack_context_before_progress() {
    for variant in 0..10 {
        let (source, mut receiver) = retained_repair_pair(160, 1);
        let mut source = source.with_batched_joint_repair();
        let effects = source.step(Event::Campaign).unwrap();
        let mut message = durable(&mut source, effects)
            .into_iter()
            .find_map(|e| match e {
                Effect::Send(m) if matches!(m.rpc, Rpc::LearnerRepair { .. }) => Some(m),
                _ => None,
            })
            .unwrap();
        match variant {
            0 => message.sender.identity = store(99),
            1 => message.context.origin.identity = store(99),
            2 => message.configuration = cid(4),
            3 => message.term = 0,
            4 => message.context.sequence = 0,
            5 => receiver = follower(staged(), 3),
            _ => {
                let Rpc::LearnerRepair { entries, joint, .. } = &mut message.rpc else {
                    unreachable!()
                };
                match variant {
                    6 => entries[0].term = message.term + 1,
                    7 => {
                        let EntryPayload::Command { bytes, .. } = &mut entries[0].payload else {
                            unreachable!()
                        };
                        bytes[0] ^= 1;
                    }
                    8 => entries[0].payload = joint.payload.clone(),
                    9 => joint.index = 1,
                    _ => unreachable!(),
                }
            }
        }
        let before = (
            receiver.state().clone(),
            receiver.role(),
            receiver.election_reset_sequence(),
        );
        assert_eq!(
            receiver.step(Event::Receive(message)),
            Err(RaftError::InvalidMessage),
            "variant {variant}"
        );
        assert_eq!(
            (
                receiver.state().clone(),
                receiver.role(),
                receiver.election_reset_sequence()
            ),
            before
        );
        assert!(!receiver.has_pending_dependency());
    }
    let (source, mut receiver) = retained_repair_pair(160, 0);
    let mut source = source.with_batched_joint_repair();
    let effects = source.step(Event::Campaign).unwrap();
    let request = durable(&mut source, effects)
        .into_iter()
        .find_map(|e| match e {
            Effect::Send(m) if matches!(m.rpc, Rpc::LearnerRepair { .. }) => Some(m),
            _ => None,
        })
        .unwrap();
    let effects = receiver.step(Event::Receive(request)).unwrap();
    let ack = one_reply(durable(&mut receiver, effects));
    let mut stale = ack.clone();
    stale.context.sequence += 99;
    assert!(source.step(Event::Receive(stale)).unwrap().is_empty());
    let mut wrong = ack.clone();
    if let Rpc::LearnerRepaired { matching_index, .. } = &mut wrong.rpc {
        *matching_index += 1;
    }
    assert_eq!(
        source.step(Event::Receive(wrong)),
        Err(RaftError::InvalidMessage)
    );
    assert_eq!(source.repair_requests.len(), 1);
    let mut higher = ack;
    higher.term += 1;
    let before = source.state().hard_state.term;
    let effects = source.step(Event::Receive(higher)).unwrap();
    assert_eq!(source.state().hard_state.term, before);
    assert!(source.has_pending_dependency());
    assert!(durable(&mut source, effects).is_empty());
    assert_eq!(source.state().hard_state.term, before + 1);
    assert_eq!(source.role(), Role::Follower);
    assert!(source.repair_requests.is_empty());
}

#[test]
fn batched_repair_rejects_committed_divergence_and_accepted_configuration_work() {
    for accepted_joint in [false, true] {
        let (source, receiver) = retained_repair_pair(160, 1);
        let mut source = source.with_batched_joint_repair();
        let effects = source.step(Event::Campaign).unwrap();
        let mut request = durable(&mut source, effects)
            .into_iter()
            .find_map(|e| match e {
                Effect::Send(m) if matches!(m.rpc, Rpc::LearnerRepair { .. }) => Some(m),
                _ => None,
            })
            .unwrap();
        let mut receiver = if accepted_joint {
            follower(joint(false), 4)
        } else {
            let mut state = receiver.state().clone();
            state.commit_index = 3;
            Raft::recover_member(node(4), receiver.binding, state, receiver.limits).unwrap()
        };
        // The proposed replacement is well formed and differs in term from the
        // protected committed entry. It must fail even though it is not a
        // same-term payload fork. Accepted configuration work is a separate gate.
        if let Rpc::LearnerRepair { entries, joint, .. } = &mut request.rpc {
            for entry in entries {
                entry.term = request.term;
            }
            joint.term = request.term;
        }
        let before = (
            receiver.state().clone(),
            receiver.role(),
            receiver.election_reset_sequence(),
        );
        assert_eq!(
            receiver.step(Event::Receive(request)),
            Err(RaftError::InvalidMessage)
        );
        assert_eq!(
            (
                receiver.state().clone(),
                receiver.role(),
                receiver.election_reset_sequence()
            ),
            before
        );
        assert!(!receiver.has_pending_dependency());
    }
}
#[test]
fn batched_repair_uses_a_matching_learner_checkpoint_hint_without_commit_authority() {
    let (source, mut receiver) = retained_repair_pair(160, 140);
    let mut source = source.with_batched_joint_repair();
    committed_fixture(&mut receiver, 142);
    let reference = SnapshotRef {
        store: store(4),
        group: receiver.state().bootstrap.group,
        generation: SnapshotGeneration::new(1).unwrap(),
        configuration: cid(2),
        index: 142,
        term: 1,
        application_schema: 1,
        file_bytes: 128,
        checksum: 7,
    };
    let effects = receiver.begin_compact(reference).unwrap();
    durable(&mut receiver, effects);
    let effects = source.step(Event::Campaign).unwrap();
    let request = durable(&mut source, effects)
        .into_iter()
        .find_map(|e| match e {
            Effect::Send(m) if matches!(m.rpc, Rpc::LearnerRepair { .. }) => Some(m),
            _ => None,
        })
        .unwrap();
    let effects = receiver.step(Event::Receive(request)).unwrap();
    let ack = one_reply(durable(&mut receiver, effects));
    assert!(matches!(
        ack.rpc,
        Rpc::LearnerRepaired {
            success: false,
            matching_index: 142,
            matching_term: 1
        }
    ));
    let next = one_reply(source.step(Event::Receive(ack)).unwrap());
    assert!(matches!(
        next.rpc,
        Rpc::LearnerRepair {
            previous_index: 142,
            ..
        }
    ));
    let effects = receiver.step(Event::Receive(next)).unwrap();
    let ack = one_reply(durable(&mut receiver, effects));
    assert!(matches!(
        one_reply(source.step(Event::Receive(ack)).unwrap()).rpc,
        Rpc::Vote { .. }
    ));
    assert_eq!(source.state().commit_index, 2);
    assert_eq!(receiver.state().commit_index, 142);
    assert_eq!(receiver.state().base_index(), 142);
    assert!(receiver.local_voter());
}

#[test]
fn committed_joint_candidate_can_repair_a_missing_learner_without_exporting_commit_authority() {
    let (mut source, mut receiver) = retained_repair_pair(130, 0);
    committed_fixture(&mut source, 133);
    let mut source = source.with_batched_joint_repair();
    let effects = source.step(Event::Campaign).unwrap();
    let mut request = durable(&mut source, effects)
        .into_iter()
        .find_map(|e| match e {
            Effect::Send(m) if matches!(m.rpc, Rpc::LearnerRepair { .. }) => Some(m),
            _ => None,
        })
        .unwrap();
    let mut batches = 0;
    loop {
        let effects = receiver.step(Event::Receive(request)).unwrap();
        let ack = one_reply(durable(&mut receiver, effects));
        assert_eq!(receiver.state().commit_index, 2);
        let next = one_reply(source.step(Event::Receive(ack)).unwrap());
        assert_eq!(source.state().commit_index, 133);
        assert_eq!(source.role(), Role::Candidate);
        batches += 1;
        if matches!(next.rpc, Rpc::Vote { .. }) {
            break;
        }
        request = next;
        assert!(batches < 4);
    }
    assert_eq!(batches, 3);
    assert!(receiver.local_voter());
    assert_eq!(receiver.state().entries, source.state().entries);
}

#[test]
fn batched_repair_missing_compacted_joint_keeps_ordinary_election_available() {
    let (mut source, _) = retained_repair_pair(3, 0);
    committed_fixture(&mut source, 6);
    let reference = SnapshotRef {
        store: store(2),
        group: source.state().bootstrap.group,
        generation: SnapshotGeneration::new(1).unwrap(),
        configuration: cid(3),
        index: 6,
        term: 1,
        application_schema: 1,
        file_bytes: 128,
        checksum: 7,
    };
    let effects = source.begin_compact(reference).unwrap();
    durable(&mut source, effects);
    let mut source = source.with_batched_joint_repair();
    let effects = source.step(Event::Campaign).unwrap();
    let effects = durable(&mut source, effects);
    assert!(effects.iter().any(|e| matches!(
        e,
        Effect::Send(Message {
            rpc: Rpc::Vote { .. },
            ..
        })
    )));
    assert!(!effects.iter().any(|e| matches!(
        e,
        Effect::Send(Message {
            rpc: Rpc::LearnerRepair { .. },
            ..
        })
    )));
    assert_eq!(source.role(), Role::Candidate);
    assert_eq!(source.state().base_index(), 6);
}

#[test]
fn pending_configuration_view_precedes_durability_without_releasing_effects() {
    let mut core = core();
    let initial = core.state().clone();
    let effects = accept(
        &mut core,
        100,
        ConfigurationChange::Learners(configuration(2, &[1, 2, 3], &[4, 5])),
    );
    assert!(matches!(&effects[..], [Effect::Persist(_)]));
    assert_eq!(core.membership().id(), cid(2));
    assert_eq!(core.state(), &initial);
    assert_eq!(core.step(Event::Heartbeat), Err(RaftError::Busy));
    assert_eq!(core.membership().replica_store(node(4)), Some(store(4)));
    assert_eq!(core.membership().voter_store(node(4)), None);
    core.storage_failed();
    assert_eq!(core.membership().id(), cid(1));
    assert_eq!(core.state(), &initial);
    assert_eq!(core.step(Event::Campaign), Err(RaftError::Fenced));
}
#[test]
fn learner_replication_is_separate_from_ballots_reads_and_identity_authority() {
    let mut core = staged();
    assert_eq!(core.peers(), vec![node(2), node(3), node(4), node(5)]);
    assert_eq!(core.voting_peers(), vec![node(2), node(3)]);
    let effects = core.step(Event::Heartbeat).unwrap();
    assert_eq!(effects.len(), 4);
    assert!(effects
        .iter()
        .all(|e| matches!(e,Effect::Send(m) if m.configuration == cid(2))));
    assert!(acknowledge(&mut core, 4).is_empty());
    assert_eq!(core.progress[&node(4)], 2);
    assert_eq!(core.durable.commit_index, 2);
    let before = core.durable.hard_state;
    for rpc in [
        Rpc::Vote {
            last_index: 2,
            last_term: 1,
        },
        Rpc::Voted { granted: true },
        Rpc::ReadProbe,
        Rpc::ReadAck,
    ] {
        let mut message = reply(
            &core,
            4,
            RequestContext {
                origin: core.binding,
                sequence: 99,
            },
            rpc,
        );
        message.term = 99;
        assert_eq!(
            core.step(Event::Receive(message)),
            Err(RaftError::WrongIdentity)
        );
        assert_eq!(core.durable.hard_state, before);
        assert!(!core.has_pending_dependency());
    }
    let sent = core.requests[&node(5)];
    let mut wrong = reply(
        &core,
        5,
        sent.context,
        Rpc::Appended {
            success: true,
            matching_index: sent.end,
        },
    );
    wrong.sender.identity.incarnation = StoreIncarnation::new(2).unwrap();
    assert_eq!(
        core.step(Event::Receive(wrong)),
        Err(RaftError::WrongIdentity)
    );
}
#[test]
fn joint_commit_uses_old_and_new_and_never_counts_a_learner() {
    let mut core = joint(false);
    assert_eq!(core.durable.commit_index, 2);
    assert_eq!(core.progress[&node(1)], 3);
    assert!(acknowledge(&mut core, 2).is_empty()); // old satisfied, new not
    assert_eq!(core.durable.commit_index, 2);
    assert!(acknowledge(&mut core, 5).is_empty()); // learner in both
    assert_eq!(core.durable.commit_index, 2);
    let effects = acknowledge(&mut core, 4); // promoted exact-store voter
    assert!(matches!(&effects[..],[Effect::Persist(u)] if u.commit_index == 3));
    assert_eq!(core.durable.commit_index, 2);
    durable(&mut core, effects);
    assert_eq!(core.durable.commit_index, 3);
    assert_eq!(core.membership().id(), cid(3));
}
#[test]
fn same_voter_policy_changes_use_joint_rules_for_elections_and_reads() {
    let mut core = joint(true);
    let effects = core
        .step(Event::Read {
            request: ReadRequestId::new(1).unwrap(),
        })
        .unwrap();
    assert_eq!(effects.len(), 2); // no learner probes
    let context = core.read.as_ref().unwrap().barrier.context;
    let ack = reply(&core, 2, context, Rpc::ReadAck);
    assert!(core.step(Event::Receive(ack)).unwrap().is_empty());
    let ack = reply(&core, 3, context, Rpc::ReadAck);
    let effects = core.step(Event::Receive(ack)).unwrap();
    assert!(matches!(&effects[..],[Effect::ReadReady(b)] if b.configuration() == cid(3)));
    core.role = Role::Follower;
    let effects = core.step(Event::Campaign).unwrap();
    let effects = durable(&mut core, effects);
    assert_eq!(effects.len(), 2);
    assert!(effects.iter().all(|e| matches!(e,Effect::Send(m) if m.configuration == cid(3) && matches!(m.rpc,Rpc::Vote {..}))));
    let context = core.vote_context.unwrap();
    let ack = reply(&core, 2, context, Rpc::Voted { granted: true });
    assert!(core.step(Event::Receive(ack)).unwrap().is_empty());
    assert_eq!(core.role(), Role::Candidate);
    let ack = reply(&core, 3, context, Rpc::Voted { granted: true });
    let effects = core.step(Event::Receive(ack)).unwrap();
    assert!(matches!(&effects[..], [Effect::Persist(_)]));
    durable(&mut core, effects);
    assert_eq!(core.role(), Role::Leader);
}
#[test]
fn accepted_final_clears_prior_authority_and_rollback_restores_joint_base() {
    let mut core = joint(true);
    committed_fixture(&mut core, 3);
    let stale = core.requests[&node(2)];
    let effects = core
        .step(Event::Read {
            request: ReadRequestId::new(1).unwrap(),
        })
        .unwrap();
    assert_eq!(effects.len(), 2);
    let context = core.read.as_ref().unwrap().barrier.context;
    for n in [2, 3] {
        let message = reply(&core, n, context, Rpc::ReadAck);
        core.step(Event::Receive(message)).unwrap();
    }
    let barrier = core.ready_read.unwrap();
    let effects = accept(&mut core, 101, ConfigurationChange::Final { id: cid(4) });
    assert_eq!(core.membership().id(), cid(4));
    assert_eq!(core.state().membership().unwrap().id(), cid(3));
    assert_eq!(
        core.finish_read(&barrier, core.durable.commit_index),
        Err(RaftError::StaleRead)
    );
    durable(&mut core, effects);
    assert_eq!(core.progress[&node(2)], 0); // recollect exact-config evidence
    let mut old = reply(
        &core,
        2,
        stale.context,
        Rpc::Appended {
            success: true,
            matching_index: stale.end,
        },
    );
    old.configuration = cid(3);
    old.term = 100;
    assert_eq!(
        core.step(Event::Receive(old)),
        Err(RaftError::WrongIdentity)
    );
    assert_eq!(core.durable.hard_state.term, 1);
    core.role = Role::Follower;
    let effects = core
        .persist(
            HardState {
                term: 2,
                voted_for: None,
            },
            3,
            Some(Suffix {
                from: 4,
                entries: vec![LogEntry {
                    index: 4,
                    term: 2,
                    payload: EntryPayload::Noop,
                }],
            }),
            After::Reply,
            None,
        )
        .unwrap();
    assert_eq!(core.membership().id(), cid(3));
    durable(&mut core, effects);
    assert_eq!(core.membership(), &core.state().membership().unwrap());
    assert!(core.membership().joint().is_some());
    let reference = SnapshotRef {
        store: core.binding.identity,
        group: core.durable.bootstrap.group,
        configuration: cid(3),
        index: 3,
        term: 1,
        application_schema: 1,
        generation: SnapshotGeneration::new(1).unwrap(),
        file_bytes: 128,
        checksum: 7,
    };
    let effects = core.begin_compact(reference).unwrap();
    durable(&mut core, effects);
    assert_eq!(core.membership(), &core.state().membership().unwrap());
    assert!(core
        .state()
        .snapshot_membership
        .as_ref()
        .unwrap()
        .joint()
        .is_some());
    assert!(matches!(
        Raft::recover_verified(core.node, core.binding, core.state().clone(), core.limits),
        Err(RaftError::InvalidRecovery)
    ));
}
#[test]
fn removed_or_demoted_local_replica_stops_service_and_campaigning() {
    let mut core = joint(false);
    committed_fixture(&mut core, 3);
    let effects = accept(&mut core, 101, ConfigurationChange::Final { id: cid(4) });
    durable(&mut core, effects);
    assert_eq!(core.role(), Role::Leader); // finish final commitment first
    assert_eq!(core.step(Event::Campaign), Err(RaftError::NotVoter));
    assert_eq!(
        core.step(Event::Propose {
            operation: operation(200),
            bytes: vec![1]
        }),
        Err(RaftError::NotLeader)
    );
    assert_eq!(
        core.step(Event::Read {
            request: ReadRequestId::new(1).unwrap()
        }),
        Err(RaftError::NotLeader)
    );
    assert!(acknowledge(&mut core, 2).is_empty());
    let effects = acknowledge(&mut core, 3);
    assert!(matches!(&effects[..],[Effect::Persist(u)] if u.commit_index == 4));
    durable(&mut core, effects);
    assert_eq!(core.durable.commit_index, 4);
    assert_eq!(core.role(), Role::Follower);
    assert!(core.step(Event::Heartbeat).unwrap().is_empty());
    assert_eq!(core.step(Event::Campaign), Err(RaftError::NotVoter));
    let sender = StoreBinding {
        identity: store(2),
        session: StoreSession::new(1).unwrap(),
    };
    let mut vote = reply(
        &core,
        2,
        RequestContext {
            origin: sender,
            sequence: 90,
        },
        Rpc::Vote {
            last_index: 4,
            last_term: 1,
        },
    );
    vote.sender = sender;
    vote.term = 2;
    let effects = core.step(Event::Receive(vote)).unwrap();
    let effects = durable(&mut core, effects);
    assert!(matches!(
        &effects[..],
        [Effect::Send(Message {
            rpc: Rpc::Voted { granted: false },
            ..
        })]
    ));
}

#[test]
fn a_reused_candidate_node_with_a_new_store_needs_a_new_term_ballot() {
    let (mut core, original, replacement) = reused_candidate_fixture();
    core.role = Role::Follower;
    let sender = StoreBinding {
        identity: replacement,
        session: StoreSession::new(1).unwrap(),
    };
    let mut vote = reply(
        &core,
        2,
        RequestContext {
            origin: sender,
            sequence: 51,
        },
        Rpc::Vote {
            last_index: 6,
            last_term: 1,
        },
    );
    vote.sender = sender;
    let effects = core.step(Event::Receive(vote.clone())).unwrap();
    assert!(matches!(
        &effects[..],
        [Effect::Send(Message {
            rpc: Rpc::Voted { granted: false },
            ..
        })]
    ));
    assert_eq!(core.durable.ballot_origin, Some(original));
    vote.term = 2;
    let effects = core.step(Event::Receive(vote)).unwrap();
    assert_eq!(core.durable.ballot_origin, Some(original));
    let effects = durable(&mut core, effects);
    assert!(matches!(
        &effects[..],
        [Effect::Send(Message {
            rpc: Rpc::Voted { granted: true },
            ..
        })]
    ));
    assert_eq!(
        core.durable.ballot_origin,
        Some(BallotOrigin {
            configuration: cid(6),
            candidate_store: replacement
        })
    );
    // The replaced physical replica cannot campaign as the new store merely
    // because it still has the same NodeId in its recovered local assignment.
    core.node = node(2);
    core.binding.identity = store(2);
    core.role = Role::Leader;
    let before = core.durable.hard_state;
    assert_eq!(core.step(Event::Campaign), Err(RaftError::NotVoter));
    assert_eq!(
        core.step(Event::Propose {
            operation: operation(200),
            bytes: vec![1]
        }),
        Err(RaftError::NotLeader)
    );
    assert_eq!(
        core.step(Event::Read {
            request: ReadRequestId::new(2).unwrap()
        }),
        Err(RaftError::NotLeader)
    );
    assert_eq!(core.durable.hard_state, before);
}

fn follower(mut core: Raft, id: u64) -> Raft {
    core.node = node(id);
    core.binding.identity = store(id);
    core.role = Role::Follower;
    core.requests.clear();
    core.progress.clear();
    core.next_index.clear();
    core
}
fn sent(effect: Effect) -> Message {
    let Effect::Send(message) = effect else {
        panic!("expected peer message")
    };
    message
}
fn one_reply(effects: Vec<Effect>) -> Message {
    assert_eq!(effects.len(), 1);
    sent(effects.into_iter().next().unwrap())
}
#[test]
fn lagging_follower_probes_and_joint_replication_echo_the_request_scope() {
    let mut leader = joint(true);
    let mut follower = follower(staged(), 2);
    let probe = sent(leader.append_for(node(2)).unwrap());
    assert_eq!(probe.configuration, cid(3));
    let rejection = one_reply(follower.receive_inner(probe).unwrap());
    assert_eq!(rejection.configuration, cid(3));
    assert_eq!(follower.membership().id(), cid(2));
    assert!(matches!(
        rejection.rpc,
        Rpc::Appended {
            success: false,
            matching_index: 2
        }
    ));
    let append = one_reply(leader.step(Event::Receive(rejection)).unwrap());
    let retry = sent(leader.append_for(node(2)).unwrap());
    assert_eq!(retry, append);
    assert!(matches!(&append.rpc, Rpc::Append { entries, .. } if entries.len() == 1));
    // The public gate is retained. The actual private receive path is exercised
    // with host-asserted exact completions, not an online administrator.
    assert_eq!(
        follower.step(Event::Receive(append.clone())),
        Err(RaftError::InvalidMessage)
    );
    let effects = follower.receive_inner(append.clone()).unwrap();
    assert_eq!(follower.membership().id(), cid(3));
    assert_eq!(follower.state().membership().unwrap().id(), cid(2));
    assert!(matches!(&effects[..], [Effect::Persist(_)]));
    let reply = one_reply(durable(&mut follower, effects));
    assert_eq!(reply.configuration, append.configuration);
    assert!(matches!(
        reply.rpc,
        Rpc::Appended {
            success: true,
            matching_index: 3
        }
    ));
    check_scoped_append_reply(&mut leader, reply);
}
#[test]
fn older_request_scope_can_roll_back_uncommitted_final_without_early_reply() {
    let mut leader = joint(true);
    committed_fixture(&mut leader, 3);
    let mut ahead = joint(true);
    committed_fixture(&mut ahead, 3);
    let effects = accept(&mut ahead, 101, ConfigurationChange::Final { id: cid(4) });
    durable(&mut ahead, effects);
    let mut follower = follower(ahead, 2);
    assert_eq!(follower.membership().id(), cid(4));
    let effects = leader
        .persist(
            HardState {
                term: 2,
                voted_for: None,
            },
            3,
            Some(Suffix {
                from: 4,
                entries: vec![LogEntry {
                    index: 4,
                    term: 2,
                    payload: EntryPayload::Noop,
                }],
            }),
            After::LeaderAppend,
            None,
        )
        .unwrap();
    durable(&mut leader, effects);
    leader.requests.remove(&node(2));
    leader.next_index.insert(node(2), 4);
    let append = sent(leader.append_for(node(2)).unwrap());
    assert_eq!(append.configuration, cid(3));
    let effects = follower.receive_inner(append).unwrap();
    assert_eq!(follower.membership().id(), cid(3));
    assert_eq!(follower.state().membership().unwrap().id(), cid(4));
    let reply = one_reply(durable(&mut follower, effects));
    assert_eq!(reply.configuration, cid(3));
    assert_eq!(follower.state().hard_state.term, 2);
    assert!(follower.membership().joint().is_some());
    leader.step(Event::Receive(reply)).unwrap();
    assert_eq!(leader.progress[&node(2)], 4);
}
#[test]
fn partial_catch_up_scopes_matching_prefix_without_claiming_follower_activation() {
    let mut leader = joint(true);
    committed_fixture(&mut leader, 3);
    let effects = accept(&mut leader, 101, ConfigurationChange::Final { id: cid(4) });
    durable(&mut leader, effects);
    let mut follower = follower(core(), 2);
    let context = leader.context().unwrap();
    // A bounded chunk can precede the configuration represented by the sender's
    // accepted head. Prepare that chunk explicitly to isolate scope semantics.
    leader.requests.insert(
        node(2),
        Replication {
            context,
            configuration: cid(4),
            start: 2,
            end: 2,
            snapshot: None,
        },
    );
    let append = leader.scoped_message(
        cid(4),
        node(2),
        context,
        Rpc::Append {
            previous_index: 1,
            previous_term: 1,
            entries: vec![leader.state().entries[1].clone()],
            leader_commit: 3,
        },
    );
    let effects = follower.receive_inner(append).unwrap();
    let reply = one_reply(
        durable(&mut follower, effects)
            .into_iter()
            .filter(|e| matches!(e, Effect::Send(_)))
            .collect(),
    );
    assert_eq!(follower.membership().id(), cid(2));
    assert_eq!(reply.configuration, cid(4));
    assert!(matches!(
        reply.rpc,
        Rpc::Appended {
            matching_index: 2,
            success: true
        }
    ));
    let append = one_reply(leader.step(Event::Receive(reply)).unwrap());
    assert_eq!(leader.progress[&node(2)], 2);
    let effects = follower.receive_inner(append).unwrap();
    let reply = one_reply(
        durable(&mut follower, effects)
            .into_iter()
            .filter(|e| matches!(e, Effect::Send(_)))
            .collect(),
    );
    assert_eq!(follower.membership().id(), cid(4));
    assert_eq!(follower.state().commit_index, 3);
    assert_eq!(reply.configuration, cid(4));
    assert!(matches!(
        reply.rpc,
        Rpc::Appended {
            matching_index: 4,
            success: true
        }
    ));
}

fn compacted_joint() -> (Raft, SnapshotRef, Snapshot) {
    let mut leader = joint(true);
    committed_fixture(&mut leader, 3);
    let snapshot = Snapshot {
        metadata: crate::snapshot::SnapshotMetadata {
            bootstrap: leader.state().bootstrap.clone(),
            membership: leader.state().checkpoint_membership(3).unwrap(),
            index: 3,
            term: 1,
            application_schema: 1,
        },
        application: vec![7],
    };
    // Host-asserted verified pin, not a snapshot publication/storage claim.
    let reference = SnapshotRef {
        store: store(1),
        group: leader.state().bootstrap.group,
        generation: SnapshotGeneration::new(1).unwrap(),
        configuration: cid(3),
        index: 3,
        term: 1,
        application_schema: 1,
        file_bytes: 128,
        checksum: 7,
    };
    let effects = leader.begin_compact(reference).unwrap();
    durable(&mut leader, effects);
    (leader, reference, snapshot)
}
#[test]
fn snapshot_reply_keeps_head_scope_through_log_and_application_dependencies() {
    let (mut leader, reference, snapshot) = compacted_joint();
    let effects = accept(&mut leader, 101, ConfigurationChange::Final { id: cid(4) });
    durable(&mut leader, effects);
    let mut follower = follower(staged(), 2);
    leader.requests.remove(&node(2));
    leader.next_index.insert(node(2), 2);
    let Effect::SnapshotRequired {
        to,
        context,
        reference: requested,
    } = leader.append_for(node(2)).unwrap()
    else {
        panic!("expected snapshot dependency")
    };
    assert_eq!(requested, reference);
    let message = one_reply(
        leader
            .snapshot_send(to, context, reference, snapshot)
            .unwrap(),
    );
    assert_eq!(message.configuration, cid(4));
    assert!(
        matches!(&message.rpc, Rpc::Snapshot { snapshot } if snapshot.metadata.configuration() == cid(3))
    );
    assert_eq!(
        follower.step(Event::Receive(message.clone())),
        Err(RaftError::InvalidMessage)
    );
    let effects = follower.receive_inner(message.clone()).unwrap();
    assert!(matches!(&effects[..], [Effect::StageSnapshot(m)] if m == &message));
    let local_reference = SnapshotRef {
        store: store(2),
        ..reference
    };
    let effects = follower.snapshot_stored(local_reference).unwrap();
    assert!(matches!(&effects[..], [Effect::Persist(_)]));
    let effects = durable(&mut follower, effects);
    assert!(matches!(&effects[..], [Effect::SnapshotInstalled(r)] if *r == local_reference));
    assert_eq!(follower.membership().id(), cid(3));
    assert!(follower.has_pending_dependency());
    assert!(matches!(
        follower.snapshot_applied(local_reference, 2),
        Err(RaftError::WrongCompletion)
    ));
    let reply = one_reply(follower.snapshot_applied(local_reference, 3).unwrap());
    assert_eq!(reply.configuration, cid(4));
    assert_eq!(reply.context, context);
    check_snapshot_ack_then_retry(&mut leader, reply, message, reference, to, context);
}
#[test]
fn compacted_hint_echoes_request_scope_and_only_advances_a_matching_prefix() {
    let (follower_core, _, _) = compacted_joint();
    let mut follower = follower(follower_core, 2);
    let mut leader = joint(true);
    committed_fixture(&mut leader, 3);
    let effects = accept(&mut leader, 101, ConfigurationChange::Final { id: cid(4) });
    durable(&mut leader, effects);
    leader.requests.remove(&node(2));
    leader.next_index.insert(node(2), 2);
    let append = sent(leader.append_for(node(2)).unwrap());
    let hint = one_reply(follower.receive_inner(append).unwrap());
    assert_eq!(hint.configuration, cid(4));
    assert!(matches!(hint.rpc, Rpc::Compacted { index: 3, term: 1 }));
    let append = one_reply(leader.step(Event::Receive(hint)).unwrap());
    assert_eq!(leader.progress[&node(2)], 3);
    assert!(matches!(
        append.rpc,
        Rpc::Append {
            previous_index: 3,
            ..
        }
    ));
}
#[test]
fn request_scope_bridge_does_not_authorize_learners_or_reads() {
    let mut core = staged();
    let context = RequestContext {
        origin: StoreBinding {
            identity: store(4),
            session: StoreSession::new(1).unwrap(),
        },
        sequence: 1,
    };
    let mut message = reply(
        &core,
        4,
        context,
        Rpc::Append {
            previous_index: 2,
            previous_term: 1,
            entries: vec![],
            leader_commit: 2,
        },
    );
    message.configuration = cid(3);
    let before = core.state().clone();
    assert_eq!(
        core.step(Event::Receive(message.clone())),
        Err(RaftError::WrongIdentity)
    );
    message.rpc = Rpc::Vote {
        last_index: 2,
        last_term: 1,
    };
    assert_eq!(
        core.step(Event::Receive(message.clone())),
        Err(RaftError::WrongIdentity)
    );
    message.from = node(2);
    message.sender.identity = store(2);
    message.context.origin = message.sender;
    message.rpc = Rpc::ReadProbe;
    assert_eq!(
        core.step(Event::Receive(message.clone())),
        Err(RaftError::WrongIdentity)
    );
    assert_eq!(core.state(), &before);
    // Crossing configuration IDs does not permit an entry from beyond the
    // sender's declared accepted head, or an impossible newer snapshot base.
    let leader = joint(true);
    message.configuration = cid(2);
    message.rpc = Rpc::Append {
        previous_index: 2,
        previous_term: 1,
        entries: vec![leader.state().entries[2].clone()],
        leader_commit: 2,
    };
    assert_eq!(
        core.receive_inner(message.clone()),
        Err(RaftError::InvalidMessage)
    );
    let (_, _, snapshot) = compacted_joint();
    message.rpc = Rpc::Snapshot {
        snapshot: Box::new(snapshot),
    };
    assert_eq!(core.receive_inner(message), Err(RaftError::InvalidMessage));
    assert_eq!(core.state(), &before);
}

#[test]
fn configuration_changes_discard_old_request_authority_even_with_retained_peer_and_prefix() {
    let mut leader = joint(true);
    let old = leader.requests[&node(2)];
    let old_reply = reply(
        &leader,
        2,
        old.context,
        Rpc::Appended {
            success: true,
            matching_index: old.end,
        },
    );
    committed_fixture(&mut leader, 3);
    let effects = accept(&mut leader, 101, ConfigurationChange::Final { id: cid(4) });
    durable(&mut leader, effects);
    let current = leader.requests[&node(2)];
    assert_ne!(old.context, current.context);
    assert_eq!(current.configuration, cid(4));
    assert_eq!(leader.progress[&node(2)], 0);
    assert_eq!(
        leader.step(Event::Receive(old_reply.clone())),
        Err(RaftError::WrongIdentity)
    );
    // Relabeling a stale reply's configuration cannot make its old context
    // satisfy the newly admitted request, even with the right sender store.
    let mut relabeled = old_reply;
    relabeled.configuration = cid(4);
    assert!(leader.step(Event::Receive(relabeled)).unwrap().is_empty());
    assert_eq!(leader.progress[&node(2)], 0);
    assert_eq!(leader.requests[&node(2)].context, current.context);
    let ack = reply(
        &leader,
        2,
        current.context,
        Rpc::Appended {
            success: true,
            matching_index: current.end,
        },
    );
    leader.step(Event::Receive(ack)).unwrap();
    assert_eq!(leader.progress[&node(2)], current.end);

    let (mut leader, reference, snapshot) = compacted_joint();
    leader.requests.remove(&node(2));
    leader.next_index.insert(node(2), 2);
    let Effect::SnapshotRequired { context, .. } = leader.append_for(node(2)).unwrap() else {
        panic!()
    };
    let effects = accept(&mut leader, 101, ConfigurationChange::Final { id: cid(4) });
    durable(&mut leader, effects);
    assert!(matches!(
        leader.snapshot_send(node(2), context, reference, snapshot),
        Err(RaftError::WrongCompletion)
    ));
}

#[test]
fn reservation_covers_rollback_until_the_shrink_is_committed() {
    let mut leader = staged();
    assert_eq!(leader.rollback_replicas(), 5);
    let effects = accept(
        &mut leader,
        102,
        ConfigurationChange::Learners(configuration(3, &[1, 2, 3], &[])),
    );
    durable(&mut leader, effects);
    assert_eq!(leader.membership().replicas().count(), 3);
    assert_eq!(leader.rollback_replicas(), 5);
    assert_eq!(
        leader.effect_reservation(100),
        leader.reserve_effects(100, 5)
    );
    committed_fixture(&mut leader, 3);
    assert_eq!(leader.rollback_replicas(), 3);
    assert_eq!(
        leader.effect_reservation(100),
        leader.reserve_effects(100, 3)
    );
}

#[test]
fn reservation_covers_joint_rollback_and_compacted_membership() {
    let mut leader = staged();
    let effects = accept(
        &mut leader,
        103,
        ConfigurationChange::Joint {
            id: cid(3),
            next: configuration(4, &[1, 2, 3], &[]),
        },
    );
    durable(&mut leader, effects);
    committed_fixture(&mut leader, 3);
    let effects = accept(&mut leader, 103, ConfigurationChange::Final { id: cid(4) });
    durable(&mut leader, effects);
    assert_eq!(leader.membership().replicas().count(), 3);
    assert_eq!(leader.rollback_replicas(), 5);
    committed_fixture(&mut leader, 4);
    assert_eq!(leader.rollback_replicas(), 3);
    let (compacted, _, _) = compacted_joint();
    assert!(compacted.state().entries.is_empty());
    assert_eq!(compacted.rollback_replicas(), 5);
}

#[test]
fn prospective_reservation_is_pure_and_does_not_authorize_configuration_ingress() {
    let mut leader = core();
    let before = leader.state().clone();
    let message = reply(
        &leader,
        2,
        RequestContext {
            origin: leader.binding,
            sequence: 500,
        },
        Rpc::Append {
            previous_index: 1,
            previous_term: 1,
            leader_commit: 1,
            entries: vec![LogEntry {
                index: 2,
                term: 1,
                payload: EntryPayload::Configuration(Box::new(ConfigurationRecord {
                    operation: operation(500),
                    expected: cid(1),
                    change: ConfigurationChange::Learners(configuration(2, &[1, 2, 3], &[4, 5, 6])),
                })),
            }],
        },
    );
    let event = Event::Receive(message);
    assert_eq!(
        leader.event_effect_reservation(&event, 100),
        leader.reserve_effects(100, 6)
    );
    assert_eq!(leader.state(), &before);
    assert_eq!(leader.membership().id(), cid(1));
    assert_eq!(
        leader.event_connection_replicas(&event, 6).unwrap().len(),
        6
    );
    assert!(leader.event_connection_replicas(&event, 5).is_err());
    assert_eq!(leader.event_effect_reservation(&event, usize::MAX), None);
    assert_eq!(leader.step(event), Err(RaftError::InvalidMessage));
    assert_eq!(leader.state(), &before);
    let (_, _, snapshot) = compacted_joint();
    let event = Event::Receive(reply(
        &leader,
        2,
        RequestContext {
            origin: leader.binding,
            sequence: 501,
        },
        Rpc::Snapshot {
            snapshot: Box::new(snapshot),
        },
    ));
    assert_eq!(
        leader.event_effect_reservation(&event, 100),
        leader.reserve_effects(100, 5)
    );
    let Rpc::Snapshot { snapshot } = &match &event {
        Event::Receive(m) => m,
        _ => unreachable!(),
    }
    .rpc
    else {
        unreachable!()
    };
    let expected = snapshot
        .metadata
        .membership
        .as_ref()
        .unwrap()
        .replicas()
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        leader.event_connection_replicas(&event, 5).unwrap(),
        expected
    );
    for rpc in [
        Rpc::LearnerRepairSnapshot {
            snapshot: snapshot.clone(),
        },
        Rpc::CommittedLearnerRepairSnapshot {
            snapshot: snapshot.clone(),
        },
    ] {
        let Event::Receive(mut message) = event.clone() else {
            unreachable!()
        };
        message.rpc = rpc;
        let repair = Event::Receive(message);
        assert_eq!(
            leader.event_effect_reservation(&repair, 100),
            leader.reserve_effects(100, 5)
        );
        assert_eq!(
            leader.event_connection_replicas(&repair, 5).unwrap(),
            expected
        );
    }
    assert_eq!(leader.state(), &before);
}

#[test]
fn prospective_reservation_retains_intermediate_joint_fanout_after_final() {
    let leader = core();
    let changes = [
        ConfigurationChange::Learners(configuration(2, &[1, 2, 3], &[4, 5, 6])),
        ConfigurationChange::Joint {
            id: cid(3),
            next: configuration(4, &[1, 2, 3], &[4]),
        },
        ConfigurationChange::Final { id: cid(4) },
    ];
    let entries = changes
        .into_iter()
        .enumerate()
        .map(|(offset, change)| LogEntry {
            index: offset as u64 + 2,
            term: 1,
            payload: EntryPayload::Configuration(Box::new(ConfigurationRecord {
                operation: operation(if offset == 0 { 701 } else { 702 }),
                expected: cid(offset as u64 + 1),
                change,
            })),
        })
        .collect();
    let event = Event::Receive(reply(
        &leader,
        2,
        RequestContext {
            origin: leader.binding,
            sequence: 700,
        },
        Rpc::Append {
            previous_index: 1,
            previous_term: 1,
            entries,
            leader_commit: 3,
        },
    ));
    // Conservative union bound (six old + four new), despite final shrink.
    assert_eq!(
        leader.event_effect_reservation(&event, 200),
        leader.reserve_effects(200, 10)
    );
    assert_eq!(leader.rollback_replicas(), 3);
    assert_eq!(
        leader
            .event_connection_replicas(&event, 6)
            .unwrap()
            .keys()
            .copied()
            .collect::<Vec<_>>(),
        (1..=6).map(node).collect::<Vec<_>>()
    );
    assert!(leader.event_connection_replicas(&event, 5).is_err());
}

#[test]
fn prospective_connection_inspection_rejects_store_conflicts_and_wrong_scope_without_mutation() {
    let mut c = core();
    let before = c.state().clone();
    let mut next = configuration(2, &[1, 2, 3], &[4]);
    next = Configuration::new(
        next.id(),
        next.policy().clone(),
        [
            (node(1), store(1)),
            (node(2), store(22)),
            (node(3), store(3)),
        ]
        .into(),
        next.learners().clone(),
    )
    .unwrap();
    let mut message = reply(
        &c,
        2,
        RequestContext {
            origin: c.binding,
            sequence: 800,
        },
        Rpc::Append {
            previous_index: 0,
            previous_term: 0,
            leader_commit: 0,
            entries: vec![LogEntry {
                index: 1,
                term: 1,
                payload: EntryPayload::Configuration(Box::new(ConfigurationRecord {
                    operation: operation(800),
                    expected: cid(1),
                    change: ConfigurationChange::Learners(next),
                })),
            }],
        },
    );
    assert_eq!(
        c.event_connection_replicas(&Event::Receive(message.clone()), 6),
        Err(RaftError::WrongIdentity)
    );
    message.rpc = Rpc::ReadProbe;
    message.to = node(3);
    assert_eq!(
        c.event_connection_replicas(&Event::Receive(message), 6),
        Err(RaftError::WrongIdentity)
    );
    assert_eq!(c.state(), &before);
    c.storage_failed();
    assert_eq!(
        c.event_connection_replicas(&Event::Heartbeat, 6),
        Err(RaftError::Fenced)
    );
}

fn retiring_leader(removed: bool) -> Raft {
    let mut core = staged();
    let next = configuration(4, &[2, 3, 4], if removed { &[5] } else { &[1, 5] });
    let effects = accept(
        &mut core,
        101,
        ConfigurationChange::Joint { id: cid(3), next },
    );
    durable(&mut core, effects);
    committed_fixture(&mut core, 3);
    let effects = accept(&mut core, 101, ConfigurationChange::Final { id: cid(4) });
    durable(&mut core, effects);
    core
}
#[test]
fn retiring_leader_announces_only_after_final_durability_and_receivers_preserve_authority() {
    for removed in [false, true] {
        let mut leader = retiring_leader(removed);
        let mut receiver = Raft::recover_member(
            node(4),
            StoreBinding {
                identity: store(4),
                session: StoreSession::new(7).unwrap(),
            },
            leader.state().clone(),
            leader.limits,
        )
        .unwrap();
        assert!(acknowledge(&mut leader, 2).is_empty());
        let commit = acknowledge(&mut leader, 3);
        assert!(matches!(&commit[..],[Effect::Persist(u)] if u.commit_index==4));
        assert_eq!(leader.state().commit_index, 3);
        assert_eq!(
            leader.complete(&DurableLog { tickets: vec![] }),
            Err(RaftError::WrongCompletion)
        );
        let reservation = leader.effect_reservation(0).unwrap();
        let effects = durable(&mut leader, commit);
        assert_eq!(leader.role(), Role::Follower);
        assert!(leader.requests.is_empty() && leader.progress.is_empty());
        let notices = effects
            .into_iter()
            .filter_map(|e| {
                if let Effect::Send(m) = e {
                    Some(m)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        assert_eq!(notices.len(), 4);
        assert!(notices.iter().all(|m|m.configuration==cid(4) && m.term==1 && m.context.origin==leader.binding
            && matches!(&m.rpc,Rpc::Append { previous_index:4,previous_term:1,entries,leader_commit:4 } if entries.is_empty())));
        assert!(reservation > notices.len() * std::mem::size_of::<Message>());
        let notice = notices.into_iter().find(|m| m.to == node(4)).unwrap();
        receive_retirement_notice(&mut receiver, notice);
        assert!(leader.step(Event::Heartbeat).unwrap().is_empty());
    }
}
#[test]
fn retired_commit_exception_cannot_append_raise_terms_or_authorize_learners() {
    let leader = retiring_leader(false);
    let base = leader.state().clone();
    let sender = leader.binding;
    let notice = Message {
        group: base.bootstrap.group,
        configuration: cid(4),
        from: node(1),
        sender,
        to: node(4),
        term: 1,
        context: RequestContext {
            origin: sender,
            sequence: 91,
        },
        rpc: Rpc::Append {
            previous_index: 4,
            previous_term: 1,
            entries: vec![],
            leader_commit: 4,
        },
    };
    for variant in 0..11 {
        let mut receiver = Raft::recover_member(
            node(4),
            StoreBinding {
                identity: store(4),
                session: StoreSession::new(7).unwrap(),
            },
            base.clone(),
            leader.limits,
        )
        .unwrap();
        let mut invalid = notice.clone();
        match variant {
            0 => invalid.term = 2,
            1 => invalid.configuration = cid(3),
            2 => invalid.sender.identity = store(99),
            3 => invalid.context.origin.session = StoreSession::new(99).unwrap(),
            4 => invalid.context.sequence = 0,
            5 => {
                invalid.from = node(5);
                invalid.sender.identity = store(5);
                invalid.context.origin = invalid.sender;
            }
            6 => invalid.rpc = Rpc::ReadProbe,
            7 => {
                invalid.rpc = Rpc::Append {
                    previous_index: 4,
                    previous_term: 1,
                    entries: vec![LogEntry {
                        index: 5,
                        term: 1,
                        payload: EntryPayload::Noop,
                    }],
                    leader_commit: 4,
                }
            }
            8 => {
                invalid.rpc = Rpc::Append {
                    previous_index: 4,
                    previous_term: 2,
                    entries: vec![],
                    leader_commit: 4,
                }
            }
            9 => {
                invalid.rpc = Rpc::Append {
                    previous_index: 4,
                    previous_term: 1,
                    entries: vec![],
                    leader_commit: 5,
                }
            }
            10 => {
                let effects = receiver.step(Event::Campaign).unwrap();
                durable(&mut receiver, effects);
                invalid.term = receiver.state().hard_state.term;
            }
            _ => unreachable!(),
        }
        let before = receiver.state().clone();
        let reset = receiver.election_reset_sequence();
        assert!(
            receiver.step(Event::Receive(invalid)).is_err(),
            "variant {variant}"
        );
        assert_eq!(receiver.state(), &before);
        assert!(!receiver.has_pending_dependency());
        assert_eq!(receiver.election_reset_sequence(), reset);
    }
}

#[test]
fn retired_commit_uses_verified_joint_snapshot_history_but_not_compacted_final_history() {
    let mut leader = retiring_leader(true);
    let reference = SnapshotRef {
        store: leader.binding.identity,
        group: leader.state().bootstrap.group,
        configuration: cid(3),
        index: 3,
        term: 1,
        application_schema: 1,
        generation: SnapshotGeneration::new(1).unwrap(),
        file_bytes: 128,
        checksum: 7,
    };
    // This internal fixture asserts an already verified/pinned joint checkpoint.
    let compact = leader.begin_compact(reference).unwrap();
    durable(&mut leader, compact);
    let binding = StoreBinding {
        identity: store(4),
        session: StoreSession::new(7).unwrap(),
    };
    // The host-verified local pin belongs to the receiver's physical store.
    let mut receiver_state = leader.state().clone();
    receiver_state.snapshot.as_mut().unwrap().store = binding.identity;
    let mut receiver =
        Raft::recover_member_verified(node(4), binding, receiver_state, leader.limits).unwrap();
    assert!(acknowledge(&mut leader, 2).is_empty());
    let commit = acknowledge(&mut leader, 3);
    let notice = durable(&mut leader, commit)
        .into_iter()
        .find_map(|e| match e {
            Effect::Send(m) if m.to == node(4) => Some(m),
            _ => None,
        })
        .unwrap();
    let effects = receiver.step(Event::Receive(notice.clone())).unwrap();
    durable(&mut receiver, effects);
    assert_eq!(receiver.state().commit_index, 4);
    let final_reference = SnapshotRef {
        store: binding.identity,
        configuration: cid(4),
        index: 4,
        generation: SnapshotGeneration::new(2).unwrap(),
        ..reference
    };
    let compact = receiver.begin_compact(final_reference).unwrap();
    durable(&mut receiver, compact);
    assert_eq!(receiver.state().base_index(), 4);
    assert_eq!(
        receiver.step(Event::Receive(notice)),
        Err(RaftError::WrongIdentity)
    );
    assert_eq!(receiver.state().commit_index, 4);
}

fn authority_query(receiver: &mut Raft, head: u64) -> Message {
    one_reply(
        receiver
            .step(Event::AuthorizeReplication {
                witness: PeerIdentity {
                    node: node(1),
                    store: store(1),
                },
                candidate: PeerIdentity {
                    node: node(4),
                    store: store(4),
                },
                configuration: cid(head),
            })
            .unwrap(),
    )
}
fn authority_witness() -> Raft {
    let mut witness = joint(false);
    committed_fixture(&mut witness, 3);
    witness
}
fn promoted_message(receiver: &Raft, head: u64, rpc: Rpc) -> Message {
    let mut message = reply(
        receiver,
        4,
        RequestContext {
            origin: StoreBinding {
                identity: store(4),
                session: StoreSession::new(1).unwrap(),
            },
            sequence: 41,
        },
        rpc,
    );
    message.configuration = cid(head);
    message
}
#[test]
fn witnessed_promoted_leader_catches_up_only_after_exact_durability() {
    let mut receiver = follower(staged(), 2);
    let mut witness = authority_witness();
    let replication = promoted_message(
        &receiver,
        3,
        Rpc::Append {
            previous_index: 2,
            previous_term: 1,
            entries: vec![witness.durable.entries[2].clone()],
            leader_commit: 3,
        },
    );
    assert_eq!(
        receiver.receive_inner(replication.clone()),
        Err(RaftError::WrongIdentity)
    );
    let before = (
        receiver.durable.clone(),
        receiver.role(),
        receiver.election_reset_sequence(),
    );
    let query = authority_query(&mut receiver, 3);
    let grant = one_reply(witness.step(Event::Receive(query)).unwrap());
    assert!(matches!(
        grant.rpc,
        Rpc::AuthorityReply {
            granted: true,
            committed_index: 3,
            ..
        }
    ));
    assert!(receiver.step(Event::Receive(grant)).unwrap().is_empty());
    assert_eq!(
        (
            receiver.durable.clone(),
            receiver.role(),
            receiver.election_reset_sequence()
        ),
        before
    );
    // The public activation gate remains closed even with a permit.
    assert_eq!(
        receiver.step(Event::Receive(replication.clone())),
        Err(RaftError::InvalidMessage)
    );
    let effects = receiver.receive_inner(replication).unwrap();
    assert!(matches!(effects.as_slice(), [Effect::Persist(_)]));
    assert!(receiver.replication_permit.is_some());
    assert_eq!(receiver.durable.last_index(), 2);
    let completed = durable(&mut receiver, effects);
    assert!(completed.iter().any(|effect| matches!(
        effect,
        Effect::Send(Message {
            rpc: Rpc::Appended { success: true, .. },
            ..
        })
    )));
    assert_eq!(receiver.membership().id(), cid(3));
    assert_eq!(receiver.durable.commit_index, 3);
    assert!(receiver.replication_permit.is_none());
}
#[test]
fn witness_never_grants_from_accepted_uncommitted_promotion_or_future_head() {
    for (commit, head, granted) in [(2, 3, false), (3, 3, true), (3, 4, true), (3, 5, false)] {
        let mut witness = joint(false);
        if commit == 3 {
            committed_fixture(&mut witness, 3);
        }
        let mut receiver = follower(staged(), 2);
        let mut query = authority_query(&mut receiver, head);
        query.term = 99; // Control traffic cannot force a term update.
        let before = (
            witness.durable.clone(),
            witness.role(),
            witness.election_reset_sequence(),
        );
        let result = one_reply(witness.step(Event::Receive(query)).unwrap());
        assert!(
            matches!(result.rpc, Rpc::AuthorityReply { granted: actual, .. } if actual == granted)
        );
        assert_eq!(
            (
                witness.durable.clone(),
                witness.role(),
                witness.election_reset_sequence()
            ),
            before
        );
        receiver.step(Event::Receive(result)).unwrap();
        assert_eq!(receiver.replication_permit.is_some(), granted);
    }
}
#[test]
fn witness_reply_is_bound_to_exact_pending_request_and_local_restart_session() {
    let mut witness = authority_witness();
    let mut receiver = follower(staged(), 2);
    let query = authority_query(&mut receiver, 3);
    let grant = one_reply(witness.step(Event::Receive(query.clone())).unwrap());
    let mut invalid = Vec::new();
    let mut message = grant.clone();
    message.from = node(3);
    invalid.push(message);
    let mut message = grant.clone();
    message.sender.identity = store(9);
    invalid.push(message);
    let mut message = grant.clone();
    message.context.sequence += 1;
    invalid.push(message);
    let mut message = grant.clone();
    message.context.origin.session = StoreSession::new(2).unwrap();
    invalid.push(message);
    let mut message = grant.clone();
    message.configuration = cid(1);
    invalid.push(message);
    let mut message = grant.clone();
    message.group.incarnation = GroupIncarnation::new(2).unwrap();
    invalid.push(message);
    let mut message = grant.clone();
    message.to = node(3);
    invalid.push(message);
    let mut message = grant.clone();
    if let Rpc::AuthorityReply { candidate, .. } = &mut message.rpc {
        candidate.store = store(9);
    }
    invalid.push(message);
    let mut message = grant.clone();
    if let Rpc::AuthorityReply { committed_term, .. } = &mut message.rpc {
        *committed_term = 99;
    }
    invalid.push(message);
    let mut message = grant.clone();
    if let Rpc::AuthorityReply {
        committed_index, ..
    } = &mut message.rpc
    {
        *committed_index = receiver.membership().last_configuration_index();
    }
    invalid.push(message);
    for message in invalid {
        assert!(receiver.step(Event::Receive(message)).is_err());
        assert!(receiver.authority_request.is_some());
        assert!(receiver.replication_permit.is_none());
    }
    assert_eq!(
        receiver.step(Event::AuthorizeReplication {
            witness: PeerIdentity {
                node: node(1),
                store: store(1)
            },
            candidate: PeerIdentity {
                node: node(4),
                store: store(4)
            },
            configuration: cid(3),
        }),
        Err(RaftError::Busy)
    );
    receiver
        .step(Event::CancelReplicationAuthorization)
        .unwrap();
    let newer = authority_query(&mut receiver, 3);
    assert_ne!(query.context, newer.context);
    assert_eq!(
        receiver.step(Event::Receive(grant.clone())),
        Err(RaftError::WrongIdentity)
    );
    let mut binding = receiver.binding;
    binding.session = StoreSession::new(2).unwrap();
    let mut recovered =
        Raft::recover_member(node(2), binding, receiver.durable.clone(), receiver.limits).unwrap();
    let after_restart = authority_query(&mut recovered, 3);
    assert_ne!(after_restart.context.origin, grant.context.origin);
    assert_eq!(
        recovered.step(Event::Receive(grant)),
        Err(RaftError::WrongIdentity)
    );
}
#[test]
fn permit_survives_partial_progress_but_cannot_authorize_vote_read_or_other_heads() {
    let mut receiver = follower(staged(), 2);
    let mut witness = authority_witness();
    let query = authority_query(&mut receiver, 3);
    let grant = one_reply(witness.step(Event::Receive(query)).unwrap());
    receiver.step(Event::Receive(grant.clone())).unwrap();
    assert_eq!(
        receiver.step(Event::Receive(grant)),
        Err(RaftError::WrongIdentity)
    );
    for rpc in [
        Rpc::ReadProbe,
        Rpc::Vote {
            last_index: 3,
            last_term: 1,
        },
        Rpc::ReadAck,
    ] {
        assert_eq!(
            receiver.step(Event::Receive(promoted_message(&receiver, 3, rpc))),
            Err(RaftError::WrongIdentity)
        );
    }
    let probe = promoted_message(
        &receiver,
        3,
        Rpc::Append {
            previous_index: 3,
            previous_term: 1,
            entries: vec![],
            leader_commit: 3,
        },
    );
    let before = receiver.election_reset_sequence();
    let reply = one_reply(receiver.step(Event::Receive(probe.clone())).unwrap());
    assert!(matches!(reply.rpc, Rpc::Appended { success: false, .. }));
    assert!(receiver.replication_permit.is_some());
    assert!(receiver.election_reset_sequence() > before);
    let mut wrong = probe.clone();
    wrong.configuration = cid(4);
    assert_eq!(
        receiver.step(Event::Receive(wrong)),
        Err(RaftError::WrongIdentity)
    );
    let mut wrong = probe.clone();
    wrong.sender.identity = store(9);
    wrong.context.origin = wrong.sender;
    assert_eq!(
        receiver.step(Event::Receive(wrong)),
        Err(RaftError::WrongIdentity)
    );
    receiver
        .step(Event::CancelReplicationAuthorization)
        .unwrap();
    assert_eq!(
        receiver.step(Event::Receive(probe)),
        Err(RaftError::WrongIdentity)
    );
}
#[test]
fn witness_requires_authenticated_historical_assignment_and_surviving_history() {
    let mut receiver = follower(staged(), 2);
    let mut witness = authority_witness();
    let query = authority_query(&mut receiver, 3);
    let mut wrong = query.clone();
    wrong.sender.identity = store(9);
    wrong.context.origin = wrong.sender;
    assert_eq!(
        witness.step(Event::Receive(wrong)),
        Err(RaftError::WrongIdentity)
    );
    let mut wrong = query.clone();
    wrong.context.origin.session = StoreSession::new(2).unwrap();
    assert_eq!(
        witness.step(Event::Receive(wrong)),
        Err(RaftError::WrongIdentity)
    );
    let mut wrong = query.clone();
    wrong.configuration = cid(9);
    assert_eq!(
        witness.step(Event::Receive(wrong)),
        Err(RaftError::WrongIdentity)
    );
    // A now-retired witness retains authority to describe its committed history.
    let effects = accept(&mut witness, 101, ConfigurationChange::Final { id: cid(4) });
    durable(&mut witness, effects);
    committed_fixture(&mut witness, 4);
    let mut final_query = query;
    if let Rpc::AuthorityRequest { configuration, .. } = &mut final_query.rpc {
        *configuration = cid(4);
    }
    assert!(matches!(
        one_reply(witness.step(Event::Receive(final_query)).unwrap()).rpc,
        Rpc::AuthorityReply { granted: true, .. }
    ));
}

#[test]
fn witness_compaction_preserves_only_the_locally_retained_authorization_base() {
    let mut witness = authority_witness();
    let mut receiver = follower(staged(), 2);
    let query = authority_query(&mut receiver, 3);
    let reference = SnapshotRef {
        store: store(1),
        group: witness.state().bootstrap.group,
        generation: SnapshotGeneration::new(1).unwrap(),
        configuration: cid(2),
        index: 2,
        term: 1,
        application_schema: 1,
        file_bytes: 128,
        checksum: 7,
    };
    // Host-verified pin fixture, as in the other core compaction histories.
    let effects = witness.begin_compact(reference).unwrap();
    durable(&mut witness, effects);
    assert!(matches!(
        one_reply(witness.step(Event::Receive(query.clone())).unwrap()).rpc,
        Rpc::AuthorityReply { granted: true, .. }
    ));
    let effects = witness
        .begin_compact(SnapshotRef {
            configuration: cid(3),
            index: 3,
            generation: SnapshotGeneration::new(2).unwrap(),
            ..reference
        })
        .unwrap();
    durable(&mut witness, effects);
    assert_eq!(
        witness.step(Event::Receive(query)),
        Err(RaftError::WrongIdentity)
    );
}

#[test]
fn promoted_term_observation_waits_for_storage_and_retains_same_base_permit() {
    let mut receiver = follower(staged(), 2);
    let mut witness = authority_witness();
    let query = authority_query(&mut receiver, 4);
    let grant = one_reply(witness.step(Event::Receive(query)).unwrap());
    receiver.step(Event::Receive(grant)).unwrap();
    let mut probe = promoted_message(
        &receiver,
        4,
        Rpc::Append {
            previous_index: 2,
            previous_term: 1,
            entries: vec![],
            leader_commit: 2,
        },
    );
    probe.term = 2;
    let effects = receiver.step(Event::Receive(probe.clone())).unwrap();
    assert!(matches!(effects.as_slice(), [Effect::Persist(_)]));
    assert_eq!(receiver.state().hard_state.term, 1);
    assert!(receiver.replication_permit.is_some());
    assert_eq!(
        receiver.complete(&DurableLog { tickets: vec![] }),
        Err(RaftError::WrongCompletion)
    );
    let result = durable(&mut receiver, effects);
    assert!(matches!(
        one_reply(result).rpc,
        Rpc::Appended {
            success: true,
            matching_index: 2
        }
    ));
    assert_eq!(receiver.state().hard_state.term, 2);
    assert!(receiver.replication_permit.is_some());
    receiver.storage_failed();
    assert!(receiver.authority_request.is_none());
    assert!(receiver.replication_permit.is_none());
    assert_eq!(receiver.step(Event::Receive(probe)), Err(RaftError::Fenced));
}
#[test]
fn in_flight_promotion_cannot_emit_witness_evidence_before_completion() {
    let mut receiver = follower(staged(), 2);
    let query = authority_query(&mut receiver, 3);
    let mut witness = staged();
    let effects = accept(
        &mut witness,
        101,
        ConfigurationChange::Joint {
            id: cid(3),
            next: configuration(4, &[2, 3, 4], &[1, 5]),
        },
    );
    assert_eq!(witness.membership().id(), cid(3));
    assert_eq!(
        witness.step(Event::Receive(query.clone())),
        Err(RaftError::Busy)
    );
    durable(&mut witness, effects);
    assert!(matches!(
        one_reply(witness.step(Event::Receive(query)).unwrap()).rpc,
        Rpc::AuthorityReply { granted: false, .. }
    ));
}

#[test]
fn connection_retention_includes_pending_and_rollback_views_until_exact_commit() {
    let mut core = staged();
    let initial = core.connection_replicas(6).unwrap();
    let effects = accept(
        &mut core,
        200,
        ConfigurationChange::Learners(configuration(3, &[1, 2, 3], &[4, 6])),
    );
    assert_eq!(core.state().membership().unwrap().id(), cid(2));
    let pending = core.connection_replicas(6).unwrap();
    assert_eq!(
        pending.keys().copied().collect::<Vec<_>>(),
        (1..=6).map(node).collect::<Vec<_>>()
    );
    assert!(core.connection_replicas(5).is_err());
    durable(&mut core, effects);
    assert_eq!(core.connection_replicas(6).unwrap(), pending);
    // Roll back the accepted learner assignment through the actual suffix path.
    let effects = core
        .persist(
            core.state().hard_state,
            2,
            Some(Suffix {
                from: 3,
                entries: vec![],
            }),
            After::Reply,
            None,
        )
        .unwrap();
    assert_eq!(core.connection_replicas(6).unwrap(), pending);
    durable(&mut core, effects);
    assert_eq!(core.connection_replicas(6).unwrap(), initial);
    let effects = accept(
        &mut core,
        201,
        ConfigurationChange::Learners(configuration(3, &[1, 2, 3], &[4, 6])),
    );
    durable(&mut core, effects);
    let effects = core
        .persist(core.state().hard_state, 3, None, After::Commit, None)
        .unwrap();
    // Commit intent is insufficient to revoke the old learner's connection.
    assert_eq!(core.connection_replicas(6).unwrap(), pending);
    durable(&mut core, effects);
    let committed = core.connection_replicas(6).unwrap();
    assert!(!committed.contains_key(&node(5)));
    assert!(committed.contains_key(&node(6)));
    core.storage_failed();
    assert_eq!(core.connection_replicas(6), Err(RaftError::Fenced));
}
#[test]
fn connection_retention_keeps_committed_joint_predecessor_until_final_is_durable() {
    let mut core = staged();
    let effects = accept(
        &mut core,
        202,
        ConfigurationChange::Joint {
            id: cid(3),
            next: configuration(4, &[2, 3, 4], &[5]),
        },
    );
    durable(&mut core, effects);
    committed_fixture(&mut core, 3);
    let effects = accept(&mut core, 202, ConfigurationChange::Final { id: cid(4) });
    assert!(core.connection_replicas(6).unwrap().contains_key(&node(1)));
    durable(&mut core, effects);
    assert!(core.connection_replicas(6).unwrap().contains_key(&node(1)));
    let effects = core
        .persist(core.state().hard_state, 4, None, After::Commit, None)
        .unwrap();
    assert!(core.connection_replicas(6).unwrap().contains_key(&node(1)));
    durable(&mut core, effects);
    assert!(!core.connection_replicas(6).unwrap().contains_key(&node(1)));
}

#[test]
fn owner_connection_budget_retains_pending_and_rollback_history_and_fences_unreserved_mutation() {
    use crate::runtime::{
        ConnectionBudget, MonoTime, RuntimeError, RuntimeOwner, Shard, ShardLimits,
    };
    use crate::secure::LocalIdentity;
    let c = staged();
    let group = c.state().bootstrap.group;
    let binding = c.storage_binding();
    let mut shard = Shard::new(
        RuntimeOwner {
            store: binding,
            lane: ExecutionLaneId::new(1).unwrap(),
            generation: RuntimeGeneration::new(1).unwrap(),
        },
        ShardLimits {
            max_groups: 16,
            ..ShardLimits::default()
        },
        MembershipReady::default(),
    )
    .unwrap();
    shard.register(c).unwrap();
    shard
        .set_connection_budget(
            ConnectionBudget::new(
                LocalIdentity {
                    node: node(1),
                    store: binding,
                },
                5,
                BTreeMap::new(),
            )
            .unwrap(),
        )
        .unwrap();
    assert_eq!(shard.reserved_connection_peers(), Ok(Some(4)));
    shard.admit(group, Event::Heartbeat).unwrap();
    let visit = shard.poll(MonoTime(0)).unwrap().unwrap();
    let effects = shard
        .with_core(visit, |core| {
            accept(
                core,
                300,
                ConfigurationChange::Learners(configuration(3, &[1, 2, 3], &[4, 6])),
            )
        })
        .unwrap();
    assert!(shard.core(group).unwrap().has_pending_dependency());
    assert_eq!(shard.reserved_connection_peers(), Ok(Some(5)));
    shard
        .with_core(visit, |core| durable(core, effects))
        .unwrap();
    let effects = shard
        .with_core(visit, |core| {
            core.persist(
                core.state().hard_state,
                2,
                Some(Suffix {
                    from: 3,
                    entries: vec![],
                }),
                After::Reply,
                None,
            )
            .unwrap()
        })
        .unwrap();
    shard
        .with_core(visit, |core| durable(core, effects))
        .unwrap();
    assert!(!shard
        .core(group)
        .unwrap()
        .connection_replicas(6)
        .unwrap()
        .contains_key(&node(6)));
    // The accepted connection identity remains a bounded inactive history record.
    assert_eq!(shard.reserved_connection_peers(), Ok(Some(5)));
    let result = shard.with_core(visit, |core| {
        accept(
            core,
            301,
            ConfigurationChange::Learners(configuration(3, &[1, 2, 3], &[4, 7])),
        )
    });
    assert_eq!(result, Err(RuntimeError::PeerCapacity));
    assert!(shard.core(group).unwrap().is_fenced());
    assert_eq!(shard.reserved_connection_peers(), Ok(Some(5)));
}

#[test]
fn connection_reservations_cover_staged_snapshot_and_verified_promoted_peer_permit() {
    let mut receiver = follower(core(), 2);
    let (leader, _, snapshot) = compacted_joint();
    let mut message = reply(
        &receiver,
        1,
        RequestContext {
            origin: leader.binding,
            sequence: 900,
        },
        Rpc::Snapshot {
            snapshot: Box::new(snapshot),
        },
    );
    message.configuration = cid(3);
    assert_eq!(receiver.connection_replicas(6).unwrap().len(), 3);
    assert_eq!(
        receiver
            .event_connection_replicas(&Event::Receive(message.clone()), 6)
            .unwrap()
            .len(),
        5
    );
    let effects = receiver.receive_inner(message).unwrap();
    assert!(matches!(&effects[..], [Effect::StageSnapshot(_)]));
    assert!(receiver.staged_snapshot.is_some());
    assert!(receiver.pending.is_none());
    assert_eq!(receiver.connection_replicas(6).unwrap().len(), 5);
    assert!(receiver.connection_replicas(4).is_err());

    let mut receiver = follower(core(), 2);
    let mut witness = authority_witness();
    let query = authority_query(&mut receiver, 3);
    assert!(!receiver
        .connection_replicas(4)
        .unwrap()
        .contains_key(&node(4)));
    let reply = one_reply(witness.step(Event::Receive(query)).unwrap());
    assert!(matches!(
        reply.rpc,
        Rpc::AuthorityReply { granted: true, .. }
    ));
    assert_eq!(
        receiver
            .event_connection_replicas(&Event::Receive(reply.clone()), 4)
            .unwrap()
            .len(),
        4
    );
    receiver.step(Event::Receive(reply)).unwrap();
    assert_eq!(receiver.membership().id(), cid(1));
    assert_eq!(receiver.connection_replicas(4).unwrap()[&node(4)], store(4));
    receiver
        .step(Event::CancelReplicationAuthorization)
        .unwrap();
    assert!(!receiver
        .connection_replicas(4)
        .unwrap()
        .contains_key(&node(4)));
}

fn finalized_repair_pair() -> (Raft, Raft) {
    let (mut source, receiver) = retained_repair_pair(130, 0);
    let joint_index = source.state().last_index();
    committed_fixture(&mut source, joint_index);
    let entry = LogEntry {
        index: joint_index + 1,
        term: 1,
        payload: EntryPayload::Configuration(Box::new(ConfigurationRecord {
            operation: operation(101),
            expected: cid(3),
            change: ConfigurationChange::Final { id: cid(4) },
        })),
    };
    let effects = source
        .persist(
            source.durable.hard_state,
            joint_index + 1,
            Some(Suffix {
                from: entry.index,
                entries: vec![entry],
            }),
            After::Commit,
            None,
        )
        .unwrap();
    durable(&mut source, effects);
    assert!(source.membership().joint().is_none());
    source = source.with_batched_joint_repair();
    (source, receiver)
}

#[test]
fn stable_final_candidate_repairs_retained_promotion_before_ordinary_ballot() {
    let (mut source, mut receiver) = finalized_repair_pair();
    let joint_index = source.state().last_index() - 1;
    let effects = source.step(Event::Campaign).unwrap();
    let mut request = durable(&mut source, effects)
        .into_iter()
        .find_map(|effect| match effect {
            Effect::Send(m) if m.to == node(4) && matches!(m.rpc, Rpc::LearnerRepair { .. }) => {
                Some(m)
            }
            _ => None,
        })
        .expect("finalized candidate must recover the lagging promoted learner");
    // Lose the first durable repair reply and restart both volatile owners.
    let effects = receiver.step(Event::Receive(request)).unwrap();
    let lost_ack = one_reply(durable(&mut receiver, effects));
    let restart = |core: Raft, id| {
        let mut binding = core.binding;
        binding.session = StoreSession::new(2).unwrap();
        Raft::recover_member(node(id), binding, core.state().clone(), core.limits).unwrap()
    };
    source = restart(source, 2).with_batched_joint_repair();
    receiver = restart(receiver, 4);
    let effects = source.step(Event::Campaign).unwrap();
    request = durable(&mut source, effects)
        .into_iter()
        .find_map(|effect| match effect {
            Effect::Send(m) if m.to == node(4) && matches!(m.rpc, Rpc::LearnerRepair { .. }) => {
                Some(m)
            }
            _ => None,
        })
        .unwrap();
    assert_eq!(
        source.step(Event::Receive(lost_ack)),
        Err(RaftError::WrongIdentity)
    );
    let mut batches = 0;
    loop {
        assert_eq!(request.configuration, cid(3));
        let before = receiver.state().clone();
        let effects = receiver.step(Event::Receive(request)).unwrap();
        assert_eq!(receiver.state(), &before);
        let ack = one_reply(durable(&mut receiver, effects));
        let mut stale = ack.clone();
        stale.context.sequence += 1;
        assert!(source.step(Event::Receive(stale)).unwrap().is_empty());
        let mut foreign = ack.clone();
        foreign.sender.identity = store(99);
        assert_eq!(
            source.step(Event::Receive(foreign)),
            Err(RaftError::WrongIdentity)
        );
        let next = one_reply(source.step(Event::Receive(ack.clone())).unwrap());
        let duplicate = source.step(Event::Receive(ack));
        if matches!(next.rpc, Rpc::Vote { .. }) {
            assert_eq!(duplicate, Err(RaftError::WrongIdentity));
        } else {
            assert!(duplicate.unwrap().is_empty());
        }
        assert_eq!(source.role(), Role::Candidate);
        assert_eq!(source.state().commit_index, joint_index + 1);
        assert_eq!(receiver.state().commit_index, 2);
        batches += 1;
        if matches!(next.rpc, Rpc::Vote { .. }) {
            assert_eq!(next.configuration, cid(4));
            assert!(receiver.local_voter());
            assert_eq!(receiver.state().last_index(), joint_index);
            let effects = receiver.step(Event::Receive(next)).unwrap();
            let ballot = one_reply(durable(&mut receiver, effects));
            assert!(matches!(ballot.rpc, Rpc::Voted { granted: true }));
            let effects = source.step(Event::Receive(ballot)).unwrap();
            durable(&mut source, effects);
            assert_eq!(source.role(), Role::Leader);
            break;
        }
        request = next;
        assert!(batches < 4);
    }
    assert_eq!(batches, 3);
}

#[test]
fn historical_repair_higher_term_needs_exact_context_and_durable_observation() {
    let (mut source, mut receiver) = finalized_repair_pair();
    let effects = source.step(Event::Campaign).unwrap();
    let request = durable(&mut source, effects)
        .into_iter()
        .find_map(|effect| match effect {
            Effect::Send(m) if m.to == node(4) && matches!(m.rpc, Rpc::LearnerRepair { .. }) => {
                Some(m)
            }
            _ => None,
        })
        .unwrap();
    let effects = receiver.step(Event::Receive(request)).unwrap();
    let mut reply = one_reply(durable(&mut receiver, effects));
    reply.term += 1;
    let before = source.state().clone();
    let mut stale = reply.clone();
    stale.context.sequence += 1;
    assert!(source.step(Event::Receive(stale)).unwrap().is_empty());
    assert_eq!(source.state(), &before);
    assert_eq!(source.role(), Role::Candidate);
    let effects = source.step(Event::Receive(reply.clone())).unwrap();
    assert_eq!(source.role(), Role::Follower);
    assert_eq!(source.state(), &before);
    assert!(matches!(effects.as_slice(), [Effect::Persist(_)]));
    assert!(durable(&mut source, effects).is_empty());
    assert_eq!(source.state().hard_state.term, reply.term);
    assert_eq!(source.state().hard_state.voted_for, None);
    assert_eq!(source.state().commit_index, before.commit_index);
    assert_eq!(source.membership().id(), cid(4));
}

#[test]
fn historical_repair_requires_old_voter_source_and_retained_promotion_evidence() {
    let (source, _) = finalized_repair_pair();
    let mut promoted = Raft::recover_member(
        node(4),
        StoreBinding {
            identity: store(4),
            session: StoreSession::new(1).unwrap(),
        },
        source.state().clone(),
        source.limits,
    )
    .unwrap()
    .with_batched_joint_repair();
    let effects = promoted.step(Event::Campaign).unwrap();
    let effects = durable(&mut promoted, effects);
    assert!(!effects.iter().any(|effect| matches!(
        effect,
        Effect::Send(Message {
            rpc: Rpc::LearnerRepair { .. },
            ..
        })
    )));
    let mut compacted = source;
    let reference = SnapshotRef {
        store: store(2),
        group: compacted.state().bootstrap.group,
        generation: SnapshotGeneration::new(1).unwrap(),
        configuration: cid(4),
        index: compacted.state().commit_index,
        term: 1,
        application_schema: 1,
        file_bytes: 128,
        checksum: 7,
    };
    let effects = compacted.begin_compact(reference).unwrap();
    durable(&mut compacted, effects);
    let effects = compacted.step(Event::Campaign).unwrap();
    let effects = durable(&mut compacted, effects);
    assert!(effects.iter().any(|effect| matches!(
        effect,
        Effect::Send(Message {
            rpc: Rpc::Vote { .. },
            ..
        })
    )));
    assert!(!effects.iter().any(|effect| matches!(
        effect,
        Effect::Send(Message {
            rpc: Rpc::LearnerRepair { .. },
            ..
        })
    )));
}

fn message_to(effects: Vec<Effect>, id: u64) -> Message {
    effects
        .into_iter()
        .find_map(|e| match e {
            Effect::Send(m) if m.to == node(id) => Some(m),
            _ => None,
        })
        .unwrap()
}
fn recover_membership_peer(id: u64, state: GroupLog, session: u64) -> Raft {
    Raft::recover_member(
        node(id),
        StoreBinding {
            identity: store(id),
            session: StoreSession::new(session).unwrap(),
        },
        state,
        LogLimits::default(),
    )
    .unwrap()
}
fn partial_joint_survivors() -> BTreeMap<u64, Raft> {
    fn weighted(voters: &[u64], required: u64) -> Policy {
        Policy::new(
            Tree::Weighted(
                voters
                    .iter()
                    .map(|id| WeightedChild {
                        weight: if *id == required { 3 } else { 1 },
                        node: Tree::Voter(node(*id)),
                    })
                    .collect(),
            ),
            Limits::default(),
        )
        .unwrap()
    }

    let initial = core();
    let mut state = initial.state().clone();
    state.bootstrap.policy = weighted(&[1, 2, 3], 2);
    let mut leader = recover_membership_peer(1, state, 1);
    leader.role = Role::Leader;
    leader.initialize_replication().unwrap();
    let learners = Configuration::new(
        cid(2),
        leader.state().bootstrap.policy.clone(),
        leader.state().bootstrap.voter_stores.clone(),
        [(node(5), store(5))].into(),
    )
    .unwrap();
    let effects = accept(&mut leader, 100, ConfigurationChange::Learners(learners));
    durable(&mut leader, effects);
    committed_fixture(&mut leader, 2);
    let old = leader.state().clone();
    let next = Configuration::new(
        cid(4),
        weighted(&[2, 3, 5], 5),
        [2, 3, 5]
            .into_iter()
            .map(|id| (node(id), store(id)))
            .collect(),
        BTreeMap::new(),
    )
    .unwrap();
    let effects = accept(
        &mut leader,
        101,
        ConfigurationChange::Joint { id: cid(3), next },
    );
    durable(&mut leader, effects);
    let joint = leader.state().clone();
    assert_eq!(joint.commit_index, 2);
    // Only node 2 received the joint record. Lose node 1 before further delivery.
    // Nodes 2/3/5 contain both policy quorums, but node 5 is still a learner.
    let survivors: BTreeMap<_, _> = [(2, joint), (3, old.clone()), (5, old)]
        .into_iter()
        .map(|(id, state)| (id, recover_membership_peer(id, state, 1)))
        .collect();
    survivors
}
fn lose_joint_repair_rounds(survivors: &mut BTreeMap<u64, Raft>) {
    let heads = survivors
        .iter()
        .map(|(&id, core)| (id, core.state().last_index()))
        .collect::<BTreeMap<_, _>>();
    let mut denied_learner_votes = 0;
    let mut denied_stale_votes = 0;
    for round in 0..4 {
        for candidate in [2, 3] {
            let (learner_votes, stale_votes) = lose_joint_repair_campaign(survivors, candidate);
            denied_learner_votes += learner_votes;
            denied_stale_votes += stale_votes;
        }
        for (&id, core) in survivors.iter() {
            assert_ne!(core.role(), Role::Leader);
            assert_eq!(core.state().commit_index, 2);
            assert_eq!(core.state().last_index(), heads[&id]);
        }
        let learner = survivors.get_mut(&5).unwrap();
        let term = learner.state().hard_state.term;
        assert_eq!(learner.step(Event::Campaign), Err(RaftError::NotVoter));
        assert_eq!(learner.state().hard_state.term, term);
        // Restart cannot manufacture delivery or promote the missing joint view.
        *survivors = std::mem::take(survivors)
            .into_iter()
            .map(|(id, core)| {
                (
                    id,
                    recover_membership_peer(id, core.state().clone(), round + 2),
                )
            })
            .collect();
    }
    assert_eq!(denied_learner_votes, 4);
    assert_eq!(denied_stale_votes, 4);
}
fn recursive_joint_peers() -> BTreeMap<u64, Raft> {
    fn nested(optional: u64, required: [u64; 2]) -> Policy {
        Policy::new(
            Tree::Weighted(vec![
                WeightedChild {
                    weight: 1,
                    node: Tree::Voter(node(optional)),
                },
                WeightedChild {
                    weight: 3,
                    node: Tree::Majority(required.map(|id| Tree::Voter(node(id))).to_vec()),
                },
            ]),
            Limits::default(),
        )
        .unwrap()
    }
    let mut base = core().state().clone();
    base.bootstrap.policy = nested(1, [2, 3]);
    let mut leader = Raft::recover_member(
        node(1),
        StoreBinding {
            identity: store(1),
            session: StoreSession::new(1).unwrap(),
        },
        base,
        LogLimits::default(),
    )
    .unwrap();
    leader.role = Role::Leader;
    leader.initialize_replication().unwrap();
    let learners = Configuration::new(
        cid(2),
        leader.state().bootstrap.policy.clone(),
        leader.state().bootstrap.voter_stores.clone(),
        [(node(5), store(5))].into(),
    )
    .unwrap();
    let effects = accept(&mut leader, 100, ConfigurationChange::Learners(learners));
    durable(&mut leader, effects);
    committed_fixture(&mut leader, 2);
    let old = leader.state().clone();
    let target = Configuration::new(
        cid(4),
        nested(2, [3, 5]),
        [2, 3, 5].map(|id| (node(id), store(id))).into(),
        BTreeMap::new(),
    )
    .unwrap();
    let effects = accept(
        &mut leader,
        101,
        ConfigurationChange::Joint {
            id: cid(3),
            next: target,
        },
    );
    durable(&mut leader, effects);
    let joint = leader.state().clone();
    let peers: BTreeMap<_, _> = [(2, joint), (3, old.clone()), (5, old)]
        .into_iter()
        .map(|(id, state)| {
            let core = Raft::recover_member(
                node(id),
                StoreBinding {
                    identity: store(id),
                    session: StoreSession::new(2).unwrap(),
                },
                state,
                LogLimits::default(),
            )
            .unwrap()
            .with_batched_joint_repair()
            .with_configuration_replication();
            (id, core)
        })
        .collect();
    peers
}
fn deliver_recursive_effect(
    peers: &mut BTreeMap<u64, Raft>,
    effect: Effect,
    queue: &mut std::collections::VecDeque<Effect>,
    ready: &mut Vec<ReadBarrier>,
) {
    match effect {
        Effect::Send(message) => {
            let Some(peer) = peers.get_mut(&message.to.get()) else {
                return;
            };
            let effects = peer.step(Event::Receive(message)).unwrap();
            queue.extend(if peer.has_pending_dependency() {
                durable(peer, effects)
            } else {
                effects
            });
        }
        Effect::ReadReady(barrier) => ready.push(barrier),
        Effect::Committed { .. } => (),
        other => panic!("unexpected effect {other:?}"),
    }
}
fn reused_candidate_fixture() -> (Raft, BallotOrigin, StoreIdentity) {
    let mut core = staged();
    core.role = Role::Follower;
    let sender = StoreBinding {
        identity: store(2),
        session: StoreSession::new(1).unwrap(),
    };
    let context = RequestContext {
        origin: sender,
        sequence: 50,
    };
    let vote = reply(
        &core,
        2,
        context,
        Rpc::Vote {
            last_index: 2,
            last_term: 1,
        },
    );
    let effects = core.step(Event::Receive(vote)).unwrap();
    let effects = durable(&mut core, effects);
    assert!(matches!(
        &effects[..],
        [Effect::Send(Message {
            rpc: Rpc::Voted { granted: true },
            ..
        })]
    ));
    let original = core.durable.ballot_origin.unwrap();
    core.role = Role::Leader;
    let effects = accept(
        &mut core,
        101,
        ConfigurationChange::Joint {
            id: cid(3),
            next: configuration(4, &[1, 3], &[2, 4, 5]),
        },
    );
    durable(&mut core, effects);
    committed_fixture(&mut core, 3);
    let effects = accept(&mut core, 101, ConfigurationChange::Final { id: cid(4) });
    durable(&mut core, effects);
    committed_fixture(&mut core, 4);
    let replacement = StoreIdentity {
        id: StoreId::new(20).unwrap(),
        incarnation: StoreIncarnation::new(2).unwrap(),
    };
    let next = configuration(5, &[1, 3], &[2, 4, 5]);
    let mut learners = next.learners().clone();
    learners.insert(node(2), replacement);
    let next = Configuration::new(
        cid(5),
        next.policy().clone(),
        next.voter_stores().clone(),
        learners,
    )
    .unwrap();
    let effects = accept(&mut core, 102, ConfigurationChange::Learners(next));
    durable(&mut core, effects);
    committed_fixture(&mut core, 5);
    let next = configuration(7, &[1, 2, 3], &[4, 5]);
    let mut voters = next.voter_stores().clone();
    voters.insert(node(2), replacement);
    let next = Configuration::new(
        cid(7),
        next.policy().clone(),
        voters,
        next.learners().clone(),
    )
    .unwrap();
    let effects = accept(
        &mut core,
        103,
        ConfigurationChange::Joint { id: cid(6), next },
    );
    durable(&mut core, effects);
    assert_eq!(core.durable.ballot_origin, Some(original));
    (core, original, replacement)
}
fn check_scoped_append_reply(leader: &mut Raft, reply: Message) {
    let context = leader.requests[&node(2)].context;
    let mut stale = reply.clone();
    stale.configuration = cid(2);
    assert_eq!(
        leader.step(Event::Receive(stale)),
        Err(RaftError::WrongIdentity)
    );
    assert_eq!(leader.requests[&node(2)].context, context);
    assert_eq!(leader.progress[&node(2)], 0);
    assert!(leader
        .step(Event::Receive(reply.clone()))
        .unwrap()
        .is_empty());
    assert_eq!(leader.progress[&node(2)], 3);
    assert_eq!(leader.state().commit_index, 2); // weighted new side still lacks node 3
    assert!(leader.step(Event::Receive(reply)).unwrap().is_empty());
    assert_eq!(leader.progress[&node(2)], 3);
}
fn check_snapshot_ack_then_retry(
    leader: &mut Raft,
    reply: Message,
    message: Message,
    reference: SnapshotRef,
    to: NodeId,
    context: RequestContext,
) {
    let mut stale = reply.clone();
    stale.configuration = cid(3);
    assert_eq!(
        leader.step(Event::Receive(stale)),
        Err(RaftError::WrongIdentity)
    );
    assert_eq!(leader.progress[&node(2)], 0);
    let append = one_reply(leader.step(Event::Receive(reply)).unwrap());
    assert_eq!(leader.progress[&node(2)], 3);
    assert_eq!(append.configuration, cid(4));
    assert!(
        matches!(&append.rpc, Rpc::Append { previous_index: 3, entries, .. } if entries.len() == 1)
    );
    assert!(matches!(
        leader.snapshot_send(
            to,
            context,
            reference,
            match message.rpc {
                Rpc::Snapshot { snapshot } => *snapshot,
                _ => unreachable!(),
            }
        ),
        Err(RaftError::WrongCompletion)
    ));
}
fn receive_retirement_notice(receiver: &mut Raft, notice: Message) {
    let reset = receiver.election_reset_sequence();
    let hard = receiver.state().hard_state;
    let effects = receiver.step(Event::Receive(notice.clone())).unwrap();
    assert!(
        matches!(&effects[..],[Effect::Persist(u)] if u.commit_index==4 && u.suffix.is_none() && u.hard_state==hard)
    );
    assert_eq!(receiver.state().commit_index, 3);
    assert_eq!(
        receiver.complete(&DurableLog { tickets: vec![] }),
        Err(RaftError::WrongCompletion)
    );
    let effects = durable(receiver, effects);
    assert_eq!(receiver.state().commit_index, 4);
    assert_eq!(receiver.election_reset_sequence(), reset);
    assert_eq!(receiver.state().hard_state, hard);
    assert_eq!(receiver.role(), Role::Follower);
    assert!(effects.iter().any(
        |e| matches!(e,Effect::Committed(entries) if entries.len()==1 && entries[0].index==4)
    ));
    assert!(effects.iter().any(|e|matches!(e,Effect::Send(m) if m.to==node(1) && matches!(m.rpc,Rpc::Appended{success:true,matching_index:4}))));
    let duplicate = receiver.step(Event::Receive(notice)).unwrap();
    assert!(matches!(&duplicate[..], [Effect::Send(_)]));
    assert_eq!(receiver.election_reset_sequence(), reset);
}
#[derive(Default)]
struct MembershipReady(BTreeSet<GroupIdentity>);
impl crate::runtime::ReadyScheduler for MembershipReady {
    fn capacity(&self) -> usize {
        16
    }
    fn enqueue(&mut self, group: GroupIdentity) -> Result<(), crate::runtime::RuntimeError> {
        self.0.insert(group);
        Ok(())
    }
    fn pop(&mut self) -> Option<GroupIdentity> {
        self.0.pop_first()
    }
    fn cancel(&mut self, group: GroupIdentity) {
        self.0.remove(&group);
    }
    fn len(&self) -> usize {
        self.0.len()
    }
}

fn lose_joint_repair_campaign(
    survivors: &mut BTreeMap<u64, Raft>,
    candidate: u64,
) -> (usize, usize) {
    let mut denied_learner_votes = 0;
    let mut denied_stale_votes = 0;
    let effects = survivors
        .get_mut(&candidate)
        .unwrap()
        .step(Event::Campaign)
        .unwrap();
    let requests = durable(survivors.get_mut(&candidate).unwrap(), effects);
    for request in requests {
        let Effect::Send(request) = request else {
            panic!("campaign must send votes only")
        };
        if matches!(request.rpc, Rpc::Append { .. }) {
            assert_eq!(request.to, node(5));
            continue; // Lose repair traffic during these rounds.
        }
        assert!(matches!(request.rpc, Rpc::Vote { .. }));
        let target = request.to.get();
        let Some(receiver) = survivors.get_mut(&target) else {
            continue;
        };
        let effects = receiver.step(Event::Receive(request)).unwrap();
        let effects = if receiver.has_pending_dependency() {
            durable(receiver, effects)
        } else {
            effects
        };
        let response = one_reply(effects);
        if target == 5 {
            assert!(matches!(response.rpc, Rpc::Voted { granted: false }));
            assert!(receiver.state().hard_state.voted_for.is_none());
            denied_learner_votes += 1;
        }
        if candidate == 3 && target == 2 {
            assert!(matches!(response.rpc, Rpc::Voted { granted: false }));
            denied_stale_votes += 1;
        }
        let sender = survivors.get_mut(&candidate).unwrap();
        let effects = sender.step(Event::Receive(response)).unwrap();
        if sender.has_pending_dependency() {
            durable(sender, effects);
        } else {
            assert!(effects.is_empty());
        }
    }
    (denied_learner_votes, denied_stale_votes)
}
