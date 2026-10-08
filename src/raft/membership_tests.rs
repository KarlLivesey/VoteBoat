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
fn replication_scope_bridge_does_not_authorize_learners_or_reads_or_elections() {
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
    message.from = node(2);
    message.sender.identity = store(2);
    message.context.origin = message.sender;
    for rpc in [
        Rpc::ReadProbe,
        Rpc::Vote {
            last_index: 2,
            last_term: 1,
        },
    ] {
        message.rpc = rpc;
        assert_eq!(
            core.step(Event::Receive(message.clone())),
            Err(RaftError::WrongIdentity)
        );
    }
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
}
