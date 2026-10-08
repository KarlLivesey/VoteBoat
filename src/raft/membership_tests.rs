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
