// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Pure correlation controls; fixture completion asserts no provider durability.
use super::*;
use crate::quorum::{Limits, Policy, Tree};
fn node(id: u64) -> NodeId {
    NodeId::new(id).unwrap()
}
fn binding(id: u64) -> StoreBinding {
    StoreBinding {
        identity: StoreIdentity {
            id: StoreId::new(u128::from(id)).unwrap(),
            incarnation: StoreIncarnation::new(1).unwrap(),
        },
        session: StoreSession::new(1).unwrap(),
    }
}
fn campaign() -> (Raft, Message) {
    let bootstrap = Bootstrap {
        group: GroupIdentity {
            id: GroupId::new(1).unwrap(),
            incarnation: GroupIncarnation::new(1).unwrap(),
        },
        configuration: ConfigurationId::new(1).unwrap(),
        policy: Policy::new(
            Tree::Majority((1..=3).map(|id| Tree::Voter(node(id))).collect()),
            Limits::default(),
        )
        .unwrap(),
        voter_stores: (1..=3).map(|id| (node(id), binding(id).identity)).collect(),
    };
    let mut states = BTreeMap::new();
    apply_batch(
        &mut states,
        &[LogMutation::Create(bootstrap.clone())],
        LogLimits::default(),
    )
    .unwrap();
    let mut core = Raft::recover(
        node(1),
        binding(1),
        states.remove(&bootstrap.group).unwrap(),
        LogLimits::default(),
    )
    .unwrap();
    let effects = core.step(Event::Campaign).unwrap();
    let effects = durable(&mut core, effects);
    let Effect::Send(message) = effects.into_iter().next().unwrap() else {
        panic!("original Vote")
    };
    (core, message)
}
#[test]
fn only_exact_current_candidate_vote_refreshes() {
    let (mut core, message) = campaign();
    let reset = core.election_reset_sequence();
    for mutate in [
        |m: &mut Message| m.term += 1,
        |m: &mut Message| m.context.sequence += 1,
        |m: &mut Message| m.context.origin = binding(2),
        |m: &mut Message| m.sender = binding(2),
        |m: &mut Message| m.from = node(2),
        |m: &mut Message| m.to = node(1),
        |m: &mut Message| m.to = node(4),
        |m: &mut Message| m.configuration = ConfigurationId::new(2).unwrap(),
        |m: &mut Message| m.group.incarnation = GroupIncarnation::new(2).unwrap(),
        |m: &mut Message| {
            m.rpc = Rpc::Vote {
                last_index: 1,
                last_term: 1,
            }
        },
        |m: &mut Message| m.rpc = Rpc::Voted { granted: false },
        |m: &mut Message| m.rpc = Rpc::Voted { granted: true },
    ] {
        let mut wrong = message.clone();
        mutate(&mut wrong);
        core.note_logical_vote_transferred(&wrong).unwrap();
        assert_eq!(core.election_reset_sequence(), reset);
    }
    core.note_logical_vote_transferred(&message).unwrap();
    assert_eq!(core.election_reset_sequence(), reset + 1);
    assert_eq!(core.state().hard_state.term, 1);
}
#[test]
fn unrelated_roles_and_old_campaign_are_unchanged_and_overflow_refuses() {
    let (mut core, message) = campaign();
    let reset = core.election_reset_sequence();
    for role in [Role::Follower, Role::Leader] {
        core.role = role;
        core.note_logical_vote_transferred(&message).unwrap();
        assert_eq!(core.election_reset_sequence(), reset);
    }
    core.role = Role::Candidate;
    core.vote_context = None;
    core.note_logical_vote_transferred(&message).unwrap();
    assert_eq!(core.election_reset_sequence(), reset);
    core.vote_context = Some(message.context);
    core.election_reset = u64::MAX;
    assert_eq!(
        core.note_logical_vote_transferred(&message),
        Err(RaftError::Exhausted)
    );
    assert_eq!(core.election_reset_sequence(), u64::MAX);
}

fn durable(core: &mut Raft, effects: Vec<Effect>) -> Vec<Effect> {
    assert!(matches!(effects.as_slice(), [Effect::Persist(_)]));
    let next = core.pending.as_ref().unwrap().next.metadata();
    let ticket = LogTicket {
        binding: core.binding,
        batch: core.last_batch + 1,
        group: core.durable.bootstrap.group,
        revision: next.revision,
        generation: next.generation,
        last_index: next.last_index,
        term: next.hard_state.term,
    };
    core.admitted(ticket).unwrap();
    core.complete(&DurableLog {
        tickets: vec![ticket],
    })
    .unwrap()
}
fn granted_reply() -> (Raft, Message) {
    let (mut core, _) = campaign();
    let sender = binding(2);
    let request = Message {
        group: core.durable.bootstrap.group,
        configuration: core.membership().id(),
        from: node(2),
        sender,
        to: node(1),
        term: 2,
        context: RequestContext {
            origin: sender,
            sequence: 1,
        },
        rpc: Rpc::Vote {
            last_index: 0,
            last_term: 0,
        },
    };
    let effects = core.step(Event::Receive(request)).unwrap();
    let effects = durable(&mut core, effects);
    let Effect::Send(message) = effects.into_iter().next().unwrap() else {
        panic!("original grant")
    };
    assert!(matches!(message.rpc, Rpc::Voted { granted: true }));
    (core, message)
}
#[test]
fn granted_reply_requires_exact_original_follower_ballot_context_and_store() {
    let (mut core, message) = granted_reply();
    let reset = core.election_reset_sequence();
    for mutate in [
        |m: &mut Message| m.term += 1,
        |m: &mut Message| m.context.sequence += 1,
        |m: &mut Message| m.context.origin.session = StoreSession::new(2).unwrap(),
        |m: &mut Message| m.context.origin = binding(3),
        |m: &mut Message| m.sender = binding(2),
        |m: &mut Message| m.from = node(2),
        |m: &mut Message| m.to = node(3),
        |m: &mut Message| m.configuration = ConfigurationId::new(2).unwrap(),
        |m: &mut Message| m.group.incarnation = GroupIncarnation::new(2).unwrap(),
        |m: &mut Message| m.rpc = Rpc::Voted { granted: false },
    ] {
        let mut wrong = message.clone();
        mutate(&mut wrong);
        core.note_logical_vote_transferred(&wrong).unwrap();
        assert_eq!(core.election_reset_sequence(), reset);
    }
    for role in [Role::Candidate, Role::Leader] {
        core.role = role;
        core.note_logical_vote_transferred(&message).unwrap();
        assert_eq!(core.election_reset_sequence(), reset);
    }
    core.role = Role::Follower;
    let ballot = core.durable.ballot_origin.unwrap();
    core.durable.ballot_origin.as_mut().unwrap().candidate_store = binding(3).identity;
    core.note_logical_vote_transferred(&message).unwrap();
    assert_eq!(core.election_reset_sequence(), reset);
    core.durable.ballot_origin = Some(ballot);
    core.durable.hard_state.voted_for = Some(node(3));
    core.note_logical_vote_transferred(&message).unwrap();
    assert_eq!(core.election_reset_sequence(), reset);
    core.durable.hard_state.voted_for = Some(node(2));
    core.note_logical_vote_transferred(&message).unwrap();
    assert_eq!(core.election_reset_sequence(), reset + 1);
    assert_eq!(core.state().hard_state.term, 2);
    let mut recovered =
        Raft::recover(node(1), binding(1), core.durable.clone(), core.limits).unwrap();
    recovered.note_logical_vote_transferred(&message).unwrap();
    assert_eq!(
        recovered.election_reset_sequence(),
        0,
        "old volatile reply cannot reset a recovered owner"
    );
}
