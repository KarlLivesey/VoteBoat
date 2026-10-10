// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use voteboat::secure::PeerIdentity;
pub(super) fn request(target: u64) -> LeadershipTransferRequest {
    LeadershipTransferRequest {
        operation: OperationId::new(100).unwrap(),
        target: PeerIdentity {
            node: node(target),
            store: identity(target.into()),
        },
        configuration: ConfigurationId::new(1).unwrap(),
    }
}
pub(super) fn elected() -> Cluster<HostLogStore> {
    let mut c = Cluster::new(bootstrap(1, 3), |id| HostLogStore::new(id.into()));
    c.act(1, Event::Campaign);
    c.pump();
    c.propose(1, 1, 7);
    c
}
pub(super) fn signal(c: &mut Cluster<HostLogStore>) -> Message {
    let at = c
        .messages
        .iter()
        .position(|m| matches!(m.rpc, Rpc::TimeoutNow { .. }))
        .expect("handoff signal");
    c.messages.remove(at).unwrap()
}
#[test]
fn transfer_uses_ordinary_quorum_election_and_preserves_committed_commands() {
    let mut c = elected();
    c.act(1, Event::TransferLeadership(request(2)));
    assert_eq!(
        c.replicas.get_mut(&1).unwrap().core.step(Event::Propose {
            operation: OperationId::new(2).unwrap(),
            bytes: vec![8],
        }),
        Err(RaftError::Busy)
    );
    c.pump();
    assert_eq!(c.replicas[&2].core.role(), Role::Leader);
    assert_eq!(c.replicas[&1].core.role(), Role::Follower);
    assert!(c.replicas[&1].core.leadership_transfer().is_none());
    c.propose(2, 2, 8);
    for id in 1..=3 {
        assert_eq!(c.commands(id), vec![vec![7], vec![8]]);
    }
}
#[test]
fn lagging_target_waits_for_durable_prefix_and_exact_cancel_releases_quiescence() {
    let mut c = elected();
    c.partition(&[1, 3]);
    c.propose(1, 2, 8);
    c.act(1, Event::TransferLeadership(request(2)));
    assert!(!c
        .messages
        .iter()
        .any(|m| matches!(m.rpc, Rpc::TimeoutNow { .. })));
    let t = c.replicas[&1].core.leadership_transfer().unwrap();
    assert!(!t.signal_sent);
    let mut stale = t.context;
    stale.sequence += 1;
    assert_eq!(
        c.replicas
            .get_mut(&1)
            .unwrap()
            .core
            .step(Event::CancelLeadershipTransfer { context: stale }),
        Err(RaftError::StaleTransfer)
    );
    c.act(1, Event::CancelLeadershipTransfer { context: t.context });
    c.propose(1, 3, 9);
    c.act(1, Event::TransferLeadership(request(2)));
    c.heal();
    c.act(1, Event::Heartbeat);
    c.pump();
    assert_eq!(c.replicas[&2].core.role(), Role::Leader);
    assert_eq!(c.commands(2), vec![vec![7], vec![8], vec![9]]);
}
#[test]
fn an_uncommitted_source_tail_cannot_emit_a_handoff_signal() {
    let mut c = elected();
    c.partition(&[1]);
    c.propose(1, 2, 8);
    c.act(1, Event::TransferLeadership(request(2)));
    let t = c.replicas[&1].core.leadership_transfer().unwrap();
    assert!(t.index > c.replicas[&1].core.state().commit_index);
    assert!(!t.signal_sent);
    assert!(!c
        .messages
        .iter()
        .any(|m| matches!(m.rpc, Rpc::TimeoutNow { .. })));
    c.heal();
    c.act(1, Event::Heartbeat);
    c.pump();
    assert_eq!(c.replicas[&2].core.role(), Role::Leader);
    assert_eq!(c.commands(2), vec![vec![7], vec![8]]);
}
#[test]
fn target_catch_up_must_complete_persistence_before_it_can_trigger_handoff() {
    let mut c = elected();
    c.partition(&[1, 3]);
    c.propose(1, 2, 8);
    c.act(1, Event::TransferLeadership(request(2)));
    let attempt = c.replicas[&1].core.leadership_transfer().unwrap();
    c.heal();
    let at = c
        .messages
        .iter()
        .position(|m| m.to == node(2) && matches!(m.rpc, Rpc::Append { .. }))
        .unwrap();
    let append = c.messages.remove(at).unwrap();
    let r = c.replicas.get_mut(&2).unwrap();
    let old_index = r.core.state().last_index();
    let pending = r.core.step(Event::Receive(append)).unwrap();
    assert!(matches!(pending.as_slice(), [Effect::Persist(_)]));
    assert_eq!(r.core.state().last_index(), old_index);
    c.act(1, Event::Heartbeat);
    assert!(
        !c.replicas[&1]
            .core
            .leadership_transfer()
            .unwrap()
            .signal_sent
    );
    c.effects(2, pending);
    c.pump();
    assert!(c.replicas[&1].core.leadership_transfer().is_none());
    assert_eq!(c.replicas[&2].core.role(), Role::Leader);
    assert!(c.replicas[&2].core.state().last_index() > attempt.index);
    assert_eq!(c.commands(2), vec![vec![7], vec![8]]);
}
#[test]
fn timeout_signal_cannot_send_votes_until_target_term_and_ballot_are_durable() {
    let mut c = elected();
    c.act(1, Event::TransferLeadership(request(2)));
    let m = signal(&mut c);
    let before = c.replicas[&2].core.state().hard_state;
    let effects = c
        .replicas
        .get_mut(&2)
        .unwrap()
        .core
        .step(Event::Receive(m.clone()))
        .unwrap();
    assert!(matches!(effects.as_slice(), [Effect::Persist(_)]));
    assert_eq!(c.replicas[&2].core.state().hard_state, before);
    c.effects(2, effects);
    c.pump();
    assert_eq!(c.replicas[&2].core.role(), Role::Leader);
    let now = c.replicas[&2].core.state().hard_state;
    assert!(c
        .replicas
        .get_mut(&2)
        .unwrap()
        .core
        .step(Event::Receive(m))
        .unwrap()
        .is_empty());
    assert_eq!(c.replicas[&2].core.state().hard_state, now);
}
#[test]
fn target_restart_before_ballot_persistence_requires_fresh_leader_contact() {
    let mut c = elected();
    c.act(1, Event::TransferLeadership(request(2)));
    let m = signal(&mut c);
    let r = c.replicas.get_mut(&2).unwrap();
    let old = r.store.state(group(1)).unwrap().hard_state;
    let pending = r.core.step(Event::Receive(m.clone())).unwrap();
    assert!(matches!(pending.as_slice(), [Effect::Persist(_)]));
    r.core = Raft::recover(
        node(2),
        r.store.binding(),
        r.store.state(group(1)).unwrap(),
        r.store.limits(),
    )
    .unwrap();
    assert_eq!(r.core.state().hard_state, old);
    assert_eq!(
        r.core.step(Event::Receive(m)),
        Err(RaftError::WrongIdentity)
    );
    c.act(1, Event::Heartbeat);
    c.pump();
    assert_eq!(c.replicas[&2].core.role(), Role::Leader);
}
#[test]
fn source_restart_discards_volatile_attempt_and_does_not_infer_completion() {
    let mut c = elected();
    c.partition(&[1, 3]);
    c.propose(1, 2, 8);
    c.act(1, Event::TransferLeadership(request(2)));
    assert!(
        !c.replicas[&1]
            .core
            .leadership_transfer()
            .unwrap()
            .signal_sent
    );
    let r = c.replicas.get_mut(&1).unwrap();
    r.core = Raft::recover(
        node(1),
        r.store.binding(),
        r.store.state(group(1)).unwrap(),
        r.store.limits(),
    )
    .unwrap();
    assert!(r.core.leadership_transfer().is_none());
    assert_eq!(r.core.role(), Role::Follower);
    c.act(1, Event::Campaign);
    c.pump();
    c.propose(1, 3, 9);
    assert_eq!(c.commands(1), vec![vec![7], vec![8], vec![9]]);
}
#[test]
fn wrong_signal_origin_config_tail_and_unobserved_leader_are_rejected() {
    let mut c = elected();
    c.act(1, Event::TransferLeadership(request(2)));
    let m = signal(&mut c);
    let before = c.replicas[&2].core.state().hard_state;
    let mut wrong = m.clone();
    wrong.context.origin = c.replicas[&2].store.binding();
    assert_eq!(
        c.replicas
            .get_mut(&2)
            .unwrap()
            .core
            .step(Event::Receive(wrong)),
        Err(RaftError::WrongIdentity)
    );
    let mut wrong = m.clone();
    wrong.configuration = ConfigurationId::new(2).unwrap();
    assert_eq!(
        c.replicas
            .get_mut(&2)
            .unwrap()
            .core
            .step(Event::Receive(wrong)),
        Err(RaftError::WrongIdentity)
    );
    let mut wrong = m.clone();
    wrong.from = node(3);
    wrong.sender = c.replicas[&3].store.binding();
    wrong.context.origin = wrong.sender;
    assert_eq!(
        c.replicas
            .get_mut(&2)
            .unwrap()
            .core
            .step(Event::Receive(wrong)),
        Err(RaftError::WrongIdentity)
    );
    let mut wrong = m.clone();
    let Rpc::TimeoutNow { index, .. } = &mut wrong.rpc else {
        unreachable!()
    };
    *index += 1;
    assert_eq!(
        c.replicas
            .get_mut(&2)
            .unwrap()
            .core
            .step(Event::Receive(wrong)),
        Err(RaftError::InvalidMessage)
    );
    assert_eq!(c.replicas[&2].core.state().hard_state, before);
    let r = c.replicas.get_mut(&2).unwrap();
    r.core = Raft::recover(
        node(2),
        r.store.binding(),
        r.store.state(group(1)).unwrap(),
        r.store.limits(),
    )
    .unwrap();
    assert_eq!(
        r.core.step(Event::Receive(m)),
        Err(RaftError::WrongIdentity)
    );
}
#[test]
fn signal_without_election_quorum_is_not_transfer_success() {
    let mut c = elected();
    c.act(1, Event::TransferLeadership(request(2)));
    let m = signal(&mut c);
    let t = c.replicas[&1].core.leadership_transfer().unwrap();
    c.messages.clear();
    c.partition(&[1, 3]);
    c.act(2, Event::Receive(m));
    c.pump();
    assert_eq!(c.replicas[&2].core.role(), Role::Candidate);
    c.act(1, Event::CancelLeadershipTransfer { context: t.context });
    c.propose(1, 2, 8);
    assert_eq!(c.commands(1), vec![vec![7], vec![8]]);
}
#[test]
fn transfer_rejects_self_unknown_and_stale_target_identity_without_mutation() {
    let mut c = elected();
    for r in [
        request(1),
        request(99),
        LeadershipTransferRequest {
            target: PeerIdentity {
                store: identity(99),
                ..request(2).target
            },
            ..request(2)
        },
        LeadershipTransferRequest {
            configuration: ConfigurationId::new(2).unwrap(),
            ..request(2)
        },
    ] {
        assert_eq!(
            c.replicas
                .get_mut(&1)
                .unwrap()
                .core
                .step(Event::TransferLeadership(r)),
            Err(RaftError::WrongIdentity)
        );
        assert!(c.replicas[&1].core.leadership_transfer().is_none());
    }
}
#[test]
fn recursive_quorum_policy_still_decides_the_target_election() {
    let mut b = bootstrap(1, 9);
    b.policy = Policy::new(
        Tree::Majority(
            (0..3)
                .map(|site| {
                    Tree::Majority((1..=3).map(|n| Tree::Voter(node(site * 3 + n))).collect())
                })
                .collect(),
        ),
        Limits::default(),
    )
    .unwrap();
    let mut c = Cluster::new(b, |id| HostLogStore::new(id.into()));
    c.act(1, Event::Campaign);
    c.pump();
    c.propose(1, 1, 7);
    c.act(1, Event::TransferLeadership(request(5)));
    c.pump();
    assert_eq!(c.replicas[&5].core.role(), Role::Leader);
    c.propose(5, 2, 8);
    assert_eq!(c.commands(5), vec![vec![7], vec![8]]);
}
