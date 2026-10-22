// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

#[test]
fn dropped_duplicate_delayed_and_reordered_notices_need_no_ack_and_heartbeat_repairs() {
    for volatile in [true, false] {
        let mut c = Cluster::new(true, volatile);
        c.propose(1);
        let held = c.notices();
        assert_eq!(held.len(), 2);
        assert!(c.cores[0].requests.is_empty());
        assert_eq!(c.apps[0].read_applied(0).unwrap(), 1);
        c.deliver(held[0].clone());
        let revision = c.cores[1].state().revision;
        c.deliver(held[0].clone());
        assert_eq!(c.cores[1].state().revision, revision);
        assert!(c.queue.is_empty());
        // Drop the other notice; ordinary heartbeat repairs in one exchange.
        c.act(0, Event::Heartbeat);
        c.drain();
        c.assert_converged(1);
        c.propose(2);
        c.deliver(held[1].clone()); // old notice interleaved before new data
        let newer = c.notices();
        assert_eq!(newer.len(), 2);
        for m in newer.into_iter().rev() {
            c.deliver(m);
        }
        for m in held {
            c.deliver(m);
        } // old boundary never regresses/reapplies
        c.drain();
        c.assert_converged(2);
    }
}

#[test]
fn missing_boundary_is_contact_but_never_commit_and_invalid_input_is_neither() {
    let mut c = Cluster::new(true, true);
    c.propose(1);
    let held = c.notices();
    let m = held[0].clone();
    let at = m.to.get() as usize - 1;
    let original = c.cores[at].state().clone();
    let reset = c.cores[at].election_reset_sequence();
    let mut missing = m.clone();
    missing.rpc = Rpc::CommitNotice {
        index: 100,
        log_term: m.term,
    };
    c.deliver(missing);
    assert_eq!(c.cores[at].state(), &original);
    assert!(c.cores[at].election_reset_sequence() > reset);
    assert_eq!(c.cores[at].leader_contact, Some((m.sender, m.from, m.term)));
    for case in 0..10 {
        let mut wrong = m.clone();
        match case {
            0 => wrong.to = node(3),
            1 => wrong.group.incarnation = GroupIncarnation::new(2).unwrap(),
            2 => wrong.sender.identity.incarnation = StoreIncarnation::new(2).unwrap(),
            3 => wrong.context.origin.session = StoreSession::new(2).unwrap(),
            4 => wrong.configuration = ConfigurationId::new(2).unwrap(),
            5 => wrong.context.sequence = 0,
            6 => wrong.term = 0,
            7 => {
                wrong.rpc = Rpc::CommitNotice {
                    index: 0,
                    log_term: m.term,
                }
            }
            8 => {
                wrong.rpc = Rpc::CommitNotice {
                    index: 3,
                    log_term: 0,
                }
            }
            _ => {
                wrong.rpc = Rpc::CommitNotice {
                    index: 3,
                    log_term: m.term + 1,
                }
            }
        }
        let reset = c.cores[at].election_reset_sequence();
        assert!(
            c.cores[at].step(Event::Receive(wrong)).is_err(),
            "case {case}"
        );
        assert_eq!(c.cores[at].state(), &original);
        assert_eq!(c.cores[at].election_reset_sequence(), reset);
    }
    let mut old = m.clone();
    old.term -= 1;
    old.rpc = Rpc::CommitNotice {
        index: 3,
        log_term: old.term,
    };
    let reset = c.cores[at].election_reset_sequence();
    assert!(c.cores[at].step(Event::Receive(old)).unwrap().is_empty());
    assert_eq!(c.cores[at].election_reset_sequence(), reset);
    let mut mismatch = m;
    mismatch.rpc = Rpc::CommitNotice {
        index: 3,
        log_term: 1,
    };
    c.deliver(mismatch);
    assert_eq!(c.cores[at].state(), &original);
}

#[test]
fn higher_term_notice_waits_for_exact_completion_and_failure_never_applies() {
    for fail in [false, true] {
        let mut c = Cluster::new(true, true);
        c.propose(1);
        let mut m = c.notices().remove(0);
        m.term += 1;
        let at = m.to.get() as usize - 1;
        let before = c.cores[at].state().clone();
        let effects = c.cores[at].step(Event::Receive(m.clone())).unwrap();
        let [Effect::PersistVolatile(token)] = effects.as_slice() else {
            panic!()
        };
        let token = *token;
        assert_eq!(c.cores[at].state(), &before);
        assert_eq!(c.apps[at].read_applied(0).unwrap(), 0);
        if fail {
            c.cores[at].storage_failed();
            assert!(persist_volatile(&mut c.cores[at], &c.logs[at], token).is_err());
            assert_eq!(c.cores[at].state(), &before);
            assert_eq!(c.apps[at].read_applied(0).unwrap(), 0);
        } else {
            let done = persist_volatile(&mut c.cores[at], &c.logs[at], token).unwrap();
            assert!(done.following().is_empty());
            c.apps[at].apply_entries(done.committed().unwrap()).unwrap();
            assert_eq!(c.cores[at].state().hard_state.term, m.term);
            assert_eq!(c.apps[at].read_applied(0).unwrap(), 1);
            assert!(persist_volatile(&mut c.cores[at], &c.logs[at], token).is_err());
            assert_eq!(c.apps[at].read_applied(0).unwrap(), 1);
        }
    }
}

#[test]
fn lost_data_ack_retries_the_exact_append_instead_of_inventing_progress() {
    let mut c = Cluster::new(true, true);
    c.propose(1);
    let data = c.queue.pop_front().unwrap();
    c.deliver(data.clone());
    let lost = c.queue.pop_back().unwrap();
    assert!(matches!(lost.rpc, Rpc::Appended { .. }));
    c.act(0, Event::Heartbeat);
    let retry = c.sent.iter().rev().find(|m| m.to == data.to).unwrap();
    assert_eq!(retry.context, data.context);
    assert!(matches!(retry.rpc, Rpc::Append { .. }));
    assert!(c.cores[0].progress[&data.to] < c.cores[0].state().last_index());
    c.drain();
    c.assert_converged(1);
}

#[test]
fn ordinary_heartbeat_and_noop_probe_remain_acknowledged_with_notices_enabled() {
    let mut c = Cluster::new(true, true);
    c.propose(1);
    c.drain();
    c.reset();
    c.act(0, Event::Heartbeat);
    assert_eq!(c.queue.len(), 2);
    assert!(c
        .queue
        .iter()
        .all(|m| matches!(&m.rpc,Rpc::Append {entries,..} if entries.is_empty())));
    c.drain();
    c.assert_converged(1);
    assert_eq!(c.counts.messages, 4);
}

#[test]
fn granted_replication_bridge_still_requires_acknowledged_append_and_refuses_notice() {
    use crate::raft::membership_tests::store;
    let mut c = Cluster::new(true, false);
    c.propose(1);
    c.drain();
    let candidate = PeerIdentity {
        node: node(4),
        store: store(4),
    };
    let configuration = ConfigurationId::new(2).unwrap();
    let witness_store = c.cores[2].binding.identity;
    let out = c.cores[1]
        .step(Event::AuthorizeReplication {
            witness: PeerIdentity {
                node: node(3),
                store: witness_store,
            },
            candidate,
            configuration,
        })
        .unwrap();
    let [Effect::Send(query)] = out.as_slice() else {
        panic!()
    };
    let grant = Message {
        from: node(3),
        sender: c.cores[2].binding,
        to: node(2),
        rpc: Rpc::AuthorityReply {
            candidate,
            configuration,
            committed_index: 3,
            committed_term: 2,
            granted: true,
        },
        ..query.clone()
    };
    assert!(c.cores[1].step(Event::Receive(grant)).unwrap().is_empty());
    assert!(matches!(
        c.cores[1].replication_authorization_status(),
        ReplicationAuthorizationStatus::Granted { .. }
    ));
    let sender = StoreBinding {
        identity: candidate.store,
        session: StoreSession::new(1).unwrap(),
    };
    let mut input = Message {
        group: query.group,
        configuration,
        from: candidate.node,
        sender,
        to: node(2),
        term: query.term,
        context: RequestContext {
            origin: sender,
            sequence: 9,
        },
        rpc: Rpc::CommitNotice {
            index: 3,
            log_term: 2,
        },
    };
    let before = c.cores[1].state().clone();
    assert_eq!(
        c.cores[1].step(Event::Receive(input.clone())),
        Err(RaftError::WrongIdentity)
    );
    assert_eq!(c.cores[1].state(), &before);
    input.rpc = Rpc::Append {
        previous_index: 3,
        previous_term: 2,
        entries: EntryBatch::default(),
        leader_commit: 3,
    };
    let out = c.cores[1].step(Event::Receive(input)).unwrap();
    assert!(matches!(
        out.as_slice(),
        [Effect::Send(Message {
            rpc: Rpc::Appended { success: true, .. },
            ..
        })]
    ));
}

#[test]
fn higher_term_follower_is_discovered_by_acknowledged_heartbeat_not_a_notice_reply() {
    let mut c = Cluster::new(true, true);
    c.propose(1);
    let held = c.notices();
    c.act(1, Event::Campaign);
    c.queue.clear(); // Drop this separate campaign's Vote requests.
    assert_eq!(c.cores[1].state().hard_state.term, 3);
    let reset = c.cores[1].election_reset_sequence();
    c.deliver(held[0].clone());
    assert!(c.queue.is_empty());
    assert_eq!(c.cores[1].election_reset_sequence(), reset);
    assert_eq!(c.cores[0].role(), Role::Leader);
    assert_eq!(c.cores[0].state().hard_state.term, 2);
    c.act(0, Event::Heartbeat);
    c.drain();
    assert_eq!(c.cores[0].role(), Role::Follower);
    assert_eq!(c.cores[0].state().hard_state.term, 3);
    assert!(c.cores[0].commit_notice_emissions.is_empty());
    assert_eq!(c.apps[0].read_applied(0).unwrap(), 1);
}
