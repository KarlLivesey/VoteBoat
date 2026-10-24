// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Original core exchanges; fixture completions assert no filesystem durability.
use super::*;

#[test]
fn source_preflight_preserves_higher_term_reply_without_collector() {
    let mut leader = core();
    let request = message_to(leader.step(Event::Heartbeat).unwrap(), 2);
    let mut peer = recover_membership_peer(2, leader.state().clone(), 1);
    for term in [2, 3] {
        let campaign = peer.step(Event::Campaign).unwrap();
        durable(&mut peer, campaign);
        assert_eq!(peer.state().hard_state.term, term);
        let response = one_reply(peer.step(Event::Receive(request.clone())).unwrap());
        assert!(matches!(response.rpc, Rpc::Appended { success: false, .. }));
        assert_eq!(response.term, term);
        assert_eq!(response.context, request.context);
        if term == 3 {
            assert!(leader.requests.is_empty());
        }
        let before = (leader.state().clone(), leader.role(), leader.requests.len());
        leader.authenticated_message_source(&response).unwrap();
        assert_eq!(
            (leader.state().clone(), leader.role(), leader.requests.len()),
            before
        );
        let effects = leader.step(Event::Receive(response)).unwrap();
        durable(&mut leader, effects);
        assert_eq!(leader.state().hard_state.term, term);
        assert_eq!(leader.role(), Role::Follower);
        assert_eq!(leader.state().commit_index, 1);
    }
}

#[test]
fn source_preflight_allows_actual_not_caught_up_readiness_reply() {
    let mut leader = staged();
    let mut learner = recover_membership_peer(4, leader.state().clone(), 7);
    let effects = leader
        .persist(
            leader.state().hard_state,
            2,
            Some(Suffix {
                from: 3,
                entries: vec![LogEntry {
                    index: 3,
                    term: 1,
                    payload: EntryPayload::Noop,
                }]
                .into(),
            }),
            After::LeaderAppend,
            None,
        )
        .unwrap()
        .into_owned();
    durable(&mut leader, effects);
    committed_fixture(&mut leader, 3);
    let request = message_to(
        leader
            .step(Event::CheckLearnerReadiness {
                learner: PeerIdentity {
                    node: node(4),
                    store: store(4),
                },
                session: StoreSession::new(7).unwrap(),
                requirements: ReadinessRequirements {
                    application_schema: 1,
                    command_bytes: 128,
                    snapshot_bytes: 256,
                },
            })
            .unwrap(),
        4,
    );
    learner.authenticated_message_source(&request).unwrap();
    let response = one_reply(learner.step(Event::Receive(request)).unwrap());
    assert!(matches!(
        response.rpc,
        Rpc::LearnerReadinessReply { ready: false, .. }
    ));
    let mut foreign = response.clone();
    foreign.sender.session = StoreSession::new(8).unwrap();
    assert_eq!(
        leader.authenticated_message_source(&foreign),
        Err(RaftError::WrongIdentity)
    );
    assert!(leader.pending_learner_readiness().is_some());
    leader.authenticated_message_source(&response).unwrap();
    assert!(leader.step(Event::Receive(response)).unwrap().is_empty());
    assert!(leader.pending_learner_readiness().is_none());
    assert!(leader.ready_learner().is_none());
}

#[test]
fn source_preflight_keeps_historical_authority_base_and_exact_pending_reply() {
    let mut witness = authority_witness();
    let mut receiver = follower(staged(), 2);
    let query = authority_query(&mut receiver, 3);
    assert_ne!(query.configuration, witness.membership().id());
    witness.authenticated_message_source(&query).unwrap();
    let response = one_reply(witness.step(Event::Receive(query)).unwrap());
    assert!(matches!(
        response.rpc,
        Rpc::AuthorityReply { granted: true, .. }
    ));
    let mut foreign = response.clone();
    foreign.context.sequence += 1;
    assert_eq!(
        receiver.authenticated_message_source(&foreign),
        Err(RaftError::WrongIdentity)
    );
    receiver.authenticated_message_source(&response).unwrap();
    receiver.step(Event::Receive(response.clone())).unwrap();
    assert!(matches!(
        receiver.replication_authorization_status(),
        ReplicationAuthorizationStatus::Granted { .. }
    ));
    assert_eq!(
        receiver.authenticated_message_source(&response),
        Err(RaftError::WrongIdentity)
    );
}

#[test]
fn source_preflight_accepts_learner_replication_but_refuses_learner_read_authority() {
    let mut leader = staged();
    let request = message_to(leader.step(Event::Heartbeat).unwrap(), 4);
    let mut learner = recover_membership_peer(4, leader.state().clone(), 1);
    learner.authenticated_message_source(&request).unwrap();
    let response = one_reply(learner.step(Event::Receive(request)).unwrap());
    assert!(matches!(response.rpc, Rpc::Appended { success: true, .. }));
    leader.authenticated_message_source(&response).unwrap();
    let mut read = response.clone();
    read.rpc = Rpc::ReadAck;
    assert_eq!(
        leader.authenticated_message_source(&read),
        Err(RaftError::WrongIdentity)
    );
    assert_eq!(
        leader.step(Event::Receive(read)),
        Err(RaftError::WrongIdentity)
    );
    leader.step(Event::Receive(response)).unwrap();
}
