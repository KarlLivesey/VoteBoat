// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Direct expected values keep shared-walk omissions out of differential oracles.
use super::*;
use crate::{membership::*, quorum::*};

fn node(n: u64) -> NodeId {
    NodeId::new(n).unwrap()
}
fn store(n: u64) -> StoreIdentity {
    StoreIdentity {
        id: StoreId::new(u128::from(n) * 10).unwrap(),
        incarnation: StoreIncarnation::new(n + 1).unwrap(),
    }
}
fn joint() -> ConfigurationRecord {
    ConfigurationRecord {
        operation: OperationId::new(u128::MAX).unwrap(),
        expected: ConfigurationId::new(1).unwrap(),
        change: ConfigurationChange::Joint {
            id: ConfigurationId::new(2).unwrap(),
            next: Configuration::new(
                ConfigurationId::new(3).unwrap(),
                Policy::new(
                    Tree::Majority(vec![
                        Tree::Voter(node(5)),
                        Tree::Voter(node(1)),
                        Tree::Voter(node(3)),
                    ]),
                    Limits::default(),
                )
                .unwrap(),
                [
                    (node(5), store(5)),
                    (node(1), store(1)),
                    (node(3), store(3)),
                ]
                .into(),
                [(node(4), store(4)), (node(2), store(2))].into(),
            )
            .unwrap(),
        },
    }
}
fn receive(rpc: Rpc) -> Event {
    let binding = StoreBinding {
        identity: store(1),
        session: StoreSession::new(1).unwrap(),
    };
    Event::Receive(Message {
        group: GroupIdentity {
            id: GroupId::new(1).unwrap(),
            incarnation: GroupIncarnation::new(1).unwrap(),
        },
        configuration: ConfigurationId::new(1).unwrap(),
        from: node(2),
        sender: binding,
        to: node(1),
        term: 1,
        context: RequestContext {
            origin: binding,
            sequence: 1,
        },
        rpc,
    })
}
fn events() -> [Event; 2] {
    [
        Event::Configure(Box::new(ConfigurationProposal {
            record: joint(),
            readiness: vec![],
            requirements: ReadinessRequirements {
                application_schema: 1,
                command_bytes: 8,
                snapshot_bytes: 64,
            },
        })),
        receive(Rpc::LearnerRepair {
            joint: Box::new(LogEntry {
                index: 1,
                term: 1,
                payload: EntryPayload::Configuration(Box::new(joint())),
            }),
            previous_index: 0,
            previous_term: 0,
            entries: vec![LogEntry {
                index: 2,
                term: 1,
                payload: EntryPayload::Noop,
            }],
        }),
    ]
}
// Voter map order, then learner map order; not global sorting across both maps.
fn expected() -> Vec<(NodeId, StoreIdentity)> {
    vec![
        (node(1), store(1)),
        (node(3), store(3)),
        (node(5), store(5)),
        (node(2), store(2)),
        (node(4), store(4)),
    ]
}

#[test]
fn configure_joint_and_learner_repair_joint_visit_exact_full_stores_in_original_order() {
    let expected = expected();
    let expected_map = [
        (node(1), store(1)),
        (node(2), store(2)),
        (node(3), store(3)),
        (node(4), store(4)),
        (node(5), store(5)),
    ]
    .into_iter()
    .collect::<BTreeMap<_, _>>();
    for event in events() {
        let mut visited = Vec::new();
        let result: Result<(), ()> = visit_event_connection_stores(&event, |node, store| {
            visited.push((node, store));
            Ok(())
        });
        assert_eq!(result, Ok(()));
        assert_eq!(visited, expected);
        assert_eq!(
            visited.into_iter().collect::<BTreeMap<_, _>>(),
            expected_map
        );
    }
}

#[test]
fn visitor_propagates_first_callback_error_without_visiting_later_joint_pairs() {
    for event in events() {
        let mut visited = Vec::new();
        let result = visit_event_connection_stores(&event, |node, store| {
            visited.push((node, store));
            if node == self::node(5) {
                Err("exact callback error")
            } else {
                Ok(())
            }
        });
        assert_eq!(result, Err("exact callback error"));
        assert_eq!(
            visited,
            vec![
                (node(1), store(1)),
                (node(3), store(3)),
                (node(5), store(5))
            ]
        );
    }
}

#[test]
fn authority_reply_visits_only_exact_granted_candidate_and_propagates_its_error() {
    let candidate = PeerIdentity {
        node: node(9),
        store: store(9),
    };
    for granted in [false, true] {
        let event = receive(Rpc::AuthorityReply {
            candidate,
            configuration: ConfigurationId::new(2).unwrap(),
            committed_index: 7,
            committed_term: 1,
            granted,
        });
        let mut visited = Vec::new();
        let result = visit_event_connection_stores(&event, |node, store| {
            visited.push((node, store));
            Err("candidate callback")
        });
        assert_eq!(
            visited,
            if granted {
                vec![(candidate.node, candidate.store)]
            } else {
                vec![]
            }
        );
        assert_eq!(
            result,
            if granted {
                Err("candidate callback")
            } else {
                Ok(())
            }
        );
    }
}
