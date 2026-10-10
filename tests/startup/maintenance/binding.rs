// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

#[test]
fn original_handoff_binding_admitted_after_target_election_tcp() {
    history(NativePeerProtocol::TcpTls, false);
}

#[cfg(feature = "quic")]
#[test]
fn original_handoff_binding_admitted_after_target_election_quic_checkpoint() {
    history(NativePeerProtocol::Quic, true);
}

fn history(protocol: NativePeerProtocol, checkpoint: bool) {
    let mut h = History::new(protocol, 8);
    h.elect(0);
    let intent = LeadershipIntent {
        source: PeerIdentity {
            node: h.core(0).local_node(),
            store: h.core(0).storage_binding().identity,
        },
        request: LeadershipTransferRequest {
            operation: OperationId::new(23901).unwrap(),
            configuration: h.core(0).membership().id(),
            target: PeerIdentity {
                node: h.core(1).local_node(),
                store: h.core(1).storage_binding().identity,
            },
        },
    };
    assert!(matches!(
        h.submit(
            0,
            OperationId::new(23902).unwrap(),
            Maintenance::<HostApplication>::data(&7i64.to_le_bytes(), 100).unwrap()
        ),
        ClientOutcome::Applied { .. }
    ));
    h.elect(1);
    assert!(h.app(1).record(intent.request.operation).is_none());
    rejects_unassigned_source(&mut h, intent);
    let term = h.core(1).state().hard_state.term;
    let begun = h.control(1, LeadershipCommand::Begin(intent));
    assert_eq!(begun.intent, intent);
    assert_eq!(begun.term, term);
    let LeadershipAction::Complete(complete) = h.app(1).leadership_action(h.core(1)).unwrap()
    else {
        panic!("bound target must complete in its current term");
    };
    let done = h.control(1, complete);
    assert!(matches!(done.phase, LeadershipPhase::Completed { term: t, .. } if t == term));
    assert_eq!(h.control(1, LeadershipCommand::Begin(intent)), done);
    let mut changed = intent;
    changed.source.node = h.core(2).local_node();
    changed.source.store = h.core(2).storage_binding().identity;
    let request = ClientRequest {
        group: group(),
        operation: intent.request.operation,
        bytes: LeadershipCommand::Begin(changed).encode().unwrap(),
    };
    let before = h.core(1).state().clone();
    assert!(h.nodes[1].propose_maintenance(request).is_err());
    assert_eq!(h.core(1).state(), &before);
    h.reopen(checkpoint);
    h.elect(1);
    assert_eq!(h.read_record(1, intent.request.operation), done);
    assert_eq!(h.control(1, LeadershipCommand::Begin(intent)), done);
    assert!(matches!(
        h.submit(
            1,
            OperationId::new(23902).unwrap(),
            Maintenance::<HostApplication>::data(&7i64.to_le_bytes(), 100).unwrap()
        ),
        ClientOutcome::Applied {
            receipt: MaintenanceReceipt::Data(CounterReceipt {
                duplicate: true,
                ..
            }),
            ..
        }
    ));
    drain_wire_nodes(std::mem::take(&mut h.nodes));
}

fn rejects_unassigned_source(h: &mut History, intent: LeadershipIntent) {
    for (node, store, incarnation) in [(4, 4, 1), (1, 999, 1), (1, 1, 999)] {
        let mut invalid = intent;
        invalid.source = PeerIdentity {
            node: NodeId::new(node).unwrap(),
            store: StoreIdentity {
                id: StoreId::new(store).unwrap(),
                incarnation: StoreIncarnation::new(incarnation).unwrap(),
            },
        };
        let before = h.core(1).state().clone();
        let rejected = h.nodes[1]
            .propose_maintenance(ClientRequest {
                group: group(),
                operation: intent.request.operation,
                bytes: LeadershipCommand::Begin(invalid).encode().unwrap(),
            })
            .unwrap_err();
        assert_eq!(
            rejected.reason,
            voteboat::runtime::ClientError::Application(ApplicationError::InvalidCommand)
        );
        assert_eq!(h.core(1).state(), &before);
    }
}
