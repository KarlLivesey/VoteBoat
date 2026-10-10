// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

fn drain(protocol: NativePeerProtocol) {
    let mut h = History::new(protocol, 8);
    h.elect(0);
    let operation = OperationId::new(91).unwrap();
    let intent = LeadershipIntent {
        request: LeadershipTransferRequest {
            operation,
            configuration: h.core(0).membership().id(),
            target: PeerIdentity {
                node: h.core(1).local_node(),
                store: h.core(1).storage_binding().identity,
            },
        },
        source: PeerIdentity {
            node: h.core(0).local_node(),
            store: h.core(0).storage_binding().identity,
        },
    };
    h.nodes[0]
        .begin_local_drain(LocalDrainRequest {
            operation,
            groups: vec![DrainGroup {
                group: group(),
                configuration: intent.request.configuration,
            }],
        })
        .unwrap();
    let rejected = h.nodes[0]
        .propose(ClientRequest {
            group: group(),
            operation: OperationId::new(100).unwrap(),
            bytes: Maintenance::<HostApplication>::data(&7i64.to_le_bytes(), 100).unwrap(),
        })
        .unwrap_err();
    assert_eq!(rejected.reason, ClientError::Draining);
    h.control(0, LeadershipCommand::Begin(intent));
    assert!(!h.core(0).campaigning_enabled());
    assert_eq!(h.core(0).role(), Role::Leader);
    assert!(!h.nodes[0].local_drain_status().unwrap().locally_quiescent);
    h.nodes[0]
        .control(group(), NodeControl::TransferLeadership(intent.request))
        .unwrap();
    h.wait("drain target leadership", |h| {
        h.core(1).role() == Role::Leader
            && matches!(
                h.app(1).leadership_action(h.core(1)),
                Ok(LeadershipAction::Complete(_))
            )
    });
    let LeadershipAction::Complete(command) = h.app(1).leadership_action(h.core(1)).unwrap() else {
        unreachable!();
    };
    h.control(1, command);
    h.wait("local drain quiescence", |h| {
        h.nodes[0].local_drain_status().unwrap().locally_quiescent
    });
    assert_eq!(
        h.nodes[0].control(group(), NodeControl::Campaign),
        Err(NodeError::Draining)
    );
    // Shutdown still owns all accepted effects; readiness itself never stops workers.
    let source = h.nodes.remove(0);
    drain_wire_nodes(vec![source]);
    assert!(matches!(
        h.submit(0, rejected.request.operation, rejected.request.bytes),
        ClientOutcome::Applied { .. }
    ));
    assert_eq!(
        h.app(0).inner().0.read_applied(h.app(0).applied_index()),
        Ok(7)
    );
    h.close();
}
#[test]
fn local_drain_handoff_keeps_two_voters_serving_tcp() {
    drain(NativePeerProtocol::TcpTls);
}
#[cfg(feature = "quic")]
#[test]
fn local_drain_handoff_keeps_two_voters_serving_quic() {
    drain(NativePeerProtocol::Quic);
}
