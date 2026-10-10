// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use voteboat::{drain::*, native::drain_journal::*};

fn reopen_journal(h: &History, owner: PeerIdentity, recover: bool) -> NativeDrainJournal {
    let io = FileDrainRecord::new(h.directory.join("DRAIN"));
    let opened = if recover {
        NativeDrainJournal::recover(io, owner)
    } else {
        NativeDrainJournal::new(io, owner)
    };
    opened.ok().unwrap()
}
fn history(protocol: NativePeerProtocol, checkpoint: bool) {
    let mut h = History::new(protocol, 8);
    h.elect(0);
    let owner = PeerIdentity {
        node: h.core(0).local_node(),
        store: h.core(0).storage_binding().identity,
    };
    let request = LeadershipTransferRequest {
        operation: OperationId::new(96).unwrap(),
        configuration: h.core(0).membership().id(),
        target: PeerIdentity {
            node: h.core(1).local_node(),
            store: h.core(1).storage_binding().identity,
        },
    };
    let mut journal = reopen_journal(&h, owner, false);
    journal
        .publish(DrainRecord {
            owner,
            sequence: 1,
            request: LocalDrainRequest {
                operation: request.operation,
                groups: vec![DrainGroup {
                    group: group(),
                    configuration: request.configuration,
                }],
            },
            phase: DrainPhase::Active,
        })
        .unwrap();
    h.nodes[0].restore_drain(&journal).unwrap();
    h.control(
        0,
        LeadershipCommand::Begin(LeadershipIntent {
            source: owner,
            request,
        }),
    );
    // Restart before a handoff invitation. Durable intent must precede all polling.
    h.reopen(checkpoint);
    journal = reopen_journal(&h, owner, true);
    h.nodes[0].restore_drain(&journal).unwrap();
    assert_eq!(
        h.nodes[0]
            .read(group(), MaintenanceQuery::Data(()))
            .unwrap_err()
            .reason,
        ReadInvocationError::Draining
    );
    assert_eq!(
        h.nodes[0].control(group(), NodeControl::Campaign),
        Err(NodeError::Draining)
    );
    h.elect(1);
    assert!(!h.core(0).campaigning_enabled());
    let LeadershipAction::Complete(command) = h.app(1).leadership_action(h.core(1)).unwrap() else {
        panic!("original handoff intent");
    };
    h.control(1, command);
    h.wait("recovered gate quiescence", |h| {
        h.nodes[0].local_drain_status().unwrap().locally_quiescent
    });
    let mut cancelled = journal.latest().unwrap().unwrap();
    cancelled.phase = DrainPhase::Cancelled;
    journal.publish(cancelled.clone()).unwrap();
    h.nodes[0].restore_drain(&journal).unwrap();
    assert!(h.nodes[0].local_drain_status().unwrap().resuming);
    h.wait("confirmed enable", |h| {
        h.nodes[0].local_drain_status().is_none()
    });
    assert!(h.core(0).campaigning_enabled());
    h.reopen(checkpoint);
    journal = reopen_journal(&h, owner, true);
    assert_eq!(journal.latest().unwrap(), Some(cancelled));
    h.nodes[0].restore_drain(&journal).unwrap();
    h.wait("recovered cancellation", |h| {
        h.nodes[0].local_drain_status().is_none()
    });
    h.elect(0);
    assert!(matches!(
        h.submit(
            0,
            OperationId::new(201).unwrap(),
            Maintenance::<HostApplication>::data(&7i64.to_le_bytes(), 100).unwrap()
        ),
        ClientOutcome::Applied { .. }
    ));
    h.close();
}
#[test]
fn durable_drain_and_cancellation_recover_with_native_tcp_wal() {
    history(NativePeerProtocol::TcpTls, false);
}
#[cfg(feature = "quic")]
#[test]
fn durable_drain_and_cancellation_recover_with_native_quic_checkpoints() {
    history(NativePeerProtocol::Quic, true);
}
