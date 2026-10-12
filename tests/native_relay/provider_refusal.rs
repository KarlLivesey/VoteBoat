// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! A selected decoder's violation cannot rewrite genuine authenticated intake.
use super::{cluster::*, history::*, support::*};
use std::time::{Duration, Instant};
use voteboat::{
    native::{connect::NativePeerProtocol, wire::NativeWireCodec},
    outbound::{LocalSendResult, OutboundQueue},
    runtime::*,
    transport::TransportPollBudget,
    wire::{WireCodec, WireScope},
    worker::PersistenceWorker,
};

fn history(protocol: NativePeerProtocol) {
    let mut c = Cluster::new(protocol);
    c.campaign();
    write(&mut c, 35251, 3);
    let before = c.boats[0]
        .local()
        .owner
        .core(group(352))
        .unwrap()
        .state()
        .clone();
    let read = c.boats[0].read(group(352), ()).unwrap();
    c.scope_fault.set(true);
    let end = Instant::now() + Duration::from_secs(15);
    loop {
        let now = c.now();
        c.helper
            .poll(now, 3, TransportPollBudget::default())
            .unwrap();
        let result = c.boats[0].poll(now, NodePollBudget::default());
        if let Err(error) = result {
            assert_eq!(
                error,
                NodeError::Peer(PeerDriverError::Relay(
                    voteboat::native::relay::RelayNetworkError::InvalidFrame,
                ))
            );
            break;
        }
        for boat in &mut c.boats[1..] {
            boat.poll(now, NodePollBudget::default()).unwrap();
        }
        assert!(
            Instant::now() < end,
            "selected decoder violation not detected"
        );
        std::thread::park_timeout(Duration::from_millis(1));
    }
    let failed = c.boats.remove(0);
    assert_eq!(failed.state(), NodeState::RecoveryRequired);
    assert_eq!(
        failed
            .local()
            .owner
            .core(group(352))
            .unwrap()
            .state()
            .commit_index,
        before.commit_index
    );
    let mut recovered = failed
        .into_recovery()
        .unwrap_or_else(|_| panic!("missing Node recovery"));
    let network = recovered
        .peers
        .take()
        .unwrap()
        .into_recovery()
        .unwrap_or_else(|_| panic!("missing selected-network recovery"));
    assert_original_packet(&network);
    let completion = recovered.local.reads.poll().unwrap();
    assert_eq!(completion.ticket(), read);
    assert!(matches!(
        recovered.local.reads.complete(completion).unwrap(),
        ReadOutcome::Unavailable(ReadUnavailable::Aborted)
    ));
    assert_eq!(recovered.local.reads.usage().requests, 0);
    for output in network.outputs {
        output
            .complete(&mut recovered.local.outbound, LocalSendResult::Failed)
            .unwrap_or_else(|_| panic!("original output owner lost"));
    }
    for output in network.quarantine {
        recovered
            .local
            .outbound
            .complete(output, LocalSendResult::Failed)
            .unwrap();
    }
    for output in recovered.local.outbound.poll(4096) {
        recovered
            .local
            .outbound
            .complete(output, LocalSendResult::Cancelled)
            .unwrap();
    }
    assert!(recovered.local.outbound.is_drained());
    let mut hop = network.parts.hop;
    hop.take_received();
    if let Some(completion) = hop.take_send() {
        assert_eq!(completion.result, LocalSendResult::Failed);
    }
    assert!(hop.into_session().is_ok());
    reclaim_workers(&mut recovered.local);
    c.retiring = true;
    std::fs::remove_dir_all(finish(c)).unwrap();
}

fn assert_original_packet(
    network: &voteboat::native::relay::NativeRelayRecovery<
        Box<dyn voteboat::secure::SecureSession>,
        super::observed_wire::ObservedWire,
    >,
) {
    let packet = network.rejected_receive.as_ref().unwrap();
    let source = packet.source_view().unwrap();
    assert_eq!(packet.ticket().connection, source.receiving_access());
    assert_eq!(source.receiving_access().peer.node, node(9));
    assert_ne!(source.source().node, node(99));
    let scope = WireScope {
        from: source.source().node,
        sender: source.source().store,
        to: node(1),
    };
    let actual = NativeWireCodec::with_leadership_transfer(Default::default())
        .unwrap()
        .decode_batch(scope, packet.frame().wire_bytes().unwrap())
        .unwrap();
    assert!(actual.iter().all(|m| scope.matches(m)));
    assert_eq!(network.rejected_messages.len(), actual.len());
    assert_eq!(network.rejected_messages[0].from, node(99));
    assert_eq!(network.rejected_messages[0].context, actual[0].context);
}

fn reclaim_workers(
    local: &mut voteboat::native::node::NativeLocalParts<voteboat::application::Counter>,
) {
    let persistence = &mut local.persistence;
    let snapshots = &mut local.snapshots.as_mut().unwrap().worker;
    let end = Instant::now() + Duration::from_secs(5);
    let (mut log, mut snapshot) = (false, false);
    while !log || !snapshot {
        if !log {
            let _original_completions = persistence.poll(4096);
            log = persistence.try_reclaim().unwrap().is_some();
        }
        if !snapshot {
            use voteboat::snapshot_worker::SnapshotWorker;
            let _original_completions = snapshots.poll(4096);
            snapshot = snapshots.try_reclaim().unwrap().is_some();
        }
        assert!(Instant::now() < end, "recovery workers not reclaimed");
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
#[test]
fn actual_tcp_decoder_violation_returns_original_authenticated_frame_and_messages() {
    history(NativePeerProtocol::TcpTls);
}
#[cfg(feature = "quic")]
#[test]
fn actual_quic_decoder_violation_returns_original_authenticated_frame_and_messages() {
    history(NativePeerProtocol::Quic);
}
