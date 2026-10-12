// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use voteboat::{
    native::{
        outbound::NativeOutbound,
        quic::{NativeQuicSession, QuicSessionOptions},
        transport::NativePeerTransport,
        wire::NativeWireCodec,
    },
    outbound::OutboundBinding,
    secure::{LocalIdentity, PeerIdentity, SecureSession, SessionLimits, SessionPollBudget},
    transport::{PeerTransport, TransportLimits, TransportPollBudget},
    wire::WireLimits,
};
type Channel = NativePeerTransport<NativeQuicSession, NativeWireCodec>;
fn local(n: u64) -> LocalIdentity {
    LocalIdentity {
        node: support::node(n),
        store: StoreBinding {
            identity: support::identity(n.into()),
            session: StoreSession::new(15).unwrap(),
        },
    }
}
fn await_packet(socket: &UdpSocket, source: std::net::SocketAddr) {
    let end = Instant::now() + Duration::from_secs(2);
    let mut bytes = [0; 1200];
    loop {
        match socket.peek_from(&mut bytes) {
            Ok((n, from)) => {
                assert!(n > 0);
                assert_eq!(from, source);
                return;
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                assert!(
                    Instant::now() < end,
                    "actual encrypted UDP input must arrive"
                );
                std::thread::park_timeout(Duration::from_millis(1));
            }
            Err(e) => panic!("UDP readiness: {e}"),
        }
    }
}
fn long_rtt() -> (NativeQuicSession, u64) {
    let a = UdpSocket::bind("127.0.0.1:0").unwrap();
    let b = UdpSocket::bind("127.0.0.1:0").unwrap();
    let observers = [a.try_clone().unwrap(), b.try_clone().unwrap()];
    let addresses = [a.local_addr().unwrap(), b.local_addr().unwrap()];
    let options = |n, peer, remote| QuicSessionOptions {
        local: local(n),
        peer: support::tls::peer(local(peer)),
        remote,
        generation: SecureSessionGeneration::new(150000).unwrap(),
        limits: SessionLimits::default(),
    };
    let mut a = NativeQuicSession::client(
        a,
        &support::tls::configuration(3),
        options(3, 1, addresses[1]),
        MonoTime(0),
    )
    .unwrap();
    let mut b = NativeQuicSession::server(
        b,
        &support::tls::configuration(1),
        options(1, 3, addresses[0]),
        MonoTime(0),
    )
    .unwrap();
    let budget = SessionPollBudget::default();
    assert!(a.poll(MonoTime(0), budget).unwrap().written_bytes > 0);
    await_packet(&observers[1], addresses[0]);
    let first = b.poll(MonoTime(3500), budget).unwrap();
    if first.written_bytes > 0 {
        await_packet(&observers[0], addresses[1]);
    }
    for now in 3501..5000 {
        for (i, session) in [&mut a, &mut b].into_iter().enumerate() {
            let progress = session.poll(MonoTime(now), budget).unwrap();
            if progress.written_bytes > 0 {
                await_packet(&observers[1 - i], addresses[i]);
            }
        }
        if a.state() == SessionState::Ready
            && b.state() == SessionState::Ready
            && a.is_flushed()
            && b.is_flushed()
        {
            assert_eq!(a.binding().unwrap().peer, local(1));
            drop(b);
            return (a, now);
        }
    }
    panic!("controlled delayed actual handshake must complete");
}
fn inspected(channel: &mut Channel) -> PeerDrainStatus {
    let transport = channel.drain_status();
    // Roster flags are fixture metadata; every transport/session/timer field
    // comes from the actual selected native provider via the public seam.
    PeerDrainStatus {
        peer: PeerIdentity {
            node: transport.binding.peer.node,
            store: transport.binding.peer.store.identity,
        },
        closed: true,
        fenced: false,
        authorized: true,
        retiring: false,
        attempt: None,
        binding: Some(transport.binding),
        accepted: None,
        transport: Some(transport),
    }
}
#[test]
fn actual_native_long_rtt_close_retains_original_timer_past_fixture_cutoff() {
    let (session, start) = long_rtt();
    let queue = NativeOutbound::new(
        OutboundBinding {
            node: local(3).node,
            store: local(3).store,
            generation: OutboundGeneration::new(1).unwrap(),
        },
        Default::default(),
    )
    .unwrap();
    let mut channel = NativePeerTransport::new(
        session,
        NativeWireCodec::new(WireLimits::default()).unwrap(),
        &queue,
        TransportLimits::default(),
    )
    .unwrap();
    channel.close();
    let budget = TransportPollBudget::default();
    channel.poll(MonoTime(start), budget).unwrap();
    let original = ProtocolClose::inspect(inspected(&mut channel)).unwrap();
    let cutoff = MonoTime(start + 15000);
    assert!(original.deadline > cutoff);
    channel.poll(cutoff, budget).unwrap();
    let at_cutoff = ProtocolClose::inspect(inspected(&mut channel)).unwrap();
    assert_eq!(at_cutoff, original);
    let pending = [((2, support::node(1)), at_cutoff)].into();
    let mut window = ProtocolWindow::pin(pending, cutoff).unwrap();
    assert_eq!(window.end, original.deadline);
    let before = MonoTime(original.deadline.0 - 1);
    channel.poll(before, budget).unwrap();
    let retained = [(
        (2, support::node(1)),
        ProtocolClose::inspect(inspected(&mut channel)).unwrap(),
    )]
    .into();
    assert!(window.retains(&retained, before));
    assert!(!window.retains(&BTreeMap::new(), before));
    assert_eq!(channel.state(), TransportState::Draining);
    assert!(queue.is_drained());
    channel.poll(original.deadline, budget).unwrap();
    assert_eq!(channel.state(), TransportState::Closed);
    assert_eq!(channel.usage(), TransportUsage::default());
    assert!(channel.drain_status().session.is_none());
    assert!(channel.take_send().is_none());
    assert!(channel.take_received().is_none());
    assert!(queue.is_drained());
}
