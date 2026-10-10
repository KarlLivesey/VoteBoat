// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
// Unless explicitly acquired and licensed from Licensor under another license,
// the contents of this file are subject to the Reciprocal Public License
// ("RPL") Version 1.5, or subsequent versions as allowed by the RPL, and You may
// not copy or use this file in either source code or executable form, except
// in compliance with the terms and conditions of the RPL.
//
// All software distributed under the RPL is provided strictly on an "AS IS"
// basis, WITHOUT WARRANTY OF ANY KIND, EITHER EXPRESS OR IMPLIED, AND LICENSOR
// HEREBY DISCLAIMS ALL SUCH WARRANTIES, INCLUDING WITHOUT LIMITATION, ANY
// WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, QUIET
// ENJOYMENT, OR NON-INFRINGEMENT. See the RPL for specific language governing
// rights and limitations under the RPL.
#![cfg(feature = "quic")]
#[path = "quic/chunk_progress.rs"]
mod chunk_progress;
mod support;
use std::net::UdpSocket;
use support::*;
use voteboat::{identity::*, native::quic::*, runtime::MonoTime, secure::*};
fn local(n: u64) -> LocalIdentity {
    LocalIdentity {
        node: node(n),
        store: StoreBinding {
            identity: identity(n as u128),
            session: StoreSession::new(n + 10).unwrap(),
        },
    }
}
fn pair() -> (NativeQuicSession, NativeQuicSession) {
    versioned_pair(1, 1)
}
fn versioned_pair(a_version: u16, b_version: u16) -> (NativeQuicSession, NativeQuicSession) {
    let a = UdpSocket::bind("127.0.0.1:0").unwrap();
    let b = UdpSocket::bind("127.0.0.1:0").unwrap();
    let aa = a.local_addr().unwrap();
    let ba = b.local_addr().unwrap();
    let options = |n, peer, remote| QuicSessionOptions {
        local: local(n),
        peer: support::tls::peer(local(peer)),
        remote,
        generation: SecureSessionGeneration::new(n).unwrap(),
        limits: SessionLimits {
            write_buffer_bytes: 256,
            ..SessionLimits::default()
        },
    };
    (
        NativeQuicSession::client(
            a,
            &support::tls::configuration(1)
                .with_wire_version(a_version)
                .unwrap(),
            options(1, 2, ba),
            MonoTime(0),
        )
        .unwrap(),
        NativeQuicSession::server(
            b,
            &support::tls::configuration(2)
                .with_wire_version(b_version)
                .unwrap(),
            options(2, 1, aa),
            MonoTime(0),
        )
        .unwrap(),
    )
}
fn ready(a: &mut NativeQuicSession, b: &mut NativeQuicSession) -> u64 {
    for now in 0..5000 {
        a.poll(MonoTime(now), SessionPollBudget::default()).unwrap();
        b.poll(MonoTime(now), SessionPollBudget::default()).unwrap();
        if a.state() == SessionState::Ready && b.state() == SessionState::Ready {
            return now;
        }
    }
    panic!("QUIC handshake stalled: {:?} {:?}", a.state(), b.state());
}
#[test]
fn authenticated_udp_sessions_preserve_identity_and_partial_ordered_bytes() {
    let (mut a, mut b) = pair();
    assert_eq!(require_authenticated(&a), Err(SessionError::NotReady));
    assert_eq!(
        a.write_plaintext(b"before auth"),
        Err(SessionError::NotReady)
    );
    let start = ready(&mut a, &mut b);
    assert_eq!(require_authenticated(&a).unwrap().peer, local(2));
    assert_eq!(require_authenticated(&b).unwrap().peer, local(1));
    let source: Vec<_> = (0..2000).map(|n| (n % 251) as u8).collect();
    let mut written = 0;
    let mut received = Vec::new();
    for now in start..start + 5000 {
        match a.write_plaintext(&source[written..]) {
            Ok(n) => written += n,
            Err(SessionError::WouldBlock) => (),
            Err(e) => panic!("write {e:?}"),
        }
        a.poll(MonoTime(now), SessionPollBudget::default()).unwrap();
        b.poll(MonoTime(now), SessionPollBudget::default()).unwrap();
        let mut bytes = [0; 17];
        match b.read_plaintext(&mut bytes) {
            Ok(n) => received.extend_from_slice(&bytes[..n]),
            Err(SessionError::WouldBlock) => (),
            Err(e) => panic!("read {e:?}"),
        }
        if received.len() == source.len() && a.is_flushed() {
            break;
        }
    }
    assert_eq!(written, source.len());
    assert_eq!(received, source);
    a.revoke();
    assert_eq!(a.write_plaintext(b"revoked"), Err(SessionError::Revoked));
    assert_eq!(
        a.poll(MonoTime(start + 6000), SessionPollBudget::default()),
        Err(SessionError::Revoked)
    );
}

#[test]
fn single_call_budgets_are_fair_and_clock_reversal_fences() {
    let (mut a, mut b) = pair();
    assert_eq!(
        a.poll(
            MonoTime(10000),
            SessionPollBudget {
                io_calls: 1025,
                ..SessionPollBudget::default()
            }
        ),
        Err(SessionError::InvalidLimits)
    );
    assert_eq!(a.state(), SessionState::Handshaking);
    assert!(a.binding().is_none());
    let tiny = SessionPollBudget {
        io_calls: 1,
        read_bytes: 1200,
        write_bytes: 1200,
    };
    for now in 0..5000 {
        for session in [&mut a, &mut b] {
            let p = session.poll(MonoTime(now), tiny).unwrap();
            assert!(p.io_calls <= 1 && p.read_bytes <= 1200 && p.written_bytes <= 1200);
        }
        if a.state() == SessionState::Ready && b.state() == SessionState::Ready {
            break;
        }
    }
    assert_eq!(a.state(), SessionState::Ready);
    assert_eq!(b.state(), SessionState::Ready);
    let deadline = a.next_deadline().unwrap();
    assert!(deadline > MonoTime(0));
    assert_eq!(a.poll(MonoTime(0), tiny), Err(SessionError::TimeWentBack));
    assert_eq!(a.state(), SessionState::Failed);
}

#[test]
fn close_drains_sent_bytes_and_clean_eof_does_not_discard_unread_data() {
    let (mut a, mut b) = pair();
    let start = ready(&mut a, &mut b);
    let mut sent = false;
    let mut close = false;
    for now in start..start + 4000 {
        if !sent {
            match a.write_plaintext(b"retained through clean close") {
                Ok(n) => {
                    assert_eq!(n, 28);
                    sent = true
                }
                Err(SessionError::WouldBlock) => (),
                Err(e) => panic!("{e:?}"),
            }
        }
        a.poll(MonoTime(now), SessionPollBudget::default()).unwrap();
        b.poll(MonoTime(now), SessionPollBudget::default()).unwrap();
        if sent && a.is_flushed() && !close {
            a.close();
            close = true;
        }
        if b.state() == SessionState::Closed {
            break;
        }
    }
    assert!(sent && close);
    assert_eq!(b.state(), SessionState::Closed);
    let mut bytes = [0; 128];
    assert_eq!(b.read_plaintext(&mut bytes).unwrap(), 28);
    assert_eq!(&bytes[..28], b"retained through clean close");
    assert_eq!(b.read_plaintext(&mut bytes).unwrap(), 0);
    assert_eq!(b.write_plaintext(b"closed"), Err(SessionError::Closed));
}

#[test]
fn invalid_certificate_name_or_exact_store_never_exposes_a_binding() {
    for kind in 0..3 {
        let sa = UdpSocket::bind("127.0.0.1:0").unwrap();
        let sb = UdpSocket::bind("127.0.0.1:0").unwrap();
        let aa = sa.local_addr().unwrap();
        let ba = sb.local_addr().unwrap();
        let mut peer = support::tls::peer(local(2));
        match kind {
            0 => peer.certificate = support::tls::peer(local(3)).certificate,
            1 => peer.server_name = "wrong.voteboat.test".into(),
            _ => peer.identity.store = identity(99),
        }
        let mut a = NativeQuicSession::client(
            sa,
            &support::tls::configuration(1),
            QuicSessionOptions {
                local: local(1),
                peer,
                remote: ba,
                generation: SecureSessionGeneration::new(1).unwrap(),
                limits: SessionLimits::default(),
            },
            MonoTime(0),
        )
        .unwrap();
        let mut b = NativeQuicSession::server(
            sb,
            &support::tls::configuration(2),
            QuicSessionOptions {
                local: local(2),
                peer: support::tls::peer(local(1)),
                remote: aa,
                generation: SecureSessionGeneration::new(1).unwrap(),
                limits: SessionLimits::default(),
            },
            MonoTime(0),
        )
        .unwrap();
        let mut failure = None;
        for now in 0..2000 {
            if let Err(e) = a.poll(MonoTime(now), SessionPollBudget::default()) {
                failure = Some(e);
                break;
            }
            let _ = b.poll(MonoTime(now), SessionPollBudget::default());
        }
        assert_eq!(
            failure,
            Some(if kind == 1 {
                SessionError::Authentication
            } else {
                SessionError::WrongPeer
            })
        );
        assert!(a.binding().is_none());
        assert!(require_authenticated(&a).is_err());
        assert_eq!(a.state(), SessionState::Failed);
    }
}

#[test]
fn native_framed_transport_retains_original_credits_until_exact_send_completion() {
    use voteboat::{
        native::{outbound::NativeOutbound, transport::NativePeerTransport, wire::NativeWireCodec},
        outbound::*,
        raft::*,
        transport::*,
        wire::*,
    };
    let (mut sa, mut sb) = pair();
    let start = ready(&mut sa, &mut sb);
    let queue = |n| {
        NativeOutbound::new(
            OutboundBinding {
                node: local(n).node,
                store: local(n).store,
                generation: OutboundGeneration::new(1).unwrap(),
            },
            OutboundLimits::default(),
        )
        .unwrap()
    };
    let mut qa = queue(1);
    let qb = queue(2);
    let codec = || NativeWireCodec::new(WireLimits::default()).unwrap();
    let mut a = NativePeerTransport::new(sa, codec(), &qa, TransportLimits::default()).unwrap();
    let mut b = NativePeerTransport::new(sb, codec(), &qb, TransportLimits::default()).unwrap();
    let message = Message {
        group: group(1),
        configuration: ConfigurationId::new(1).unwrap(),
        from: node(1),
        to: node(2),
        sender: local(1).store,
        term: 2,
        context: RequestContext {
            origin: local(1).store,
            sequence: 1,
        },
        rpc: Rpc::Append {
            previous_index: 0,
            previous_term: 0,
            leader_commit: 0,
            entries: vec![voteboat::log::LogEntry {
                index: 1,
                term: 2,
                payload: voteboat::log::EntryPayload::Command {
                    operation: OperationId::new(1).unwrap(),
                    bytes: vec![7; 4096],
                },
            }],
        },
    };
    qa.submit(vec![message.clone()]).unwrap();
    let batch = qa.poll(1).pop().unwrap();
    let ticket = batch.ticket;
    a.submit(batch).unwrap();
    let budget = TransportPollBudget {
        plaintext_calls: 2,
        read_bytes: 31,
        write_bytes: 29,
        ..TransportPollBudget::default()
    };
    for now in start..start + 10000 {
        for transport in [&mut a, &mut b] {
            let p = transport.poll(MonoTime(now), budget).unwrap();
            assert!(p.read_bytes <= 31 && p.written_bytes <= 29 && p.plaintext_calls <= 2);
        }
        if a.usage().completion && b.received_info().is_some() {
            break;
        }
    }
    assert_eq!(qa.usage().batches, 1);
    let received = b.take_received().unwrap();
    assert_eq!(received.messages, vec![message]);
    assert_eq!(received.connection, b.binding());
    let completion = a.take_send().unwrap();
    assert_eq!(completion.batch.ticket, ticket);
    assert_eq!(completion.result, LocalSendResult::Sent);
    qa.complete(completion.batch, completion.result).unwrap();
    assert!(qa.is_drained());
}

#[test]
fn lost_initial_and_application_datagrams_retransmit_without_changing_bytes() {
    let aa = UdpSocket::bind("127.0.0.1:0").unwrap();
    let ba = UdpSocket::bind("127.0.0.1:0").unwrap();
    let proxy = UdpSocket::bind("127.0.0.1:0").unwrap();
    proxy.set_nonblocking(true).unwrap();
    let ad = aa.local_addr().unwrap();
    let bd = ba.local_addr().unwrap();
    let pd = proxy.local_addr().unwrap();
    let options = |n, peer| QuicSessionOptions {
        local: local(n),
        peer: support::tls::peer(local(peer)),
        remote: pd,
        generation: SecureSessionGeneration::new(1).unwrap(),
        limits: SessionLimits {
            write_buffer_bytes: 256,
            ..SessionLimits::default()
        },
    };
    let mut a = NativeQuicSession::client(
        aa,
        &support::tls::configuration(1),
        options(1, 2),
        MonoTime(0),
    )
    .unwrap();
    let mut b = NativeQuicSession::server(
        ba,
        &support::tls::configuration(2),
        options(2, 1),
        MonoTime(0),
    )
    .unwrap();
    let mut dropped = 0;
    let mut initial_dropped = false;
    let mut ready_at = None;
    let mut sent = false;
    let mut received = Vec::new();
    for now in 0..10000 {
        a.poll(MonoTime(now), SessionPollBudget::default()).unwrap();
        b.poll(MonoTime(now), SessionPollBudget::default()).unwrap();
        if a.state() == SessionState::Ready && b.state() == SessionState::Ready {
            let start = *ready_at.get_or_insert(now);
            if !sent {
                match a.write_plaintext(&[42; 192]) {
                    Ok(n) => {
                        assert_eq!(n, 192);
                        sent = true
                    }
                    Err(SessionError::WouldBlock) => (),
                    Err(e) => panic!("{e:?}"),
                }
            }
            let mut bytes = [0; 37];
            match b.read_plaintext(&mut bytes) {
                Ok(n) => received.extend_from_slice(&bytes[..n]),
                Err(SessionError::WouldBlock) => (),
                Err(e) => panic!("{e:?}"),
            }
            if now < start + 200 && sent {
                assert!(!a.is_flushed(), "lost data cannot be reported flushed")
            }
        }
        for _ in 0..16 {
            let mut packet = [0; 1200];
            let (n, from) = match proxy.recv_from(&mut packet) {
                Ok(v) => v,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(e) => panic!("{e}"),
            };
            if !initial_dropped && from == ad {
                initial_dropped = true;
                dropped += 1;
                continue;
            }
            if ready_at.is_some_and(|start| now < start + 200) && from == ad {
                dropped += 1;
                continue;
            }
            proxy
                .send_to(&packet[..n], if from == ad { bd } else { ad })
                .unwrap();
        }
        if received.len() == 192 && a.is_flushed() {
            break;
        }
    }
    assert!(dropped >= 2);
    assert!(sent);
    assert_eq!(received, vec![42; 192]);
    assert!(a.is_flushed());
}

mod raft_cluster {
    use super::*;
    use std::collections::{BTreeMap, VecDeque};
    use voteboat::{
        application::*,
        log::*,
        native::{outbound::NativeOutbound, transport::NativePeerTransport, wire::NativeWireCodec},
        outbound::*,
        raft::{self, *},
        transport::*,
        wire::*,
    };
    type Channel = NativePeerTransport<NativeQuicSession, NativeWireCodec>;
    struct Replica {
        core: Raft,
        log: HostLogStore,
        application: Counter,
        queue: NativeOutbound,
        channels: BTreeMap<NodeId, Channel>,
        pending: VecDeque<OutboundBatch>,
    }
    impl Replica {
        fn effects(&mut self, effects: Vec<Effect>) {
            let mut todo = VecDeque::from(effects);
            while let Some(effect) = todo.pop_front() {
                match effect {
                    Effect::Persist(update) => {
                        todo.extend(persist_effect(&mut self.core, &mut self.log, update).unwrap())
                    }
                    Effect::Send(message) => {
                        self.queue.submit(vec![message]).unwrap();
                    }
                    Effect::Committed(entries) => {
                        self.application.apply_batch(&entries).unwrap();
                    }
                    other => panic!("unexpected {other:?}"),
                }
            }
        }
        fn input(&mut self, event: raft::Event) {
            let effects = self.core.step(event).unwrap();
            self.effects(effects);
        }
        fn tick(&mut self, now: u64) {
            let mut incoming = Vec::new();
            for channel in self.channels.values_mut() {
                channel
                    .poll(MonoTime(now), TransportPollBudget::default())
                    .unwrap();
                if let Some(done) = channel.take_send() {
                    self.queue.complete(done.batch, done.result).unwrap();
                }
                if let Some(received) = channel.take_received() {
                    incoming.extend(received.messages);
                }
            }
            for message in incoming {
                self.input(raft::Event::Receive(message));
            }
            self.pending.extend(self.queue.poll(2));
            let count = self.pending.len();
            for _ in 0..count {
                let batch = self.pending.pop_front().unwrap();
                match self
                    .channels
                    .get_mut(&batch.ticket.peer)
                    .unwrap()
                    .submit(batch)
                {
                    Ok(()) => (),
                    Err(rejected) if rejected.reason == TransportError::Overloaded => {
                        self.pending.push_back(*rejected.batch)
                    }
                    Err(rejected) => panic!("{rejected:?}"),
                }
            }
        }
    }

    fn create_replicas() -> Vec<Replica> {
        let replicas: Vec<_> = (1..=3)
            .map(|n| {
                let mut log = HostLogStore::new(n);
                append(&mut log, vec![LogMutation::Create(bootstrap(1, 3))]);
                let core = Raft::recover(
                    node(n as u64),
                    log.binding(),
                    log.state(group(1)).unwrap(),
                    log.limits(),
                )
                .unwrap();
                let queue = NativeOutbound::new(
                    OutboundBinding {
                        node: node(n as u64),
                        store: log.binding(),
                        generation: OutboundGeneration::new(1).unwrap(),
                    },
                    OutboundLimits::default(),
                )
                .unwrap();
                Replica {
                    core,
                    log,
                    application: Counter::new(16).unwrap(),
                    queue,
                    channels: BTreeMap::new(),
                    pending: VecDeque::new(),
                }
            })
            .collect();
        replicas
    }
    fn connect(replicas: &mut [Replica]) -> u64 {
        let mut now = 0;
        for i in 0..3 {
            for j in i + 1..3 {
                let sa = UdpSocket::bind("127.0.0.1:0").unwrap();
                let sb = UdpSocket::bind("127.0.0.1:0").unwrap();
                let aa = sa.local_addr().unwrap();
                let ba = sb.local_addr().unwrap();
                let a = LocalIdentity {
                    node: node(i as u64 + 1),
                    store: replicas[i].log.binding(),
                };
                let b = LocalIdentity {
                    node: node(j as u64 + 1),
                    store: replicas[j].log.binding(),
                };
                let options = |local, remote, addr| QuicSessionOptions {
                    local,
                    peer: support::tls::peer(remote),
                    remote: addr,
                    generation: SecureSessionGeneration::new(1).unwrap(),
                    limits: SessionLimits::default(),
                };
                let mut qa = NativeQuicSession::client(
                    sa,
                    &support::tls::configuration(a.node.get()),
                    options(a, b, ba),
                    MonoTime(0),
                )
                .unwrap();
                let mut qb = NativeQuicSession::server(
                    sb,
                    &support::tls::configuration(b.node.get()),
                    options(b, a, aa),
                    MonoTime(0),
                )
                .unwrap();
                now = now.max(ready(&mut qa, &mut qb));
                let ca = NativePeerTransport::new(
                    qa,
                    NativeWireCodec::new(WireLimits::default()).unwrap(),
                    &replicas[i].queue,
                    TransportLimits::default(),
                )
                .unwrap();
                let cb = NativePeerTransport::new(
                    qb,
                    NativeWireCodec::new(WireLimits::default()).unwrap(),
                    &replicas[j].queue,
                    TransportLimits::default(),
                )
                .unwrap();
                replicas[i].channels.insert(b.node, ca);
                replicas[j].channels.insert(a.node, cb);
            }
        }
        now
    }
    pub(super) fn run() {
        let mut replicas = create_replicas();
        let now = connect(&mut replicas);
        replicas[0].input(raft::Event::Campaign);
        let mut proposed = false;
        for time in now..now + 10000 {
            for replica in &mut replicas {
                replica.tick(time);
            }
            if !proposed && replicas.iter().all(|r| r.application.applied_index() >= 1) {
                assert_eq!(replicas[0].core.role(), Role::Leader);
                replicas[0].input(raft::Event::Propose {
                    operation: OperationId::new(70).unwrap(),
                    bytes: 7i64.to_le_bytes().to_vec(),
                });
                proposed = true;
            }
            if proposed && replicas.iter().all(|r| r.application.applied_index() >= 2) {
                break;
            }
        }
        assert!(proposed);
        for replica in &replicas {
            assert_eq!(replica.core.state().commit_index, 2);
            assert_eq!(replica.application.read_applied(2).unwrap(), 7);
            assert_eq!(replica.log.state(group(1)).unwrap(), *replica.core.state());
        }
    }
}

#[test]
fn three_raft_replicas_commit_through_quic_framing_and_exact_log_completions() {
    raft_cluster::run();
}

#[test]
fn closing_an_unacknowledged_send_fails_on_peer_loss_instead_of_hanging() {
    let (mut a, mut b) = pair();
    let start = ready(&mut a, &mut b);
    assert_eq!(a.write_plaintext(b"uncertain"), Ok(9));
    assert!(!a.is_flushed());
    a.close();
    drop(b);
    assert_eq!(a.write_plaintext(b"after close"), Err(SessionError::Closed));
    let error = a
        .poll(MonoTime(start + 60000), SessionPollBudget::default())
        .unwrap_err();
    assert_eq!(error, SessionError::Truncated);
    assert_eq!(a.state(), SessionState::Failed);
    assert!(!a.is_flushed());
    assert_eq!(a.next_deadline(), None);
}

#[test]
fn handshake_timeout_byte_ceiling_and_foreign_sources_preserve_identity_gate() {
    let a = UdpSocket::bind("127.0.0.1:0").unwrap();
    let b = UdpSocket::bind("127.0.0.1:0").unwrap();
    let aa = a.local_addr().unwrap();
    let ba = b.local_addr().unwrap();
    let options = |n, peer, remote, limits| QuicSessionOptions {
        local: local(n),
        peer: support::tls::peer(local(peer)),
        remote,
        generation: SecureSessionGeneration::new(1).unwrap(),
        limits,
    };
    let mut server = NativeQuicSession::server(
        b,
        &support::tls::configuration(2),
        options(2, 1, aa, SessionLimits::default()),
        MonoTime(500),
    )
    .unwrap();
    assert_eq!(server.next_deadline(), Some(MonoTime(10500)));
    let foreign = UdpSocket::bind("127.0.0.1:0").unwrap();
    foreign.send_to(&[0; 1200], ba).unwrap();
    let p = server
        .poll(MonoTime(500), SessionPollBudget::default())
        .unwrap();
    assert_eq!(p.read_bytes, 1200);
    assert!(server.binding().is_none());
    assert_eq!(
        server.poll(MonoTime(10500), SessionPollBudget::default()),
        Err(SessionError::Timeout)
    );
    assert_eq!(server.state(), SessionState::Failed);
    let a = UdpSocket::bind("127.0.0.1:0").unwrap();
    let b = UdpSocket::bind("127.0.0.1:0").unwrap();
    let aa = a.local_addr().unwrap();
    let ba = b.local_addr().unwrap();
    let mut client = NativeQuicSession::client(
        a,
        &support::tls::configuration(1),
        options(1, 2, ba, SessionLimits::default()),
        MonoTime(0),
    )
    .unwrap();
    let mut server = NativeQuicSession::server(
        b,
        &support::tls::configuration(2),
        options(
            2,
            1,
            aa,
            SessionLimits {
                handshake_bytes: 128,
                ..SessionLimits::default()
            },
        ),
        MonoTime(0),
    )
    .unwrap();
    client
        .poll(MonoTime(0), SessionPollBudget::default())
        .unwrap();
    assert_eq!(
        server.poll(MonoTime(0), SessionPollBudget::default()),
        Err(SessionError::HandshakeTooLarge)
    );
    assert!(server.binding().is_none());
}

#[test]
fn quic_authenticates_exact_selected_wire_version_without_downgrade() {
    for version in [2, 3, 4, 8] {
        let (mut a, mut b) = versioned_pair(version, version);
        ready(&mut a, &mut b);
        assert_eq!(require_authenticated(&a).unwrap().wire_version, version);
        assert_eq!(require_authenticated(&b).unwrap().wire_version, version);
    }
    for (av, bv) in [(1, 3), (2, 3), (3, 1), (4, 3), (3, 4), (7, 8), (8, 7)] {
        let (mut a, mut b) = versioned_pair(av, bv);
        let mut rejected = false;
        for now in 0..5000 {
            for s in [&mut a, &mut b] {
                if let Err(error) = s.poll(MonoTime(now), SessionPollBudget::default()) {
                    assert_eq!(error, SessionError::IncompatibleProtocol);
                    rejected = true;
                }
                assert!(s.binding().is_none());
                assert_ne!(s.state(), SessionState::Ready);
            }
            if rejected {
                break;
            }
        }
        assert!(rejected);
    }
}
