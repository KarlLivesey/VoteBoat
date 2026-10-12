// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Real authenticated native transport; no parent message simulation.
use super::{parent::Parent, setup, support};
use std::{
    collections::{BTreeMap, VecDeque},
    net::{SocketAddr, TcpListener, UdpSocket},
    sync::Arc,
    time::{Duration, Instant},
};
use voteboat::{
    connect::*,
    dial::*,
    identity::*,
    logical_voter::*,
    native::{connect::*, dial::NativeTcpDialer, tls::TlsPeer, worker::ThreadWake},
    outbound::*,
    runtime::*,
    secure::*,
    transport::*,
};
type Session = LogicalPeerSession<Box<dyn SecureSession>>;
pub struct Channels {
    connectors: Vec<NativeServiceConnector>,
    sessions: BTreeMap<(usize, usize), Session>,
    pending: Vec<VecDeque<LogicalNetworkBatch>>,
    addresses: Vec<SocketAddr>,
}
impl Channels {
    pub fn new(
        descriptors: &[LogicalPeerDescriptor],
        parents: &[Parent],
        protocol: NativePeerProtocol,
        now: MonoTime,
    ) -> Self {
        let held = (0..3)
            .map(|_| {
                let tcp = TcpListener::bind("127.0.0.1:0").unwrap();
                let udp = UdpSocket::bind(tcp.local_addr().unwrap()).unwrap();
                (tcp, udp)
            })
            .collect::<Vec<_>>();
        let addresses = held
            .iter()
            .map(|(tcp, _)| tcp.local_addr().unwrap())
            .collect::<Vec<_>>();
        let connectors = held
            .into_iter()
            .enumerate()
            .map(|(slot, sockets)| connector(slot, descriptors, &addresses, sockets, protocol, now))
            .collect();
        let mut result = Self {
            connectors,
            sessions: BTreeMap::new(),
            pending: (0..3).map(|_| VecDeque::new()).collect(),
            addresses,
        };
        result.establish(descriptors, parents, now);
        result
    }
    fn establish(
        &mut self,
        descriptors: &[LogicalPeerDescriptor],
        parents: &[Parent],
        start: MonoTime,
    ) {
        for (from, connector) in self.connectors.iter_mut().enumerate() {
            for (to, descriptor) in descriptors.iter().enumerate().filter(|(to, _)| *to != from) {
                connector
                    .submit(
                        ConnectRequest {
                            ticket: ConnectTicket {
                                local: descriptors[from].physical(),
                                peer: PeerIdentity {
                                    node: descriptor.physical().node,
                                    store: descriptor.physical().store.identity,
                                },
                                generation: SecureSessionGeneration::new(1).unwrap(),
                            },
                            direction: if from < to {
                                ConnectDirection::Dial(self.addresses[to])
                            } else {
                                ConnectDirection::Accept
                            },
                            deadline: MonoTime(start.0 + 10000),
                        },
                        start,
                    )
                    .unwrap();
            }
        }
        let clock = Instant::now();
        while self.sessions.len() < 6 {
            let now = MonoTime(start.0 + clock.elapsed().as_millis() as u64);
            for (from, connector) in self.connectors.iter_mut().enumerate() {
                for output in connector.poll(now, ConnectPollBudget::default()).unwrap() {
                    let to = descriptors
                        .iter()
                        .position(|d| d.physical().node == output.ticket.peer.node)
                        .unwrap();
                    let session = output.result.unwrap();
                    let plan = LogicalPeerPlan::new(
                        descriptors[from].clone(),
                        descriptors
                            .iter()
                            .enumerate()
                            .filter(|(i, _)| *i != from)
                            .map(|(_, d)| d.clone())
                            .collect(),
                    )
                    .unwrap();
                    let session = LogicalPeerSession::new(
                        session,
                        plan,
                        parents[from].owner.authorization(),
                        parents[from].outbound.binding(),
                        LogicalNetworkLimits::default(),
                    )
                    .map_err(|e| e.reason)
                    .unwrap();
                    assert_eq!(session.binding().wire_version, LOGICAL_NETWORK_VERSION);
                    assert!(self.sessions.insert((from, to), session).is_none());
                }
            }
            assert!(
                clock.elapsed() < Duration::from_secs(10),
                "actual outer handshake timeout"
            );
            std::thread::park_timeout(Duration::from_millis(1));
        }
    }
    pub fn poll(&mut self, parents: &mut [Parent], now: MonoTime) {
        for (from, parent) in parents.iter_mut().enumerate() {
            self.pending[from].extend(parent.outbound.poll_logical(8));
            for _ in 0..self.pending[from].len() {
                let batch = self.pending[from].pop_front().unwrap();
                let to = (batch.ticket().peer.get() - 11) as usize;
                match self.sessions.get_mut(&(from, to)).unwrap().submit(batch) {
                    Ok(()) => {}
                    Err(e)
                        if e.reason == LogicalNetworkError::Outbound(OutboundError::Overloaded) =>
                    {
                        self.pending[from].push_back(*e.batch)
                    }
                    Err(e) => panic!("actual logical send rejected: {:?}", e.reason),
                }
            }
        }
        for (&(from, _to), session) in &mut self.sessions {
            session.poll(now, TransportPollBudget::default()).unwrap();
            if let Some(output) = session.take_send() {
                assert_eq!(output.result(), LocalSendResult::Sent);
                parents[from]
                    .outbound
                    .complete_logical(output)
                    .map_err(|e| e.reason)
                    .unwrap();
            }
            if let Some(received) = session
                .take_received(parents[from].owner.authorization())
                .unwrap()
            {
                let message = received
                    .into_message(parents[from].owner.authorization())
                    .map_err(|e| e.reason)
                    .unwrap();
                assert_eq!(message.to, parents[from].mapping.voter);
                parents[from]
                    .owner
                    .admit(message.group, voteboat::raft::Event::Receive(message))
                    .unwrap();
            }
        }
    }
    pub fn is_quiescent(&self) -> bool {
        self.pending.iter().all(VecDeque::is_empty)
            && self.sessions.values().all(|s| !s.has_received())
    }
    pub fn close(mut self, parents: &mut [Parent], start: MonoTime) {
        assert!(parents.iter().all(|p| p.outbound.is_drained()));
        assert!(self.is_quiescent());
        for session in self.sessions.values_mut() {
            session.close();
        }
        for connector in &mut self.connectors {
            connector.close();
        }
        let clock = Instant::now();
        while !self.sessions.values().all(Session::is_drained)
            || !self.connectors.iter().all(PeerConnector::is_drained)
        {
            let now = MonoTime(start.0 + clock.elapsed().as_millis() as u64);
            for session in self.sessions.values_mut() {
                session.poll(now, TransportPollBudget::default()).unwrap();
            }
            for connector in &mut self.connectors {
                assert!(connector
                    .poll(now, ConnectPollBudget::default())
                    .unwrap()
                    .is_empty());
            }
            assert!(
                clock.elapsed() < Duration::from_secs(15),
                "actual outer close timeout"
            );
            std::thread::park_timeout(Duration::from_millis(1));
        }
        self.sessions.clear();
        for connector in self.connectors {
            if let Some(mut dialer) = connector
                .into_dialer()
                .unwrap_or_else(|_| panic!("outer connector drain"))
            {
                while !dialer.try_finish().unwrap() {
                    assert!(clock.elapsed() < Duration::from_secs(15));
                    std::thread::park_timeout(Duration::from_millis(1));
                }
            }
        }
        for address in self.addresses {
            drop((
                TcpListener::bind(address).unwrap(),
                UdpSocket::bind(address).unwrap(),
            ));
        }
    }
}
fn connector(
    slot: usize,
    descriptors: &[LogicalPeerDescriptor],
    _addresses: &[SocketAddr],
    sockets: (TcpListener, UdpSocket),
    protocol: NativePeerProtocol,
    now: MonoTime,
) -> NativeServiceConnector {
    let local = descriptors[slot].physical();
    let peers = descriptors
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != slot)
        .map(|(_, d)| {
            let physical = d.physical();
            let id = setup::certificate_slot(physical.node.get());
            (
                physical.node,
                TlsPeer {
                    identity: PeerIdentity {
                        node: physical.node,
                        store: physical.store.identity,
                    },
                    certificate: setup::certificate(id).to_vec(),
                    server_name: format!("node{id}.voteboat.test"),
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    let config = NativeConnectConfig {
        local,
        limits: ConnectLimits::default(),
        session: SessionLimits::default(),
    };
    let tls = support::tls::configuration(setup::certificate_slot(local.node.get()))
        .with_logical_profile();
    match protocol {
        NativePeerProtocol::TcpTls => {
            drop(sockets.1);
            let dialer = NativeTcpDialer::spawn(
                local,
                peers
                    .iter()
                    .map(|(id, p)| (*id, p.identity.store))
                    .collect(),
                DialLimits::default(),
                Arc::new(ThreadWake::current()),
            )
            .unwrap();
            NativeServiceConnector::Tcp(Box::new(
                NativePeerConnector::new(config, tls, peers, dialer, Some(sockets.0), now).unwrap(),
            ))
        }
        #[cfg(feature = "quic")]
        NativePeerProtocol::Quic => {
            drop(sockets.0);
            let peers = peers
                .into_iter()
                .map(|(id, p)| {
                    let i = descriptors
                        .iter()
                        .position(|d| d.physical().node == id)
                        .unwrap();
                    (id, (_addresses[i], p))
                })
                .collect();
            NativeServiceConnector::Quic(Box::new(
                voteboat::native::quic_connect::NativeQuicConnector::new(
                    config, tls, peers, sockets.1, now,
                )
                .map_err(|e| e.reason)
                .unwrap(),
            ))
        }
    }
}
