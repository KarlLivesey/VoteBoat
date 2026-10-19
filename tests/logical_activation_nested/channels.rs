// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Real authenticated native transport; no parent message simulation.
use super::{
    harness::{self, FixtureClock, Observed, SimClock},
    setup, support,
};
use std::{collections::BTreeMap, net::SocketAddr, sync::Arc, time::Duration};
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
    addresses: Vec<SocketAddr>,
    // Each connector adopts only its own protocol's bound half. The lease
    // keeps the unused half bound and the lock held for the whole workflow,
    // and rebinds the adopted half after close to prove its release.
    ports: support::ports::PortLease,
    clock: SimClock,
}
impl Channels {
    pub fn new(
        descriptors: &[LogicalPeerDescriptor],
        bindings: &[(RuntimeAuthorization, OutboundBinding)],
        protocol: NativePeerProtocol,
        clock: &SimClock,
        target_links: bool,
    ) -> Self {
        let now = clock.now();
        let mut ports = support::ports::PortLease::acquire(descriptors.len());
        let addresses = ports.address_list();
        let connectors = (0..addresses.len())
            .map(|slot| connector(slot, descriptors, &addresses, &mut ports, protocol, now))
            .collect();
        let mut result = Self {
            connectors,
            sessions: BTreeMap::new(),
            addresses,
            ports,
            clock: clock.clone(),
        };
        result.establish(descriptors, bindings, now, target_links);
        result
    }
    fn establish(
        &mut self,
        descriptors: &[LogicalPeerDescriptor],
        bindings: &[(RuntimeAuthorization, OutboundBinding)],
        start: MonoTime,
        target_links: bool,
    ) {
        for (from, connector) in self.connectors.iter_mut().enumerate() {
            for (to, descriptor) in descriptors.iter().enumerate().filter(|(to, _)| {
                *to != from
                    && (!target_links
                        || from == descriptors.len() - 1
                        || *to == descriptors.len() - 1)
            }) {
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
        let end = harness::Wait::new(&self.clock, harness::HANDSHAKE_MS);
        while self.sessions.len()
            < if target_links {
                2 * (descriptors.len() - 1)
            } else {
                descriptors.len() * (descriptors.len() - 1)
            }
        {
            let now = self.clock.round(self.observed());
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
                        bindings[from].0,
                        bindings[from].1,
                        setup::network_limits(),
                    )
                    .map_err(|e| e.reason)
                    .unwrap();
                    assert_eq!(session.binding().wire_version, LOGICAL_NETWORK_VERSION);
                    assert!(self.sessions.insert((from, to), session).is_none());
                }
            }
            assert!(end.within(&self.clock), "actual outer handshake timeout");
            std::thread::park_timeout(Duration::from_millis(1));
        }
    }
    pub fn take_sessions(
        &mut self,
        slot: usize,
        descriptors: &[LogicalPeerDescriptor],
    ) -> BTreeMap<voteboat::identity::NodeId, Session> {
        let keys = self
            .sessions
            .keys()
            .copied()
            .filter(|(from, _)| *from == slot)
            .collect::<Vec<_>>();
        keys.into_iter()
            .map(|key| {
                (
                    descriptors[key.1].mapping().voter,
                    self.sessions.remove(&key).unwrap(),
                )
            })
            .collect()
    }
    pub fn close(mut self, sessions: impl IntoIterator<Item = Session>) {
        assert!(self.sessions.is_empty());
        self.sessions = sessions
            .into_iter()
            .enumerate()
            .map(|(i, s)| ((i, 0), s))
            .collect();
        for session in self.sessions.values_mut() {
            session.close();
        }
        for connector in &mut self.connectors {
            connector.close();
        }
        let end = harness::Wait::new(&self.clock, harness::LIVENESS_MS);
        while !self.sessions.values().all(Session::is_drained)
            || !self.connectors.iter().all(PeerConnector::is_drained)
        {
            let now = self.clock.round(self.observed());
            for session in self.sessions.values_mut() {
                if !session.is_drained() {
                    session.poll(now, TransportPollBudget::default()).unwrap();
                }
            }
            for connector in &mut self.connectors {
                assert!(connector
                    .poll(now, ConnectPollBudget::default())
                    .unwrap()
                    .is_empty());
            }
            assert!(end.within(&self.clock), "actual outer close timeout");
            std::thread::park_timeout(Duration::from_millis(1));
        }
        self.sessions.clear();
        for connector in self.connectors {
            if let Some(mut dialer) = connector
                .into_dialer()
                .unwrap_or_else(|_| panic!("outer connector drain"))
            {
                // Joining the dial worker takes no protocol time: only the
                // hang guard applies.
                while !dialer.try_finish().unwrap() {
                    assert!(end.within(&self.clock), "actual outer dialer join");
                    std::thread::park_timeout(Duration::from_millis(1));
                }
            }
        }
        self.ports.reclaim();
    }
    /// Dial work holds protocol time; so does any session or connector
    /// progress since the previous round.
    fn observed(&self) -> Observed {
        let drained = self.sessions.values().filter(|s| s.is_drained()).count();
        self.connectors
            .iter()
            .fold(Observed::default(), Observed::connector)
            .progress((self.sessions.len(), drained))
    }
}
fn connector(
    slot: usize,
    descriptors: &[LogicalPeerDescriptor],
    _addresses: &[SocketAddr],
    ports: &mut support::ports::PortLease,
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
                NativePeerConnector::new(
                    config,
                    tls,
                    peers,
                    dialer,
                    Some(ports.adopt_tcp(slot)),
                    now,
                )
                .unwrap(),
            ))
        }
        #[cfg(feature = "quic")]
        NativePeerProtocol::Quic => {
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
                    config,
                    tls,
                    peers,
                    ports.adopt_udp(slot),
                    now,
                )
                .map_err(|e| e.reason)
                .unwrap(),
            ))
        }
    }
}
