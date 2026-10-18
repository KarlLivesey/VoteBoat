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
//! Actual authenticated parent sessions retaining original envelope ownership.
use super::{groups as setup, support};
use crate::harness::{self, FixtureClock, Observed, PortLease, SimClock};
use std::{
    collections::BTreeMap,
    net::{SocketAddr, TcpListener, UdpSocket},
    sync::Arc,
    time::Duration,
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
#[path = "channels/dial.rs"]
mod dial;
type Session = LogicalPeerSession<Box<dyn SecureSession>>;
pub struct Channels {
    connectors: Vec<NativeServiceConnector>,
    sessions: BTreeMap<(usize, usize), Session>,
    addresses: Vec<SocketAddr>,
    // Each connector adopts its lease's bound sockets; the leases keep the
    // ports until both halves are proven released after close.
    leases: Vec<PortLease>,
    clock: SimClock,
}
impl Channels {
    pub fn new(
        descriptors: &[LogicalPeerDescriptor],
        bindings: &[(RuntimeAuthorization, OutboundBinding)],
        protocol: NativePeerProtocol,
        clock: &SimClock,
    ) -> Self {
        Self::new_wrapped(descriptors, bindings, protocol, clock, |_, _, session| {
            session
        })
    }
    pub fn new_wrapped(
        descriptors: &[LogicalPeerDescriptor],
        bindings: &[(RuntimeAuthorization, OutboundBinding)],
        protocol: NativePeerProtocol,
        clock: &SimClock,
        mut wrap: impl FnMut(usize, usize, Box<dyn SecureSession>) -> Box<dyn SecureSession>,
    ) -> Self {
        let now = clock.now();
        let mut leases = (0..3)
            .map(|_| PortLease::acquire(protocol))
            .collect::<Vec<_>>();
        let addresses = leases.iter().map(PortLease::address).collect::<Vec<_>>();
        let connectors = leases
            .iter_mut()
            .enumerate()
            .map(|(slot, lease)| {
                connector(
                    slot,
                    descriptors,
                    &addresses,
                    lease.take_sockets(),
                    protocol,
                    now,
                )
            })
            .collect();
        let mut result = Self {
            connectors,
            sessions: BTreeMap::new(),
            addresses,
            leases,
            clock: clock.clone(),
        };
        result.establish(descriptors, bindings, now, &mut wrap);
        result
    }
    fn establish(
        &mut self,
        descriptors: &[LogicalPeerDescriptor],
        bindings: &[(RuntimeAuthorization, OutboundBinding)],
        start: MonoTime,
        wrap: &mut impl FnMut(usize, usize, Box<dyn SecureSession>) -> Box<dyn SecureSession>,
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
        let end = harness::Wait::new(&self.clock, harness::HANDSHAKE_MS);
        while self.sessions.len() < 6 {
            let now = self.clock.round(self.observed());
            for (from, connector) in self.connectors.iter_mut().enumerate() {
                for output in connector.poll(now, ConnectPollBudget::default()).unwrap() {
                    let to = descriptors
                        .iter()
                        .position(|d| d.physical().node == output.ticket.peer.node)
                        .unwrap();
                    let session = wrap(from, to, output.result.unwrap());
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
                        LogicalNetworkLimits::default(),
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
                while !dialer.try_finish().unwrap() {
                    assert!(end.within(&self.clock), "actual outer dialer join");
                    std::thread::park_timeout(Duration::from_millis(1));
                }
            }
        }
        for lease in &mut self.leases {
            lease.reclaim_both();
        }
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
