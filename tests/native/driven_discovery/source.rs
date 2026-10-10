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
use super::*;
use std::{net::TcpListener, sync::Arc};
use voteboat::{
    connect::{ConnectPollBudget, ConnectRequest},
    dial::DialLimits,
    native::{connect::NativeConnectConfig, dial::NativeTcpDialer, worker::ThreadWake},
    secure::{require_authenticated, LocalIdentity, SecureSession, SessionLimits},
    transport::ConnectTicket,
};

pub(super) struct SourceServer {
    acceptor: NativePeerConnector,
    responder: Option<Responder>,
    peer: PeerIdentity,
    source: Source,
    next: u64,
    pending: Option<ConnectTicket>,
    accepted_generation: u64,
    closed: bool,
}
impl SourceServer {
    pub(super) fn new(
        local: LocalIdentity,
        server: LocalIdentity,
        source: Source,
    ) -> (Remote, Self) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = listener.local_addr().unwrap();
        let client_connector = connector(local, server, None);
        let acceptor = connector(server, local, Some(listener));
        let (client, session) = support::tls::pair(local, server, 1);
        let peer = PeerIdentity {
            node: local.node,
            store: local.store.identity,
        };
        let responder = NativeDiscoveryResponder::new(
            session,
            peer,
            source.clone(),
            RemoteDiscoveryConfig::default(),
            MonoTime(0),
        )
        .ok()
        .unwrap();
        let remote = NativeRemotePeerDiscovery::new(
            client,
            PeerIdentity {
                node: server.node,
                store: server.store.identity,
            },
            RemoteDiscoveryConfig::default(),
            MonoTime(0),
        )
        .ok()
        .unwrap();
        let remote = ReconnectingPeerDiscovery::new(
            remote,
            client_connector,
            SourceReconnectConfig {
                endpoint,
                first_generation: SecureSessionGeneration::new(2).unwrap(),
                last_generation: SecureSessionGeneration::new(1000).unwrap(),
                retry_ms: 10,
            },
            MonoTime(0),
        )
        .ok()
        .unwrap();
        (
            remote,
            Self {
                acceptor,
                responder: Some(responder),
                peer,
                source,
                next: 2,
                pending: None,
                accepted_generation: 1,
                closed: false,
            },
        )
    }
    pub(super) fn disconnect(&mut self) {
        if let Some(mut responder) = self.responder.take() {
            responder.close();
        }
    }
    pub(super) fn accepted_generation(&self) -> u64 {
        self.accepted_generation
    }
    pub(super) fn poll(&mut self, now: MonoTime) {
        if !self.closed && self.responder.is_none() && self.pending.is_none() {
            let ticket = ConnectTicket {
                local: self.acceptor.local(),
                peer: self.peer,
                generation: SecureSessionGeneration::new(self.next).unwrap(),
            };
            self.next += 1;
            self.acceptor
                .submit(
                    ConnectRequest {
                        ticket,
                        direction: ConnectDirection::Accept,
                        deadline: MonoTime(now.0 + 10_000),
                    },
                    now,
                )
                .unwrap();
            self.pending = Some(ticket);
        }
        for done in self
            .acceptor
            .poll(now, ConnectPollBudget::default())
            .unwrap()
        {
            assert_eq!(self.pending.take(), Some(done.ticket));
            if self.closed {
                if let Ok(mut session) = done.result {
                    session.close();
                }
            } else {
                let session = done.result.unwrap();
                let binding = require_authenticated(&session).unwrap();
                assert_eq!(binding.generation, done.ticket.generation);
                assert_eq!(binding.peer.node, self.peer.node);
                assert_eq!(binding.peer.store.identity, self.peer.store);
                self.accepted_generation = binding.generation.get();
                assert!(self.responder.is_none());
                self.responder = Some(
                    NativeDiscoveryResponder::new(
                        session,
                        self.peer,
                        self.source.clone(),
                        RemoteDiscoveryConfig::default(),
                        now,
                    )
                    .ok()
                    .unwrap(),
                );
            }
        }
        if let Some(responder) = &mut self.responder {
            responder.poll(now, SessionPollBudget::default()).unwrap();
        }
    }
    pub(super) fn finish(mut self, now: MonoTime) {
        self.closed = true;
        self.disconnect();
        self.acceptor.close();
        let deadline = Instant::now() + Duration::from_secs(5);
        while !self.acceptor.is_drained() {
            self.poll(now);
            assert!(Instant::now() < deadline, "source acceptor did not drain");
            std::thread::park_timeout(Duration::from_millis(1));
        }
        finish_connector(self.acceptor);
    }
}
fn connector(
    local: LocalIdentity,
    peer: LocalIdentity,
    listener: Option<TcpListener>,
) -> NativePeerConnector {
    let dialer = NativeTcpDialer::spawn(
        local,
        [(peer.node, peer.store.identity)].into(),
        DialLimits::default(),
        Arc::new(ThreadWake::current()),
    )
    .unwrap();
    NativePeerConnector::new(
        NativeConnectConfig {
            local,
            limits: Default::default(),
            session: SessionLimits::default(),
        },
        support::tls::configuration(local.node.get()),
        [(peer.node, support::tls::peer(peer))].into(),
        dialer,
        listener,
        MonoTime(0),
    )
    .unwrap_or_else(|r| panic!("discovery connector: {:?}", r.reason))
}
pub(super) fn finish_connector(connector: NativePeerConnector) {
    let mut dialer = connector.into_dialer().ok().unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while !dialer.try_finish().unwrap() {
        assert!(
            Instant::now() < deadline,
            "discovery dial worker did not join"
        );
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
