// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use voteboat::transport::ConnectTicket;

/// Test-owned bounded connector; each accepted attempt creates an actual TLS or
/// QUIC session pair. Production source reconnection still owns retry policy.
pub(super) struct Connector {
    local: LocalIdentity,
    source: LocalIdentity,
    protocol: NativePeerProtocol,
    clock: Instant,
    view: View,
    server: Rc<RefCell<Option<Server>>>,
    pending: Option<ConnectCompletion<Box<dyn SecureSession>>>,
    deadline: Option<MonoTime>,
    closed: bool,
}
impl Connector {
    pub fn new(
        local: LocalIdentity,
        source: LocalIdentity,
        protocol: NativePeerProtocol,
        clock: Instant,
        view: View,
        server: Rc<RefCell<Option<Server>>>,
    ) -> Self {
        Self {
            local,
            source,
            protocol,
            clock,
            view,
            server,
            pending: None,
            deadline: None,
            closed: false,
        }
    }
}
impl PeerConnector for Connector {
    type Endpoint = std::net::SocketAddr;
    type Session = Box<dyn SecureSession>;
    fn local(&self) -> LocalIdentity {
        self.local
    }
    fn limits(&self) -> ConnectLimits {
        ConnectLimits {
            requests: 1,
            anonymous: 0,
            timeout_ms: 10_000,
        }
    }
    fn usage(&self) -> ConnectUsage {
        ConnectUsage {
            requests: usize::from(self.pending.is_some()),
            ..Default::default()
        }
    }
    fn next_deadline(&self) -> Option<MonoTime> {
        self.deadline
    }
    fn supports_peer(&self, peer: PeerIdentity) -> bool {
        peer.node == self.source.node && peer.store == self.source.store.identity
    }
    fn submit(
        &mut self,
        request: ConnectRequest<Self::Endpoint>,
        now: MonoTime,
    ) -> Result<(), ConnectRejected<Self::Endpoint>> {
        assert!(!self.closed && self.pending.is_none());
        assert_eq!(request.ticket.local, self.local);
        assert!(self.supports_peer(request.ticket.peer));
        assert!(request.deadline > now);
        assert!(matches!(request.direction, ConnectDirection::Dial(_)));
        let (client, server) = sessions::pair_generation(
            self.protocol,
            self.local,
            self.source,
            &self.clock,
            request.ticket.generation.get(),
        );
        let source = NativeDiscoveryResponder::new(
            server,
            PeerIdentity {
                node: self.local.node,
                store: self.local.store.identity,
            },
            self.view.clone(),
            RemoteDiscoveryConfig::default(),
            super::now(&self.clock),
        )
        .ok()
        .unwrap();
        self.server.borrow_mut().replace(source);
        self.pending = Some(ConnectCompletion {
            ticket: request.ticket,
            result: Ok(client),
        });
        self.deadline = Some(request.deadline);
        self.view.repaired.borrow_mut().insert(self.local.node);
        Ok(())
    }
    fn cancel(&mut self, ticket: ConnectTicket) -> bool {
        let Some(pending) = &mut self.pending else {
            return false;
        };
        if pending.ticket != ticket {
            return false;
        }
        if let Ok(session) = &mut pending.result {
            session.close();
        }
        pending.result = Err(ConnectError::Cancelled);
        true
    }
    fn poll(
        &mut self,
        now: MonoTime,
        budget: ConnectPollBudget,
    ) -> Result<Vec<ConnectCompletion<Self::Session>>, ConnectError> {
        budget.validate()?;
        if budget.completions == 0 {
            return Ok(vec![]);
        }
        let Some(mut pending) = self.pending.take() else {
            return Ok(vec![]);
        };
        if self.deadline.take().is_some_and(|d| now >= d) {
            if let Ok(session) = &mut pending.result {
                session.close();
            }
            pending.result = Err(ConnectError::Timeout);
        }
        Ok(vec![pending])
    }
    fn close(&mut self) {
        self.closed = true;
        if let Some(pending) = &self.pending {
            self.cancel(pending.ticket);
        }
        self.server.borrow_mut().take();
    }
}
