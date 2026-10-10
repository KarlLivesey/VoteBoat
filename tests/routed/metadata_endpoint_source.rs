// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use crate::native::automatic_lookup::remote_manifest::sessions;
use std::{
    cell::{Cell, RefCell},
    collections::BTreeSet,
    rc::Rc,
};
use voteboat::{
    connect::*, discovery::*, identity::SecureSessionGeneration, native::remote_discovery::*,
    secure::*,
};
#[path = "metadata_source_connector.rs"]
mod connector;
type Server = NativeDiscoveryResponder<Box<dyn SecureSession>, View>;
#[derive(Clone)]
pub(super) struct View {
    hints: Rc<RefCell<BTreeMap<NodeId, PeerEndpointHint>>>,
    stale: Rc<RefCell<BTreeSet<NodeId>>>,
    repaired: Rc<RefCell<BTreeSet<NodeId>>>,
    source_epoch: Rc<Cell<u64>>,
}
impl View {
    pub fn new(configs: &[NativeStartup]) -> Self {
        Self {
            hints: Rc::new(RefCell::new(
                configs
                    .iter()
                    .map(|c| {
                        (
                            c.node,
                            PeerEndpointHint {
                                peer: PeerIdentity {
                                    node: c.node,
                                    store: c.store,
                                },
                                generation: HintGeneration::new(2).unwrap(),
                                endpoint: c.listen,
                                expires_at: MonoTime(0),
                            },
                        )
                    })
                    .collect(),
            )),
            stale: Rc::default(),
            repaired: Rc::default(),
            source_epoch: Rc::default(),
        }
    }
    pub fn disconnect_sources(&self) {
        self.source_epoch.set(self.source_epoch.get() + 1);
    }
    pub fn lower_peers_repaired(&self) -> bool {
        [support::node(1), support::node(2)]
            .iter()
            .all(|n| self.repaired.borrow().contains(n))
    }
    pub fn lower_peers_refused_stale(&self) -> bool {
        [support::node(1), support::node(2)]
            .iter()
            .all(|n| self.stale.borrow().contains(n))
    }
    pub fn update(&self, cfg: &NativeStartup, generation: u64) {
        self.publish((cfg.node, cfg.store, cfg.listen), generation);
    }
    pub fn publish(
        &self,
        (node, store, address): (NodeId, StoreIdentity, std::net::SocketAddr),
        generation: u64,
    ) {
        let mut hints = self.hints.borrow_mut();
        let hint = hints.get_mut(&node).unwrap();
        assert_eq!(hint.peer.store, store);
        hint.endpoint = address;
        hint.generation = HintGeneration::new(generation).unwrap();
    }
    pub fn driver(
        &self,
        local: LocalIdentity,
        protocol: NativePeerProtocol,
        clock: &Instant,
    ) -> Box<dyn DiscoveryDriver> {
        let source = LocalIdentity {
            node: support::node(if local.node == support::node(3) { 2 } else { 3 }),
            store: StoreBinding {
                identity: support::identity(300),
                session: StoreSession::new(1).unwrap(),
            },
        };
        let (a, b) = sessions::pair(protocol, local, source, clock);
        let peer = |id: LocalIdentity| PeerIdentity {
            node: id.node,
            store: id.store.identity,
        };
        let remote = NativeRemotePeerDiscovery::new(
            a,
            peer(source),
            RemoteDiscoveryConfig::default(),
            now(clock),
        )
        .ok()
        .unwrap();
        let server = NativeDiscoveryResponder::new(
            b,
            peer(local),
            self.clone(),
            RemoteDiscoveryConfig::default(),
            now(clock),
        )
        .ok()
        .unwrap();
        let server = Rc::new(RefCell::new(Some(server)));
        let connector = connector::Connector::new(
            local,
            source,
            protocol,
            *clock,
            self.clone(),
            server.clone(),
        );
        let remote = ReconnectingPeerDiscovery::new(
            remote,
            connector,
            SourceReconnectConfig {
                endpoint: "127.0.0.1:4567".parse().unwrap(),
                first_generation: SecureSessionGeneration::new(2).unwrap(),
                last_generation: SecureSessionGeneration::new(8).unwrap(),
                retry_ms: 10,
            },
            now(clock),
        )
        .ok()
        .unwrap();
        Box::new(Driven {
            remote,
            server,
            view: self.clone(),
            local: local.node,
            source_epoch: self.source_epoch.get(),
        })
    }
}
impl PeerDiscovery for View {
    fn resolve(
        &mut self,
        peer: PeerIdentity,
        now: MonoTime,
    ) -> Result<PeerEndpointHint, DiscoveryError> {
        let hint = *self
            .hints
            .borrow()
            .get(&peer.node)
            .ok_or(DiscoveryError::Missing)?;
        PeerEndpointHint {
            expires_at: MonoTime(now.0 + 60_000),
            ..hint
        }
        .validate(peer, now)
    }
    fn invalidate(&mut self, _: PeerIdentity, _: HintGeneration) -> bool {
        false
    }
    fn close(&mut self) {}
}
struct Driven {
    remote: ReconnectingPeerDiscovery<connector::Connector>,
    server: Rc<RefCell<Option<Server>>>,
    view: View,
    local: NodeId,
    source_epoch: u64,
}
impl PeerDiscovery for Driven {
    fn resolve(
        &mut self,
        peer: PeerIdentity,
        now: MonoTime,
    ) -> Result<PeerEndpointHint, DiscoveryError> {
        let result = self.remote.resolve(peer, now);
        if result == Err(DiscoveryError::StaleGeneration) {
            self.view.stale.borrow_mut().insert(self.local);
        }
        result
    }
    fn invalidate(&mut self, peer: PeerIdentity, generation: HintGeneration) -> bool {
        self.remote.invalidate(peer, generation)
    }
    fn close(&mut self) {
        self.remote.close();
        self.server.borrow_mut().take();
    }
}
impl DiscoveryDriver for Driven {
    fn poll_discovery(
        &mut self,
        now: MonoTime,
        budget: SessionPollBudget,
    ) -> Result<(), DiscoveryError> {
        if self.source_epoch != self.view.source_epoch.get() {
            self.source_epoch = self.view.source_epoch.get();
            self.server.borrow_mut().take();
        }
        let mut server = self.server.borrow_mut();
        if let Some(source) = server.as_mut() {
            match source.poll(now, budget) {
                Ok(_) => (),
                Err(RemoteDiscoveryError::Session(
                    SessionError::Closed | SessionError::Truncated,
                )) => {
                    server.take();
                }
                other => panic!("source poll: {other:?}"),
            }
        }
        drop(server);
        self.remote.poll_discovery(now, budget)?;
        Ok(())
    }
    fn discovery_pending(&self) -> bool {
        self.remote.discovery_pending()
    }
    fn discovery_deadline(&self) -> Option<MonoTime> {
        self.remote.discovery_deadline()
    }
}
pub(super) fn now(clock: &Instant) -> MonoTime {
    MonoTime(clock.elapsed().as_millis() as u64)
}
pub(super) fn open(cfg: NativeStartup, view: &View, env: &Environment<'_>) -> Node<Owner> {
    let limits = cfg.limits;
    let node = cfg.node;
    let mut parts = cfg
        .prepare_for_discovery(
            env.protocol,
            TimerConfig::default(),
            owner(),
            Arc::new(ThreadWake::current()),
            now(env.clock),
        )
        .unwrap_or_else(|r| panic!("discovery preparation: {:?}", r.reason));
    let local = LocalIdentity {
        node,
        store: parts.local.owner.identity().store,
    };
    let discovery = view.driver(local, env.protocol, env.clock);
    let mut peers = parts.peers.take().unwrap();
    peers.connector = peers
        .connector
        .with_discovery(discovery, now(env.clock))
        .ok()
        .unwrap();
    for (node, route) in &mut peers.routes {
        if *node > local.node {
            *route = voteboat::connect::ConnectDirection::Dial("127.0.0.1:1".parse().unwrap());
        }
    }
    parts.peers = Some(peers);
    Node::from_parts(parts, limits, now(env.clock))
        .unwrap_or_else(|r| panic!("assembly: {:?}", r.reason))
}
