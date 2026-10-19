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
//! NT1 round 2 (Opus P2): a listener that stops accepting must be visible to
//! an operator. An `EMFILE` is injected into the TCP listener of a real native
//! node whose connector is wrapped for discovery (`Discovered`, the variant the
//! binaries use with a discovery source). Time is simulated.
//!
//! NT1 round 3: a discovery source whose own counter is spent refuses a dial
//! lookup with `DiscoveryError::Exhausted`. That fails the attempt only; the
//! node stays in service and the condition reaches the same surfaces.
use super::*;
use crate::{
    application::Counter,
    discovery::*,
    log::Bootstrap,
    native::{
        node::NativeNode,
        observability::{NativeCounterObserver, NativeEventObserver},
        startup::*,
        tls::TlsCredentials,
        worker::ThreadWake,
    },
    observability::*,
    quorum::{Limits, Policy, Tree},
    runtime::*,
};
use std::{cell::Cell, rc::Rc, sync::Arc, time::Duration};

/// Resolves nothing and never needs polling: the node only accepts.
struct NoDiscovery;
impl PeerDiscovery for NoDiscovery {
    fn resolve(
        &mut self,
        _: PeerIdentity,
        _: MonoTime,
    ) -> Result<PeerEndpointHint, DiscoveryError> {
        Err(DiscoveryError::Unavailable)
    }
    fn invalidate(&mut self, _: PeerIdentity, _: HintGeneration) -> bool {
        false
    }
    fn close(&mut self) {}
}
impl DiscoveryDriver for NoDiscovery {
    fn poll_discovery(&mut self, _: MonoTime, _: SessionPollBudget) -> Result<(), DiscoveryError> {
        Ok(())
    }
    fn discovery_pending(&self) -> bool {
        false
    }
    fn discovery_deadline(&self) -> Option<MonoTime> {
        None
    }
}

/// A source whose counter is spent: every lookup is refused as `Exhausted`,
/// as `voteboat-counter`'s discovery source does at its limit.
struct SpentDiscovery(Rc<Cell<usize>>);
impl PeerDiscovery for SpentDiscovery {
    fn resolve(
        &mut self,
        _: PeerIdentity,
        _: MonoTime,
    ) -> Result<PeerEndpointHint, DiscoveryError> {
        self.0.set(self.0.get() + 1);
        Err(DiscoveryError::Exhausted)
    }
    fn invalidate(&mut self, _: PeerIdentity, _: HintGeneration) -> bool {
        false
    }
    fn close(&mut self) {}
}
impl DiscoveryDriver for SpentDiscovery {
    fn poll_discovery(&mut self, _: MonoTime, _: SessionPollBudget) -> Result<(), DiscoveryError> {
        Ok(())
    }
    fn discovery_pending(&self) -> bool {
        false
    }
    fn discovery_deadline(&self) -> Option<MonoTime> {
        None
    }
}

type Node = NativeNode<Counter, NativeServiceConnector>;

fn store(id: u128) -> StoreIdentity {
    StoreIdentity {
        id: StoreId::new(id).unwrap(),
        incarnation: StoreIncarnation::new(1).unwrap(),
    }
}

fn unused_address() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}

fn open(root: &std::path::Path) -> (Node, SocketAddr) {
    open_with(root, false, Box::new(NoDiscovery), true)
}

/// Node 1, alone or with voter 2 (which node 1 dials, since 1 < 2).
fn open_with(
    root: &std::path::Path,
    with_peer: bool,
    source: Box<dyn DiscoveryDriver>,
    emfile: bool,
) -> (Node, SocketAddr) {
    let node = NodeId::new(1).unwrap();
    let store = store(1);
    let listen = unused_address();
    let other = NodeId::new(2).unwrap();
    let (tree, voter_stores, peers) = if with_peer {
        let peer = NativeStartupPeer {
            address: unused_address(),
            certificate: include_bytes!("../../../tests/fixtures/tls/node2.der").to_vec(),
            server_name: "node2.voteboat.test".into(),
        };
        (
            Tree::Majority(vec![Tree::Voter(node), Tree::Voter(other)]),
            [(node, store), (other, self::store(2))].into(),
            [(other, peer)].into(),
        )
    } else {
        (Tree::Voter(node), [(node, store)].into(), BTreeMap::new())
    };
    let config = NativeStartup {
        directory: root.to_path_buf(),
        mode: NativeOpenMode::Create,
        node,
        store,
        bootstrap: Bootstrap {
            group: GroupIdentity {
                id: GroupId::new(7).unwrap(),
                incarnation: GroupIncarnation::new(1).unwrap(),
            },
            configuration: ConfigurationId::new(9).unwrap(),
            policy: Policy::new(tree, Limits::default()).unwrap(),
            voter_stores,
        },
        listen,
        peers,
        entropy_seed: 1,
        limits: NodeLimits::default(),
        ready_visit_items: 16,
        tls: NativeTlsConfig::new(TlsCredentials {
            roots: vec![include_bytes!("../../../tests/fixtures/tls/ca.der").to_vec()],
            certificate_chain: vec![
                include_bytes!("../../../tests/fixtures/tls/node1.der").to_vec()
            ],
            private_key: include_bytes!("../../../tests/fixtures/tls/node1-key.der").to_vec(),
        })
        .unwrap(),
    };
    let mut parts = config
        .prepare_for_discovery(
            NativePeerProtocol::TcpTls,
            NativeTimingProfile::Throughput.timers(),
            Counter::new(100).unwrap(),
            Arc::new(ThreadWake::current()),
            MonoTime(0),
        )
        .unwrap_or_else(|e| panic!("prepare: {:?}", e.reason));
    let mut peers = parts.peers.take().unwrap();
    let NativeServiceConnector::Tcp(tcp) = &mut peers.connector else {
        panic!("static startup selects the plain TCP connector");
    };
    if emfile {
        tcp.injected_accept_errors
            .push_back(io::Error::from_raw_os_error(libc::EMFILE));
    }
    peers.connector = peers
        .connector
        .with_discovery(source, MonoTime(0))
        .unwrap_or_else(|(e, _, _)| panic!("discovery wrap: {e:?}"));
    parts.peers = Some(peers);
    let node = NativeNode::from_parts(parts, NodeLimits::default(), MonoTime(0))
        .unwrap_or_else(|e| panic!("assemble: {:?}", e.reason));
    (node, listen)
}

/// Joins the node's workers. The bound only turns a deadlock into a failure;
/// it is not a timing assertion.
fn close(mut node: Node, mut now: u64) {
    node.begin_shutdown();
    for _ in 0..100_000 {
        if node.is_drained() {
            break;
        }
        now += 1;
        node.poll(MonoTime(now), NodePollBudget::default()).unwrap();
        std::thread::park_timeout(Duration::from_millis(1));
    }
    let mut p = node.into_parts().unwrap_or_else(|_| panic!("not drained"));
    let mut dialer = p
        .peers
        .take()
        .unwrap()
        .connector
        .into_dialer()
        .unwrap_or_else(|_| panic!("connector"));
    let mut snapshots = p.local.snapshots.take().unwrap();
    let (mut log, mut snap) = (false, false);
    for _ in 0..600_000 {
        let dial = dialer.as_mut().is_none_or(|d| d.try_finish().unwrap());
        log = log || p.local.persistence.try_reclaim().unwrap().is_some();
        snap = snap || snapshots.worker.try_reclaim().unwrap().is_some();
        if dial && log && snap {
            return;
        }
        std::thread::park_timeout(Duration::from_millis(1));
    }
    panic!("hang guard: workers did not join");
}

/// The host side of the binaries' diagnostics: counters and events sampled
/// after each poll.
struct Host {
    owner: RuntimeOwner,
    counters: NativeCounterObserver,
    events: NativeEventObserver,
    reporter: EventReporter,
}
impl Host {
    fn new(owner: RuntimeOwner) -> Self {
        let binding = EventBinding {
            owner,
            generation: EventGeneration::new(1).unwrap(),
        };
        Self {
            owner,
            counters: NativeCounterObserver::new(owner),
            events: NativeEventObserver::new(binding, EventLimits::default()).unwrap(),
            reporter: EventReporter::new(binding),
        }
    }
    fn sample(&mut self, node: &mut Node, now: u64) -> CounterSnapshot {
        let result = node.poll(MonoTime(now), NodePollBudget::default());
        let s = NodeObservation::from_poll(self.owner, MonoTime(now), node.state(), &result);
        result.unwrap();
        assert_eq!(node.state(), NodeState::Running);
        self.counters.record_bounded(s).unwrap();
        self.reporter.record_sample(&mut self.events, s).unwrap();
        self.counters.snapshot_counters()
    }
}

fn scratch(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "voteboat-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

#[test]
fn injected_emfile_reaches_counters_events_and_connector_status() {
    let root = scratch("accept-health");
    let (mut node, address) = open(&root);
    let mut host = Host::new(node.local().owner.identity());
    // Every poll runs the accept phase, so the first consumes the error.
    let snapshot = host.sample(&mut node, 1);
    let c = snapshot.counters;
    assert_eq!((c.accept_errors, c.accept_backoffs), (1, 1));
    assert!(snapshot.network.accept_paused);
    assert!(!snapshot.network.generations_exhausted);
    let status = node.peers().unwrap().connector().accept_status();
    let status = status.expect("Discovered reports its TCP listener");
    assert_eq!(status.transient_errors, 1);
    assert_eq!(status.backoffs, 1);
    assert_eq!(status.last_os_error, Some(libc::EMFILE));
    assert_eq!(status.retry_at, Some(MonoTime(11)));
    let page = host.events.read_events(None, 16).unwrap();
    assert!(
        page.records.iter().any(|r| r.event.kind
            == EventKind::Pressure {
                ingress_blocked: 0,
                snapshot_refusals: 0,
                connection_failures: 0,
                accept_errors: 1,
                accept_backoffs: 1,
                generations_exhausted: false,
            }),
        "{page:?}"
    );
    // Still paused before the retry time; nothing is counted twice.
    let snapshot = host.sample(&mut node, 10);
    assert!(snapshot.network.accept_paused);
    assert_eq!(snapshot.counters.accept_errors, 1);
    // After the delay the listener is back and accepts a real connection.
    let _client = TcpStream::connect(address).unwrap();
    let mut now = 11;
    let mut accepted = false;
    for _ in 0..10_000 {
        let snapshot = host.sample(&mut node, now);
        assert!(!snapshot.network.accept_paused);
        assert_eq!(snapshot.counters.accept_errors, 1);
        if node.peers().unwrap().connector().usage().anonymous == 1 {
            accepted = true;
            break;
        }
        now += 1;
        std::thread::park_timeout(Duration::from_millis(1));
    }
    assert!(accepted, "listener stopped accepting");
    assert_eq!(
        node.peers()
            .unwrap()
            .connector()
            .accept_status()
            .unwrap()
            .retry_at,
        None
    );
    close(node, now);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn an_exhausted_discovery_source_fails_the_dial_without_failing_the_node() {
    let root = scratch("discovery-exhausted");
    let lookups = Rc::new(Cell::new(0));
    let source = Box::new(SpentDiscovery(lookups.clone()));
    let (mut node, _) = open_with(&root, true, source, false);
    let mut host = Host::new(node.local().owner.identity());
    // The first poll dials node 2; `submit` resolves synchronously and the
    // source refuses. `sample` asserts the node is still Running.
    let snapshot = host.sample(&mut node, 1);
    assert_eq!(lookups.get(), 1, "the dial went through the source");
    assert_eq!(snapshot.counters.connection_failures, 1);
    assert_eq!(snapshot.counters.failed_polls, 0);
    assert!(snapshot.network.generations_exhausted);
    let page = host.events.read_events(None, 16).unwrap();
    assert!(
        page.records.iter().any(|r| matches!(
            r.event.kind,
            EventKind::Pressure {
                connection_failures: 1,
                generations_exhausted: true,
                ..
            }
        )),
        "{page:?}"
    );
    // The roster backs off: nothing is retried at the same instant.
    let snapshot = host.sample(&mut node, 1);
    assert_eq!(lookups.get(), 1);
    assert_eq!(snapshot.counters.connection_failures, 1);
    // After the backoff the roster retries, fails the attempt again and the
    // node is still in service. The bound only turns a stall into a failure.
    let mut now = 1;
    while lookups.get() < 2 {
        now += 1;
        assert!(now < 1_000_000, "the roster never retried");
        let snapshot = host.sample(&mut node, now);
        assert!(snapshot.network.generations_exhausted);
    }
    assert_eq!(host.sample(&mut node, now).counters.connection_failures, 2);
    close(node, now);
    std::fs::remove_dir_all(root).unwrap();
}
