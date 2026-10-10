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
use std::{cell::Cell, net::SocketAddr, rc::Rc};
use voteboat::{
    connect::{ConnectDirection, DiscoveryConnector, PeerConnector},
    discovery::*,
    native::{connect::NativePeerConnector, remote_discovery::*},
    secure::{PeerIdentity, SessionPollBudget},
};
#[path = "driven_discovery/source.rs"]
mod source;
use source::SourceServer;
type Remote = ReconnectingPeerDiscovery<NativePeerConnector>;
type Boat = NativeNode<Counter, DiscoveryConnector<NativePeerConnector, Remote>>;
type Responder = NativeDiscoveryResponder<Session, Source>;
#[derive(Clone)]
struct Source {
    hints: BTreeMap<NodeId, PeerEndpointHint>,
    calls: Rc<Cell<usize>>,
}
impl PeerDiscovery for Source {
    fn resolve(
        &mut self,
        peer: PeerIdentity,
        now: MonoTime,
    ) -> Result<PeerEndpointHint, DiscoveryError> {
        self.calls.set(self.calls.get() + 1);
        let hint = self.hints.get(&peer.node).ok_or(DiscoveryError::Missing)?;
        PeerEndpointHint {
            expires_at: MonoTime(now.0 + 50),
            ..*hint
        }
        .validate(peer, now)
    }
    fn invalidate(&mut self, _: PeerIdentity, _: HintGeneration) -> bool {
        false
    }
    fn close(&mut self) {}
}
struct Cluster {
    now: MonoTime,
    nodes: Vec<Boat>,
    sources: Vec<SourceServer>,
    calls: Vec<Rc<Cell<usize>>>,
}
impl Cluster {
    fn open(root: &std::path::Path, recover: bool) -> Self {
        let mut fixtures = (1..=3)
            .map(|id| {
                make_snapshots_with_timers(
                    id,
                    &root.join(id.to_string()),
                    recover,
                    true,
                    TimerConfig {
                        election_min_ms: 10_000,
                        election_spread_ms: 1_000,
                        ..Default::default()
                    },
                )
            })
            .collect::<Vec<_>>();
        let networks = mesh_parts(&mut fixtures, |_| {
            NativeTransportFactory::new(
                NativeWireCodec::new(Default::default()).unwrap(),
                Default::default(),
            )
            .unwrap()
        });
        let identities = networks
            .iter()
            .map(|p| p.connector.local())
            .collect::<Vec<_>>();
        let hints = networks
            .iter()
            .map(|p| {
                let local = p.connector.local();
                (
                    local.node,
                    PeerEndpointHint {
                        peer: PeerIdentity {
                            node: local.node,
                            store: local.store.identity,
                        },
                        generation: HintGeneration::new(1).unwrap(),
                        endpoint: p.connector.listener_addr().unwrap().unwrap(),
                        expires_at: MonoTime(0),
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        let mut cluster = Self {
            now: MonoTime(0),
            nodes: Vec::new(),
            sources: Vec::new(),
            calls: Vec::new(),
        };
        for (i, (n, p)) in fixtures.into_iter().zip(networks).enumerate() {
            let source = identities[(i + 1) % identities.len()];
            let calls = Rc::new(Cell::new(0));
            let (remote, server) = SourceServer::new(
                identities[i],
                source,
                Source {
                    hints: hints.clone(),
                    calls: calls.clone(),
                },
            );
            cluster.sources.push(server);
            cluster.calls.push(calls);
            cluster.nodes.push(discovered_node(n, p, remote));
        }
        cluster
    }
    fn poll(&mut self, source_online: bool) {
        self.now = MonoTime(self.now.0 + 1);
        let now = self.now;
        if source_online {
            for source in &mut self.sources {
                source.poll(now);
            }
        }
        for n in &mut self.nodes {
            let mut budget = NodePollBudget::default();
            budget.peers.connector.visits = 1;
            budget.replica.steps = 100;
            let progress = n.poll(now, budget).unwrap();
            if let Some(p) = progress.replica {
                for step in p.steps {
                    assert!(step.error.is_none(), "{:?}", step.error);
                }
            }
        }
    }
    #[track_caller]
    fn until(&mut self, source_online: bool, mut done: impl FnMut(&mut Self) -> bool) {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            self.poll(source_online);
            if done(self) {
                break;
            }
            if Instant::now() >= deadline {
                let state = self
                    .nodes
                    .iter()
                    .map(|n| {
                        let app = &n.local().applications[&group(1)];
                        (
                            n.local().owner.core(group(1)).unwrap().role(),
                            n.peers().unwrap().roster().usage(),
                            n.peers().unwrap().connector_usage(),
                            n.peers().unwrap().next_deadline(),
                            app.read_applied(app.applied_index()),
                        )
                    })
                    .collect::<Vec<_>>();
                panic!(
                    "discovered Node stalled at {}, calls {:?}, state {state:?}",
                    self.now.0,
                    self.calls.iter().map(|v| v.get()).collect::<Vec<_>>()
                );
            }
            std::thread::park_timeout(Duration::from_millis(1));
        }
    }
    fn write(
        &mut self,
        operation: u128,
        delta: i64,
        expected: i64,
        source_online: bool,
    ) -> CounterReceipt {
        let ticket = self.nodes[0]
            .propose(ClientRequest {
                group: group(1),
                operation: OperationId::new(operation).unwrap(),
                bytes: delta.to_le_bytes().to_vec(),
            })
            .unwrap();
        let mut completed = None;
        self.until(source_online, |c| {
            if let Some(reply) = c.nodes[0].poll_client() {
                assert_eq!(reply.ticket(), ticket);
                let outcome = c.nodes[0].complete_client(reply).unwrap();
                let ClientOutcome::Applied { receipt, .. } = outcome else {
                    panic!("operation {operation}: {outcome:?}");
                };
                assert_eq!(receipt.operation, OperationId::new(operation).unwrap());
                completed = Some(receipt);
            }
            completed.is_some()
                && c.nodes.iter().all(|n| {
                    let app = &n.local().applications[&group(1)];
                    app.read_applied(app.applied_index()) == Ok(expected)
                })
        });
        completed.unwrap()
    }
    fn close(mut self) {
        for n in &mut self.nodes {
            n.begin_shutdown();
        }
        self.until(false, |c| c.nodes.iter().all(|n| n.is_drained()));
        for source in self.sources {
            source.finish(self.now);
        }
        for n in self.nodes {
            let mut p = n.into_parts().unwrap_or_else(|_| panic!("not drained"));
            let (connector, remote) = p.peers.take().unwrap().connector.into_parts().ok().unwrap();
            let (remote, source_connector) = remote.into_parts().ok().unwrap();
            drop(remote.into_optional_session().ok().unwrap());
            source::finish_connector(source_connector);
            let mut dialer = connector.into_dialer().ok().unwrap();
            let deadline = Instant::now() + Duration::from_secs(5);
            while !dialer.try_finish().unwrap() {
                assert!(Instant::now() < deadline);
                std::thread::park_timeout(Duration::from_millis(1));
            }
            let mut snapshots = p.local.snapshots.take().unwrap();
            loop {
                if snapshots.worker.try_reclaim().unwrap().is_some() {
                    break;
                }
                assert!(Instant::now() < deadline);
                std::thread::park_timeout(Duration::from_millis(1));
            }
            loop {
                if p.local.persistence.try_reclaim().unwrap().is_some() {
                    break;
                }
                assert!(Instant::now() < deadline);
                std::thread::park_timeout(Duration::from_millis(1));
            }
        }
    }
}

#[test]
fn owning_node_drives_discovery_renews_reconnects_and_reopens_original_data() {
    let root =
        std::env::temp_dir().join(format!("voteboat-driven-discovery-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    let mut cluster = Cluster::open(&root, false);
    cluster.nodes[0]
        .control(group(1), NodeControl::Campaign)
        .unwrap();
    cluster.until(true, |c| {
        c.nodes[0].local().owner.core(group(1)).unwrap().role() == Role::Leader
    });
    cluster.write(1, 7, 7, true);
    let before = cluster.calls[0].get();
    let old_binding = cluster.nodes[0]
        .peers()
        .unwrap()
        .roster()
        .binding(node(2))
        .unwrap();
    assert!(before >= 2);
    cluster.now = MonoTime(cluster.now.0 + 100);
    cluster.nodes[0].disconnect(node(2), cluster.now).unwrap();
    cluster.nodes[1].disconnect(node(1), cluster.now).unwrap();
    cluster.until(true, |c| {
        c.calls[0].get() > before
            && c.nodes[0]
                .peers()
                .unwrap()
                .roster()
                .binding(node(2))
                .is_some_and(|b| b.generation > old_binding.generation)
            && c.nodes[1]
                .peers()
                .unwrap()
                .roster()
                .binding(node(1))
                .is_some()
    });
    cluster.nodes[0]
        .control(group(1), NodeControl::Campaign)
        .unwrap();
    cluster.until(true, |c| {
        c.nodes[0].local().owner.core(group(1)).unwrap().role() == Role::Leader
    });
    cluster.write(2, 3, 10, true);
    // Expired hints and offline sources do not stop an established data path.
    cluster.now = MonoTime(cluster.now.0 + 100);
    cluster.write(3, 5, 15, false);
    cluster.write(1, 7, 15, false);
    let calls = cluster.calls[0].get();
    cluster.nodes[0].disconnect(node(2), cluster.now).unwrap();
    cluster.nodes[1].disconnect(node(1), cluster.now).unwrap();
    cluster.until(false, |c| {
        c.nodes[0].peers().unwrap().connector_usage().requests > 0
    });
    assert_eq!(cluster.calls[0].get(), calls);
    // Node shutdown must release the accepted request despite the offline source.
    cluster.close();
    let mut recovered = Cluster::open(&root, true);
    recovered.nodes[0]
        .control(group(1), NodeControl::Campaign)
        .unwrap();
    recovered.until(true, |c| {
        c.nodes[0].local().owner.core(group(1)).unwrap().role() == Role::Leader
    });
    recovered.write(1, 7, 15, true);
    recovered.write(2, 3, 15, false);
    recovered.write(3, 5, 15, false);
    recovered.close();
    std::fs::remove_dir_all(root).unwrap();
}

fn discovered_node(
    n: super::Node,
    mut p: PeerParts<NativePeerConnector, NativeTransportFactory<NativeWireCodec>>,
    remote: Remote,
) -> Boat {
    for route in p.routes.values_mut() {
        if matches!(route, ConnectDirection::Dial(_)) {
            *route = ConnectDirection::Dial("127.0.0.1:1".parse::<SocketAddr>().unwrap());
        }
    }
    let connector = DiscoveryConnector::new_driven(p.connector, remote, MonoTime(0))
        .ok()
        .unwrap();
    NativeNode::from_parts(
        NativeNodeParts {
            local: NativeLocalParts {
                owner: n.owner,
                persistence: n.worker,
                applications: n.apps,
                results: n.results,
                clients: n.clients,
                reads: n.read_requests,
                outbound: n.outbound,
                snapshots: Some(NodeSnapshots {
                    router: n.router.unwrap(),
                    worker: n.snapshots.unwrap(),
                }),
            },
            peers: Some(PeerParts {
                connector,
                roster: p.roster,
                factory: p.factory,
                ingress: p.ingress,
                routes: p.routes,
                admission_routes: p.admission_routes,
            }),
        },
        NodeLimits::default(),
        MonoTime(0),
    )
    .unwrap_or_else(|r| panic!("{:?}", r.reason))
}

#[test]
fn owning_node_repairs_closed_discovery_source_without_stopping_healthy_quorum() {
    let root =
        std::env::temp_dir().join(format!("voteboat-reconnecting-node-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    let mut cluster = Cluster::open(&root, false);
    cluster.nodes[0]
        .control(group(1), NodeControl::Campaign)
        .unwrap();
    cluster.until(true, |c| {
        c.nodes[0].local().owner.core(group(1)).unwrap().role() == Role::Leader
    });
    cluster.write(1, 7, 7, true);
    let old = cluster.nodes[0]
        .peers()
        .unwrap()
        .roster()
        .binding(node(2))
        .unwrap();
    let calls = cluster.calls[0].get();
    cluster.sources[0].disconnect();
    cluster.now = MonoTime(cluster.now.0 + 100);
    cluster.nodes[0].disconnect(node(2), cluster.now).unwrap();
    cluster.nodes[1].disconnect(node(1), cluster.now).unwrap();
    cluster.until(false, |c| {
        c.nodes[0].peers().unwrap().connector_usage().requests > 0
    });
    assert_eq!(cluster.calls[0].get(), calls);
    // The unaffected peer sustains a real committed write while source repair waits.
    let ticket = cluster.nodes[0]
        .propose(ClientRequest {
            group: group(1),
            operation: OperationId::new(2).unwrap(),
            bytes: 3_i64.to_le_bytes().to_vec(),
        })
        .unwrap();
    let mut applied = false;
    cluster.until(false, |c| {
        if let Some(reply) = c.nodes[0].poll_client() {
            assert_eq!(reply.ticket(), ticket);
            assert!(matches!(
                c.nodes[0].complete_client(reply).unwrap(),
                ClientOutcome::Applied { .. }
            ));
            applied = true;
        }
        applied
            && [0, 2].into_iter().all(|i| {
                let app = &c.nodes[i].local().applications[&group(1)];
                app.read_applied(app.applied_index()) == Ok(10)
            })
    });
    cluster.until(true, |c| {
        c.sources[0].accepted_generation() > 1
            && c.calls[0].get() > calls
            && c.nodes[0]
                .peers()
                .unwrap()
                .roster()
                .binding(node(2))
                .is_some_and(|binding| binding.generation > old.generation)
    });
    cluster.write(2, 3, 10, true);
    let receipt = cluster.write(1, 7, 10, true);
    assert!(receipt.duplicate);
    assert_eq!(receipt.outcome, CounterOutcome::Value(7));
    cluster.close();
    let mut recovered = Cluster::open(&root, true);
    recovered.nodes[0]
        .control(group(1), NodeControl::Campaign)
        .unwrap();
    recovered.until(true, |c| {
        c.nodes[0].local().owner.core(group(1)).unwrap().role() == Role::Leader
    });
    let receipt = recovered.write(1, 7, 10, true);
    assert!(receipt.duplicate);
    assert_eq!(receipt.outcome, CounterOutcome::Value(7));
    recovered.write(2, 3, 10, true);
    recovered.write(3, 5, 15, true);
    recovered.close();
    std::fs::remove_dir_all(root).unwrap();
}
