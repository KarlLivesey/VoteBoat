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
//! Genuine bootstrap files and member Nodes; no configuration or proof seeding.
use super::*;
use harness::{FixtureClock, Observed, PortLease, SimClock};
use std::{
    net::SocketAddr,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};
use voteboat::{
    application::StateMachine,
    log::{LogMutation, LogStore},
    native::{
        log_store::{FileLogIo, NativeLogStore},
        snapshot_store::{FileSnapshotIo, NativeSnapshotStore},
        startup::*,
        worker::ThreadWake,
    },
    snapshot::{SnapshotIdentity, SnapshotLimits},
};
pub struct Members {
    pub slot: usize,
    pub nodes: Vec<cluster::ChildNode>,
    pub clock: SimClock,
    pub root: PathBuf,
    pub protocol: NativePeerProtocol,
    addresses: Vec<SocketAddr>,
    // Owned for the whole test, including the enrolled learner's address
    // before enrollment.
    leases: Vec<PortLease>,
}
impl Members {
    pub fn new(protocol: NativePeerProtocol, slot: usize, clock: &SimClock) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        let root = std::env::temp_dir().join(format!(
            "voteboat-activation-member-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&root).unwrap();
        let leases = (0..groups::ids(slot).len())
            .map(|_| PortLease::acquire(protocol))
            .collect::<Vec<_>>();
        let addresses = leases.iter().map(PortLease::address).collect();
        let mut host = Self {
            root,
            slot,
            protocol,
            addresses,
            leases,
            clock: clock.clone(),
            nodes: Vec::new(),
        };
        for id in groups::ids(slot).into_iter().take(3) {
            host.create_bootstrap(id);
        }
        for id in groups::ids(slot).into_iter().take(3) {
            let node = host.open(id);
            host.nodes.push(node);
        }
        host.nodes[0]
            .control(groups::mapping(slot).child, NodeControl::Campaign)
            .unwrap();
        host.wait(|h| h.leader().is_some());
        host
    }
    fn create_bootstrap(&self, id: u64) {
        let directory = self.root.join(id.to_string());
        let mut log = NativeLogStore::create(
            FileLogIo::create(&directory).unwrap(),
            support::identity(id.into()),
            groups::physical_log_limits(),
        )
        .unwrap();
        let original = log
            .append_batch(vec![LogMutation::Create(groups::child(self.slot))])
            .unwrap();
        log.barrier(&original).unwrap();
        NativeSnapshotStore::create(
            FileSnapshotIo::create(directory.join("snapshots")).unwrap(),
            SnapshotIdentity {
                store: support::identity(id.into()),
                group: groups::mapping(self.slot).child,
            },
            SnapshotLimits::default(),
        )
        .unwrap();
    }
    pub fn config(&self, id: u64) -> NativeMemberStartup {
        NativeMemberStartup {
            startup: NativeStartup {
                directory: self.root.join(id.to_string()),
                mode: NativeOpenMode::Recover,
                node: support::node(id),
                store: support::identity(id.into()),
                bootstrap: groups::child(self.slot),
                listen: self.addresses[groups::ids(self.slot)
                    .iter()
                    .position(|n| *n == id)
                    .unwrap()],
                peers: groups::ids(self.slot)
                    .into_iter()
                    .filter(|peer| *peer != id)
                    .map(|peer| {
                        (
                            support::node(peer),
                            NativeStartupPeer {
                                address: self.addresses[groups::ids(self.slot)
                                    .iter()
                                    .position(|n| *n == peer)
                                    .unwrap()],
                                certificate: setup::certificate(groups::certificate_slot(peer))
                                    .to_vec(),
                                server_name: format!(
                                    "node{}.voteboat.test",
                                    groups::certificate_slot(peer)
                                ),
                            },
                        )
                    })
                    .collect(),
                tls: support::tls::configuration(groups::certificate_slot(id))
                    .with_wire_version(8)
                    .unwrap(),
                entropy_seed: id * 17,
                limits: NodeLimits::default(),
                ready_visit_items: 16,
            },
            provisioned_stores: groups::stores(self.slot),
        }
    }
    fn position(&self, id: u64) -> usize {
        groups::ids(self.slot)
            .iter()
            .position(|n| *n == id)
            .unwrap()
    }
    pub fn open(&mut self, id: u64) -> cluster::ChildNode {
        let slot = self.position(id);
        self.leases[slot].handoff();
        let selected = self.config(id);
        let limits = selected.startup.limits;
        let now = self.now();
        let mut parts = selected
            .prepare_for_discovery_with_log_limits(
                self.protocol,
                NativeTimingProfile::Throughput.timers(),
                groups::physical_log_limits(),
                groups::application(self.slot),
                Arc::new(ThreadWake::current()),
                now,
            )
            .unwrap_or_else(|e| rejection::prepared(e));
        let factory = match voteboat::native::transport::NativeTransportFactory::new(
            groups::wire_codec(),
            voteboat::transport::TransportLimits::default(),
        ) {
            Ok(factory) => factory,
            Err(reason) => rejection::parts(parts, reason, now),
        };
        assert!(parts.local.clients.is_drained());
        let clients =
            match ClientRouter::new(parts.local.clients.binding(), groups::client_limits()) {
                Ok(clients) => clients,
                Err(reason) => rejection::parts(parts, reason, now),
            };
        parts.local.clients = clients;
        parts.peers.as_mut().unwrap().factory = factory;
        Node::from_parts(parts, limits, now)
            .unwrap_or_else(|e| rejection::parts(*e.parts, e.reason, now))
    }
    pub fn enroll(&mut self, image: &voteboat::snapshot::Snapshot) {
        let mut selected = self.config(4);
        selected.startup.mode = NativeOpenMode::Create;
        selected
            .enroll_snapshot_with_log_limits(
                image,
                &mut groups::application(self.slot),
                groups::physical_log_limits(),
            )
            .unwrap();
        let node = self.open(4);
        self.nodes.push(node);
    }
    pub fn now(&self) -> MonoTime {
        self.clock.now()
    }
    pub fn leader(&self) -> Option<usize> {
        self.nodes.iter().position(|n| {
            let c = n
                .local()
                .owner
                .core(groups::mapping(self.slot).child)
                .unwrap();
            c.role() == Role::Leader
                && !c.has_pending_dependency()
                && c.state().term_at(c.state().commit_index) == Some(c.state().hard_state.term)
                && n.local().applications[&groups::mapping(self.slot).child].applied_index()
                    >= c.state().commit_index
        })
    }
    pub fn poll(&mut self) {
        let now = self.clock.round(Observed::nodes(&self.nodes));
        for n in &mut self.nodes {
            n.poll(now, NodePollBudget::default()).unwrap();
        }
        std::thread::park_timeout(Duration::from_millis(1));
    }
    pub fn wait(&mut self, done: impl Fn(&Self) -> bool) {
        let end = harness::Wait::new(&self.clock, harness::LIVENESS_MS);
        while !done(self) {
            self.poll();
            assert!(
                end.within(&self.clock),
                "actual member progress: {}",
                self.root.display()
            );
        }
    }
    pub fn finish(mut self) {
        // The live retained set at the end of the workflow (consumed records
        // are retired since LV2), within the lifetime preflight budget.
        let retained = retained::group(&self.nodes, self.slot);
        eprintln!(
            "336 retained group={} protocol_time={:?} {retained:?}",
            groups::mapping(self.slot).child.id.get(),
            self.clock.now(),
        );
        assert!(
            retained.protocol <= retained::PROTOCOL_ACTIONS,
            "group {} retained {} protocol actions; the lifetime preflight budgets {}",
            groups::mapping(self.slot).child.id.get(),
            retained.protocol,
            retained::PROTOCOL_ACTIONS
        );
        for n in &mut self.nodes {
            n.begin_shutdown();
        }
        self.wait(|h| h.nodes.iter().all(|n| n.is_drained()));
        // Rebind each address as soon as its own node has joined: this proves
        // the node released it, while the lease still owns the port.
        // Nodes are opened in `groups::ids` order, the same order as leases.
        for (n, lease) in self.nodes.drain(..).zip(&mut self.leases) {
            super::super::cleanup::join(n);
            lease.reclaim();
        }
        std::fs::remove_dir_all(self.root).unwrap();
    }
}
