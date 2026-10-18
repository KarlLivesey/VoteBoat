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
    collections::BTreeMap,
    net::SocketAddr,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};
use voteboat::{
    application::StateMachine,
    log::{LogLimits, LogMutation, LogStore},
    native::{
        log_store::{FileLogIo, NativeLogStore},
        snapshot_store::{FileSnapshotIo, NativeSnapshotStore},
        startup::*,
        worker::ThreadWake,
    },
    snapshot::{SnapshotIdentity, SnapshotLimits},
};
pub struct Member {
    pub nodes: Vec<cluster::ChildNode>,
    pub clock: SimClock,
    pub root: PathBuf,
    pub protocol: NativePeerProtocol,
    addresses: [SocketAddr; 4],
    // Owned for the whole test, including the learner's address before it
    // is enrolled.
    leases: [PortLease; 4],
}
impl Member {
    pub fn new(protocol: NativePeerProtocol) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        let root = std::env::temp_dir().join(format!(
            "voteboat-activation-member-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&root).unwrap();
        let leases: [PortLease; 4] = std::array::from_fn(|_| PortLease::acquire(protocol));
        let addresses = leases.each_ref().map(PortLease::address);
        let mut host = Self {
            root,
            protocol,
            addresses,
            leases,
            clock: SimClock::default(),
            nodes: Vec::new(),
        };
        for id in 1..=3 {
            host.create_bootstrap(id);
        }
        for id in 1..=3 {
            let node = host.open(id);
            host.nodes.push(node);
        }
        host.nodes[0]
            .control(setup::mapping().child, NodeControl::Campaign)
            .unwrap();
        host.wait(|h| h.leader().is_some());
        host
    }
    fn create_bootstrap(&self, id: u64) {
        let directory = self.root.join(id.to_string());
        let mut log = NativeLogStore::create(
            FileLogIo::create(&directory).unwrap(),
            support::identity(id.into()),
            LogLimits::default(),
        )
        .unwrap();
        let original = log
            .append_batch(vec![LogMutation::Create(setup::child())])
            .unwrap();
        log.barrier(&original).unwrap();
        NativeSnapshotStore::create(
            FileSnapshotIo::create(directory.join("snapshots")).unwrap(),
            SnapshotIdentity {
                store: support::identity(id.into()),
                group: setup::mapping().child,
            },
            SnapshotLimits::default(),
        )
        .unwrap();
    }
    pub fn config(&self, id: u64) -> NativeMemberStartup {
        let slot = |id: u64| (id - 1) % 3 + 1;
        NativeMemberStartup {
            startup: NativeStartup {
                directory: self.root.join(id.to_string()),
                mode: NativeOpenMode::Recover,
                node: support::node(id),
                store: support::identity(id.into()),
                bootstrap: setup::child(),
                listen: self.addresses[id as usize - 1],
                peers: (1..=4)
                    .filter(|peer| *peer != id)
                    .map(|peer| {
                        (
                            support::node(peer),
                            NativeStartupPeer {
                                address: self.addresses[peer as usize - 1],
                                certificate: setup::certificate(slot(peer)).to_vec(),
                                server_name: format!("node{}.voteboat.test", slot(peer)),
                            },
                        )
                    })
                    .collect(),
                tls: support::tls::configuration(slot(id))
                    .with_wire_version(8)
                    .unwrap(),
                entropy_seed: id * 17,
                limits: NodeLimits::default(),
                ready_visit_items: 16,
            },
            provisioned_stores: (1..=4)
                .map(|id| (support::node(id), support::identity(id.into())))
                .collect::<BTreeMap<_, _>>(),
        }
    }
    pub fn open(&mut self, id: u64) -> cluster::ChildNode {
        self.leases[id as usize - 1].handoff();
        self.config(id)
            .open_with_protocol_and_timers(
                self.protocol,
                NativeTimingProfile::Throughput.timers(),
                fixture::child_application(),
                Arc::new(ThreadWake::current()),
                self.now(),
            )
            .unwrap_or_else(|e| panic!("actual member startup: {:?}", e.reason))
    }
    pub fn enroll(&mut self, image: &voteboat::snapshot::Snapshot) {
        let mut selected = self.config(4);
        selected.startup.mode = NativeOpenMode::Create;
        selected
            .enroll_snapshot(image, &mut fixture::child_application())
            .unwrap();
        let node = self.open(4);
        self.nodes.push(node);
    }
    pub fn now(&self) -> MonoTime {
        self.clock.now()
    }
    pub fn leader(&self) -> Option<usize> {
        self.nodes.iter().position(|n| {
            let c = n.local().owner.core(setup::mapping().child).unwrap();
            c.role() == Role::Leader
                && !c.has_pending_dependency()
                && c.state().term_at(c.state().commit_index) == Some(c.state().hard_state.term)
                && n.local().applications[&setup::mapping().child].applied_index()
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
        for n in &mut self.nodes {
            n.begin_shutdown();
        }
        self.wait(|h| h.nodes.iter().all(|n| n.is_drained()));
        // Rebind each address as soon as its own node has joined: this proves
        // the node released it, while the lease still owns the port. Nodes
        // are opened in id order, the same order as leases.
        for (n, lease) in self.nodes.drain(..).zip(&mut self.leases) {
            cleanup::join(n);
            lease.reclaim();
        }
        std::fs::remove_dir_all(self.root).unwrap();
    }
}
