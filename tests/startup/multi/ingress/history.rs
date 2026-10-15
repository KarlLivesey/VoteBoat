// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
impl ScopedHistory {
    pub(super) fn new(protocol: NativePeerProtocol) -> Self {
        let root = root();
        std::fs::create_dir(&root).unwrap();
        let sockets = (0..3)
            .map(|_| {
                let tcp = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
                let udp = std::net::UdpSocket::bind(tcp.local_addr().unwrap()).unwrap();
                (tcp, udp)
            })
            .collect::<Vec<_>>();
        let addresses = sockets
            .iter()
            .map(|(s, _)| s.local_addr().unwrap())
            .collect();
        drop(sockets);
        let mut history = Self {
            root,
            addresses,
            nodes: vec![],
            clock: Instant::now(),
            protocol,
            probe: Rc::default(),
        };
        history.open(NativeOpenMode::Create);
        history
    }
    pub(super) fn open(&mut self, mode: NativeOpenMode) {
        self.assert_idle();
        self.clock = Instant::now();
        self.nodes = (1..=3)
            .map(|n| {
                let mut config = selected(&self.root, &self.addresses, n, mode);
                config.startup.ready_visit_items = 64;
                let applications = groups()
                    .into_iter()
                    .map(|b| {
                        (
                            b.group,
                            PreparedHost {
                                inner: app(),
                                probe: Rc::clone(&self.probe),
                            },
                        )
                    })
                    .collect();
                config
                    .open(
                        self.protocol,
                        timers(),
                        applications,
                        Arc::new(ThreadWake::current()),
                        MonoTime(0),
                    )
                    .unwrap()
            })
            .collect();
    }
    pub(super) fn assert_idle(&self) {
        assert_eq!(self.probe.live.get(), 0);
        assert_eq!(self.probe.factories.get(), self.probe.drops.get());
    }
    pub(super) fn poll(&mut self) {
        self.assert_idle();
        let now = MonoTime(self.clock.elapsed().as_millis() as u64);
        for node in &mut self.nodes {
            node.poll(now, NodePollBudget::default()).unwrap();
        }
        std::thread::park_timeout(Duration::from_millis(1));
    }
    pub(super) fn wait(&mut self, predicate: impl Fn(&Self) -> bool) {
        let deadline = Instant::now() + Duration::from_secs(15);
        while !predicate(self) {
            self.poll();
            assert!(
                Instant::now() < deadline,
                "scoped native timeout: {}",
                self.root.display()
            );
        }
    }
    pub(super) fn campaign(&mut self) {
        for b in groups() {
            self.nodes[0]
                .control(b.group, NodeControl::Campaign)
                .unwrap();
        }
        self.wait(|h| {
            groups().iter().all(|b| {
                let core = h.nodes[0].local().owner.core(b.group).unwrap();
                core.role() == Role::Leader
                    && core.state().term_at(core.state().commit_index)
                        == Some(core.state().hard_state.term)
                    && h.nodes.iter().all(|node| {
                        node.local().applications[&b.group].applied_index()
                            == core.state().commit_index
                    })
            })
        });
    }
    pub(super) fn collect(&mut self, expected: &[Expected]) {
        let deadline = Instant::now() + Duration::from_secs(15);
        let mut seen = std::collections::BTreeSet::new();
        while seen.len() < expected.len() {
            self.poll();
            while let Some(completion) = self.nodes[0].poll_client() {
                let ticket = completion.ticket();
                assert!(seen.insert(ticket.sequence));
                let required = expected.iter().find(|item| item.ticket == ticket).unwrap();
                let ClientOutcome::Applied { position, receipt } =
                    self.nodes[0].complete_client(completion).unwrap()
                else {
                    panic!("scoped original not applied")
                };
                let state = self.nodes[0]
                    .local()
                    .owner
                    .core(ticket.group)
                    .unwrap()
                    .state();
                assert!(state.commit_index >= position.index);
                let entry = state.entry_at(position.index).unwrap();
                assert_eq!(entry.term, position.term);
                assert!(
                    matches!(entry.payload, EntryPayload::Command { operation, .. } if operation == ticket.operation)
                );
                assert_eq!(receipt.operation, ticket.operation);
                assert_eq!(receipt.index, position.index);
                assert_eq!(receipt.outcome, required.outcome);
                assert_eq!(receipt.duplicate, required.duplicate);
            }
            assert!(Instant::now() < deadline);
        }
        assert!(self.nodes[0].poll_client().is_none());
        self.wait(|h| {
            groups().iter().all(|b| {
                let committed = h.nodes[0]
                    .local()
                    .owner
                    .core(b.group)
                    .unwrap()
                    .state()
                    .commit_index;
                h.nodes
                    .iter()
                    .all(|n| n.local().applications[&b.group].applied_index() >= committed)
            })
        });
    }
    pub(super) fn readback(&self) {
        for node in &self.nodes {
            for (bootstrap, expected) in groups().into_iter().zip([15, 4, 6]) {
                let application = &node.local().applications[&bootstrap.group];
                assert_eq!(
                    application.read_at(application.applied_index(), ()),
                    Ok(expected)
                );
            }
        }
    }
    pub(super) fn checkpoint(&mut self) {
        for node in &mut self.nodes {
            for b in groups() {
                node.control(b.group, NodeControl::Checkpoint).unwrap();
            }
        }
        self.wait(|h| {
            h.nodes.iter().all(|n| {
                groups()
                    .iter()
                    .all(|b| n.local().owner.core(b.group).unwrap().state().base_index() > 0)
                    && n.local().snapshots.as_ref().unwrap().router.is_drained()
            })
        });
    }
    pub(super) fn close(&mut self) {
        self.assert_idle();
        drain_wire_nodes(std::mem::take(&mut self.nodes), self.clock);
        self.assert_idle();
    }
}
