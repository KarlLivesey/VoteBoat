// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use crate::{counter_application::Application, setup};
use std::{path::PathBuf, sync::Arc};
use voteboat::{
    application::*,
    native::{connect::NativePeerProtocol, worker::ThreadWake},
};

struct History {
    root: PathBuf,
    nodes: Vec<Service>,
    start: Instant,
    paused: Vec<usize>,
}
impl History {
    fn new(protocol: NativePeerProtocol) -> Self {
        let reservation = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = reservation.local_addr().unwrap().port();
        let root =
            std::env::temp_dir().join(format!("voteboat-terminal-{}-{port}", std::process::id()));
        std::fs::create_dir(&root).unwrap();
        let tls = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/tls");
        let peers = root.join("peers");
        let sockets = (1..=3)
            .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
            .collect::<Vec<_>>();
        std::fs::write(
            &peers,
            sockets
                .iter()
                .enumerate()
                .map(|(i, s)| {
                    format!(
                        "{} {} node{}.voteboat.test\n",
                        i + 1,
                        s.local_addr().unwrap(),
                        i + 1
                    )
                })
                .collect::<String>(),
        )
        .unwrap();
        drop(sockets);
        let nodes = (1..=3)
            .map(|id| {
                let mut c = setup::configuration(
                    &root.join(id.to_string()),
                    id,
                    1000,
                    &tls,
                    true,
                    setup::PeerInput::Legacy(Some(&peers)),
                )
                .unwrap();
                c.startup.tls = c.startup.tls.with_wire_version(8).unwrap();
                let app = Application::new(setup::application().unwrap(), true).unwrap();
                c.startup
                    .open_with_protocol(protocol, app, Arc::new(ThreadWake::current()), MonoTime(0))
                    .unwrap()
            })
            .collect();
        Self {
            root,
            nodes,
            start: Instant::now(),
            paused: Vec::new(),
        }
    }
    fn poll(&mut self) {
        let now = MonoTime(self.start.elapsed().as_millis() as u64);
        for (i, node) in self.nodes.iter_mut().enumerate() {
            if !self.paused.contains(&i) {
                node.poll(now, NodePollBudget::default()).unwrap();
            }
        }
        std::thread::park_timeout(Duration::from_millis(1));
    }
    fn until(&mut self, mut ready: impl FnMut(&Self) -> bool) {
        let end = Instant::now() + Duration::from_secs(10);
        while !ready(self) {
            assert!(Instant::now() < end, "native terminal history deadline");
            self.poll();
        }
    }
    fn leader(&mut self) -> usize {
        self.until(|h| {
            h.nodes
                .iter()
                .any(|n| n.local().owner.core(setup::group()).unwrap().role() == Role::Leader)
        });
        self.nodes
            .iter()
            .position(|n| n.local().owner.core(setup::group()).unwrap().role() == Role::Leader)
            .unwrap()
    }
    fn record(&self, id: usize) -> Option<LeadershipRecord> {
        application(&self.nodes[id])
            .unwrap()
            .record(OperationId::new(19701).unwrap())
    }
    fn last_index(&self, id: usize) -> u64 {
        self.nodes[id]
            .local()
            .owner
            .core(setup::group())
            .unwrap()
            .state()
            .last_index()
    }
    fn transfer(&self, id: usize) -> Option<LeadershipTransfer> {
        self.nodes[id]
            .local()
            .owner
            .core(setup::group())
            .unwrap()
            .leadership_transfer()
    }
    fn submit(&mut self, id: usize, command: LeadershipCommand) -> ClientTicket {
        propose(&mut self.nodes[id], command).unwrap()
    }
    fn completion(&mut self, id: usize, ticket: ClientTicket) -> ClientOutcome<Receipt> {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            self.poll();
            while let Some(output) = self.nodes[id].poll_client() {
                let found = output.ticket() == ticket;
                let outcome = self.nodes[id].complete_client(output).unwrap();
                if found {
                    return outcome;
                }
            }
            assert!(Instant::now() < deadline, "missing original completion");
        }
    }
    fn unrelated(
        &mut self,
        id: usize,
        driver: &mut Driver,
        mut request: LeadershipTransferRequest,
    ) {
        request.operation = OperationId::new(19704).unwrap();
        self.nodes[id]
            .control(setup::group(), NodeControl::TransferLeadership(request))
            .unwrap();
        self.until(|h| h.transfer(id).is_some());
        let before = self.transfer(id).unwrap();
        driver.tick(&mut self.nodes[id], false).unwrap();
        Driver::new(setup::group())
            .tick(&mut self.nodes[id], false)
            .unwrap();
        self.poll();
        assert_eq!(self.transfer(id), Some(before));
    }
    fn finish(mut self) {
        self.paused.clear();
        for n in &mut self.nodes {
            n.begin_shutdown();
        }
        self.until(|h| h.nodes.iter().all(Service::is_drained));
        for n in self.nodes {
            setup::join(n).unwrap();
        }
        std::fs::remove_dir_all(self.root).unwrap();
    }
}

fn terminal(protocol: NativePeerProtocol) {
    let mut h = History::new(protocol);
    let leader = h.leader();
    let target = (leader + 1) % 3;
    let core = h.nodes[leader].local().owner.core(setup::group()).unwrap();
    let intent = LeadershipIntent {
        source: PeerIdentity {
            node: core.local_node(),
            store: core.storage_binding().identity,
        },
        request: LeadershipTransferRequest {
            operation: OperationId::new(19701).unwrap(),
            configuration: core.membership().id(),
            target: PeerIdentity {
                node: NodeId::new((target + 1) as u64).unwrap(),
                store: StoreIdentity {
                    id: StoreId::new((target + 1) as u128).unwrap(),
                    incarnation: StoreIncarnation::new(1).unwrap(),
                },
            },
        },
    };
    let begun = h.submit(leader, LeadershipCommand::Begin(intent));
    assert!(matches!(
        h.completion(leader, begun),
        ClientOutcome::Applied { .. }
    ));
    h.until(|h| h.record(leader).is_some());
    let original = h.record(leader).unwrap();
    assert_eq!(original.phase, LeadershipPhase::Pending);
    // The transfer must start with the Cancel appended but not committed.
    // Both followers wait until the leader has appended it, because the
    // shard's control and data turns otherwise decide whether the transfer
    // (control) or the Cancel (data) reaches the core first; then only the
    // target stays paused and the other follower commits the Cancel.
    let other = 3 - leader - target;
    h.paused = vec![target, other];
    let tip = h.last_index(leader);
    let cancelled = h.submit(
        leader,
        LeadershipCommand::Cancel {
            intent,
            index: original.index,
        },
    );
    assert_eq!(h.record(leader), Some(original));
    h.until(|h| h.last_index(leader) > tip);
    let mut driver = Driver::new(setup::group());
    driver.tick(&mut h.nodes[leader], false).unwrap();
    h.until(|h| h.transfer(leader).is_some());
    assert_eq!(h.record(leader), Some(original));
    h.paused = vec![target];
    h.until(|h| {
        h.record(leader)
            .is_some_and(|r| matches!(r.phase, LeadershipPhase::Cancelled { .. }))
    });
    let terminal = h.record(leader).unwrap();
    assert!(matches!(
        h.completion(leader, cancelled),
        ClientOutcome::Applied { .. }
    ));
    assert!(h.transfer(leader).is_some());
    driver.tick(&mut h.nodes[leader], false).unwrap();
    let cleanup = Instant::now() + Duration::from_millis(250);
    while h.transfer(leader).is_some() && Instant::now() < cleanup {
        h.poll();
    }
    let released = h.transfer(leader).is_none();
    let data = h.nodes[leader].local().applications[&setup::group()]
        .data(2)
        .unwrap();
    let result = h.nodes[leader].propose(ClientRequest {
        group: setup::group(),
        operation: OperationId::new(19703).unwrap(),
        bytes: data,
    });
    let outcome = result.ok().map(|ticket| h.completion(leader, ticket));
    if released {
        h.unrelated(leader, &mut driver, intent.request);
    }
    assert_eq!(h.record(leader), Some(terminal));
    driver.tick(&mut h.nodes[leader], true).unwrap();
    h.finish();
    assert!(released, "terminal intent left its owned transfer active");
    assert!(
        matches!(
            outcome,
            Some(ClientOutcome::Applied {
                receipt: Receipt::Data(CounterReceipt {
                    outcome: CounterOutcome::Value(2),
                    duplicate: false,
                    ..
                }),
                ..
            })
        ),
        "{outcome:?}"
    );
}

#[test]
fn terminal_intent_releases_owned_transfer_tcp() {
    terminal(NativePeerProtocol::TcpTls);
}
#[cfg(feature = "quic")]
#[test]
fn terminal_intent_releases_owned_transfer_quic() {
    terminal(NativePeerProtocol::Quic);
}
