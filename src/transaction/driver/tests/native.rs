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
use crate::{
    application::StateMachine,
    log::Bootstrap,
    native::{
        connect::{NativePeerProtocol, NativeServiceConnector},
        node::NativeNode,
        startup::*,
        tls::{NativeTlsConfig, TlsCredentials},
        worker::ThreadWake,
    },
    quorum::{Limits, Policy, Tree},
};
use std::{
    net::TcpListener,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
static NEXT: AtomicU64 = AtomicU64::new(1);
type TestNode = NativeNode<TransferApplication, NativeServiceConnector>;
fn group(id: u128) -> GroupIdentity {
    GroupIdentity {
        id: GroupId::new(id).unwrap(),
        incarnation: GroupIncarnation::new(1).unwrap(),
    }
}
fn profile() -> TransferProfile {
    TransferProfile::new(group(901), [group(902), group(903)]).unwrap()
}
fn spec() -> TransferSpec {
    TransferSpec::new(
        TransactionId::new(1).unwrap(),
        profile(),
        AccountRef::new(group(902), AccountId::new(1).unwrap()),
        AccountRef::new(group(903), AccountId::new(1).unwrap()),
        10,
        PhaseIds::new(
            [1, 2, 3, 4].map(|i| OperationId::new(i).unwrap()),
            [[5, 6, 7], [8, 9, 10]].map(|row| row.map(|i| OperationId::new(i).unwrap())),
        )
        .unwrap(),
    )
    .unwrap()
}
struct Host {
    nodes: Vec<TestNode>,
    root: PathBuf,
    clock: Instant,
}
impl Host {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "voteboat-private-driver-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&root).unwrap();
        let node = NodeId::new(1).unwrap();
        let store = StoreIdentity {
            id: StoreId::new(1).unwrap(),
            incarnation: StoreIncarnation::new(1).unwrap(),
        };
        let boot = |g| Bootstrap {
            group: g,
            configuration: ConfigurationId::new(1).unwrap(),
            policy: Policy::new(Tree::Voter(node), Limits::default()).unwrap(),
            voter_stores: BTreeMap::from([(node, store)]),
        };
        let tls = NativeTlsConfig::new(TlsCredentials {
            roots: vec![include_bytes!("../../../../tests/fixtures/tls/ca.der").to_vec()],
            certificate_chain: vec![
                include_bytes!("../../../../tests/fixtures/tls/node1.der").to_vec()
            ],
            private_key: include_bytes!("../../../../tests/fixtures/tls/node1-key.der").to_vec(),
        })
        .unwrap()
        .with_wire_version(8)
        .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let listen = listener.local_addr().unwrap();
        drop(listener);
        let config = NativeMultiStartup {
            startup: NativeStartup {
                directory: root.clone(),
                mode: NativeOpenMode::Create,
                node,
                store,
                bootstrap: boot(profile().coordinator()),
                listen,
                peers: BTreeMap::new(),
                tls,
                entropy_seed: 1,
                limits: NodeLimits::default(),
                ready_visit_items: 16,
                append_window: None,
            },
            additional_groups: profile().participants().map(boot).to_vec(),
            provisioned_stores: BTreeMap::from([(node, store)]),
        };
        let limits = TransferLimits::new(4, 1).unwrap();
        let mut applications = BTreeMap::from([(
            profile().coordinator(),
            TransferApplication::coordinator(profile().coordinator(), profile(), limits).unwrap(),
        )]);
        for g in profile().participants() {
            applications.insert(
                g,
                TransferApplication::participant(
                    g,
                    profile(),
                    &[(AccountId::new(1).unwrap(), 100)],
                    limits,
                )
                .unwrap(),
            );
        }
        let mut actual = config
            .open(
                NativePeerProtocol::TcpTls,
                NativeTimingProfile::Throughput.timers(),
                applications,
                Arc::new(ThreadWake::current()),
                MonoTime(0),
            )
            .unwrap();
        for g in [profile().coordinator(), group(902), group(903)] {
            actual.control(g, NodeControl::Campaign).unwrap();
        }
        let mut host = Self {
            nodes: vec![actual],
            root,
            clock: Instant::now(),
        };
        let end = Instant::now() + Duration::from_secs(5);
        while !host.nodes[0]
            .local()
            .applications
            .values()
            .all(|a| a.applied_index() > 0)
        {
            host.poll();
            assert!(Instant::now() < end);
        }
        host
    }
    fn poll(&mut self) {
        self.nodes[0]
            .poll(
                MonoTime(self.clock.elapsed().as_millis() as u64),
                NodePollBudget::default(),
            )
            .unwrap();
        std::thread::park_timeout(Duration::from_millis(1));
    }
    fn finish(mut self) {
        self.nodes[0].begin_shutdown();
        let end = Instant::now() + Duration::from_secs(5);
        while !self.nodes[0].is_drained() {
            self.poll();
            assert!(Instant::now() < end);
        }
        let node = self.nodes.pop().unwrap();
        let mut parts = node
            .into_parts()
            .unwrap_or_else(|_| panic!("Node not drained"));
        let mut dialer = parts
            .peers
            .take()
            .unwrap()
            .connector
            .into_dialer()
            .unwrap_or_else(|_| panic!("connector not drained"));
        let mut snapshots = parts.local.snapshots.take().unwrap();
        let (mut wal, mut snapshot) = (false, false);
        loop {
            let dial = dialer.as_mut().is_none_or(|d| d.try_finish().unwrap());
            if !wal {
                wal = parts.local.persistence.try_reclaim().unwrap().is_some();
            }
            if !snapshot {
                snapshot = snapshots.worker.try_reclaim().unwrap().is_some();
            }
            if dial && wal && snapshot {
                break;
            }
            assert!(Instant::now() < end);
            std::thread::park_timeout(Duration::from_millis(1));
        }
        std::fs::remove_dir_all(self.root).unwrap();
    }
}
fn check_client_corruption(which: usize) {
    let mut host = Host::new();
    let clock = host.clock;
    let mut driver = TransferDriver::new(
        profile(),
        TransferDriverGeneration::new(1).unwrap(),
        TransferDriverLimits::new(1).unwrap(),
        &mut host.nodes,
    )
    .unwrap();
    let business = driver.submit(spec()).unwrap();
    let end = Instant::now() + Duration::from_secs(5);
    while driver.nodes[0].local().applications[&profile().coordinator()]
        .status(spec().id())
        .is_none()
    {
        driver
            .advance(
                MonoTime(clock.elapsed().as_millis() as u64),
                DriverBudget {
                    completions: 0,
                    transitions: 0,
                    ..DriverBudget::default()
                },
            )
            .unwrap();
        assert!(Instant::now() < end);
        std::thread::park_timeout(Duration::from_millis(1));
    }
    let work = driver.work.get_mut(&business.sequence).unwrap();
    let Some(Attempt::Client {
        node,
        ticket,
        command,
        ..
    }) = &mut work.attempt
    else {
        panic!("original client attempt");
    };
    match which {
        0 => *node = 1,
        1 => ticket.sequence += 1,
        2 => command.step = TransferStep::Prepare,
        _ => ticket.operation = spec().phases().coordinator()[1],
    }
    let error = driver
        .advance(
            MonoTime(clock.elapsed().as_millis() as u64),
            DriverBudget {
                node_polls: 0,
                transitions: 0,
                ..DriverBudget::default()
            },
        )
        .unwrap_err();
    assert_eq!(error, DriverError::ProviderViolation);
    assert_eq!(
        driver.nodes[0].local().applications[&profile().coordinator()]
            .status(spec().id())
            .unwrap()
            .phase,
        TransferPhase::Started
    );
    assert!(driver.nodes[0].local().applications[&group(902)]
        .status(spec().id())
        .is_none());
    while !driver.is_drained() {
        let _ = driver.advance(
            MonoTime(clock.elapsed().as_millis() as u64),
            DriverBudget {
                transitions: 0,
                ..DriverBudget::default()
            },
        );
        while let Some(output) = driver.poll_completion() {
            assert!(matches!(
                driver.complete(output).unwrap(),
                DriverOutcome::Unknown {
                    reason: DriverUnknown::DriverFailed,
                    ..
                }
            ));
        }
        assert!(Instant::now() < end);
    }
    drop(driver);
    host.finish();
}
#[test]
fn real_applied_output_rejects_wrong_retained_node() {
    check_client_corruption(0);
}
#[test]
fn real_applied_output_rejects_wrong_retained_ticket() {
    check_client_corruption(1);
}
#[test]
fn real_applied_output_rejects_wrong_original_command() {
    check_client_corruption(2);
}
fn check_read_corruption(which: usize) {
    let mut host = Host::new();
    let clock = host.clock;
    let mut driver = TransferDriver::new(
        profile(),
        TransferDriverGeneration::new(1).unwrap(),
        TransferDriverLimits::new(1).unwrap(),
        &mut host.nodes,
    )
    .unwrap();
    let business = driver.resume(spec().id()).unwrap();
    let work = driver.work.get_mut(&business.sequence).unwrap();
    let Some(Attempt::Read {
        node,
        ticket,
        query,
    }) = &mut work.attempt
    else {
        panic!("original read attempt");
    };
    match which {
        0 => *node = 1,
        1 => ticket.sequence += 1,
        _ => *query = TransferQuery::Account(AccountId::new(1).unwrap()),
    }
    let end = Instant::now() + Duration::from_secs(5);
    loop {
        let result = driver.advance(
            MonoTime(clock.elapsed().as_millis() as u64),
            DriverBudget {
                transitions: 0,
                ..DriverBudget::default()
            },
        );
        if let Err(error) = result {
            assert_eq!(error, DriverError::ProviderViolation);
            break;
        }
        assert!(Instant::now() < end);
        std::thread::park_timeout(Duration::from_millis(1));
    }
    assert!(driver.nodes[0]
        .local()
        .applications
        .values()
        .all(|a| a.status(spec().id()).is_none()));
    while !driver.is_drained() {
        let _ = driver.advance(
            MonoTime(clock.elapsed().as_millis() as u64),
            DriverBudget {
                transitions: 0,
                ..DriverBudget::default()
            },
        );
        while let Some(output) = driver.poll_completion() {
            assert!(matches!(
                driver.complete(output).unwrap(),
                DriverOutcome::Unknown {
                    reason: DriverUnknown::DriverFailed,
                    ..
                }
            ));
        }
        assert!(Instant::now() < end);
    }
    drop(driver);
    host.finish();
}
#[test]
fn real_quorum_output_rejects_wrong_retained_node() {
    check_read_corruption(0);
}
#[test]
fn real_quorum_output_rejects_wrong_retained_ticket() {
    check_read_corruption(1);
}
#[test]
fn real_quorum_output_rejects_wrong_original_query() {
    check_read_corruption(2);
}

#[test]
fn real_applied_output_rejects_wrong_retained_operation() {
    check_client_corruption(3);
}
