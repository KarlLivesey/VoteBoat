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
pub(super) type TestNode = NativeNode<ProgramApplication, NativeServiceConnector>;
fn group(id: u128) -> GroupIdentity {
    GroupIdentity {
        id: GroupId::new(id).unwrap(),
        incarnation: GroupIncarnation::new(1).unwrap(),
    }
}
pub(super) fn profile() -> ProgramProfile {
    ProgramProfile::new(
        1,
        vec![group(901), group(902)],
        vec![group(903), group(904), group(905)],
    )
    .unwrap()
}
pub(super) fn spec() -> Program {
    Program::new(
        TransactionId::new(1).unwrap(),
        profile(),
        profile()
            .participants()
            .iter()
            .map(|g| {
                ParticipantProgram::new(
                    *g,
                    vec![
                        KeyAccess {
                            key: KeyId::new(1).unwrap(),
                            expected_version: KeyVersion::new(0),
                            write: Some(90),
                        },
                        KeyAccess {
                            key: KeyId::new(2).unwrap(),
                            expected_version: KeyVersion::new(0),
                            write: None,
                        },
                    ],
                )
                .unwrap()
            })
            .collect(),
    )
    .unwrap()
}

pub(super) struct Host {
    pub(super) nodes: Vec<TestNode>,
    root: PathBuf,
    pub(super) clock: Instant,
}
impl Host {
    pub(super) fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "voteboat-private-program-driver-{}-{}",
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
                bootstrap: boot(profile().coordinators()[0]),
                listen,
                peers: BTreeMap::new(),
                tls,
                entropy_seed: 1,
                limits: NodeLimits::default(),
                ready_visit_items: 16,
                append_window: None,
            },
            additional_groups: profile().coordinators()[1..]
                .iter()
                .chain(profile().participants())
                .copied()
                .map(boot)
                .collect(),
            provisioned_stores: BTreeMap::from([(node, store)]),
        };
        let mut actual = config
            .open(
                NativePeerProtocol::TcpTls,
                NativeTimingProfile::Throughput.timers(),
                applications(),
                Arc::new(ThreadWake::current()),
                MonoTime(0),
            )
            .unwrap();
        for g in profile()
            .coordinators()
            .iter()
            .chain(profile().participants())
            .copied()
        {
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
    pub(super) fn finish(mut self) {
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
    let mut driver = ProgramDriver::new(
        profile(),
        ProgramDriverGeneration::new(1).unwrap(),
        ProgramDriverLimits::new(1).unwrap(),
        &mut host.nodes,
    )
    .unwrap();
    let business = driver.submit(spec()).unwrap();
    let end = Instant::now() + Duration::from_secs(5);
    while driver.nodes[0].local().applications[&spec().coordinator()]
        .status(spec().id())
        .is_none()
    {
        driver
            .advance(
                MonoTime(clock.elapsed().as_millis() as u64),
                ProgramDriverBudget {
                    completions: 0,
                    transitions: 0,
                    ..ProgramDriverBudget::default()
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
        2 => command.step = ProgramStep::Prepare,
        _ => ticket.operation = spec().operation(1).unwrap(),
    }
    let error = driver
        .advance(
            MonoTime(clock.elapsed().as_millis() as u64),
            ProgramDriverBudget {
                node_polls: 0,
                transitions: 0,
                ..ProgramDriverBudget::default()
            },
        )
        .unwrap_err();
    assert_eq!(error, ProgramDriverError::ProviderViolation);
    assert_eq!(
        driver.nodes[0].local().applications[&spec().coordinator()]
            .status(spec().id())
            .unwrap()
            .phase,
        ProgramPhase::Started
    );
    assert!(driver.nodes[0].local().applications[&group(903)]
        .status(spec().id())
        .is_none());
    while !driver.is_drained() {
        let _ = driver.advance(
            MonoTime(clock.elapsed().as_millis() as u64),
            ProgramDriverBudget {
                transitions: 0,
                ..ProgramDriverBudget::default()
            },
        );
        while let Some(output) = driver.poll_completion() {
            assert!(matches!(
                driver.complete(output).unwrap(),
                ProgramDriverOutcome::Unknown {
                    reason: ProgramDriverUnknown::DriverFailed,
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
    let mut driver = ProgramDriver::new(
        profile(),
        ProgramDriverGeneration::new(1).unwrap(),
        ProgramDriverLimits::new(1).unwrap(),
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
        _ => *query = ProgramQuery::Keys(Box::new([KeyId::new(1).unwrap()])),
    }
    let end = Instant::now() + Duration::from_secs(5);
    loop {
        let result = driver.advance(
            MonoTime(clock.elapsed().as_millis() as u64),
            ProgramDriverBudget {
                transitions: 0,
                ..ProgramDriverBudget::default()
            },
        );
        if let Err(error) = result {
            assert_eq!(error, ProgramDriverError::ProviderViolation);
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
            ProgramDriverBudget {
                transitions: 0,
                ..ProgramDriverBudget::default()
            },
        );
        while let Some(output) = driver.poll_completion() {
            assert!(matches!(
                driver.complete(output).unwrap(),
                ProgramDriverOutcome::Unknown {
                    reason: ProgramDriverUnknown::DriverFailed,
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

fn applications() -> BTreeMap<GroupIdentity, ProgramApplication> {
    let limits = ProgramLimits::new(4, 2).unwrap();
    let mut applications = BTreeMap::new();
    for g in profile().coordinators() {
        applications.insert(
            *g,
            ProgramApplication::coordinator(*g, profile(), limits).unwrap(),
        );
    }
    for g in profile().participants() {
        applications.insert(
            *g,
            ProgramApplication::participant(
                *g,
                profile(),
                &[(KeyId::new(1).unwrap(), 100), (KeyId::new(2).unwrap(), 100)],
                limits,
            )
            .unwrap(),
        );
    }
    applications
}
