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
#![cfg(feature = "tls")]
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};
use voteboat::{
    application::*,
    identity::*,
    log::*,
    native::{node::*, startup::*, tls::*, worker::*},
    quorum::*,
    runtime::*,
};
#[derive(Clone)]
struct HostApplication(Counter);
impl StateMachine for HostApplication {
    type Receipt = CounterReceipt;
    fn applied_index(&self) -> u64 {
        self.0.applied_index()
    }
    fn apply_batch(&mut self, e: &[LogEntry]) -> Result<Vec<CounterReceipt>, ApplicationError> {
        self.0.apply_batch(e)
    }
}
impl BoundedStateMachine for HostApplication {
    fn receipt_bytes_bound(&self, e: &[LogEntry]) -> Result<usize, ApplicationError> {
        self.0.receipt_bytes_bound(e)
    }
}
impl ProposalAdmission for HostApplication {
    fn validate_proposal<'a>(
        &self,
        o: OperationId,
        b: &[u8],
        p: impl Iterator<Item = (OperationId, &'a [u8])>,
    ) -> Result<usize, ApplicationError> {
        self.0.validate_proposal(o, b, p)
    }
}
impl ReadableStateMachine for HostApplication {
    type Query = ();
    type ReadResult = i64;
    fn read_at(&self, i: u64, q: ()) -> Result<i64, ApplicationError> {
        self.0.read_at(i, q)
    }
}
impl BoundedReadableStateMachine for HostApplication {
    fn query_bytes(&self, q: &(), l: usize) -> Result<usize, ApplicationError> {
        self.0.query_bytes(q, l)
    }
    fn read_result_bound(&self, q: &()) -> Result<usize, ApplicationError> {
        self.0.read_result_bound(q)
    }
    fn read_result_bytes(&self, r: &i64, l: usize) -> Result<usize, ApplicationError> {
        self.0.read_result_bytes(r, l)
    }
}
impl CheckpointStateMachine for HostApplication {
    fn schema_version(&self) -> u64 {
        self.0.schema_version()
    }
    fn checkpoint(&self, l: usize) -> Result<Vec<u8>, ApplicationError> {
        self.0.checkpoint(l)
    }
    fn restore_checkpoint(&mut self, s: u64, i: u64, b: &[u8]) -> Result<(), ApplicationError> {
        self.0.restore_checkpoint(s, i, b)
    }
}
fn app() -> HostApplication {
    HostApplication(Counter::new(100).unwrap())
}
fn group() -> GroupIdentity {
    GroupIdentity {
        id: GroupId::new(77).unwrap(),
        incarnation: GroupIncarnation::new(4).unwrap(),
    }
}
fn root() -> PathBuf {
    std::env::temp_dir().join(format!(
        "voteboat-startup-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}
fn config(directory: PathBuf, mode: NativeOpenMode) -> NativeStartup {
    let node = NodeId::new(44).unwrap();
    let store = StoreIdentity {
        id: StoreId::new(1234).unwrap(),
        incarnation: StoreIncarnation::new(3).unwrap(),
    };
    NativeStartup {
        directory,
        mode,
        node,
        store,
        bootstrap: Bootstrap {
            group: group(),
            configuration: ConfigurationId::new(9).unwrap(),
            policy: Policy::new(Tree::Voter(node), Limits::default()).unwrap(),
            voter_stores: [(node, store)].into(),
        },
        listen: "127.0.0.1:0".parse().unwrap(),
        peers: BTreeMap::new(),
        entropy_seed: 17,
        limits: NodeLimits::default(),
        tls: NativeTlsConfig::new(TlsCredentials {
            roots: vec![include_bytes!("fixtures/tls/ca.der").to_vec()],
            certificate_chain: vec![include_bytes!("fixtures/tls/node1.der").to_vec()],
            private_key: include_bytes!("fixtures/tls/node1-key.der").to_vec(),
        })
        .unwrap(),
    }
}
fn drive(n: &mut NativeNode<HostApplication>, done: impl Fn(&NativeNode<HostApplication>) -> bool) {
    let start = Instant::now();
    loop {
        n.poll(
            MonoTime(start.elapsed().as_millis() as u64),
            NodePollBudget::default(),
        )
        .unwrap();
        if done(n) {
            return;
        }
        assert!(start.elapsed() < Duration::from_secs(5));
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
fn close(mut n: NativeNode<HostApplication>) {
    // Each helper invocation starts a new local clock domain only after a complete open.
    // Use a forward time for shutdown following the earlier drive's elapsed milliseconds.
    n.begin_shutdown();
    for tick in 10000..20000 {
        n.poll(MonoTime(tick), NodePollBudget::default()).unwrap();
        if n.is_drained() {
            break;
        }
        std::thread::park_timeout(Duration::from_millis(1));
    }
    let mut p = n.into_parts().unwrap_or_else(|_| panic!("not drained"));
    let mut d = p
        .peers
        .take()
        .unwrap()
        .connector
        .into_dialer()
        .unwrap_or_else(|_| panic!("connector"));
    let mut s = p.local.snapshots.take().unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    let (mut log, mut snap) = (false, false);
    loop {
        let dial = d.try_finish().unwrap();
        if !log {
            log = p.local.persistence.try_reclaim().unwrap().is_some();
        }
        if !snap {
            snap = s.worker.try_reclaim().unwrap().is_some();
        }
        if dial && log && snap {
            break;
        }
        assert!(Instant::now() < deadline);
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
#[test]
fn host_application_and_non_demo_identities_write_and_recover_through_startup() {
    let root = root();
    let mut n = config(root.clone(), NativeOpenMode::Create)
        .open(app(), Arc::new(ThreadWake::current()), MonoTime(0))
        .unwrap();
    n.control(group(), NodeControl::Campaign).unwrap();
    drive(&mut n, |n| {
        n.local().applications[&group()].applied_index() > 0
    });
    n.propose(ClientRequest {
        group: group(),
        operation: OperationId::new(1).unwrap(),
        bytes: 7i64.to_le_bytes().to_vec(),
    })
    .unwrap();
    // Keep a single monotonic domain across the two drive phases.
    let mut reply = None;
    for tick in 5000..10000 {
        n.poll(MonoTime(tick), NodePollBudget::default()).unwrap();
        if let Some(r) = n.poll_client() {
            reply = Some(n.complete_client(r).unwrap());
            break;
        }
        std::thread::park_timeout(Duration::from_millis(1));
    }
    assert!(matches!(
        reply,
        Some(ClientOutcome::Applied {
            receipt: CounterReceipt {
                outcome: CounterOutcome::Value(7),
                ..
            },
            ..
        })
    ));
    close(n);
    let n = config(root.clone(), NativeOpenMode::Recover)
        .open(app(), Arc::new(ThreadWake::current()), MonoTime(0))
        .unwrap();
    assert_eq!(
        n.local().applications[&group()]
            .0
            .read_applied(n.local().applications[&group()].applied_index()),
        Ok(7)
    );
    close(n);
    std::fs::remove_dir_all(root).unwrap();
}
#[test]
fn late_constructor_failure_returns_application_and_joins_every_started_worker() {
    let root = root();
    let mut c = config(root.clone(), NativeOpenMode::Create);
    let reserve = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    c.listen = reserve.local_addr().unwrap();
    let address = c.listen;
    drop(reserve);
    c.limits.replica.leases = 0;
    let mut rejected = c
        .open(app(), Arc::new(ThreadWake::current()), MonoTime(0))
        .err()
        .unwrap();
    assert_eq!(rejected.reason.stage, "node assembly");
    assert!(rejected.application.is_some());
    assert!(std::net::TcpListener::bind(address).is_err());
    let deadline = Instant::now() + Duration::from_secs(5);
    while !rejected.try_cleanup().unwrap() {
        assert!(Instant::now() < deadline);
        std::thread::park_timeout(Duration::from_millis(1));
    }
    assert!(rejected.try_cleanup().unwrap());
    drop(std::net::TcpListener::bind(address).unwrap());
    let n = config(root.clone(), NativeOpenMode::Recover)
        .open(app(), Arc::new(ThreadWake::current()), MonoTime(0))
        .unwrap();
    close(n);
    std::fs::remove_dir_all(root).unwrap();
}
#[test]
fn invalid_startup_is_side_effect_free_and_returns_the_host_application() {
    let root = root();
    let mut c = config(root.clone(), NativeOpenMode::Create);
    c.store.incarnation = StoreIncarnation::new(4).unwrap();
    let mut rejected = c
        .open(app(), Arc::new(ThreadWake::current()), MonoTime(0))
        .err()
        .unwrap();
    assert_eq!(rejected.reason.stage, "configuration");
    assert!(rejected.application.is_some());
    assert!(rejected.try_cleanup().unwrap());
    assert!(!root.exists());
    let mut c = config(root.clone(), NativeOpenMode::Create);
    c.peers.insert(
        NodeId::new(8).unwrap(),
        NativeStartupPeer {
            address: "0.0.0.0:1".parse().unwrap(),
            certificate: vec![0],
            server_name: "bad name".into(),
        },
    );
    assert!(c.validate().is_err());
    assert!(!root.exists());
}

#[test]
fn stopping_one_startup_does_not_stop_another_or_the_shared_host_wake() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    #[derive(Default)]
    struct HostWake(AtomicUsize);
    impl voteboat::worker::WorkerWake for HostWake {
        fn wake(&self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }
    let (a_root, b_root) = (root(), root());
    let wake = Arc::new(HostWake::default());
    let mut a = config(a_root.clone(), NativeOpenMode::Create)
        .open(app(), wake.clone(), MonoTime(0))
        .unwrap();
    let mut b_config = config(b_root.clone(), NativeOpenMode::Create);
    b_config.bootstrap.group.id = GroupId::new(78).unwrap();
    let b_group = b_config.bootstrap.group;
    b_config.store.id = StoreId::new(5678).unwrap();
    b_config
        .bootstrap
        .voter_stores
        .insert(b_config.node, b_config.store);
    let mut b = b_config.open(app(), wake.clone(), MonoTime(0)).unwrap();
    a.control(group(), NodeControl::Campaign).unwrap();
    b.control(b_group, NodeControl::Campaign).unwrap();
    drive(&mut a, |n| {
        n.local().applications[&group()].applied_index() > 0
    });
    drive(&mut b, |n| {
        n.local().applications[&b_group].applied_index() > 0
    });
    close(a);
    let before = wake.0.load(Ordering::Relaxed);
    b.propose(ClientRequest {
        group: b_group,
        operation: OperationId::new(1).unwrap(),
        bytes: 9i64.to_le_bytes().to_vec(),
    })
    .unwrap();
    let mut reply = None;
    for tick in 5000..10000 {
        b.poll(MonoTime(tick), NodePollBudget::default()).unwrap();
        if let Some(output) = b.poll_client() {
            reply = Some(b.complete_client(output).unwrap());
            break;
        }
        std::thread::park_timeout(Duration::from_millis(1));
    }
    assert!(matches!(
        reply,
        Some(ClientOutcome::Applied {
            receipt: CounterReceipt {
                outcome: CounterOutcome::Value(9),
                ..
            },
            ..
        })
    ));
    assert!(wake.0.load(Ordering::Relaxed) > before);
    close(b);
    std::fs::remove_dir_all(a_root).unwrap();
    std::fs::remove_dir_all(b_root).unwrap();
}

#[test]
fn startup_uses_the_hosts_initial_monotonic_time_for_owner_and_deadlines() {
    let root = root();
    let mut n = config(root.clone(), NativeOpenMode::Create)
        .open(app(), Arc::new(ThreadWake::current()), MonoTime(1000))
        .unwrap();
    assert_eq!(
        n.poll(MonoTime(999), NodePollBudget::default()).err(),
        Some(NodeError::TimeWentBack)
    );
    assert_eq!(n.state(), NodeState::Running);
    n.poll(MonoTime(1000), NodePollBudget::default()).unwrap();
    let core = n.local().owner.core(group()).unwrap();
    assert_eq!(core.role(), voteboat::raft::Role::Follower);
    assert_eq!(core.state().hard_state.term, 0);
    assert!(!core.has_pending_dependency());
    close(n);
    std::fs::remove_dir_all(root).unwrap();
}
