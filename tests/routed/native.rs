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
use std::{
    collections::BTreeMap,
    net::{TcpListener, UdpSocket},
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};
use voteboat::{
    directory::*,
    native::{
        connect::*, log_store::*, node::*, routing::NativeManifestCache, startup::*,
        worker::ThreadWake,
    },
    quorum::*,
    runtime::*,
};
type Node<A> = NativeNode<A, NativeServiceConnector>;
type Child = RoutedApplication<Counter, HostPolicy>;
fn certificate(n: u64) -> &'static [u8] {
    match n {
        1 => include_bytes!("../fixtures/tls/node1.der"),
        2 => include_bytes!("../fixtures/tls/node2.der"),
        3 => include_bytes!("../fixtures/tls/node3.der"),
        _ => unreachable!(),
    }
}
fn configuration(root: &Path, g: u128, voters: &[u64], mode: NativeOpenMode) -> Vec<NativeStartup> {
    let reservations = voters
        .iter()
        .map(|_| {
            (0..32)
                .find_map(|_| {
                    let tcp = TcpListener::bind("127.0.0.1:0").ok()?;
                    let udp = UdpSocket::bind(tcp.local_addr().ok()?).ok()?;
                    Some((tcp, udp))
                })
                .expect("available TCP/UDP endpoint")
        })
        .collect::<Vec<_>>();
    let addresses = voters
        .iter()
        .copied()
        .zip(
            reservations
                .iter()
                .map(|(tcp, _)| tcp.local_addr().unwrap()),
        )
        .collect::<BTreeMap<_, _>>();
    let bootstrap = Bootstrap {
        group: group(g),
        configuration: ConfigurationId::new(1).unwrap(),
        policy: Policy::new(
            Tree::Majority(
                voters
                    .iter()
                    .map(|n| Tree::Voter(support::node(*n)))
                    .collect(),
            ),
            Limits::default(),
        )
        .unwrap(),
        voter_stores: voters
            .iter()
            .map(|n| (support::node(*n), support::identity((*n).into())))
            .collect(),
    };
    voters
        .iter()
        .map(|n| NativeStartup {
            directory: root.join(format!("{g}/{n}")),
            mode,
            node: support::node(*n),
            store: support::identity((*n).into()),
            bootstrap: bootstrap.clone(),
            listen: addresses[n],
            peers: voters
                .iter()
                .filter(|p| *p != n)
                .map(|p| {
                    (
                        support::node(*p),
                        NativeStartupPeer {
                            address: addresses[p],
                            certificate: certificate(*p).to_vec(),
                            server_name: format!("node{p}.voteboat.test"),
                        },
                    )
                })
                .collect(),
            tls: support::tls::configuration(*n),
            entropy_seed: *n + g as u64,
            limits: NodeLimits::default(),
        })
        .collect()
}
fn open<A>(
    configs: Vec<NativeStartup>,
    clock: &Instant,
    protocol: NativePeerProtocol,
    app: impl Fn() -> A,
) -> Vec<Node<A>>
where
    A: ProposalAdmission + BoundedReadableStateMachine + CheckpointStateMachine,
    A::Receipt: ApplicationReceipt,
{
    configs
        .into_iter()
        .map(|c| {
            if c.mode == NativeOpenMode::Create {
                std::fs::create_dir_all(c.directory.parent().unwrap()).unwrap();
            }
            c.open_with_protocol(
                protocol,
                app(),
                Arc::new(ThreadWake::current()),
                MonoTime(clock.elapsed().as_millis() as u64),
            )
            .unwrap_or_else(|r| panic!("startup: {:?}", r.reason))
        })
        .collect()
}
fn drive<A>(nodes: &mut [Node<A>], clock: &Instant, mut done: impl FnMut(&mut [Node<A>]) -> bool)
where
    A: ProposalAdmission + BoundedReadableStateMachine + CheckpointStateMachine,
    A::Receipt: ApplicationReceipt,
{
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        for n in nodes.iter_mut() {
            let progress = n
                .poll(
                    MonoTime(clock.elapsed().as_millis() as u64),
                    NodePollBudget {
                        replica: ReplicaPollBudget {
                            steps: 100,
                            ..Default::default()
                        },
                        ..Default::default()
                    },
                )
                .unwrap();
            if let Some(p) = progress.replica {
                for step in p.steps {
                    assert!(step.error.is_none(), "{:?}", step.error);
                }
            }
        }
        if done(nodes) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "native routed progress timed out"
        );
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
fn campaign<A>(nodes: &mut [Node<A>], clock: &Instant, g: u128)
where
    A: ProposalAdmission + BoundedReadableStateMachine + CheckpointStateMachine,
    A::Receipt: ApplicationReceipt,
{
    nodes[0].control(group(g), NodeControl::Campaign).unwrap();
    drive(nodes, clock, |ns| {
        ns[0].local().owner.core(group(g)).unwrap().role() == voteboat::raft::Role::Leader
            && ns.iter().all(|n| {
                n.local().applications[&group(g)].applied_index()
                    == ns[0]
                        .local()
                        .owner
                        .core(group(g))
                        .unwrap()
                        .state()
                        .commit_index
            })
    });
}
fn propose<A>(
    nodes: &mut [Node<A>],
    clock: &Instant,
    g: u128,
    operation: u128,
    bytes: Vec<u8>,
) -> A::Receipt
where
    A: ProposalAdmission + BoundedReadableStateMachine + CheckpointStateMachine,
    A::Receipt: ApplicationReceipt,
{
    nodes[0]
        .propose(ClientRequest {
            group: group(g),
            operation: OperationId::new(operation).unwrap(),
            bytes,
        })
        .unwrap();
    let mut receipt = None;
    drive(nodes, clock, |ns| {
        while let Some(reply) = ns[0].poll_client() {
            match ns[0]
                .complete_client(reply)
                .unwrap_or_else(|_| panic!("client completion rejected"))
            {
                ClientOutcome::Applied { receipt: r, .. } => {
                    assert!(receipt.is_none());
                    receipt = Some(r);
                }
                ClientOutcome::NotProposed(e) => {
                    panic!("group {g} operation {operation} not proposed: {e:?}")
                }
                ClientOutcome::Unknown(e) => {
                    panic!("group {g} operation {operation} unknown: {e:?}")
                }
            }
        }
        receipt.as_ref().is_some_and(|r| {
            ns.iter()
                .all(|n| n.local().applications[&group(g)].applied_index() >= r.index())
        })
    });
    receipt.unwrap()
}
fn read<A>(nodes: &mut [Node<A>], clock: &Instant, g: u128, query: A::Query) -> A::ReadResult
where
    A: ProposalAdmission + BoundedReadableStateMachine + CheckpointStateMachine,
    A::Receipt: ApplicationReceipt,
{
    nodes[0]
        .read(group(g), query)
        .unwrap_or_else(|_| panic!("read invocation rejected"));
    let mut result = None;
    drive(nodes, clock, |ns| {
        while let Some(reply) = ns[0].poll_read() {
            match ns[0]
                .complete_read(reply)
                .unwrap_or_else(|_| panic!("read completion rejected"))
            {
                ReadOutcome::Read { result: Ok(r), .. } => result = Some(r),
                _ => panic!("read failed"),
            }
        }
        result.is_some()
    });
    result.unwrap()
}
fn close<A>(
    mut nodes: Vec<Node<A>>,
    clock: &Instant,
    g: u128,
    mut background: impl FnMut(),
) -> Vec<GroupLog>
where
    A: ProposalAdmission + BoundedReadableStateMachine + CheckpointStateMachine,
    A::Receipt: ApplicationReceipt,
{
    for n in &mut nodes {
        n.begin_shutdown();
    }
    drive(&mut nodes, clock, |ns| {
        background();
        ns.iter().all(|n| n.is_drained())
    });
    nodes
        .into_iter()
        .map(|n| {
            let mut parts = n.into_parts().unwrap_or_else(|_| panic!("not drained"));
            let mut dialer = parts
                .peers
                .take()
                .unwrap()
                .connector
                .into_dialer()
                .unwrap_or_else(|_| panic!("connector not drained"));
            let mut snapshots = parts.local.snapshots.take().unwrap();
            let deadline = Instant::now() + Duration::from_secs(5);
            let mut log = None;
            let mut snap = false;
            loop {
                background();
                let dial = match &mut dialer {
                    Some(d) => d.try_finish().unwrap(),
                    None => true,
                };
                if !snap {
                    snap = snapshots.worker.try_reclaim().unwrap().is_some();
                }
                if log.is_none() {
                    if let Some(store) = parts.local.persistence.try_reclaim().unwrap() {
                        log = Some(store.state(group(g)).unwrap());
                    }
                }
                if dial && snap {
                    if let Some(log) = log.take() {
                        return log;
                    }
                }
                assert!(Instant::now() < deadline);
                std::thread::park_timeout(Duration::from_millis(1));
            }
        })
        .collect()
}
fn manifests() -> Vec<ResponsibilityManifest> {
    let mut orders = grant().into_input();
    orders.parent.as_mut().unwrap().responsibility = responsibility(4);
    let orders = ResponsibilityManifest::new(orders).unwrap();
    let mut jobs = grant().into_input();
    jobs.responsibility = responsibility(3);
    jobs.parent.as_mut().unwrap().responsibility = responsibility(5);
    jobs.scope = BucketRange::new(128, 256).unwrap();
    jobs.execution = ExecutionMode::Single(group(30));
    let jobs = ResponsibilityManifest::new(jobs).unwrap();
    let mut root = grant().into_input();
    root.responsibility = responsibility(1);
    root.parent = None;
    root.scope = BucketRange::new(0, 256).unwrap();
    let delegated = |children: Vec<ResponsibilityManifest>| {
        ExecutionMode::Delegated(
            children
                .into_iter()
                .map(|m| RouteEntry {
                    scope: m.input().scope,
                    target: RouteTarget::Child(ChildAuthority {
                        responsibility: m.input().responsibility,
                        group: group(1),
                        epoch: m.input().epoch,
                    }),
                })
                .collect(),
        )
    };
    let mut service_x = grant().into_input();
    service_x.responsibility = responsibility(4);
    service_x.execution = delegated(vec![orders.clone()]);
    let service_x = ResponsibilityManifest::new(service_x).unwrap();
    let mut service_y = jobs.clone().into_input();
    service_y.responsibility = responsibility(5);
    service_y.parent.as_mut().unwrap().responsibility = responsibility(1);
    service_y.execution = delegated(vec![jobs.clone()]);
    let service_y = ResponsibilityManifest::new(service_y).unwrap();
    root.execution = delegated(vec![service_x.clone(), service_y.clone()]);
    vec![
        ResponsibilityManifest::new(root).unwrap(),
        service_x,
        service_y,
        orders,
        jobs,
    ]
}
fn directory() -> Directory {
    Directory::new(
        DirectoryPlan::new(group(1), manifests()).unwrap(),
        DirectoryLimits {
            operations: 32,
            history_bytes: 64 * 1024,
        },
    )
    .unwrap()
}
fn child(g: u128, manifest: ResponsibilityManifest) -> Child {
    RoutedApplication::new(
        group(g),
        manifest,
        Counter::new(64).unwrap(),
        HostPolicy,
        limits(),
    )
    .unwrap_or_else(|r| panic!("{:?}", r.error))
}
#[test]
fn wrong_group_rejected_before_native_startup_creates_files() {
    let root = std::env::temp_dir().join(format!("voteboat-routed-binding-{}", std::process::id()));
    assert!(!root.exists());
    let config = configuration(&root, 21, &[1, 2], NativeOpenMode::Create).remove(0);
    let mut rejected = match config.open_with_protocol(
        NativePeerProtocol::TcpTls,
        fresh(),
        Arc::new(ThreadWake::current()),
        MonoTime(0),
    ) {
        Ok(_) => panic!("wrong group accepted"),
        Err(r) => r,
    };
    assert_eq!(rejected.reason.stage, "application");
    assert!(rejected.try_cleanup().unwrap());
    assert!(!root.exists());
    assert_eq!(rejected.application.take().unwrap().applied_index(), 0);
}
fn parent_independence(protocol: NativePeerProtocol, compact: bool) {
    let clock = Instant::now();
    let root = std::env::temp_dir().join(format!(
        "voteboat-routed-native-{}-{:?}",
        std::process::id(),
        protocol
    ));
    std::fs::create_dir_all(&root).unwrap();
    let mut parents = open(
        configuration(&root, 1, &[1, 2, 3], NativeOpenMode::Create),
        &clock,
        protocol,
        directory,
    );
    campaign(&mut parents, &clock, 1);
    let boot = directory()
        .bootstrap_command(MAX_DIRECTORY_COMMAND_BYTES)
        .unwrap();
    assert_eq!(
        propose(&mut parents, &clock, 1, 10000, boot).outcome,
        DirectoryOutcome::Initialized
    );
    for (i, manifest) in manifests().into_iter().enumerate() {
        let bytes = DirectoryCommand {
            expected: None,
            manifest,
        }
        .encode(MAX_DIRECTORY_COMMAND_BYTES)
        .unwrap();
        assert!(matches!(
            propose(&mut parents, &clock, 1, 10001 + i as u128, bytes).outcome,
            DirectoryOutcome::Published(_)
        ));
    }
    let mut cache = NativeManifestCache::new(ManifestCacheLimits {
        manifests: 8,
        bytes: 64 * 1024,
    })
    .unwrap();
    for id in 1..=5 {
        let manifest = read(&mut parents, &clock, 1, responsibility(id)).unwrap();
        cache.admit(manifest).unwrap();
    }
    let orders = resolve(&cache, &HostPolicy, responsibility(1), &[10], 3).unwrap();
    let jobs = resolve(&cache, &HostPolicy, responsibility(1), &[200], 3).unwrap();
    assert_ne!(orders.group, jobs.group);
    let orders_grant = cache.get(responsibility(2)).unwrap().clone();
    let jobs_grant = cache.get(responsibility(3)).unwrap().clone();
    let mut children = open(
        configuration(&root, 20, &[1, 2], NativeOpenMode::Create),
        &clock,
        protocol,
        || child(20, orders_grant.clone()),
    );
    let mut other = open(
        configuration(&root, 30, &[2, 3], NativeOpenMode::Create),
        &clock,
        protocol,
        || child(30, jobs_grant.clone()),
    );
    campaign(&mut children, &clock, 20);
    campaign(&mut other, &clock, 30);
    for (nodes, g, manifest) in [
        (&mut children, 20, &orders_grant),
        (&mut other, 30, &jobs_grant),
    ] {
        let bytes = child(g, manifest.clone())
            .bootstrap_command(MAX_ROUTED_COMMAND_BYTES)
            .unwrap();
        assert_eq!(
            propose(nodes, &clock, g, 10000, bytes).outcome,
            RoutedOutcome::Bootstrapped
        );
    }
    // The host must continue polling live child roles while parent roles drain.
    let parent_logs = close(parents, &clock, 1, || {
        drive(&mut children, &clock, |_| true);
        drive(&mut other, &clock, |_| true);
    });
    cache.invalidate(responsibility(1), RouteGeneration::new(1).unwrap());
    cache.invalidate(responsibility(4), RouteGeneration::new(1).unwrap());
    cache.invalidate(responsibility(5), RouteGeneration::new(1).unwrap());
    assert!(resolve(&cache, &HostPolicy, responsibility(1), &[10], 3).is_err());
    assert_eq!(
        resolve(&cache, &HostPolicy, responsibility(2), &[10], 1).unwrap(),
        orders
    );
    assert_eq!(
        resolve(&cache, &HostPolicy, responsibility(3), &[200], 1).unwrap(),
        jobs
    );
    for (nodes, g, hint, key, delta) in [
        (&mut children, 20, orders, 10, 7i64),
        (&mut other, 30, jobs, 200, 11i64),
    ] {
        let bytes =
            encode_routed(hint, &[key], &delta.to_le_bytes(), MAX_ROUTED_COMMAND_BYTES).unwrap();
        assert!(
            matches!(propose(nodes, &clock, g, 1, bytes.clone()).outcome, RoutedOutcome::Applied(CounterReceipt {outcome: CounterOutcome::Value(v), duplicate: false, ..}) if v == delta)
        );
        assert!(matches!(
            propose(nodes, &clock, g, 1, bytes).outcome,
            RoutedOutcome::Applied(CounterReceipt {
                duplicate: true,
                ..
            })
        ));
        assert_eq!(
            read(
                nodes,
                &clock,
                g,
                RoutedQuery {
                    hint,
                    key: vec![key],
                    query: ()
                }
            ),
            RoutedRead::Served(delta)
        );
        if compact {
            for n in nodes.iter_mut() {
                n.control(group(g), NodeControl::Checkpoint).unwrap();
            }
            drive(nodes, &clock, |ns| {
                ns.iter().all(|n| {
                    n.local().owner.core(group(g)).unwrap().state().base_index()
                        == n.local().applications[&group(g)].applied_index()
                })
            });
        }
    }
    close(children, &clock, 20, || {
        drive(&mut other, &clock, |_| true);
    });
    close(other, &clock, 30, || {});
    for (g, voters, manifest, hint, key, delta) in [
        (20, vec![1, 2], orders_grant, orders, 10, 7i64),
        (30, vec![2, 3], jobs_grant, jobs, 200, 11i64),
    ] {
        // Both WAL-only and checkpoint recovery must reject a changed owner
        // grant or semantic-retention envelope before exposing an application.
        for changed_grant in [false, true] {
            let mut input = manifest.clone().into_input();
            let mut selected_limits = limits();
            if changed_grant {
                input.epoch = OwnershipEpoch::new(2).unwrap();
            } else {
                selected_limits.semantic_bytes -= 1;
            }
            let app = RoutedApplication::new(
                group(g),
                ResponsibilityManifest::new(input).unwrap(),
                Counter::new(64).unwrap(),
                HostPolicy,
                selected_limits,
            )
            .unwrap_or_else(|r| panic!("{:?}", r.error));
            let config = configuration(&root, g, &voters, NativeOpenMode::Recover).remove(0);
            let mut rejected = match config.open_with_protocol(
                protocol,
                app,
                Arc::new(ThreadWake::current()),
                MonoTime(clock.elapsed().as_millis() as u64),
            ) {
                Ok(_) => panic!("changed ownership binding accepted"),
                Err(r) => r,
            };
            let deadline = Instant::now() + Duration::from_secs(5);
            while !rejected.try_cleanup().unwrap() {
                assert!(Instant::now() < deadline);
                std::thread::park_timeout(Duration::from_millis(1));
            }
            assert_eq!(rejected.application.take().unwrap().applied_index(), 0);
        }
        let mut ns = open(
            configuration(&root, g, &voters, NativeOpenMode::Recover),
            &clock,
            protocol,
            || child(g, manifest.clone()),
        );
        campaign(&mut ns, &clock, g);
        let bytes =
            encode_routed(hint, &[key], &delta.to_le_bytes(), MAX_ROUTED_COMMAND_BYTES).unwrap();
        assert!(
            matches!(propose(&mut ns, &clock, g, 1, bytes).outcome, RoutedOutcome::Applied(CounterReceipt {outcome: CounterOutcome::Value(v), duplicate: true, ..}) if v == delta)
        );
        assert_eq!(
            read(
                &mut ns,
                &clock,
                g,
                RoutedQuery {
                    hint,
                    key: vec![key],
                    query: ()
                }
            ),
            RoutedRead::Served(delta)
        );
        close(ns, &clock, g, || {});
    }
    for (n, before) in (1..=3).zip(parent_logs) {
        let log = NativeLogStore::recover(
            FileLogIo::open(root.join(format!("1/{n}"))).unwrap(),
            support::identity(n),
            LogLimits::default(),
        )
        .unwrap();
        assert_eq!(log.state(group(1)).unwrap(), before);
    }
    std::fs::remove_dir_all(root).unwrap();
}
#[test]
fn tcp_child_writes_checkpoint_and_restart_without_parent_commits() {
    for compact in [false, true] {
        parent_independence(NativePeerProtocol::TcpTls, compact);
    }
}
#[cfg(feature = "quic")]
#[test]
fn quic_child_writes_checkpoint_and_restart_without_parent_commits() {
    for compact in [false, true] {
        parent_independence(NativePeerProtocol::Quic, compact);
    }
}

use super::source_fixture;
fn source_freeze_recovery(protocol: NativePeerProtocol, compact: bool) {
    use voteboat::{bucket_counter::*, scope::*, transfer_source::*};
    let clock = Instant::now();
    let root = std::env::temp_dir().join(format!(
        "voteboat-source-freeze-{}-{protocol:?}-{compact}",
        std::process::id()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let mut nodes = open(
        configuration(&root, 20, &[1, 2, 3], NativeOpenMode::Create),
        &clock,
        protocol,
        source_fixture::fresh,
    );
    campaign(&mut nodes, &clock, 20);
    propose(
        &mut nodes,
        &clock,
        20,
        100,
        source_fixture::fresh().bootstrap_command(100000).unwrap(),
    );
    propose(&mut nodes, &clock, 20, 1, source_fixture::data(1, 7));
    propose(&mut nodes, &clock, 20, 2, source_fixture::data(200, 11));
    // Discard the client observation; recover from committed application state.
    let _ = propose(&mut nodes, &clock, 20, 200, source_fixture::freeze());
    let original_fence = nodes[0].local().applications[&group(20)].fence().unwrap();
    let original_image = nodes[0].local().applications[&group(20)]
        .export_target(group(21), 65536)
        .unwrap();
    assert_eq!(original_image.source_applied(), original_fence.index);
    if compact {
        for node in &mut nodes {
            node.control(group(20), NodeControl::Checkpoint).unwrap();
        }
        drive(&mut nodes, &clock, |ns| {
            ns.iter().all(|n| {
                n.local()
                    .owner
                    .core(group(20))
                    .unwrap()
                    .state()
                    .base_index()
                    == n.local().applications[&group(20)].applied_index()
            })
        });
    }
    close(nodes, &clock, 20, || {});
    let mut nodes = open(
        configuration(&root, 20, &[1, 2, 3], NativeOpenMode::Recover),
        &clock,
        protocol,
        source_fixture::fresh,
    );
    campaign(&mut nodes, &clock, 20);
    let retry = propose(&mut nodes, &clock, 20, 200, source_fixture::freeze());
    assert_eq!(retry.outcome, RoutedOutcome::Fenced(original_fence));
    assert_eq!(
        read(
            &mut nodes,
            &clock,
            20,
            SourceQuery::Data(RoutedQuery {
                hint: source_fixture::hint(1),
                key: vec![1],
                query: vec![1]
            })
        ),
        SourceRead::Data(RoutedRead::Rejected(RoutingError::Fenced))
    );
    let SourceRead::Freeze(Some(status)) = read(&mut nodes, &clock, 20, SourceQuery::Freeze) else {
        panic!("source status")
    };
    assert_eq!(status.fence, original_fence);
    assert_eq!(status.intent, source_fixture::intent());
    assert_eq!(
        status
            .exports
            .iter()
            .find(|e| e.target == group(21))
            .unwrap()
            .digest,
        voteboat::transfer::ContentDigest::scope_image(&original_image)
    );
    for n in &nodes {
        let source = &n.local().applications[&group(20)];
        assert!(source.applied_index() > original_fence.index);
        assert_eq!(source.routed().applied_index(), original_fence.index);
        assert_eq!(
            source.export_target(group(21), 65536).unwrap(),
            original_image
        );
        assert_eq!(source.routed().application().value(&[1]), Ok(7));
        assert_eq!(source.routed().application().value(&[200]), Ok(11));
    }
    let mut target = BucketCounter::new(
        source_fixture::range(0, 128),
        source_fixture::Policy,
        source_fixture::bucket_limits(),
    )
    .unwrap();
    target.import_scopes(&[original_image], 1).unwrap();
    assert_eq!(target.value(&[1]), Ok(7));
    assert_eq!(target.outbox().count(), 1);
    let bytes = encode_add(&[1], 7, b"effect", 1024).unwrap();
    assert!(
        target
            .apply_batch(&[source_fixture::entry(2, 1, bytes)])
            .unwrap()[0]
            .duplicate
    );
    // Target import above is local adapter evidence, not a committed activation.
    close(nodes, &clock, 20, || {});
    let nodes = open(
        configuration(&root, 20, &[1, 2, 3], NativeOpenMode::Recover),
        &clock,
        protocol,
        source_fixture::fresh,
    );
    for n in &nodes {
        assert_eq!(
            n.local().applications[&group(20)].fence(),
            Some(original_fence)
        );
    }
    close(nodes, &clock, 20, || {});
    std::fs::remove_dir_all(root).unwrap();
}
#[test]
fn tcp_transfer_source_freeze_survives_wal_reopen_and_lost_observation() {
    source_freeze_recovery(NativePeerProtocol::TcpTls, false);
}
#[test]
fn tcp_transfer_source_freeze_survives_checkpoint_and_later_log_progress() {
    source_freeze_recovery(NativePeerProtocol::TcpTls, true);
}
#[cfg(feature = "quic")]
#[test]
fn quic_transfer_source_freeze_survives_wal_reopen_and_lost_observation() {
    source_freeze_recovery(NativePeerProtocol::Quic, false);
}
#[cfg(feature = "quic")]
#[test]
fn quic_transfer_source_freeze_survives_checkpoint_and_later_log_progress() {
    source_freeze_recovery(NativePeerProtocol::Quic, true);
}

#[path = "../transfer_target/fixtures.rs"]
mod target_fixture;
fn target_import_recovery(protocol: NativePeerProtocol, compact: bool) {
    use voteboat::{transfer::ContentDigest, transfer_source::*, transfer_target::*};
    let clock = Instant::now();
    let root = std::env::temp_dir().join(format!(
        "voteboat-target-import-{}-{protocol:?}-{compact}",
        std::process::id()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let mut sources = open(
        configuration(&root, 20, &[1, 2, 3], NativeOpenMode::Create),
        &clock,
        protocol,
        target_fixture::source::fresh,
    );
    let mut targets = open(
        configuration(&root, 21, &[1, 2, 3], NativeOpenMode::Create),
        &clock,
        protocol,
        target_fixture::fresh,
    );
    let mut other_targets = open(
        configuration(&root, 22, &[1, 2, 3], NativeOpenMode::Create),
        &clock,
        protocol,
        || target_fixture::fresh_for(22),
    );
    campaign(&mut other_targets, &clock, 22);
    propose(
        &mut other_targets,
        &clock,
        22,
        200,
        target_fixture::fresh_for(22)
            .bootstrap_command(65536)
            .unwrap(),
    );
    assert!(other_targets
        .iter()
        .all(|n| n.local().applications[&group(22)]
            .status()
            .staged_index
            .is_some()));
    campaign(&mut sources, &clock, 20);
    campaign(&mut targets, &clock, 21);
    // Stage targets before committing the source's irreversible cut.
    let staged = propose(
        &mut targets,
        &clock,
        21,
        200,
        target_fixture::fresh().bootstrap_command(65536).unwrap(),
    );
    assert!(matches!(staged.outcome, TargetOutcome::Staged { .. }));
    let query = || {
        TargetQuery::Data(RoutedQuery {
            hint: target_fixture::source::hint(1),
            key: vec![1],
            query: vec![1],
        })
    };
    assert_eq!(
        read(&mut targets, &clock, 21, query()),
        TargetRead::NotActive
    );
    propose(
        &mut sources,
        &clock,
        20,
        100,
        target_fixture::source::fresh()
            .bootstrap_command(100000)
            .unwrap(),
    );
    propose(
        &mut sources,
        &clock,
        20,
        1,
        target_fixture::source::data(1, 7),
    );
    propose(
        &mut sources,
        &clock,
        20,
        2,
        target_fixture::source::data(200, 11),
    );
    propose(
        &mut sources,
        &clock,
        20,
        200,
        target_fixture::source::freeze(),
    );
    let SourceRead::Freeze(Some(status)) = read(&mut sources, &clock, 20, SourceQuery::Freeze)
    else {
        panic!("source status")
    };
    let app = &sources[0].local().applications[&group(20)];
    assert_eq!(status.fence, app.fence().unwrap());
    let source_configuration = sources[0]
        .local()
        .owner
        .core(group(20))
        .unwrap()
        .state()
        .bootstrap
        .configuration;
    let import = target_fixture::from_source(app, 21, source_configuration);
    let bytes = target_fixture::fresh()
        .import_command(&import, 65536)
        .unwrap();
    // The first successful target observation is discarded; subsequent status is authoritative.
    let _ = propose(&mut targets, &clock, 21, 200, bytes.clone());
    let TargetRead::Status(original) = read(&mut targets, &clock, 21, TargetQuery::Status) else {
        panic!("target status")
    };
    let imported = original.imported.as_ref().unwrap();
    assert_eq!(imported.digest, ContentDigest::sha256(&bytes));
    assert_eq!(imported.sources[0].fence, status.fence);
    assert_eq!(
        imported.sources[0].digest,
        status
            .exports
            .iter()
            .find(|e| e.target == group(21))
            .unwrap()
            .digest
    );
    assert_eq!(
        read(&mut targets, &clock, 21, query()),
        TargetRead::NotActive
    );
    // The selected facade rejects this synchronously, before proposal ownership.
    let request = ClientRequest {
        group: group(21),
        operation: OperationId::new(3).unwrap(),
        bytes: target_fixture::source::data(1, 1),
    };
    let ptr = request.bytes.as_ptr();
    let rejected = targets[0].propose(request).unwrap_err();
    assert_eq!(
        rejected.reason,
        ClientError::Application(ApplicationError::InvalidCommand)
    );
    assert_eq!(rejected.request.bytes.as_ptr(), ptr);
    assert!(other_targets
        .iter()
        .all(|n| n.local().applications[&group(22)]
            .status()
            .imported
            .is_none()));
    close(sources, &clock, 20, || {
        drive(&mut targets, &clock, |_| true);
        drive(&mut other_targets, &clock, |_| true);
    });
    close(other_targets, &clock, 22, || {
        drive(&mut targets, &clock, |_| true);
    });
    if compact {
        for target in &mut targets {
            target.control(group(21), NodeControl::Checkpoint).unwrap();
        }
        drive(&mut targets, &clock, |ns| {
            ns.iter().all(|n| {
                n.local()
                    .owner
                    .core(group(21))
                    .unwrap()
                    .state()
                    .base_index()
                    == n.local().applications[&group(21)].applied_index()
            })
        });
    }
    close(targets, &clock, 21, || {});
    let mut targets = open(
        configuration(&root, 21, &[1, 2, 3], NativeOpenMode::Recover),
        &clock,
        protocol,
        target_fixture::fresh,
    );
    campaign(&mut targets, &clock, 21);
    let receipt = propose(&mut targets, &clock, 21, 200, bytes);
    assert_eq!(
        receipt.outcome,
        TargetOutcome::Imported {
            index: imported.index,
            digest: imported.digest
        }
    );
    assert_eq!(
        read(&mut targets, &clock, 21, TargetQuery::Status),
        TargetRead::Status(original.clone())
    );
    assert_eq!(
        read(&mut targets, &clock, 21, query()),
        TargetRead::NotActive
    );
    for target in &targets {
        let app = &target.local().applications[&group(21)];
        assert_eq!(app.application().value(&[1]), Ok(7));
        assert_eq!(app.application().outbox().count(), 1);
        assert!(app.applied_index() > imported.index);
    }
    close(targets, &clock, 21, || {});
    let targets = open(
        configuration(&root, 21, &[1, 2, 3], NativeOpenMode::Recover),
        &clock,
        protocol,
        target_fixture::fresh,
    );
    for target in &targets {
        assert_eq!(target.local().applications[&group(21)].status(), original);
    }
    close(targets, &clock, 21, || {});
    std::fs::remove_dir_all(root).unwrap();
}
#[test]
fn tcp_target_staging_and_inline_import_survive_wal_reopen() {
    target_import_recovery(NativePeerProtocol::TcpTls, false);
}
#[test]
fn tcp_target_staging_and_inline_import_survive_checkpoint_reopen() {
    target_import_recovery(NativePeerProtocol::TcpTls, true);
}
#[cfg(feature = "quic")]
#[test]
fn quic_target_staging_and_inline_import_survive_wal_reopen() {
    target_import_recovery(NativePeerProtocol::Quic, false);
}
#[cfg(feature = "quic")]
#[test]
fn quic_target_staging_and_inline_import_survive_checkpoint_reopen() {
    target_import_recovery(NativePeerProtocol::Quic, true);
}
