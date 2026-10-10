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
#[path = "lookup_discovery.rs"]
mod automatic_lookup;
#[path = "parent_independence.rs"]
mod parent_independence;
#[path = "publication_recovery.rs"]
mod publication_recovery;
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
// Each history starts its own real worker/socket topology. Keep unrelated
// histories from oversubscribing this process; replicas/groups within one
// history remain concurrent under the production deadlines.
static NATIVE_HISTORY: std::sync::Mutex<()> = std::sync::Mutex::new(());
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
    // Keep listener candidates out of the automatic outbound ephemeral pool.
    // The old bind(:0)/drop probe could let another group's connection take the
    // proposed listener port before NativeStartup bound it. Still check TCP
    // and UDP: both transports use this fixture and no port is assumed free.
    static NEXT_LISTENER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let reservations = voters
        .iter()
        .map(|_| {
            (0..32)
                .find_map(|_| {
                    let port = 10000
                        + NEXT_LISTENER.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % 10000;
                    let tcp =
                        TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, port as u16)).ok()?;
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
#[track_caller]
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
                    // ReadRequests validates tracked cancellation completions.
                    // A term change can already have cleared the read, making
                    // its later CancelRead correctly return StaleRead.
                    let cleared_read = step.admission.is_some()
                        && step.operation.is_none()
                        && step.read.is_none()
                        && step.error == Some(voteboat::raft::RaftError::StaleRead);
                    let read_refused = step.read.is_some()
                        && matches!(
                            step.error,
                            Some(
                                voteboat::raft::RaftError::NotLeader
                                    | voteboat::raft::RaftError::ReadNotReady
                            )
                        );
                    let proposal_refused = step.operation.is_some()
                        && step.error == Some(voteboat::raft::RaftError::NotLeader);
                    assert!(
                        step.error.is_none() || cleared_read || read_refused || proposal_refused,
                        "{:?}",
                        step.error
                    );
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
#[track_caller]
fn campaign<A>(nodes: &mut [Node<A>], clock: &Instant, g: u128)
where
    A: ProposalAdmission + BoundedReadableStateMachine + CheckpointStateMachine,
    A::Receipt: ApplicationReceipt,
{
    nodes[0].control(group(g), NodeControl::Campaign).unwrap();
    let mut leader = None;
    drive(nodes, clock, |ns| {
        leader = ns.iter().position(|n| {
            let core = n.local().owner.core(group(g)).unwrap();
            let state = core.state();
            core.role() == voteboat::raft::Role::Leader
                && state.term_at(state.commit_index) == Some(state.hard_state.term)
                && ns.iter().all(|n| {
                    n.local().applications[&group(g)].applied_index() == state.commit_index
                })
        });
        leader.is_some()
    });
    // A competing timer-driven campaign may win. Subsequent helpers submit to
    // the observed ready leader, whose authority is still checked by the core.
    nodes.swap(0, leader.unwrap());
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
    propose_attempt(nodes, clock, g, operation, bytes).unwrap()
}

// Selected receipt-loss histories recover only leadership uncertainty, retaining
// the exact semantic operation and payload. Other failures remain fatal.
fn propose_recovering<A>(
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
    for _ in 0..4 {
        match propose_attempt(nodes, clock, g, operation, bytes.clone()) {
            Ok(receipt) => return receipt,
            Err(ClientUnknown::LeadershipChanged) => campaign(nodes, clock, g),
            Err(e) => panic!("group {g} operation {operation} unknown: {e:?}"),
        }
    }
    panic!("group {g} operation {operation} repeatedly lost leadership");
}
fn propose_attempt<A>(
    nodes: &mut [Node<A>],
    clock: &Instant,
    g: u128,
    operation: u128,
    bytes: Vec<u8>,
) -> Result<A::Receipt, ClientUnknown>
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
    let mut unknown = None;
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
                    if e == voteboat::raft::RaftError::NotLeader {
                        unknown = Some(ClientUnknown::LeadershipChanged);
                    } else {
                        panic!("group {g} operation {operation} not proposed: {e:?}")
                    }
                }
                ClientOutcome::Unknown(e) => {
                    unknown = Some(e);
                }
            }
        }
        unknown.is_some()
            || receipt.as_ref().is_some_and(|r| {
                ns.iter()
                    .all(|n| n.local().applications[&group(g)].applied_index() >= r.index())
            })
    });
    match unknown {
        Some(e) => Err(e),
        None => Ok(receipt.unwrap()),
    }
}
fn read<A>(nodes: &mut [Node<A>], clock: &Instant, g: u128, query: A::Query) -> A::ReadResult
where
    A: ProposalAdmission + BoundedReadableStateMachine + CheckpointStateMachine,
    A::Receipt: ApplicationReceipt,
{
    read_attempt(nodes, clock, g, query).expect("read lost leadership")
}
fn read_recovering<A>(
    nodes: &mut [Node<A>],
    clock: &Instant,
    g: u128,
    query: A::Query,
) -> A::ReadResult
where
    A: ProposalAdmission + BoundedReadableStateMachine + CheckpointStateMachine,
    A::Receipt: ApplicationReceipt,
    A::Query: Clone,
{
    for _ in 0..4 {
        if let Some(result) = read_attempt(nodes, clock, g, query.clone()) {
            return result;
        }
        campaign(nodes, clock, g);
    }
    panic!("group {g} read repeatedly lost leadership");
}
fn read_attempt<A>(
    nodes: &mut [Node<A>],
    clock: &Instant,
    g: u128,
    query: A::Query,
) -> Option<A::ReadResult>
where
    A: ProposalAdmission + BoundedReadableStateMachine + CheckpointStateMachine,
    A::Receipt: ApplicationReceipt,
{
    nodes[0]
        .read(group(g), query)
        .unwrap_or_else(|_| panic!("read invocation rejected"));
    let mut result = None;
    let mut changed = false;
    drive(nodes, clock, |ns| {
        while let Some(reply) = ns[0].poll_read() {
            match ns[0]
                .complete_read(reply)
                .unwrap_or_else(|_| panic!("read completion rejected"))
            {
                ReadOutcome::Read { result: Ok(r), .. } => result = Some(r),
                ReadOutcome::Unavailable(ReadUnavailable::LeadershipChanged)
                | ReadOutcome::NotRead(voteboat::raft::RaftError::NotLeader)
                | ReadOutcome::NotRead(voteboat::raft::RaftError::ReadNotReady) => changed = true,
                ReadOutcome::NotRead(e) => panic!("read refused: {e:?}"),
                ReadOutcome::Unavailable(e) => panic!("read unavailable: {e:?}"),
                ReadOutcome::Read { result: Err(e), .. } => {
                    panic!("application read failed: {e:?}")
                }
            }
        }
        changed || result.is_some()
    });
    result
}
fn directory_observation(
    nodes: &mut [Node<Directory>],
    clock: &Instant,
    id: ResponsibilityIdentity,
) -> (
    ReadInvocationTicket,
    ReadOutcome<Option<ResponsibilityManifest>>,
) {
    for _ in 0..4 {
        nodes[0]
            .read(group(1), id)
            .unwrap_or_else(|_| panic!("directory read rejected"));
        let mut result = None;
        drive(nodes, clock, |ns| {
            while let Some(reply) = ns[0].poll_read() {
                let ticket = reply.ticket();
                result = Some((
                    ticket,
                    ns[0]
                        .complete_read(reply)
                        .unwrap_or_else(|_| panic!("directory completion")),
                ));
            }
            result.is_some()
        });
        let outcome = result.unwrap();
        if matches!(&outcome.1, ReadOutcome::Read { result: Ok(_), .. }) {
            return outcome;
        }
        campaign(nodes, clock, 1);
    }
    panic!("directory observation repeatedly unavailable")
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
#[test]
fn tcp_child_writes_checkpoint_and_restart_without_parent_commits() {
    for compact in [false, true] {
        parent_independence::run(NativePeerProtocol::TcpTls, compact);
    }
}
#[cfg(feature = "quic")]
#[test]
fn quic_child_writes_checkpoint_and_restart_without_parent_commits() {
    for compact in [false, true] {
        parent_independence::run(NativePeerProtocol::Quic, compact);
    }
}

use super::source_fixture;
fn verify_frozen_export(
    nodes: &[Node<source_fixture::Source>],
    original_fence: voteboat::routed::OwnershipFence,
    original_image: voteboat::scope::ScopeImage,
) {
    use voteboat::{bucket_counter::*, scope::*};
    for n in nodes {
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
}
fn stage_secondary_target(
    root: &std::path::Path,
    clock: &Instant,
    protocol: NativePeerProtocol,
) -> Vec<Node<target_fixture::Target>> {
    let mut other_targets = open(
        configuration(root, 22, &[1, 2, 3], NativeOpenMode::Create),
        clock,
        protocol,
        || target_fixture::fresh_for(22),
    );
    campaign(&mut other_targets, clock, 22);
    propose(
        &mut other_targets,
        clock,
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
    other_targets
}
fn prepare_target_import(
    sources: &mut [Node<target_fixture::source::Source>],
    clock: &Instant,
) -> (
    voteboat::transfer_source::SourceFreezeStatus,
    voteboat::transfer_target::TargetImport,
) {
    use voteboat::transfer_source::*;
    propose(
        sources,
        clock,
        20,
        100,
        target_fixture::source::fresh()
            .bootstrap_command(100000)
            .unwrap(),
    );
    propose(sources, clock, 20, 1, target_fixture::source::data(1, 7));
    propose(sources, clock, 20, 2, target_fixture::source::data(200, 11));
    propose(sources, clock, 20, 200, target_fixture::source::freeze());
    let SourceRead::Freeze(Some(status)) = read(sources, clock, 20, SourceQuery::Freeze) else {
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
    (status, import)
}
fn verify_inactive_import(
    targets: &mut [Node<target_fixture::Target>],
    other_targets: &[Node<target_fixture::Target>],
    status: &voteboat::transfer_source::SourceFreezeStatus,
    original: &voteboat::transfer_target::TargetStatus,
    bytes: &[u8],
    clock: &Instant,
) {
    use voteboat::{transfer::ContentDigest, transfer_target::*};
    let imported = original.imported.as_ref().unwrap();
    assert_eq!(imported.digest, ContentDigest::sha256(bytes));
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
        read(targets, clock, 21, target_import_query()),
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
}
fn target_import_query() -> voteboat::transfer_target::TargetQuery<Vec<u8>> {
    use voteboat::transfer_target::TargetQuery;
    TargetQuery::Data(RoutedQuery {
        hint: target_fixture::source::hint(1),
        key: vec![1],
        query: vec![1],
    })
}
fn source_freeze_recovery(protocol: NativePeerProtocol, compact: bool) {
    let _history = NATIVE_HISTORY.lock().unwrap_or_else(|e| e.into_inner());
    use voteboat::transfer_source::*;
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
    verify_frozen_export(&nodes, original_fence, original_image);
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
fn verify_recovered_import(
    targets: &mut [Node<target_fixture::Target>],
    clock: &Instant,
    original: &voteboat::transfer_target::TargetStatus,
    bytes: Vec<u8>,
) {
    use voteboat::transfer_target::*;
    let imported = original.imported.as_ref().unwrap();
    campaign(targets, clock, 21);
    let receipt = propose(targets, clock, 21, 200, bytes);
    assert_eq!(
        receipt.outcome,
        TargetOutcome::Imported {
            index: imported.index,
            digest: imported.digest
        }
    );
    assert_eq!(
        read(targets, clock, 21, TargetQuery::Status),
        TargetRead::Status(original.clone())
    );
    assert_eq!(
        read(targets, clock, 21, target_import_query()),
        TargetRead::NotActive
    );
    for target in targets {
        let app = &target.local().applications[&group(21)];
        assert_eq!(app.application().value(&[1]), Ok(7));
        assert_eq!(app.application().outbox().count(), 1);
        assert!(app.applied_index() > imported.index);
    }
}
fn target_import_recovery(protocol: NativePeerProtocol, compact: bool) {
    let _history = NATIVE_HISTORY.lock().unwrap_or_else(|e| e.into_inner());
    use voteboat::transfer_target::*;
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
    let mut other_targets = stage_secondary_target(&root, &clock, protocol);
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
    assert_eq!(
        read(&mut targets, &clock, 21, target_import_query()),
        TargetRead::NotActive
    );
    let (status, import) = prepare_target_import(&mut sources, &clock);
    let bytes = target_fixture::fresh()
        .import_command(&import, 65536)
        .unwrap();
    // The first successful target observation is discarded; subsequent status is authoritative.
    let _ = propose(&mut targets, &clock, 21, 200, bytes.clone());
    let TargetRead::Status(original) = read(&mut targets, &clock, 21, TargetQuery::Status) else {
        panic!("target status")
    };
    verify_inactive_import(
        &mut targets,
        &other_targets,
        &status,
        &original,
        &bytes,
        &clock,
    );
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
    verify_recovered_import(&mut targets, &clock, &original, bytes);
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

#[test]
fn tcp_transfer_publication_survives_full_history_and_wal_reopen() {
    publication_recovery::run(NativePeerProtocol::TcpTls, false, false);
}
#[test]
fn tcp_transfer_publication_survives_checkpoint_and_lost_observation() {
    publication_recovery::run(NativePeerProtocol::TcpTls, true, false);
}
#[cfg(feature = "quic")]
#[test]
fn quic_transfer_publication_survives_full_history_and_wal_reopen() {
    publication_recovery::run(NativePeerProtocol::Quic, false, false);
}
#[cfg(feature = "quic")]
#[test]
fn quic_transfer_publication_survives_checkpoint_and_lost_observation() {
    publication_recovery::run(NativePeerProtocol::Quic, true, false);
}

#[test]
fn tcp_target_activation_serves_without_metadata_and_survives_wal_reopen() {
    publication_recovery::run(NativePeerProtocol::TcpTls, false, true);
}
#[test]
fn tcp_target_activation_preserves_retries_through_checkpoint_reopen() {
    publication_recovery::run(NativePeerProtocol::TcpTls, true, true);
}
#[cfg(feature = "quic")]
#[test]
fn quic_target_activation_serves_without_metadata_and_survives_wal_reopen() {
    publication_recovery::run(NativePeerProtocol::Quic, false, true);
}
#[cfg(feature = "quic")]
#[test]
fn quic_target_activation_preserves_retries_through_checkpoint_reopen() {
    publication_recovery::run(NativePeerProtocol::Quic, true, true);
}

#[path = "split.rs"]
mod split;

#[path = "merge.rs"]
mod merge;

#[path = "retirement.rs"]
mod retirement;

#[path = "delegation.rs"]
mod delegation;

#[path = "creation.rs"]
mod creation;

#[path = "creation_source.rs"]
mod creation_source;

#[path = "insertion.rs"]
mod insertion;
