// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Original result gates over an actual three-node child cohort. Parent peer
//! replies are trusted local controls derived from emitted requests, NOT an
//! authenticated complete parent quorum or nine-owner native network history.
use super::{
    aliases::Bridge,
    cluster::{Cluster, HISTORY},
    drive::claim_in,
    parent::Parent,
    setup, support,
};
use std::time::{Duration, Instant};
use voteboat::{
    application::*, identity::*, logical_voter::*, native::connect::NativePeerProtocol,
    outbound::OutboundQueue, raft::*, runtime::*, worker::PersistenceWorker,
};
fn poll(parent: &mut Parent) -> Result<ReplicaProgress, ReplicaError> {
    parent.driver.poll(
        &mut ReplicaParts {
            owner: &mut parent.owner,
            persistence: &mut parent.worker,
            applications: &mut parent.apps,
            results: &mut parent.routers.results,
            clients: &mut parent.routers.clients,
            reads: &mut parent.routers.reads,
            outbound: &mut parent.outbound,
            snapshots: None,
        },
        MonoTime(0),
        ReplicaPollBudget::default(),
    )
}
fn reply(parent: &mut Parent, original: Message) {
    assert!(matches!(original.to.get(), 12 | 13));
    let rpc = match &original.rpc {
        Rpc::Vote { .. } => Rpc::Voted { granted: true },
        Rpc::Append {
            previous_index,
            entries,
            ..
        } => Rpc::Appended {
            success: true,
            matching_index: entries.last().map_or(*previous_index, |e| e.index),
        },
        Rpc::ReadProbe => Rpc::ReadAck,
        _ => panic!("unsupported local parent peer control: {:?}", original.rpc),
    };
    parent
        .owner
        .admit(
            original.group,
            Event::Receive(Message {
                group: original.group,
                configuration: original.configuration,
                from: original.to,
                sender: StoreBinding {
                    identity: support::identity(u128::from(original.to.get()) + 90),
                    session: StoreSession::new(1).unwrap(),
                },
                to: original.from,
                term: original.term,
                context: original.context,
                rpc,
            }),
        )
        .unwrap();
}
fn pump(
    bridge: &mut Bridge<'_>,
    parent: &mut Parent,
    clock: Instant,
    completions: usize,
) -> Result<ReplicaProgress, ReplicaError> {
    bridge.pump_persistence(&mut parent.port).unwrap();
    bridge.pump_release(parent.driver.releases()).unwrap();
    bridge
        .advance(
            MonoTime(clock.elapsed().as_millis() as u64),
            LogicalBridgeBudget {
                node_polls: 3,
                completions,
                transitions: 1,
                node: NodePollBudget::default(),
            },
        )
        .unwrap();
    bridge.pump_persistence(&mut parent.port).unwrap();
    bridge.pump_release(parent.driver.releases()).unwrap();
    let progress = poll(parent)?;
    assert!(
        progress.steps.iter().all(|s| s.error.is_none()),
        "{:?}",
        progress.steps
    );
    for message in parent.take_sends() {
        reply(parent, message);
    }
    Ok(progress)
}
fn until(
    bridge: &mut Bridge<'_>,
    parent: &mut Parent,
    clock: Instant,
    mut done: impl FnMut(&Parent) -> bool,
) {
    let end = Instant::now() + Duration::from_secs(15);
    while !done(parent) {
        pump(bridge, parent, clock, 4).unwrap();
        assert!(
            Instant::now() < end,
            "original parent result progress timeout"
        );
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
fn elect(bridge: &mut Bridge<'_>, parent: &mut Parent, clock: Instant) {
    parent
        .owner
        .admit(setup::mapping().parent, Event::Campaign)
        .unwrap();
    until(bridge, parent, clock, |p| {
        let core = p.owner.core(setup::mapping().parent).unwrap();
        core.role() == Role::Leader
            && core.state().commit_index == 1
            && p.apps[&setup::mapping().parent].applied_index() == 1
            && p.is_drained()
    });
    assert!(bridge.is_drained());
}
fn submit(parent: &mut Parent) -> ClientTicket {
    parent
        .routers
        .clients
        .submit(
            &mut parent.owner,
            &parent.apps[&setup::mapping().parent],
            ClientRequest {
                group: setup::mapping().parent,
                operation: OperationId::new(8201).unwrap(),
                bytes: 5_i64.to_le_bytes().to_vec(),
            },
        )
        .unwrap()
}
fn hold_client(bridge: &mut Bridge<'_>, parent: &mut Parent, clock: Instant) {
    let client = parent.routers.clients.usage();
    let result = parent.routers.results.usage();
    let release = parent.driver.release_usage();
    assert_eq!(client.requests, 1);
    assert_eq!(result.receipts, 1);
    assert_eq!(release.pending, 1);
    assert!(bridge.pump_release(parent.driver.releases()).unwrap());
    for _ in 0..32 {
        pump(bridge, parent, clock, 0).unwrap();
        assert!(parent.routers.clients.poll().is_none());
        assert_eq!(parent.routers.clients.usage(), client);
        assert_eq!(parent.routers.results.usage(), result);
        assert_eq!(parent.driver.release_usage(), release);
        assert_eq!(parent.apps[&setup::mapping().parent].applied_index(), 2);
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
fn client_output(
    bridge: &mut Bridge<'_>,
    parent: &mut Parent,
    clock: Instant,
    ticket: ClientTicket,
) -> ClientCompletion<CounterReceipt> {
    let end = Instant::now() + Duration::from_secs(15);
    loop {
        pump(bridge, parent, clock, 4).unwrap();
        if let Some(output) = parent.routers.clients.poll() {
            assert_eq!(output.ticket(), ticket);
            return output;
        }
        assert!(Instant::now() < end, "original client capsule must settle");
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
fn consume_client(parent: &mut Parent, output: ClientCompletion<CounterReceipt>) {
    let original = output.ticket();
    let charges = parent.routers.clients.usage();
    let mut foreign = ClientRouter::new(
        ClientRouterBinding::new(
            RuntimeAuthorization::physical(parent.owner.identity()),
            ClientRouterGeneration::new(2).unwrap(),
        ),
        ClientRouterLimits::default(),
    )
    .unwrap();
    let rejected = foreign.complete(output).unwrap_err();
    assert_eq!(rejected.reason, ClientError::StaleTicket);
    assert_eq!(rejected.completion.ticket(), original);
    assert_eq!(parent.routers.clients.usage(), charges);
    assert_eq!(foreign.usage(), ClientUsage::default());
    let ClientOutcome::Applied { position, receipt } = parent
        .routers
        .clients
        .complete(*rejected.completion)
        .unwrap()
    else {
        panic!("original authorized client result missing")
    };
    assert_eq!(position, ProposalPosition { index: 2, term: 1 });
    assert_eq!(receipt.operation, original.operation);
    assert_eq!(receipt.index, 2);
    assert_eq!(receipt.outcome, CounterOutcome::Value(5));
    assert!(!receipt.duplicate);
    assert_eq!(parent.routers.clients.usage(), ClientUsage::default());
}
fn client_phase(bridge: &mut Bridge<'_>, parent: &mut Parent, clock: Instant) {
    let ticket = submit(parent);
    until(bridge, parent, clock, |p| {
        p.apps[&setup::mapping().parent].applied_index() == 2
    });
    assert!(parent.routers.clients.poll().is_none());
    hold_client(bridge, parent, clock);
    let output = client_output(bridge, parent, clock, ticket);
    consume_client(parent, output);
    until(bridge, parent, clock, Parent::is_drained);
    assert!(bridge.is_drained());
}
fn read_submit(parent: &mut Parent) -> ReadInvocationTicket {
    parent
        .routers
        .reads
        .submit(
            &mut parent.owner,
            &parent.apps[&setup::mapping().parent],
            setup::mapping().parent,
            (),
        )
        .unwrap()
}
fn hold_read(bridge: &mut Bridge<'_>, parent: &mut Parent, clock: Instant) {
    let charges = parent.routers.reads.usage();
    let release = parent.driver.release_usage();
    assert_eq!(charges.requests, 1);
    assert_eq!(release.pending, 1);
    assert!(bridge.pump_release(parent.driver.releases()).unwrap());
    for _ in 0..32 {
        pump(bridge, parent, clock, 0).unwrap();
        assert!(parent.routers.reads.poll().is_none());
        assert_eq!(parent.routers.reads.usage(), charges);
        assert_eq!(parent.driver.release_usage(), release);
        assert_eq!(parent.apps[&setup::mapping().parent].read_calls(), 1);
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
fn read_output(
    bridge: &mut Bridge<'_>,
    parent: &mut Parent,
    clock: Instant,
    ticket: ReadInvocationTicket,
) -> ReadCompletion<i64> {
    let end = Instant::now() + Duration::from_secs(15);
    loop {
        pump(bridge, parent, clock, 4).unwrap();
        if let Some(output) = parent.routers.reads.poll() {
            assert_eq!(output.ticket(), ticket);
            return output;
        }
        assert!(Instant::now() < end, "original read capsule must settle");
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
fn consume_read(parent: &mut Parent, output: ReadCompletion<i64>) {
    let original = output.ticket();
    let charges = parent.routers.reads.usage();
    let auth = RuntimeAuthorization::physical(parent.owner.identity());
    let execution = ReadRouter::new(
        ReadRouterBinding::new(auth, ReadRouterGeneration::new(2).unwrap()),
        ReadRouterLimits::default(),
    )
    .unwrap();
    let mut foreign = ReadRequests::<(), i64>::new(
        ReadInvocationBinding::new(auth, ReadInvocationGeneration::new(2).unwrap()),
        ReadInvocationLimits::default(),
        execution,
    )
    .unwrap();
    let rejected = foreign.complete(output).unwrap_err();
    assert_eq!(rejected.reason, ReadInvocationError::StaleTicket);
    assert_eq!(rejected.completion.ticket(), original);
    assert_eq!(parent.routers.reads.usage(), charges);
    assert_eq!(foreign.usage(), ReadInvocationUsage::default());
    let ReadOutcome::Read {
        barrier,
        result: Ok(value),
    } = parent.routers.reads.complete(*rejected.completion).unwrap()
    else {
        panic!("original authorized read result missing")
    };
    assert_eq!(barrier.group(), original.group);
    assert_eq!(barrier.request(), original.request);
    assert_eq!(barrier.index(), 2);
    assert_eq!(barrier.term(), 1);
    assert_eq!(value, 5);
    assert_eq!(parent.routers.reads.usage(), ReadInvocationUsage::default());
    assert_eq!(parent.apps[&setup::mapping().parent].read_calls(), 1);
}
fn history(protocol: NativePeerProtocol) {
    let _serial = HISTORY.lock().unwrap();
    let mut cluster = Cluster::new(protocol);
    let clock = cluster.clock;
    let mut bridge =
        LogicalChildBridge::new(&mut cluster.nodes, setup::mapping(), support::node(1)).unwrap();
    let image = claim_in(&mut bridge, clock, 8200, false);
    let mut parent = Parent::new(&image);
    elect(&mut bridge, &mut parent, clock);
    client_phase(&mut bridge, &mut parent, clock);
    let ticket = read_submit(&mut parent);
    until(&mut bridge, &mut parent, clock, |p| {
        p.apps[&setup::mapping().parent].read_calls() == 1
    });
    assert!(parent.routers.reads.poll().is_none());
    hold_read(&mut bridge, &mut parent, clock);
    let output = read_output(&mut bridge, &mut parent, clock, ticket);
    consume_read(&mut parent, output);
    until(&mut bridge, &mut parent, clock, Parent::is_drained);
    assert!(bridge.is_drained());
    bridge.close();
    drop(bridge);
    parent.close();
    cluster.finish();
}
#[test]
fn tcp_original_client_and_read_results_wait_for_consumed_child_capsules() {
    history(NativePeerProtocol::TcpTls);
}
#[cfg(feature = "quic")]
#[test]
fn quic_original_client_and_read_results_wait_for_consumed_child_capsules() {
    history(NativePeerProtocol::Quic);
}

fn discard(parent: &mut Parent) {
    parent
        .driver
        .discard_failed(&mut ReplicaParts {
            owner: &mut parent.owner,
            persistence: &mut parent.worker,
            applications: &mut parent.apps,
            results: &mut parent.routers.results,
            clients: &mut parent.routers.clients,
            reads: &mut parent.routers.reads,
            outbound: &mut parent.outbound,
            snapshots: None,
        })
        .unwrap();
}
#[test]
fn defective_bounded_read_codec_fences_once_and_preserves_original_nonpositive_completion() {
    let _serial = HISTORY.lock().unwrap();
    let mut cluster = Cluster::new(NativePeerProtocol::TcpTls);
    let clock = cluster.clock;
    let mut bridge =
        LogicalChildBridge::new(&mut cluster.nodes, setup::mapping(), support::node(1)).unwrap();
    let image = claim_in(&mut bridge, clock, 8300, false);
    let mut parent = Parent::new(&image);
    elect(&mut bridge, &mut parent, clock);
    client_phase(&mut bridge, &mut parent, clock);
    parent.apps[&setup::mapping().parent].fail_read_encoding();
    let ticket = read_submit(&mut parent);
    let end = Instant::now() + Duration::from_secs(15);
    let error = loop {
        if let Err(error) = pump(&mut bridge, &mut parent, clock, 4) {
            break error;
        }
        assert!(parent.routers.reads.poll().is_none());
        assert!(
            Instant::now() < end,
            "defective original encoder must be refused"
        );
        std::thread::park_timeout(Duration::from_millis(1));
    };
    assert_eq!(
        error,
        ReplicaError::Logical(LogicalReleaseError::Application(
            ApplicationError::ReceiptBudget
        ))
    );
    assert!(parent.owner.is_failed());
    assert_eq!(parent.apps[&setup::mapping().parent].read_calls(), 1);
    assert_eq!(
        parent.driver.release_usage(),
        LogicalReleaseUsage::default()
    );
    consume_failed_read(&mut parent, ticket);
    discard(&mut parent);
    assert_eq!(parent.owner.usage(), EffectOwnerUsage::default());
    assert!(parent.driver.is_drained());
    assert!(bridge.is_drained());
    parent.worker.close();
    for batch in parent.outbound.poll(8) {
        assert!(batch
            .messages
            .iter()
            .all(|m| matches!(m.rpc, Rpc::ReadProbe)));
        let cancelled = parent
            .outbound
            .complete(batch, voteboat::outbound::LocalSendResult::Cancelled)
            .unwrap();
        assert_eq!(
            cancelled.result,
            voteboat::outbound::LocalSendResult::Cancelled
        );
    }
    parent.outbound.close();
    assert!(parent.worker.is_drained());
    assert!(parent.port.is_drained());
    assert!(parent.outbound.is_drained());
    assert!(parent.routers.clients.is_drained());
    assert!(parent.routers.results.is_drained());
    assert!(parent.routers.reads.is_drained());
    bridge.close();
    drop(bridge);
    cluster.finish();
}

fn consume_failed_read(parent: &mut Parent, ticket: ReadInvocationTicket) {
    for _ in 0..4 {
        assert_eq!(poll(parent).unwrap_err(), ReplicaError::Fenced);
        assert_eq!(parent.apps[&setup::mapping().parent].read_calls(), 1);
    }
    let output = parent
        .routers
        .reads
        .poll()
        .expect("original unavailable completion");
    assert_eq!(output.ticket(), ticket);
    assert!(matches!(
        parent.routers.reads.complete(output).unwrap(),
        ReadOutcome::Unavailable(ReadUnavailable::OwnerFailed)
    ));
    assert_eq!(parent.routers.reads.usage(), ReadInvocationUsage::default());
}
