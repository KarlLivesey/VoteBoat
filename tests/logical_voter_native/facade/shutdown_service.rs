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
use voteboat::outbound::OutboundQueue;

fn held_client(
    bridge: &mut Bridge<'_>,
    node: &mut FacadeNode,
    clock: Instant,
) -> ClientCompletion<CounterReceipt> {
    let ticket = node
        .propose(ClientRequest {
            group: setup::mapping().parent,
            operation: Counter::operation(1, 8).unwrap(),
            bytes: Counter::command(0, 5),
        })
        .unwrap();
    let end = Instant::now() + Duration::from_secs(15);
    loop {
        step(bridge, node, clock, 4);
        if let Some(output) = node.poll_client() {
            assert_eq!(output.ticket(), ticket);
            let ClientOutcome::Applied { receipt, position } = output.outcome() else {
                panic!("original Applied output required");
            };
            assert_eq!(receipt.operation, ticket.operation);
            assert_eq!(receipt.index, position.index);
            assert_eq!(receipt.outcome, CounterOutcome::Value(5));
            return output;
        }
        assert!(Instant::now() < end, "original service write progress");
        std::thread::park_timeout(Duration::from_millis(1));
    }
}

fn held_read(
    bridge: &mut Bridge<'_>,
    node: &mut FacadeNode,
    clock: Instant,
) -> ReadCompletion<i64> {
    let before = node.local().applications[&setup::mapping().parent].read_calls();
    let required = node.local().applications[&setup::mapping().parent].applied_index();
    let ticket = node.read(setup::mapping().parent, ()).unwrap();
    let end = Instant::now() + Duration::from_secs(15);
    loop {
        step(bridge, node, clock, 4);
        if let Some(output) = node.poll_read() {
            assert_eq!(output.ticket(), ticket);
            let ReadOutcome::Read {
                barrier,
                result: Ok(5),
            } = output.outcome()
            else {
                panic!("original quorum read output required");
            };
            assert!(barrier.index() >= required);
            assert_eq!(
                node.local().applications[&setup::mapping().parent].read_calls(),
                before + 1
            );
            return output;
        }
        assert!(Instant::now() < end, "original service read progress");
        std::thread::park_timeout(Duration::from_millis(1));
    }
}

fn assert_held(node: &FacadeNode, clients: usize, reads: usize) {
    assert_eq!(node.state(), NodeState::Quiescing);
    assert!(!node.work_is_drained());
    assert!(!node.is_drained());
    assert_eq!(node.local().clients.usage().requests, clients);
    assert_eq!(node.local().reads.usage().requests, reads);
}

fn consume_client(node: &mut FacadeNode, output: ClientCompletion<CounterReceipt>) {
    assert!(matches!(
        node.complete_client(output).unwrap(),
        ClientOutcome::Applied {
            receipt: CounterReceipt {
                outcome: CounterOutcome::Value(5),
                ..
            },
            ..
        }
    ));
}

fn consume_read(node: &mut FacadeNode, output: ReadCompletion<i64>) {
    assert!(matches!(
        node.complete_read(output).unwrap(),
        ReadOutcome::Read { result: Ok(5), .. }
    ));
}

#[test]
fn logical_facade_shutdown_held_service_client_first_tcp() {
    history(NativePeerProtocol::TcpTls, true);
}
#[cfg(feature = "quic")]
#[test]
fn logical_facade_shutdown_held_service_client_first_quic() {
    history(NativePeerProtocol::Quic, true);
}

#[test]
fn logical_facade_shutdown_held_service_read_first_tcp() {
    history(NativePeerProtocol::TcpTls, false);
}
#[cfg(feature = "quic")]
#[test]
fn logical_facade_shutdown_held_service_read_first_quic() {
    history(NativePeerProtocol::Quic, false);
}

fn history(protocol: NativePeerProtocol, client_first: bool) {
    let _serial = serial();
    let mut cohort = Cluster::new_with_application(protocol, fixture::child_application());
    let clock = cohort.clock;
    let mut bridge =
        LogicalChildBridge::new(&mut cohort.nodes, setup::mapping(), support::node(1)).unwrap();
    let image = claim_in(&mut bridge, clock, 9500, false);
    let mut node = FacadeNode::from_parts(&image, build(&image), fixture::limits(), MonoTime(0))
        .unwrap_or_else(|e| panic!("actual service shutdown facade {:?}", e.reason));
    restart::elect(&mut bridge, &mut node, clock);
    let client = held_client(&mut bridge, &mut node, clock);
    let read = held_read(&mut bridge, &mut node, clock);
    let read_ticket = read.ticket();
    let inspected = node.inspect_read(read_ticket).unwrap();
    assert_eq!(inspected.ticket(), read_ticket);
    assert!(inspected.stepped() && inspected.terminal() && inspected.has_output());
    assert!(inspected.exported() && inspected.cancellation().is_none());
    let client_usage = node.local().clients.usage();
    let read_usage = node.local().reads.usage();
    assert_eq!(client_usage.requests, 1);
    assert_eq!(read_usage.requests, 1);
    assert!(client_usage.bytes > 0 && read_usage.bytes > 0);
    assert_eq!(
        node.cancel_client(client.ticket()),
        Err(ClientError::StaleTicket)
    );
    assert_eq!(
        node.cancel_read(read.ticket()),
        Err(ReadInvocationError::StaleTicket)
    );
    let before = node
        .local()
        .owner
        .core(setup::mapping().parent)
        .unwrap()
        .state()
        .clone();
    node.close_intake().unwrap();
    for _ in 0..16 {
        shutdown_step(&mut bridge, &mut node, clock);
        assert_held(&node, 1, 1);
        assert_eq!(node.local().clients.usage(), client_usage);
        assert_eq!(node.local().reads.usage(), read_usage);
        assert_eq!(
            node.local()
                .owner
                .core(setup::mapping().parent)
                .unwrap()
                .state(),
            &before
        );
        assert!(node.poll_client().is_none());
        assert!(node.poll_read().is_none());
    }
    if client_first {
        consume_client(&mut node, client);
        for _ in 0..16 {
            shutdown_step(&mut bridge, &mut node, clock);
            assert_held(&node, 0, 1);
            assert_eq!(node.local().reads.usage(), read_usage);
        }
        consume_read(&mut node, read);
    } else {
        consume_read(&mut node, read);
        for _ in 0..16 {
            shutdown_step(&mut bridge, &mut node, clock);
            assert_held(&node, 1, 0);
            assert_eq!(node.local().clients.usage(), client_usage);
        }
        consume_client(&mut node, client);
    }
    assert_eq!(
        node.inspect_read(read_ticket),
        Err(ReadInvocationError::StaleTicket)
    );
    finish(bridge, node, clock);
    cohort.finish();
}

fn finish(mut bridge: Bridge<'_>, mut node: FacadeNode, clock: Instant) {
    let end = Instant::now() + Duration::from_secs(15);
    while !node.is_drained() {
        shutdown_step(&mut bridge, &mut node, clock);
        assert!(Instant::now() < end, "original service shutdown progress");
        std::thread::park_timeout(Duration::from_millis(1));
    }
    assert_eq!(node.local().clients.usage(), ClientUsage::default());
    assert_eq!(node.local().reads.usage(), ReadInvocationUsage::default());
    assert_eq!(node.local().owner.deadline(setup::mapping().parent), None);
    let parts = node
        .into_parts()
        .unwrap_or_else(|_| panic!("original held service work must fully drain"));
    assert!(parts.persistence.is_drained());
    assert!(parts.local.owner.is_drained());
    assert!(parts.local.results.is_drained());
    assert!(parts.local.clients.is_drained());
    assert!(parts.local.reads.is_drained());
    assert!(parts.local.outbound.is_drained());
    assert!(parts.sessions.is_empty());
    bridge.close();
    assert!(bridge.is_drained());
    drop(bridge);
    drop(parts);
}

fn shutdown_step(bridge: &mut Bridge<'_>, node: &mut FacadeNode, clock: Instant) {
    node.pump_backing(|p| {
        bridge.pump_persistence(p.persistence).unwrap();
        bridge.pump_release(p.releases).unwrap();
        if let Some(snapshots) = p.snapshots {
            bridge.pump_snapshots(snapshots, p.persistence).unwrap();
        }
    });
    bridge
        .advance(MonoTime(clock.elapsed().as_millis() as u64), budget(4))
        .unwrap();
    node.pump_backing(|p| {
        bridge.pump_persistence(p.persistence).unwrap();
        bridge.pump_release(p.releases).unwrap();
        if let Some(snapshots) = p.snapshots {
            bridge.pump_snapshots(snapshots, p.persistence).unwrap();
        }
    });
    let progress = node
        .poll_shutdown(MonoTime(0), LogicalNodePollBudget::default())
        .unwrap_or_else(|r| {
            panic!(
                "original service shutdown {:?}, progress {:?}",
                r.reason, r.progress
            )
        });
    assert!(
        progress.completions.is_empty(),
        "no parent network sessions in this fixture"
    );
    assert!(progress.peer_closures.is_empty());
}
