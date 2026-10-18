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
//! Selected control progress while original ordinary Applied outputs are held.
use super::*;
use voteboat::application::{CounterOutcome, CounterReceipt};
type Output = ClientCompletion<ActivationReceipt<CounterReceipt>>;
#[test]
fn held_ordinary_client_quota_preserves_begin_retry_and_fresh_status_tcp() {
    history(NativePeerProtocol::TcpTls);
}
#[cfg(feature = "quic")]
#[test]
fn held_ordinary_client_quota_preserves_begin_retry_and_fresh_status_quic() {
    history(NativePeerProtocol::Quic);
}
fn history(protocol: NativePeerProtocol) {
    let _serial = crate::history_guard();
    let mut cohort = cluster::Cluster::simulated(protocol, fixture::child_application());
    let clock = &cohort.clock.clone();
    let mut bridge =
        LogicalChildBridge::new(&mut cohort.nodes, setup::mapping(), support::node(1)).unwrap();
    let image = drive::claim_in(&mut bridge, clock, 330_001, false);
    let mut parts = fixture::build(&image);
    let mut limits = fixture::router_limits();
    limits.clients = ClientRouterLimits {
        requests: 4,
        group_requests: 4,
        bytes: 128 * 1024,
        command_bytes: 1024,
        receipt_bytes: 8192,
        control_requests: 2,
        group_control_requests: 2,
        control_bytes: 64 * 1024,
    };
    let routers: LogicalRouterParts<
        ActivationReceipt<CounterReceipt>,
        ActivationQuery<()>,
        ActivationRead<i64>,
    > = image.routers(&parts.local.owner, limits).unwrap();
    parts.local.clients = routers.clients;
    let mut node = fixture::FacadeNode::from_parts(&image, parts, fixture::limits(), MonoTime(0))
        .unwrap_or_else(|e| panic!("actual selected parent {:?}", e.reason));
    progress::elect(&mut bridge, &mut node, clock);
    let first = business(&mut bridge, &mut node, clock, 330_010, 2, 2);
    let second = business(&mut bridge, &mut node, clock, 330_011, 3, 5);
    assert_eq!(node.local().clients.usage().requests, 2);
    ordinary_refusal(&mut node);
    let intent = super::history::intent(330_100);
    let first_position = begin(&mut bridge, &mut node, clock, &intent, false);
    assert_eq!(node.local().clients.usage().requests, 2);
    fresh_status(&mut bridge, &mut node, clock, &intent, first_position);
    assert_eq!(node.local().clients.usage().requests, 2);
    assert_eq!(
        begin(&mut bridge, &mut node, clock, &intent, true),
        first_position
    );
    assert_eq!(node.local().clients.usage().requests, 2);
    ordinary_refusal(&mut node);
    node.complete_client(first).unwrap();
    node.complete_client(second).unwrap();
    assert!(node.local().clients.is_drained());
    node.close_intake().unwrap();
    progress::drain(&mut bridge, &mut node, clock);
    bridge.close();
    assert!(bridge.is_drained());
    drop(bridge);
    let parts = node
        .into_parts()
        .unwrap_or_else(|_| panic!("original facade drain"));
    assert!(parts.persistence.is_drained());
    drop(parts);
    cohort.finish();
}
fn wait(
    bridge: &mut aliases::Bridge<'_>,
    node: &mut fixture::FacadeNode,
    clock: &harness::SimClock,
    ticket: ClientTicket,
) -> Output {
    let end = harness::Wait::new(clock, harness::LIVENESS_MS);
    loop {
        progress::step(bridge, node, clock, 4);
        if let Some(output) = node.poll_client() {
            assert_eq!(output.ticket(), ticket);
            return output;
        }
        assert!(end.within(clock), "original client outcome");
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
fn business(
    bridge: &mut aliases::Bridge<'_>,
    node: &mut fixture::FacadeNode,
    clock: &harness::SimClock,
    operation: u128,
    delta: i64,
    value: i64,
) -> Output {
    let operation = OperationId::new(operation).unwrap();
    let ticket = node
        .propose(ClientRequest {
            group: setup::mapping().parent,
            operation,
            bytes: fixture::app().business(&delta.to_le_bytes()).unwrap(),
        })
        .unwrap();
    let output = wait(bridge, node, clock, ticket);
    match output.outcome() {
        ClientOutcome::Applied {
            position,
            receipt: ActivationReceipt::Business(receipt),
        } => {
            assert_eq!(receipt.operation, operation);
            assert_eq!(receipt.index, position.index);
            assert_eq!(receipt.outcome, CounterOutcome::Value(value));
            assert!(!receipt.duplicate);
        }
        other => panic!("original business output {other:?}"),
    }
    output
}
fn ordinary_refusal(node: &mut fixture::FacadeNode) {
    let bytes = fixture::app().business(&1_i64.to_le_bytes()).unwrap();
    let pointer = bytes.as_ptr();
    let capacity = bytes.capacity();
    let request = ClientRequest {
        group: setup::mapping().parent,
        operation: OperationId::new(330_012).unwrap(),
        bytes,
    };
    let usage = node.local().clients.usage();
    let rejected = node.propose(request).unwrap_err();
    assert_eq!(rejected.reason, ClientError::Overloaded);
    assert_eq!(rejected.request.bytes.as_ptr(), pointer);
    assert_eq!(rejected.request.bytes.capacity(), capacity);
    assert_eq!(
        rejected.request.bytes,
        fixture::app().business(&1_i64.to_le_bytes()).unwrap()
    );
    assert_eq!(node.local().clients.usage(), usage);
}
fn begin(
    bridge: &mut aliases::Bridge<'_>,
    node: &mut fixture::FacadeNode,
    clock: &harness::SimClock,
    intent: &ChildActivationIntent,
    duplicate: bool,
) -> ActivationPosition {
    let ticket = node
        .propose(ClientRequest {
            group: setup::mapping().parent,
            operation: intent.operations().begin,
            bytes: fixture::app().begin(intent).unwrap(),
        })
        .unwrap();
    assert_eq!(node.local().clients.usage().requests, 3);
    let output = wait(bridge, node, clock, ticket);
    match node.complete_client(output).unwrap() {
        ClientOutcome::Applied {
            position,
            receipt: ActivationReceipt::Control(receipt),
        } => {
            assert_eq!(receipt.operation, intent.operations().begin);
            assert_eq!(receipt.index, position.index);
            assert_eq!(receipt.outcome, ChildActivationOutcome::Begun);
            assert_eq!(receipt.duplicate, duplicate);
            if !duplicate {
                assert_eq!(
                    receipt.first,
                    ActivationPosition {
                        index: position.index,
                        term: position.term
                    }
                );
            }
            receipt.first
        }
        other => panic!("original Begin output {other:?}"),
    }
}
fn fresh_status(
    bridge: &mut aliases::Bridge<'_>,
    node: &mut fixture::FacadeNode,
    clock: &harness::SimClock,
    intent: &ChildActivationIntent,
    first: ActivationPosition,
) {
    let ticket = node
        .read(
            setup::mapping().parent,
            ActivationQuery::Status(intent.operations().begin),
        )
        .unwrap();
    let end = harness::Wait::new(clock, harness::LIVENESS_MS);
    loop {
        progress::step(bridge, node, clock, 4);
        if let Some(output) = node.poll_read() {
            assert_eq!(output.ticket(), ticket);
            match node.complete_read(output).unwrap() {
                ReadOutcome::Read {
                    barrier,
                    result: Ok(ActivationRead::Status(Some(status))),
                } => {
                    assert_eq!(barrier.group(), setup::mapping().parent);
                    assert_eq!(barrier.request(), ticket.request);
                    assert!(barrier.index() >= first.index);
                    assert_eq!(status.intent, *intent);
                    assert_eq!(status.begin.first, first);
                    assert!(status.outcome.is_none());
                    assert!(status.release.is_none());
                    return;
                }
                other => panic!("original fresh Status {other:?}"),
            }
        }
        assert!(end.within(clock), "original fresh Status progress");
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
