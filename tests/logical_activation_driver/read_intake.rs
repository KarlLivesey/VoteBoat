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
//! Genuine sequential quorum reads with externally held invocation outputs.
use super::*;
use voteboat::application::CounterReceipt;
type Output = ReadCompletion<ActivationRead<i64>>;
#[test]
fn held_ordinary_read_quota_preserves_active_status_and_exact_cleanup_tcp() {
    history(NativePeerProtocol::TcpTls);
}
#[cfg(feature = "quic")]
#[test]
fn held_ordinary_read_quota_preserves_active_status_and_exact_cleanup_quic() {
    history(NativePeerProtocol::Quic);
}
fn history(protocol: NativePeerProtocol) {
    let _serial = crate::history_guard();
    let mut cohort = cluster::Cluster::simulated(protocol, fixture::child_application());
    let clock = &cohort.clock.clone();
    let mut bridge =
        LogicalChildBridge::new(&mut cohort.nodes, setup::mapping(), support::node(1)).unwrap();
    let image = drive::claim_in(&mut bridge, clock, 330_201, false);
    let mut parts = fixture::build(&image);
    let routers: LogicalRouterParts<
        ActivationReceipt<CounterReceipt>,
        ActivationQuery<()>,
        ActivationRead<i64>,
    > = image.routers(&parts.local.owner, limits()).unwrap();
    parts.local.reads = routers.reads;
    let mut node = fixture::FacadeNode::from_parts(&image, parts, fixture::limits(), MonoTime(0))
        .unwrap_or_else(|e| panic!("actual selected parent {:?}", e.reason));
    progress::elect(&mut bridge, &mut node, clock);
    let first = read(
        &mut bridge,
        &mut node,
        clock,
        ActivationQuery::Business(()),
        ActivationRead::Business(0),
    );
    let second = read(
        &mut bridge,
        &mut node,
        clock,
        ActivationQuery::Business(()),
        ActivationRead::Business(0),
    );
    assert_eq!(node.local().reads.usage().requests, 2);
    refusal(&mut node, ActivationQuery::Business(()));
    let active = read(
        &mut bridge,
        &mut node,
        clock,
        ActivationQuery::Active,
        ActivationRead::Active(None),
    );
    assert_eq!(node.local().reads.usage().requests, 3);
    refusal(&mut node, ActivationQuery::Business(()));
    let status = read(
        &mut bridge,
        &mut node,
        clock,
        ActivationQuery::Status(OperationId::new(330_210).unwrap()),
        ActivationRead::Status(None),
    );
    assert_eq!(node.local().reads.usage().requests, 4);
    refusal(&mut node, ActivationQuery::Active);
    for output in [active, status, first, second] {
        assert!(matches!(
            node.complete_read(output).unwrap(),
            ReadOutcome::Read { result: Ok(_), .. }
        ));
    }
    assert!(node.local().reads.is_drained());
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
fn limits() -> LogicalRouterLimits {
    let mut limits = fixture::router_limits();
    limits.invocations = ReadInvocationLimits {
        requests: 4,
        group_requests: 4,
        bytes: 128 * 1024,
        query_bytes: 1024,
        result_bytes: 8192,
        control_requests: 2,
        group_control_requests: 2,
        control_bytes: 64 * 1024,
    };
    limits.execution = ReadRouterLimits {
        results: 4,
        group_results: 4,
        bytes: 128 * 1024,
        query_bytes: 1024,
        result_bytes: 8192,
        control_results: 2,
        group_control_results: 2,
        control_bytes: 64 * 1024,
    };
    limits
}
fn read(
    bridge: &mut aliases::Bridge<'_>,
    node: &mut fixture::FacadeNode,
    clock: &harness::SimClock,
    query: ActivationQuery<()>,
    expected: ActivationRead<i64>,
) -> Output {
    let ticket = node.read(setup::mapping().parent, query).unwrap();
    let end = harness::Wait::new(clock, harness::LIVENESS_MS);
    loop {
        progress::step(bridge, node, clock, 4);
        if let Some(output) = node.poll_read() {
            assert_eq!(output.ticket(), ticket);
            match output.outcome() {
                ReadOutcome::Read { barrier, result } => {
                    assert_eq!(barrier.group(), setup::mapping().parent);
                    assert_eq!(barrier.request(), ticket.request);
                    assert!(barrier.index() > 0);
                    assert_eq!(result, &Ok(expected));
                }
                other => panic!("original fresh quorum read {other:?}"),
            }
            return output;
        }
        assert!(end.within(clock), "original selected read progress");
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
fn refusal(node: &mut fixture::FacadeNode, query: ActivationQuery<()>) {
    let usage = node.local().reads.usage();
    let rejected = node
        .read(setup::mapping().parent, query.clone())
        .unwrap_err();
    assert_eq!(rejected.reason, ReadInvocationError::Overloaded);
    assert_eq!(rejected.group, setup::mapping().parent);
    assert_eq!(rejected.query, query);
    assert_eq!(node.local().reads.usage(), usage);
}
