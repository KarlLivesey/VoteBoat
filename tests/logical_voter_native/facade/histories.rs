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
pub fn close_inflight(bridge: &mut Bridge<'_>, node: &mut FacadeNode, clock: Instant) {
    let ticket = node
        .propose(ClientRequest {
            group: setup::mapping().parent,
            operation: Counter::operation(1, 12).unwrap(),
            bytes: Counter::command(0, 3),
        })
        .unwrap();
    for _ in 0..16 {
        step(bridge, node, clock, 0);
        assert!(node.poll_client().is_none());
    }
    node.close_intake().unwrap();
    assert_eq!(node.state(), NodeState::Quiescing);
    assert!(!node.work_is_drained());
    let rejected = node
        .propose(ClientRequest {
            group: setup::mapping().parent,
            operation: Counter::operation(1, 11).unwrap(),
            bytes: Counter::command(0, 9),
        })
        .unwrap_err();
    assert_eq!(rejected.reason, ClientError::Closed);
    assert_eq!(rejected.request.bytes, Counter::command(0, 9));
    let end = Instant::now() + Duration::from_secs(15);
    loop {
        step(bridge, node, clock, 4);
        if let Some(output) = node.poll_client() {
            assert_eq!(output.ticket(), ticket);
            assert!(matches!(
                node.complete_client(output).unwrap(),
                ClientOutcome::Applied {
                    receipt: CounterReceipt {
                        outcome: CounterOutcome::Value(10),
                        ..
                    },
                    ..
                }
            ));
            break;
        }
        assert!(
            Instant::now() < end,
            "accepted work must survive intake close"
        );
    }
}
pub fn requests(bridge: &mut Bridge<'_>, node: &mut FacadeNode, clock: Instant) {
    let operation = Counter::operation(1, 10).unwrap();
    let ticket = node
        .propose(ClientRequest {
            group: setup::mapping().parent,
            operation,
            bytes: Counter::command(0, 7),
        })
        .unwrap();
    for _ in 0..32 {
        step(bridge, node, clock, 0);
        assert!(node.poll_client().is_none());
        assert!(!node.work_is_drained());
    }
    let end = Instant::now() + Duration::from_secs(15);
    loop {
        step(bridge, node, clock, 4);
        if let Some(output) = node.poll_client() {
            assert_eq!(output.ticket(), ticket);
            match node.complete_client(output).unwrap() {
                ClientOutcome::Applied { receipt, position } => {
                    assert_eq!(receipt.outcome, CounterOutcome::Value(7));
                    assert_eq!(receipt.operation, operation);
                    assert_eq!(position.index, receipt.index);
                }
                other => panic!("actual original client result {other:?}"),
            }
            break;
        }
        assert!(Instant::now() < end);
    }
    until(bridge, node, clock, FacadeNode::work_is_drained);
    let read = node.read(setup::mapping().parent, ()).unwrap();
    let before = node.local().applications[&setup::mapping().parent].read_calls();
    for _ in 0..32 {
        step(bridge, node, clock, 0);
        assert!(node.poll_read().is_none());
    }
    loop {
        step(bridge, node, clock, 4);
        if let Some(output) = node.poll_read() {
            assert_eq!(output.ticket(), read);
            assert!(matches!(
                node.complete_read(output).unwrap(),
                ReadOutcome::Read { result: Ok(7), .. }
            ));
            break;
        }
        assert!(Instant::now() < end);
    }
    assert_eq!(
        node.local().applications[&setup::mapping().parent].read_calls(),
        before + 1
    );
    until(bridge, node, clock, FacadeNode::work_is_drained);
}
