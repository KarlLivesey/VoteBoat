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
use voteboat::identity::OperationId;

#[test]
fn logical_facade_cancelled_waits_retain_actual_child_work_tcp() {
    history(NativePeerProtocol::TcpTls);
}
#[cfg(feature = "quic")]
#[test]
fn logical_facade_cancelled_waits_retain_actual_child_work_quic() {
    history(NativePeerProtocol::Quic);
}
fn held_capsule(bridge: &mut Bridge<'_>, node: &mut FacadeNode, clock: Instant) {
    node.pump_backing(|p| {
        assert_eq!(p.releases.usage().pending, 1);
        assert!(bridge.pump_release(p.releases).unwrap());
    });
    bridge
        .advance(MonoTime(clock.elapsed().as_millis() as u64), budget(0))
        .unwrap();
    assert_eq!(bridge.usage().child_attempts, 1);
}
fn retain(bridge: &mut Bridge<'_>, node: &mut FacadeNode, clock: Instant) {
    let original = node.pump_backing(|p| p.releases.usage());
    assert_eq!(original.pending, 1);
    for _ in 0..16 {
        step(bridge, node, clock, 0);
        assert_eq!(node.state(), NodeState::Running);
        assert!(!node.work_is_drained());
        assert_eq!(node.pump_backing(|p| p.releases.usage()), original);
        assert!(node.poll_client().is_none());
        assert!(node.poll_read().is_none());
    }
    until(bridge, node, clock, FacadeNode::work_is_drained);
    assert!(node.poll_client().is_none());
    assert!(node.poll_read().is_none());
    assert_eq!(node.state(), NodeState::Running);
}
fn write_cancel(bridge: &mut Bridge<'_>, node: &mut FacadeNode, clock: Instant) {
    let ticket = node
        .propose(ClientRequest {
            group: setup::mapping().parent,
            operation: OperationId::new(8995).unwrap(),
            bytes: 9_i64.to_le_bytes().to_vec(),
        })
        .unwrap();
    until(bridge, node, clock, |n| {
        let app = &n.local().applications[&setup::mapping().parent];
        app.0.read_applied(app.applied_index()) == Ok(9)
    });
    assert!(node.poll_client().is_none());
    held_capsule(bridge, node, clock);
    node.cancel_client(ticket).unwrap();
    assert_eq!(node.cancel_client(ticket), Err(ClientError::StaleTicket));
    let output = node.poll_client().expect("original cancelled wait");
    assert_eq!(output.ticket(), ticket);
    assert!(matches!(
        node.complete_client(output).unwrap(),
        ClientOutcome::Unknown(ClientUnknown::CancelledWait)
    ));
    retain(bridge, node, clock);
    let duplicate = restart::write(bridge, node, clock, 8995, 9);
    assert!(duplicate.duplicate);
    assert_eq!(duplicate.outcome, CounterOutcome::Value(9));
    assert_eq!(restart::read(bridge, node, clock), 9);
}
fn read_cancel(bridge: &mut Bridge<'_>, node: &mut FacadeNode, clock: Instant) {
    let group = setup::mapping().parent;
    let before = node.local().applications[&group].read_calls();
    let ticket = node.read(group, ()).unwrap();
    until(bridge, node, clock, |n| {
        n.local().applications[&group].read_calls() == before + 1
    });
    assert!(node.poll_read().is_none());
    held_capsule(bridge, node, clock);
    node.cancel_read(ticket).unwrap();
    assert_eq!(
        node.cancel_read(ticket),
        Err(ReadInvocationError::StaleTicket)
    );
    let output = node.poll_read().expect("original cancelled read wait");
    assert_eq!(output.ticket(), ticket);
    assert!(matches!(
        node.complete_read(output).unwrap(),
        ReadOutcome::Unavailable(ReadUnavailable::Cancelled)
    ));
    retain(bridge, node, clock);
    assert_eq!(node.local().applications[&group].read_calls(), before + 1);
    assert_eq!(restart::read(bridge, node, clock), 9);
    assert_eq!(node.local().applications[&group].read_calls(), before + 2);
}
fn history(protocol: NativePeerProtocol) {
    let _serial = serial();
    let mut cohort = Cluster::new_with_application(protocol, fixture::child_application());
    let clock = cohort.clock;
    let mut bridge =
        LogicalChildBridge::new(&mut cohort.nodes, setup::mapping(), support::node(1)).unwrap();
    let image = claim_in(&mut bridge, clock, 8994, false);
    let mut node = FacadeNode::from_parts(&image, build(&image), fixture::limits(), MonoTime(0))
        .unwrap_or_else(|e| panic!("actual cancellation facade {:?}", e.reason));
    restart::elect(&mut bridge, &mut node, clock);
    write_cancel(&mut bridge, &mut node, clock);
    read_cancel(&mut bridge, &mut node, clock);
    node.heartbeat().unwrap();
    until(&mut bridge, &mut node, clock, FacadeNode::work_is_drained);
    node.close_intake().unwrap();
    until(&mut bridge, &mut node, clock, FacadeNode::is_drained);
    drop(
        node.into_parts()
            .unwrap_or_else(|_| panic!("original cancellation ownership drains")),
    );
    bridge.close();
    assert!(bridge.is_drained());
    drop(bridge);
    cohort.finish();
}
