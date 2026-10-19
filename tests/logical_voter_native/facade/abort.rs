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
use fixture::FacadeRecovery;
use voteboat::{identity::OperationId, outbound::OutboundQueue, worker::PersistenceWorker};

#[test]
fn logical_facade_abort_retains_original_child_attempt_and_resolves_unknown_tcp() {
    history(NativePeerProtocol::TcpTls);
}
#[cfg(feature = "quic")]
#[test]
fn logical_facade_abort_retains_original_child_attempt_and_resolves_unknown_quic() {
    history(NativePeerProtocol::Quic);
}
fn build_node(image: &VerifiedLogicalImage) -> FacadeNode {
    FacadeNode::from_parts(image, build(image), fixture::limits(), MonoTime(0))
        .unwrap_or_else(|e| panic!("actual abort facade {:?}", e.reason))
}
fn accept_capsule(bridge: &mut Bridge<'_>, node: &mut FacadeNode, clock: Instant) -> ClientTicket {
    let ticket = node
        .propose(ClientRequest {
            group: setup::mapping().parent,
            operation: OperationId::new(8990).unwrap(),
            bytes: 9_i64.to_le_bytes().to_vec(),
        })
        .unwrap();
    until(bridge, node, clock, |n| {
        let app = &n.local().applications[&setup::mapping().parent];
        app.0.read_applied(app.applied_index()) == Ok(9) && n.local().persistence.is_drained()
    });
    assert!(node.poll_client().is_none());
    node.pump_backing(|p| {
        assert_eq!(p.releases.usage().pending, 1);
        assert!(bridge.pump_release(p.releases).unwrap());
    });
    bridge
        .advance(MonoTime(clock.elapsed().as_millis() as u64), budget(0))
        .unwrap();
    assert_eq!(bridge.usage().child_attempts, 1);
    ticket
}
fn discard(recovery: &mut FacadeRecovery) -> Result<(), ReplicaError> {
    recovery
        .replica
        .discard_failed(&mut recovery.parts.local.as_parts())
}
fn settle(bridge: &mut Bridge<'_>, recovery: &mut FacadeRecovery, clock: Instant) {
    let retained = recovery.replica.release_usage();
    assert_eq!(retained.pending, 1);
    let waiting = Err(ReplicaError::Logical(LogicalReleaseError::Overloaded));
    assert_eq!(discard(recovery), waiting);
    for _ in 0..16 {
        bridge
            .advance(MonoTime(clock.elapsed().as_millis() as u64), budget(0))
            .unwrap();
        bridge.pump_release(recovery.replica.releases()).unwrap();
        assert_eq!(discard(recovery), waiting);
        assert_eq!(recovery.replica.release_usage(), retained);
    }
    let end = Instant::now() + Duration::from_secs(15);
    loop {
        bridge
            .advance(MonoTime(clock.elapsed().as_millis() as u64), budget(4))
            .unwrap();
        bridge.pump_release(recovery.replica.releases()).unwrap();
        match discard(recovery) {
            Ok(()) => break,
            Err(ReplicaError::Logical(LogicalReleaseError::Overloaded)) => (),
            Err(e) => panic!("actual accepted-work cleanup {e:?}"),
        }
        assert!(Instant::now() < end, "original child terminal must settle");
        std::thread::park_timeout(Duration::from_millis(1));
    }
    assert!(recovery.parts.local.owner.is_failed());
    assert!(recovery.replica.is_drained());
    assert_eq!(
        recovery.replica.release_usage(),
        LogicalReleaseUsage::default()
    );
    assert!(recovery.pending.is_empty());
    assert!(recovery.incoming.is_empty());
    assert!(recovery.completions.is_empty());
    assert!(recovery.parts.local.clients.poll().is_none());
    recovery.parts.local.persistence.close();
    recovery.parts.local.outbound.close();
    assert!(recovery.parts.local.persistence.is_drained());
    assert!(recovery.parts.persistence.is_drained());
    assert!(recovery.parts.local.outbound.is_drained());
    bridge.close();
    assert!(bridge.is_drained());
}
fn history(protocol: NativePeerProtocol) {
    let _serial = serial();
    let mut cohort = Cluster::new_with_application(protocol, fixture::child_application());
    let clock = cohort.clock;
    let mut bridge =
        LogicalChildBridge::new(&mut cohort.nodes, setup::mapping(), support::node(1)).unwrap();
    let image = claim_in(&mut bridge, clock, 8980, false);
    let mut node = build_node(&image);
    restart::elect(&mut bridge, &mut node, clock);
    let ticket = accept_capsule(&mut bridge, &mut node, clock);
    node.abort();
    assert_eq!(node.state(), NodeState::RecoveryRequired);
    assert_eq!(node.failure(), Some(&LogicalNodeError::Closed));
    assert!(matches!(
        node.poll(MonoTime(0), LogicalNodePollBudget::default()),
        Err(LogicalNodeError::RecoveryRequired)
    ));
    let output = node.poll_client().expect("original abort observation");
    assert_eq!(output.ticket(), ticket);
    assert!(matches!(
        node.complete_client(output).unwrap(),
        ClientOutcome::Unknown(ClientUnknown::Aborted)
    ));
    let mut recovery = node
        .into_recovery()
        .unwrap_or_else(|_| panic!("original recovery ownership"));
    assert_eq!(recovery.parts.local.owner.identity().store, image.binding());
    settle(&mut bridge, &mut recovery, clock);
    drop(bridge);
    drop(recovery);
    let mut bridge =
        LogicalChildBridge::new(&mut cohort.nodes, setup::mapping(), support::node(2)).unwrap();
    let fresh = claim_in(&mut bridge, clock, 8981, false);
    assert_ne!(fresh.binding(), image.binding());
    let mut node = build_node(&fresh);
    let app = &node.local().applications[&setup::mapping().parent];
    assert_eq!(app.0.read_applied(app.applied_index()).unwrap(), 9);
    restart::elect(&mut bridge, &mut node, clock);
    let result = restart::write(&mut bridge, &mut node, clock, 8990, 9);
    assert!(result.duplicate);
    assert_eq!(result.outcome, CounterOutcome::Value(9));
    assert_eq!(restart::read(&mut bridge, &mut node, clock), 9);
    node.close_intake().unwrap();
    until(&mut bridge, &mut node, clock, FacadeNode::is_drained);
    drop(
        node.into_parts()
            .unwrap_or_else(|_| panic!("resolved facade drain")),
    );
    bridge.close();
    assert!(bridge.is_drained());
    drop(bridge);
    cohort.finish();
}
