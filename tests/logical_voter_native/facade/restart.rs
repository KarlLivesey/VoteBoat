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
use voteboat::{
    identity::{OperationId, StoreBinding},
    log::GroupLog,
    native::startup::NativeOpenMode,
    snapshot_worker::SnapshotWorker,
};

#[test]
fn logical_facade_checkpoint_suffix_cold_backing_recovery_tcp() {
    history(NativePeerProtocol::TcpTls);
}
#[cfg(feature = "quic")]
#[test]
fn logical_facade_checkpoint_suffix_cold_backing_recovery_quic() {
    history(NativePeerProtocol::Quic);
}

fn node(image: &VerifiedLogicalImage) -> FacadeNode {
    FacadeNode::from_parts(
        image,
        fixture::attach_snapshots(build(image), image),
        fixture::limits(),
        MonoTime(0),
    )
    .unwrap_or_else(|e| panic!("actual checkpoint facade {:?}", e.reason))
}
fn state(node: &FacadeNode) -> &GroupLog {
    node.local()
        .owner
        .core(setup::mapping().parent)
        .unwrap()
        .state()
}
pub(super) fn elect(bridge: &mut Bridge<'_>, node: &mut FacadeNode, clock: Instant) {
    node.campaign().unwrap();
    until(bridge, node, clock, |n| {
        n.local()
            .owner
            .core(setup::mapping().parent)
            .unwrap()
            .role()
            == Role::Leader
            && state(n).term_at(state(n).commit_index) == Some(state(n).hard_state.term)
            && n.work_is_drained()
    });
}
pub(super) fn write(
    bridge: &mut Bridge<'_>,
    node: &mut FacadeNode,
    clock: Instant,
    operation: u128,
    delta: i64,
) -> CounterReceipt {
    let ticket = node
        .propose(ClientRequest {
            group: setup::mapping().parent,
            operation: OperationId::new(operation).unwrap(),
            bytes: delta.to_le_bytes().to_vec(),
        })
        .unwrap();
    let end = Instant::now() + Duration::from_secs(15);
    loop {
        step(bridge, node, clock, 4);
        if let Some(output) = node.poll_client() {
            assert_eq!(output.ticket(), ticket);
            let ClientOutcome::Applied { receipt, position } =
                node.complete_client(output).unwrap()
            else {
                panic!("original applied receipt required")
            };
            assert_eq!(receipt.operation, OperationId::new(operation).unwrap());
            assert_eq!(position.index, receipt.index);
            until(bridge, node, clock, FacadeNode::work_is_drained);
            return receipt;
        }
        assert!(Instant::now() < end, "original facade write timeout");
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
fn checkpoint(bridge: &mut Bridge<'_>, node: &mut FacadeNode, clock: Instant) {
    let before = state(node).clone();
    node.checkpoint().unwrap();
    // Publish owns an original request. Withholding its child observation cannot
    // bind a parent checkpoint or reclaim the parent's earlier log entries.
    until(bridge, node, clock, |n| {
        n.local()
            .snapshots
            .as_ref()
            .unwrap()
            .worker
            .usage()
            .requests
            == 1
    });
    for _ in 0..16 {
        step(bridge, node, clock, 0);
        assert_eq!(state(node).snapshot, before.snapshot);
        assert_eq!(state(node).base_index(), before.base_index());
        assert_eq!(
            node.local()
                .snapshots
                .as_ref()
                .unwrap()
                .worker
                .usage()
                .requests,
            1
        );
    }
    until(bridge, node, clock, |n| {
        state(n).snapshot.is_some() && n.work_is_drained()
    });
    let reference = state(node).snapshot.unwrap();
    assert_eq!(reference.index, before.commit_index);
    assert_eq!(state(node).base_index(), reference.index);
    assert!(state(node).entries.is_empty());
    node.pump_backing(|p| {
        assert_eq!(
            p.snapshots
                .unwrap()
                .retained_references()
                .collect::<Vec<_>>(),
            vec![reference]
        );
    });
}
fn close(bridge: &mut Bridge<'_>, node: &mut FacadeNode, clock: Instant) {
    node.close_intake().unwrap();
    until(bridge, node, clock, FacadeNode::is_drained);
    bridge.close();
    assert!(bridge.is_drained());
}
fn prepare(cohort: &mut Cluster) -> (StoreBinding, GroupLog, CounterReceipt) {
    let clock = cohort.clock;
    let mut bridge =
        LogicalChildBridge::new(&mut cohort.nodes, setup::mapping(), support::node(1)).unwrap();
    let image = claim_in(&mut bridge, clock, 8900, false);
    let old_binding = image.binding();
    let mut parent = node(&image);
    elect(&mut bridge, &mut parent, clock);
    let first = write(&mut bridge, &mut parent, clock, 8910, 5);
    assert_eq!(first.outcome, CounterOutcome::Value(5));
    checkpoint(&mut bridge, &mut parent, clock);
    assert_eq!(
        write(&mut bridge, &mut parent, clock, 8911, 2).outcome,
        CounterOutcome::Value(7)
    );
    let expected = state(&parent).clone();
    assert!(expected.last_index() > expected.base_index());
    close(&mut bridge, &mut parent, clock);
    let parts = parent
        .into_parts()
        .unwrap_or_else(|_| panic!("actual facade drain"));
    assert!(parts.snapshots.as_ref().unwrap().is_drained());
    drop(bridge);
    drop(parts);
    (old_binding, expected, first)
}
pub(super) fn read(bridge: &mut Bridge<'_>, node: &mut FacadeNode, clock: Instant) -> i64 {
    let ticket = node.read(setup::mapping().parent, ()).unwrap();
    let end = Instant::now() + Duration::from_secs(15);
    loop {
        step(bridge, node, clock, 4);
        if let Some(output) = node.poll_read() {
            assert_eq!(output.ticket(), ticket);
            let ReadOutcome::Read {
                result: Ok(value), ..
            } = node.complete_read(output).unwrap()
            else {
                panic!("original fresh read required")
            };
            until(bridge, node, clock, FacadeNode::work_is_drained);
            return value;
        }
        assert!(Instant::now() < end, "recovered quorum read timeout");
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
fn history(protocol: NativePeerProtocol) {
    let _serial = HISTORY.lock().unwrap();
    let mut cohort = Cluster::new_with_application(protocol, fixture::child_application());
    let (old_binding, expected, first) = prepare(&mut cohort);
    cohort.checkpoint();
    cohort.close();
    cohort.open(NativeOpenMode::Recover);
    let clock = cohort.clock;
    let mut bridge =
        LogicalChildBridge::new(&mut cohort.nodes, setup::mapping(), support::node(2)).unwrap();
    let recovered = claim_in(&mut bridge, clock, 8901, false);
    assert_ne!(recovered.binding(), old_binding);
    assert_eq!(recovered.image().parent, expected);
    assert_eq!(recovered.image().pins, vec![expected.snapshot.unwrap()]);
    let missing = FacadeNode::from_parts(
        &recovered,
        build(&recovered),
        fixture::limits(),
        MonoTime(0),
    )
    .err()
    .expect("compacted facade requires original snapshots");
    assert_eq!(
        missing.reason,
        LogicalNodeError::Replica(ReplicaError::MissingSnapshots)
    );
    let mut parent = FacadeNode::from_parts(
        &recovered,
        fixture::attach_snapshots(*missing.parts, &recovered),
        fixture::limits(),
        MonoTime(0),
    )
    .unwrap_or_else(|e| panic!("recovered original parts {:?}", e.reason));
    assert_eq!(state(&parent), &expected);
    let app = &parent.local().applications[&setup::mapping().parent];
    assert_eq!(app.applied_index(), expected.commit_index);
    assert_eq!(app.0.read_applied(expected.commit_index).unwrap(), 7);
    super::super::recovery::refusal_is_atomic(&recovered, app);
    elect(&mut bridge, &mut parent, clock);
    let duplicate = write(&mut bridge, &mut parent, clock, 8910, 5);
    assert!(duplicate.duplicate);
    assert!(duplicate.index > expected.commit_index);
    assert_eq!(duplicate.index, state(&parent).commit_index);
    assert_eq!(duplicate.outcome, first.outcome);
    assert_eq!(read(&mut bridge, &mut parent, clock), 7);
    assert_eq!(
        write(&mut bridge, &mut parent, clock, 8912, 3).outcome,
        CounterOutcome::Value(10)
    );
    close(&mut bridge, &mut parent, clock);
    drop(
        parent
            .into_parts()
            .unwrap_or_else(|_| panic!("recovered facade drain")),
    );
    drop(bridge);
    cohort.finish();
}
