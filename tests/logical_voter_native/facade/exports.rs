// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Original live logical worker exports; no learner or quorum capability.
use super::*;
use voteboat::{snapshot::Snapshot, snapshot_worker::SnapshotWorker};

fn export_parts(
    image: &VerifiedLogicalImage,
) -> LogicalNodeParts<
    voteboat::native::runtime::FairScheduler,
    voteboat::native::runtime::DeadlineQueue,
    voteboat::native::runtime::JitterEntropy,
    super::super::application::App,
> {
    let mut parts = fixture::attach_snapshots(build(image), image);
    let snapshots = parts.local.snapshots.as_mut().unwrap();
    let allowance = snapshots
        .worker
        .load_reservation(setup::mapping().parent)
        .unwrap();
    // One serialized export retains the selected load and externally owned copy
    // plus a bounded metadata allowance. The old checkpoint-only fixture budgets
    // exactly two images and cannot admit the additional original export record.
    snapshots.router = SnapshotRouter::new_with_recovery_limits(
        parts.local.owner.identity(),
        snapshots.worker.binding(),
        SnapshotRouterLimits {
            requests: 4,
            max_image_bytes: allowance,
        },
        SnapshotRecoveryLimits {
            requests: 1,
            image_bytes: 2 * allowance + 4096,
        },
    )
    .unwrap();
    parts
}

#[test]
fn logical_facade_live_export_original_credits_cancel_and_close_tcp() {
    history(NativePeerProtocol::TcpTls, false);
}
#[cfg(feature = "quic")]
#[test]
fn logical_facade_live_export_original_credits_cancel_and_close_quic() {
    history(NativePeerProtocol::Quic, false);
}
#[test]
fn logical_facade_export_survives_abort_and_original_recovery_transfer_tcp() {
    history(NativePeerProtocol::TcpTls, true);
}
#[cfg(feature = "quic")]
#[test]
fn logical_facade_export_survives_abort_and_original_recovery_transfer_quic() {
    history(NativePeerProtocol::Quic, true);
}

fn history(protocol: NativePeerProtocol, abort: bool) {
    let _serial = serial();
    let mut cohort = Cluster::new_with_application(protocol, fixture::child_application());
    let clock = cohort.clock;
    let mut bridge =
        LogicalChildBridge::new(&mut cohort.nodes, setup::mapping(), support::node(1)).unwrap();
    let image = claim_in(&mut bridge, clock, 9600, false);
    let mut node =
        FacadeNode::from_parts(&image, export_parts(&image), fixture::limits(), MonoTime(0))
            .unwrap_or_else(|e| panic!("original export facade {:?}", e.reason));
    assert_eq!(
        node.export_checkpoint(setup::mapping().parent),
        Err(SnapshotRouteError::MissingAnchor)
    );
    assert_eq!(
        node.export_checkpoint(support::group(999)),
        Err(SnapshotRouteError::UnknownGroup)
    );
    assert_eq!(
        node.drain_failed_checkpoint_exports(1),
        Err(SnapshotRouteError::ProviderContract)
    );
    restart::elect(&mut bridge, &mut node, clock);
    assert_eq!(
        restart::write(&mut bridge, &mut node, clock, 6, 5).outcome,
        CounterOutcome::Value(5)
    );
    node.checkpoint().unwrap();
    until(&mut bridge, &mut node, clock, |n| {
        n.local()
            .owner
            .core(setup::mapping().parent)
            .unwrap()
            .state()
            .snapshot
            .is_some()
            && n.work_is_drained()
    });
    cancel_pending(&mut node);
    until(&mut bridge, &mut node, clock, FacadeNode::work_is_drained);
    assert!(node.poll_checkpoint_export().is_none());
    let ticket = node.export_checkpoint(setup::mapping().parent).unwrap();
    let held = node.local().snapshots.as_ref().unwrap().router.usage();
    assert_eq!(held.requests, 1);
    assert!(held.image_bytes > 0);
    let end = Instant::now() + Duration::from_secs(15);
    let output = loop {
        step(&mut bridge, &mut node, clock, 4);
        if let Some(output) = node.poll_checkpoint_export() {
            break output;
        }
        assert!(Instant::now() < end, "original selected worker load");
    };
    assert_eq!(output.ticket(), ticket);
    let SnapshotExportOutcome::Loaded(snapshot) = output.outcome() else {
        panic!("original live snapshot load");
    };
    assert_snapshot(&node, snapshot);
    assert_eq!(
        node.local().snapshots.as_ref().unwrap().router.usage(),
        held
    );
    assert!(!node.work_is_drained());
    assert!(node.poll_checkpoint_export().is_none());
    assert_eq!(
        node.cancel_checkpoint_export(ticket),
        Err(SnapshotRouteError::StaleCompletion)
    );
    assert_eq!(
        node.export_checkpoint(setup::mapping().parent),
        Err(SnapshotRouteError::Overloaded)
    );
    if abort {
        recover_held(node, output, held);
    } else {
        close_held(&mut bridge, node, clock, output, held);
    }
    bridge.close();
    assert!(bridge.is_drained());
    drop(bridge);
    cohort.finish();
}

fn cancel_pending(node: &mut FacadeNode) {
    let ticket = node.export_checkpoint(setup::mapping().parent).unwrap();
    let snapshots = node.local().snapshots.as_ref().unwrap();
    let held = snapshots.router.usage();
    let work = snapshots.worker.usage();
    assert_eq!(work.requests, 1);
    node.cancel_checkpoint_export(ticket).unwrap();
    // No port pump or worker completion consumption: cancellation cannot erase
    // an accepted original Load. This is not withholding a fresh quorum proof.
    for _ in 0..16 {
        node.poll(MonoTime(0), LogicalNodePollBudget::default())
            .unwrap();
        assert_eq!(
            node.local().snapshots.as_ref().unwrap().worker.usage(),
            work
        );
        assert_eq!(
            node.local().snapshots.as_ref().unwrap().router.usage(),
            held
        );
        assert!(node.poll_checkpoint_export().is_none());
        assert!(!node.work_is_drained());
    }
}

fn assert_snapshot(node: &FacadeNode, snapshot: &Snapshot) {
    let state = node
        .local()
        .owner
        .core(setup::mapping().parent)
        .unwrap()
        .state();
    let reference = state.snapshot.unwrap();
    assert!(reference.matches(snapshot));
    assert_eq!(snapshot.metadata.bootstrap, state.bootstrap);
    assert_eq!(snapshot.metadata.membership, state.snapshot_membership);
    assert_eq!(
        snapshot.application,
        node.local().applications[&setup::mapping().parent]
            .checkpoint(65536)
            .unwrap()
    );
    let mut restored = super::super::application::App::new();
    restored
        .restore_checkpoint(
            snapshot.metadata.application_schema,
            snapshot.metadata.index,
            &snapshot.application,
        )
        .unwrap();
    assert_eq!(restored.0.read_applied(snapshot.metadata.index).unwrap(), 5);
}

fn close_held(
    bridge: &mut Bridge<'_>,
    mut node: FacadeNode,
    clock: Instant,
    output: SnapshotExportCompletion,
    held: SnapshotRouterUsage,
) {
    let before = node
        .local()
        .owner
        .core(setup::mapping().parent)
        .unwrap()
        .state()
        .clone();
    node.close_intake().unwrap();
    assert_eq!(
        node.export_checkpoint(setup::mapping().parent),
        Err(SnapshotRouteError::Closed)
    );
    for _ in 0..16 {
        step(bridge, &mut node, clock, 4);
        assert!(!node.is_drained());
        assert_eq!(
            node.local().snapshots.as_ref().unwrap().router.usage(),
            held
        );
        assert_eq!(
            node.local()
                .owner
                .core(setup::mapping().parent)
                .unwrap()
                .state(),
            &before
        );
    }
    let ticket = output.ticket();
    let SnapshotExportOutcome::Loaded(snapshot) = node.complete_checkpoint_export(output).unwrap()
    else {
        panic!("exact original output");
    };
    assert_snapshot(&node, &snapshot);
    assert_eq!(
        node.cancel_checkpoint_export(ticket),
        Err(SnapshotRouteError::StaleCompletion)
    );
    until(bridge, &mut node, clock, FacadeNode::is_drained);
    let parts = node
        .into_parts()
        .unwrap_or_else(|_| panic!("original export drain"));
    assert!(parts.snapshots.as_ref().unwrap().is_drained());
    assert!(parts.local.snapshots.as_ref().unwrap().router.is_drained());
    drop(parts);
}

fn recover_held(mut node: FacadeNode, output: SnapshotExportCompletion, held: SnapshotRouterUsage) {
    node.abort();
    assert_eq!(
        node.export_checkpoint(setup::mapping().parent),
        Err(SnapshotRouteError::Closed)
    );
    // The original ClientRouter abort fences this exact owner. Failed cleanup
    // is now permitted but cannot consume the externally held original output.
    assert!(node.local().owner.is_failed());
    assert_eq!(node.drain_failed_checkpoint_exports(1), Ok(0));
    assert_eq!(
        node.local().snapshots.as_ref().unwrap().router.usage(),
        held
    );
    let mut recovery = node
        .into_recovery()
        .unwrap_or_else(|_| panic!("original recovery transfer"));
    assert_eq!(recovery.reason, LogicalNodeError::Closed);
    assert!(recovery.parts.persistence.is_drained());
    assert!(recovery.parts.snapshots.as_ref().unwrap().is_drained());
    let snapshots = recovery.parts.local.snapshots.as_mut().unwrap();
    assert_eq!(snapshots.router.usage(), held);
    let SnapshotExportOutcome::Loaded(snapshot) = snapshots.router.complete_export(output).unwrap()
    else {
        panic!("same held image after recovery transfer");
    };
    assert_eq!(snapshot.metadata.bootstrap.group, setup::mapping().parent);
    assert!(snapshots.router.is_drained());
    assert!(snapshots.worker.is_drained());
    drop(recovery);
}
