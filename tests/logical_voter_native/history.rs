// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::{
    cluster::{Cluster, HISTORY},
    setup, support,
};
use voteboat::{
    contracts::{HardState, StorageError},
    identity::*,
    log::*,
    logical_voter::*,
    native::{connect::NativePeerProtocol, startup::*},
    snapshot::SnapshotStore,
};
fn inspect(image: &VerifiedLogicalImage, representative: u64, epoch: u64) {
    assert_eq!(image.mapping(), setup::mapping());
    assert_eq!(
        image.representative().physical.node,
        support::node(representative)
    );
    assert_eq!(
        image.representative().physical.store,
        support::identity(representative.into())
    );
    assert_ne!(image.representative().physical.store, image.mapping().store);
    assert_ne!(image.representative().physical.node, image.mapping().voter);
    assert_eq!(image.representative().epoch.get(), epoch);
    assert_eq!(image.claim().outcome, LogicalOutcome::Claimed);
    assert_eq!(image.claim().epoch, Some(image.representative().epoch));
    assert!(image.claim().first_index > 0);
    assert_eq!(image.child_barrier().group(), setup::mapping().child);
    assert!(image.child_barrier().index() >= image.claim().first_index);
    assert!(image.image().child_applied >= image.child_barrier().index());
    assert_eq!(image.image().parent.bootstrap, setup::parent());
    assert_eq!(image.image().parent.hard_state, HardState::default());
    assert_eq!(image.image().parent.commit_index, 0);
    assert!(image.image().parent.entries.is_empty());
    assert!(image.image().published.is_none());
    assert!(image.image().pins.is_empty());
    inspect_recovery_views(image);
}
fn inspect_recovery_views(image: &VerifiedLogicalImage) {
    let (mut log, mut snapshots) = image.recovery_views();
    assert_eq!(log.binding(), image.binding());
    assert_eq!(
        log.state(setup::mapping().parent).unwrap(),
        image.image().parent
    );
    assert_eq!(
        log.state(setup::mapping().child),
        Err(StorageError::WrongIdentity)
    );
    assert_eq!(
        log.append_batch(Vec::new()),
        Err(StorageError::Rejected("immutable logical recovery view"))
    );
    assert_eq!(
        log.barrier(&[]),
        Err(StorageError::Rejected("immutable logical recovery view"))
    );
    assert!(snapshots.load().unwrap().is_none());
    assert_eq!(snapshots.binding(), image.binding());
    assert_eq!(snapshots.identity().group, setup::mapping().parent);
}
fn history(protocol: NativePeerProtocol) {
    let _serial = HISTORY.lock().unwrap();
    let mut cluster = Cluster::new(protocol);
    let first = cluster.claim(1, 7001, true);
    inspect(&first, 1, 1);
    cluster.checkpoint();
    let bases = cluster
        .nodes
        .iter()
        .map(|node| {
            node.local()
                .owner
                .core(setup::mapping().child)
                .unwrap()
                .state()
                .base_index()
        })
        .collect::<Vec<_>>();
    let second = cluster.claim(2, 7002, false);
    inspect(&second, 2, 2);
    assert_ne!(first.binding().session, second.binding().session);
    assert_eq!(first.image().parent, second.image().parent);
    cluster.catch_up();
    for (node, base) in cluster.nodes.iter().zip(&bases) {
        let core = node.local().owner.core(setup::mapping().child).unwrap();
        assert_eq!(core.state().base_index(), *base);
        assert!(core
            .state()
            .entries
            .iter()
            .any(|entry| entry.index == second.claim().first_index));
    }
    cluster.close();
    cluster.open(NativeOpenMode::Recover);
    for (node, base) in cluster.nodes.iter().zip(&bases) {
        assert_eq!(
            node.local()
                .owner
                .core(setup::mapping().child)
                .unwrap()
                .state()
                .base_index(),
            *base
        );
    }
    retained(&mut cluster, 7001, first.claim());
    retained(&mut cluster, 7002, second.claim());
    let third = cluster.claim(3, 7003, false);
    inspect(&third, 3, 3);
    assert_eq!(third.image().parent, second.image().parent);
    assert_ne!(third.binding().session, second.binding().session);
    cluster.finish();
}
fn retained(cluster: &mut Cluster, id: u128, receipt: LogicalReceipt) {
    let LogicalRead::Outcome(Some(status)) =
        cluster.read(LogicalQuery::Outcome(OperationId::new(id).unwrap()))
    else {
        panic!("original committed claim missing after cold checkpoint+suffix reopen")
    };
    assert_eq!(status.operation, receipt.operation);
    assert_eq!(status.first_index, receipt.first_index);
    assert_eq!(status.receipt, receipt);
}
#[test]
fn tcp_child_claim_checkpoint_suffix_and_cold_reopen() {
    history(NativePeerProtocol::TcpTls);
}
#[cfg(feature = "quic")]
#[test]
fn quic_child_claim_checkpoint_suffix_and_cold_reopen() {
    history(NativePeerProtocol::Quic);
}
#[test]
fn incompatible_mapping_refuses_without_consuming_child_ownership() {
    let _serial = HISTORY.lock().unwrap();
    let mut cluster = Cluster::new(NativePeerProtocol::TcpTls);
    let wrong = LogicalStoreMapping {
        store: support::identity(102),
        ..setup::mapping()
    };
    assert!(matches!(
        LogicalChildBridge::new(&mut cluster.nodes, wrong, support::node(1)),
        Err(LogicalBridgeError::InvalidAssembly)
    ));
    let image = cluster.claim(1, 7010, false);
    inspect(&image, 1, 1);
    cluster.finish();
}
