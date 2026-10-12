// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
pub(super) fn pre_intake(
    bridge: &mut LogicalBackingBridge<'_, cluster::Child>,
    app: &LogicalApplication,
) {
    let mut wrong = fixture::enrollment(app);
    wrong.mapping = LogicalStoreMapping::new(
        fixture::mapping().parent,
        fixture::node(15),
        fixture::store(105),
        fixture::mapping().child,
    )
    .unwrap();
    let rejected = bridge
        .begin_enrollment(OperationId::new(12).unwrap(), wrong)
        .unwrap_err();
    assert_eq!(rejected.reason, LogicalBridgeError::InvalidAssembly);
    assert_eq!(rejected.enrollment.mapping.voter, fixture::node(15));
    assert_eq!(rejected.enrollment.import.snapshot.metadata.index, 1);
    assert!(bridge.inspect_work(|w| w.is_none()));
    let mut wrong_record = fixture::enrollment(app);
    wrong_record.import.learners.term = 7;
    let rejected = bridge
        .begin_enrollment(OperationId::new(15).unwrap(), wrong_record)
        .unwrap_err();
    assert_eq!(
        rejected.reason,
        LogicalBridgeError::Refused(LogicalRefusal::SnapshotConflict)
    );
    assert_eq!(rejected.enrollment.import.learners.term, 7);
    assert!(bridge.inspect_work(|w| w.is_none()));
    let mut wrong_schema = fixture::enrollment(app);
    wrong_schema.import.snapshot.metadata.application_schema = 7;
    let rejected = bridge
        .begin_enrollment(OperationId::new(13).unwrap(), wrong_schema)
        .unwrap_err();
    assert_eq!(
        rejected.reason,
        LogicalBridgeError::Refused(LogicalRefusal::SnapshotConflict)
    );
    assert_eq!(
        rejected
            .enrollment
            .import
            .snapshot
            .metadata
            .application_schema,
        7
    );
    assert!(bridge.inspect_work(|w| w.is_none()));
}
pub(super) fn reclaimed(
    old: &VerifiedLogicalImage,
    new: &VerifiedLogicalImage,
    app: &LogicalApplication,
) {
    assert_eq!(new.claim().operation, OperationId::new(14).unwrap());
    assert_eq!(
        new.representative().epoch.get(),
        old.representative().epoch.get() + 1
    );
    assert_eq!(new.binding().session.get(), old.binding().session.get() + 1);
    assert_eq!(
        new.enrollment.as_ref().unwrap().view,
        old.enrollment.as_ref().unwrap().view
    );
    assert!(new.enrollment.as_ref().unwrap().barrier.index() >= new.claim().first_index);
    assert!(new.claim().first_index > old.claim().first_index);
    assert_eq!(
        LogicalPeerDescriptor::from_verified(new, app)
            .unwrap()
            .mapping(),
        old.mapping()
    );
    let mut counter = crate::application::Counter::new(16).unwrap();
    let (core, _) = new.recover_parent(&mut counter).unwrap();
    assert_eq!(core.storage_binding(), new.binding());
    assert!(!core.membership().is_voter(new.mapping().voter));
}
pub(super) fn cancel_admitted<'a>(
    mut bridge: LogicalBackingBridge<'a, cluster::Child>,
    clock: Instant,
    original: &[u8],
) -> LogicalBackingBridge<'a, cluster::Child> {
    let ticket = bridge.inspect_work(|w| w.unwrap().attempt().unwrap().ticket());
    assert!(matches!(ticket, LogicalBridgeChildTicket::Client(_)));
    bridge.stop_enrollment_waiting().unwrap();
    bridge.stop_enrollment_waiting().unwrap();
    let end = Instant::now() + Duration::from_secs(15);
    loop {
        bridge
            .advance(
                MonoTime(clock.elapsed().as_millis() as u64),
                LogicalBridgeBudget {
                    transitions: 0,
                    ..budget(1)
                },
            )
            .unwrap();
        let clear = bridge.inspect_work(|w| {
            w.is_some_and(|w| {
                assert_eq!(w.command_bytes().unwrap(), original);
                w.phase() == LogicalBridgeWorkPhase::Resolving && w.attempt().is_none()
            })
        }) && (0..3).all(|n| {
            bridge
                .inspect_backing(n, |local| {
                    local.clients.is_drained() && local.reads.is_drained()
                })
                .unwrap()
        });
        if clear {
            break;
        }
        assert!(bridge.take_image().is_none());
        assert!(
            Instant::now() < end,
            "cancelled original observation drains while lower work persists"
        );
        std::thread::park_timeout(Duration::from_millis(1));
    }
    let (recovery, nodes) = bridge
        .into_enrollment_recovery()
        .unwrap_or_else(|_| panic!("exact admitted original command handback"));
    let mut bridge =
        LogicalBackingBridge::new(nodes, fixture::mapping(), fixture::node(1)).unwrap();
    bridge
        .resume_enrollment(recovery)
        .unwrap_or_else(|_| panic!("same live original streams resolve"));
    assert_eq!(
        bridge.inspect_work(|w| w.unwrap().command_bytes().unwrap().to_vec()),
        original
    );
    bridge
}
pub(super) fn verified(
    image: &VerifiedLogicalImage,
    app: &LogicalApplication,
    source_bytes: &[u8],
    source_generation: SnapshotGeneration,
) {
    assert_eq!(image.initialization, app.initialization());
    let observed = image.enrollment.as_ref().unwrap();
    assert!(observed.barrier.index() >= image.claim().first_index);
    assert!(image.child_barrier().index() >= observed.barrier.index());
    assert_eq!(observed.ticket.request, observed.barrier.request());
    assert_eq!(observed.ticket.binding.owner, observed.owner);
    let record = observed.view.record.as_ref().unwrap();
    assert_eq!(record.operation, OperationId::new(11).unwrap());
    assert_eq!(record.first_index, image.claim().first_index);
    assert_eq!(record.origin.export.reference.generation, source_generation);
    assert_eq!(record.target_reference.generation.get(), 1);
    assert_eq!(record.target_reference.store, fixture::mapping().store);
    assert_eq!(image.image().parent.hard_state.voted_for, None);
    let mut counter = crate::application::Counter::new(16).unwrap();
    let (core, _) = image.recover_parent(&mut counter).unwrap();
    assert_eq!(
        core.membership().replica_store(fixture::mapping().voter),
        Some(fixture::mapping().store)
    );
    assert_eq!(
        core.membership().voter_store(fixture::mapping().voter),
        None
    );
    assert_eq!(counter.checkpoint(4096).unwrap(), source_bytes);
    assert_eq!(image.endpoint_chain().depth(), 1);
    assert_eq!(
        LogicalPeerDescriptor::from_verified(image, app)
            .unwrap()
            .mapping(),
        fixture::mapping()
    );
}
