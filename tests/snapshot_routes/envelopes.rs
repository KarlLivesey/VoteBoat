// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

type Envelope = (SnapshotWorkTicket, VisitTicket);

fn altered_envelopes(request: SnapshotWorkTicket, visit: VisitTicket) -> Vec<Envelope> {
    let changes: [fn(&mut Envelope); 13] = [
        |e| e.0.binding.store.identity.id = StoreId::new(99).unwrap(),
        |e| e.0.binding.store.identity.incarnation = StoreIncarnation::new(99).unwrap(),
        |e| e.0.binding.store.session = StoreSession::new(99).unwrap(),
        |e| e.0.binding.generation = SnapshotWorkerGeneration::new(99).unwrap(),
        |e| e.0.sequence += 1,
        |e| e.1.owner.store.identity.id = StoreId::new(99).unwrap(),
        |e| e.1.owner.store.identity.incarnation = StoreIncarnation::new(99).unwrap(),
        |e| e.1.owner.store.session = StoreSession::new(99).unwrap(),
        |e| e.1.owner.lane = ExecutionLaneId::new(99).unwrap(),
        |e| e.1.owner.generation = RuntimeGeneration::new(99).unwrap(),
        |e| e.1.group.id = GroupId::new(99).unwrap(),
        |e| e.1.group.incarnation = GroupIncarnation::new(99).unwrap(),
        |e| e.1.sequence += 1,
    ];
    changes
        .into_iter()
        .map(|change| {
            let mut envelope = (request, visit);
            change(&mut envelope);
            envelope
        })
        .collect()
}

#[test]
fn every_altered_snapshot_envelope_retains_the_original_live_lease() {
    let (mut owner, _, mut worker, mut router) = setup(1, 1);
    let lease = stage(&mut owner, 1);
    let visit = lease.ticket.visit;
    let mut app = Counter::new(20).unwrap();
    let request = router.submit(&mut owner, &mut worker, lease, &app).unwrap();
    let state = owner.core(group(1)).unwrap().state().clone();
    let reserved = owner.usage().reserved_bytes;
    let image_bytes = router.usage().image_bytes;
    for (request, visit) in altered_envelopes(request, visit) {
        let forged = SnapshotWorkEvent {
            request,
            visit,
            result: Err(StorageError::Uncertain("obsolete worker failure".into())),
        };
        assert_eq!(
            router.deliver(&mut owner, &mut app, forged, MonoTime(0)),
            Err(SnapshotRouteError::StaleCompletion)
        );
        assert!(!owner.is_failed());
        assert_eq!(owner.usage().reserved_bytes, reserved);
        assert_eq!(owner.usage().leased, 1);
        assert_eq!(router.usage().requests, 1);
        assert_eq!(router.usage().image_bytes, image_bytes);
        assert_eq!(owner.core(group(1)).unwrap().state(), &state);
        assert_eq!(app.applied_index(), 0);
    }
    let event = worker.poll(1).pop().unwrap();
    router
        .deliver(&mut owner, &mut app, event, MonoTime(0))
        .unwrap();
    assert!(router.is_drained());
    assert!(!owner.is_failed());
    assert_eq!(
        app.applied_index(),
        0,
        "publication must still await durable WAL installation"
    );
    let effect = owner.take_effect().unwrap().unwrap();
    assert!(matches!(effect.effect, Effect::Persist(_)));
}
