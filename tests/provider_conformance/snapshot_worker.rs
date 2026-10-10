// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Shared work ownership checks; no native I/O types enter the cases.
use super::support::*;
use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};
use voteboat::{identity::*, runtime::*, snapshot::*, snapshot_worker::*};

pub fn binding() -> SnapshotWorkerBinding {
    SnapshotWorkerBinding {
        store: StoreBinding {
            identity: identity(2),
            session: StoreSession::new(1).unwrap(),
        },
        generation: SnapshotWorkerGeneration::new(1).unwrap(),
    }
}

fn work(id: u128, binding: SnapshotWorkerBinding, job: SnapshotJob) -> SnapshotWork {
    SnapshotWork {
        visit: VisitTicket {
            owner: RuntimeOwner {
                store: binding.store,
                lane: ExecutionLaneId::new(1).unwrap(),
                generation: RuntimeGeneration::new(1).unwrap(),
            },
            group: group(id),
            sequence: 1,
        },
        job,
    }
}

pub fn image(id: u128) -> Snapshot {
    let mut metadata = super::snapshot_cases::metadata(1);
    metadata.bootstrap = bootstrap(id, 1);
    Snapshot {
        metadata,
        application: vec![id as u8; 7],
    }
}

fn publish_work(id: u128, binding: SnapshotWorkerBinding) -> SnapshotWork {
    work(
        id,
        binding,
        SnapshotJob::Publish {
            snapshot: image(id),
            durable: None,
        },
    )
}

pub fn wait(worker: &mut impl SnapshotWorker) -> SnapshotWorkEvent {
    let until = Instant::now() + Duration::from_secs(10);
    loop {
        let mut events = worker.poll(1);
        assert!(events.len() <= 1);
        if let Some(event) = events.pop() {
            return event;
        }
        assert!(Instant::now() < until, "snapshot completion deadline");
        std::thread::park_timeout(Duration::from_millis(1));
    }
}

fn rejected_image_is_returned(
    worker: &mut impl SnapshotWorker,
    work: SnapshotWork,
    reason: SnapshotWorkError,
) {
    let SnapshotJob::Publish { snapshot, .. } = &work.job else {
        panic!("publication expected")
    };
    let pointer = snapshot.application.as_ptr();
    let visit = work.visit;
    let before = worker.usage();
    let rejected = worker.submit(work).unwrap_err();
    assert_eq!(rejected.reason, reason);
    assert_eq!(rejected.work.visit, visit);
    let SnapshotJob::Publish { snapshot, .. } = rejected.work.job else {
        panic!("publication lost")
    };
    assert_eq!(snapshot.application.as_ptr(), pointer);
    assert_eq!(snapshot, image(visit.group.id.get()));
    assert_eq!(worker.usage(), before);
}

fn admit_two_groups(worker: &mut impl SnapshotWorker) -> [SnapshotWorkTicket; 2] {
    worker.limits().validate().unwrap();
    let binding = worker.binding();
    assert_eq!(worker.usage(), SnapshotWorkUsage::default());
    assert!(worker.load_reservation(group(1)).unwrap() > 0);
    assert_eq!(worker.checkpoint_bytes(group(1)), Some(32));
    assert_eq!(worker.load_reservation(group(999)), None);
    assert_eq!(worker.checkpoint_bytes(group(999)), None);
    rejected_image_is_returned(
        worker,
        publish_work(999, binding),
        SnapshotWorkError::WrongBinding,
    );
    let mut foreign = publish_work(1, binding);
    foreign.visit.owner.store.session = StoreSession::new(2).unwrap();
    rejected_image_is_returned(worker, foreign, SnapshotWorkError::WrongBinding);
    let first = worker.submit(publish_work(1, binding)).unwrap();
    rejected_image_is_returned(
        worker,
        publish_work(1, binding),
        SnapshotWorkError::Overloaded,
    );
    let second = worker.submit(publish_work(2, binding)).unwrap();
    assert_eq!(first.binding, binding);
    assert_eq!(second.binding, binding);
    assert!(first.sequence > 0 && second.sequence > first.sequence);
    [first, second]
}

pub fn close_preserves_accepted_work(worker: &mut impl SnapshotWorker) -> [SnapshotRef; 2] {
    let binding = worker.binding();
    let [first, second] = admit_two_groups(worker);
    let charge = worker.usage();
    assert_eq!(charge.requests, 2);
    assert!(charge.bytes > 0 && charge.bytes <= worker.limits().max_bytes);
    assert!(worker.poll(0).is_empty());
    assert_eq!(worker.usage(), charge);
    worker.close();
    worker.close();
    rejected_image_is_returned(worker, publish_work(1, binding), SnapshotWorkError::Closed);
    assert!(!worker.is_drained());
    let mut pending = BTreeMap::from([(first.sequence, first), (second.sequence, second)]);
    let mut pins = BTreeMap::new();
    while !pending.is_empty() {
        let event = wait(worker);
        let request = pending
            .remove(&event.request.sequence)
            .expect("duplicate or unowned completion");
        assert_eq!(event.request, request);
        let id = if request == first { 1 } else { 2 };
        assert_eq!(
            event.visit,
            work(id, binding, SnapshotJob::Readiness { reference: None }).visit
        );
        let SnapshotOutput::Published(reference) = event.result.unwrap() else {
            panic!("publication expected")
        };
        assert!(reference.matches(&image(id)));
        assert_eq!(reference.store, binding.store.identity);
        pins.insert(id, reference);
        assert_eq!(worker.usage().requests, pending.len());
        if pins.len() == 1 {
            assert!(worker.usage().bytes > 0 && worker.usage().bytes < charge.bytes);
        }
    }
    assert_eq!(worker.usage(), SnapshotWorkUsage::default());
    assert!(worker.is_drained());
    assert!(worker.poll(8).is_empty());
    [pins[&1], pins[&2]]
}

fn query(worker: &mut impl SnapshotWorker, id: u128, job: SnapshotJob) -> SnapshotOutput {
    let work = work(id, worker.binding(), job);
    let visit = work.visit;
    let ticket = worker.submit(work).unwrap();
    let event = wait(worker);
    assert_eq!(event.request, ticket);
    assert_eq!(event.visit, visit);
    assert_eq!(worker.usage(), SnapshotWorkUsage::default());
    event.result.unwrap()
}

pub fn recovered_queries(worker: &mut impl SnapshotWorker, pins: [SnapshotRef; 2]) {
    for (id, reference) in (1..=2).zip(pins) {
        let SnapshotOutput::Readiness { snapshot, limits } = query(
            worker,
            id,
            SnapshotJob::Readiness {
                reference: Some(reference),
            },
        ) else {
            panic!("readiness expected")
        };
        limits.validate().unwrap();
        assert_eq!(snapshot, Some(image(id)));
        for install in [false, true] {
            let SnapshotOutput::Loaded {
                reference: loaded,
                snapshot,
                reconciled,
            } = query(worker, id, SnapshotJob::Load { reference, install })
            else {
                panic!("loaded image expected")
            };
            assert_eq!(loaded, reference);
            assert_eq!(snapshot, image(id));
            assert_eq!(reconciled, install);
        }
        let SnapshotOutput::Reconciled(actual) =
            query(worker, id, SnapshotJob::Reconcile { reference })
        else {
            panic!("reconciliation expected")
        };
        assert_eq!(actual, reference);
    }
    worker.close();
    assert!(worker.is_drained());
}
