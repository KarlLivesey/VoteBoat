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

fn limited(owner: &Owner, worker: &Worker, requests: usize, images: usize) -> SnapshotRouter {
    SnapshotRouter::new_with_recovery_limits(
        owner.identity(),
        worker.binding(),
        SnapshotRouterLimits::default(),
        SnapshotRecoveryLimits {
            requests,
            image_bytes: images * worker.allowance,
        },
    )
    .unwrap()
}

fn finish(
    owner: &mut Owner,
    wal: &mut HostWorker,
    worker: &mut Worker,
    router: &mut SnapshotRouter,
    apps: &mut BTreeMap<GroupIdentity, Counter>,
) {
    let mut waiting = VecDeque::new();
    for _ in 0..100 {
        for event in worker.poll(1) {
            let app = apps.get_mut(&event.visit.group).unwrap();
            router.deliver(owner, app, event, MonoTime(0)).unwrap();
        }
        for event in wal.poll(1) {
            owner.deliver_worker(event, MonoTime(0)).unwrap();
        }
        while let Some(lease) = owner.take_effect().unwrap() {
            waiting.push_back(lease);
        }
        for _ in 0..waiting.len() {
            let lease = waiting.pop_front().unwrap();
            let app = apps.get_mut(&lease.ticket.visit.group).unwrap();
            match &lease.effect {
                Effect::Persist(_) => {
                    if let Err(rejected) = owner.submit_persists(wal, vec![lease], MonoTime(0)) {
                        assert_eq!(
                            rejected.reason,
                            EffectOwnerError::Worker(WorkerError::Overloaded)
                        );
                        waiting.extend(rejected.leases);
                    }
                }
                Effect::Send(_) => owner
                    .release(lease, app.applied_index(), MonoTime(0))
                    .unwrap(),
                _ => match router.submit(owner, worker, lease, app) {
                    Ok(_) => (),
                    Err(rejected) => {
                        assert_eq!(rejected.reason, SnapshotRouteError::Overloaded);
                        waiting.push_back(*rejected.lease);
                    }
                },
            }
        }
        if owner.is_drained() && waiting.is_empty() {
            assert!(router.is_drained());
            assert!(worker.is_drained());
            assert_eq!(router.recovery_usage(), SnapshotRouterUsage::default());
            return;
        }
    }
    panic!("snapshot dependencies did not drain");
}

fn admission_history(requests: usize, images: usize) {
    let (mut owner, mut wal, mut worker, _) = setup(2, 4);
    let mut router = limited(&owner, &worker, requests, images);
    let mut apps = (1..=2)
        .map(|g| (group(g), Counter::new(20).unwrap()))
        .collect::<BTreeMap<_, _>>();
    let first = stage(&mut owner, 1);
    router
        .submit(&mut owner, &mut worker, first, &apps[&group(1)])
        .unwrap();
    let second = stage(&mut owner, 2);
    let ticket = second.ticket;
    let reserved = owner.usage().reserved_bytes;
    let sequence = worker.sequence;
    let rejected = router
        .submit(&mut owner, &mut worker, second, &apps[&group(2)])
        .unwrap_err();
    assert_eq!(rejected.reason, SnapshotRouteError::Overloaded);
    assert_eq!(rejected.lease.ticket, ticket);
    assert!(owner.usage().reserved_bytes < reserved);
    assert!(owner.usage().reserved_bytes > 0);
    assert_eq!(worker.sequence, sequence);
    let event = worker.poll(1).pop().unwrap();
    assert!(worker.is_drained());
    let rejected = router
        .submit(&mut owner, &mut worker, *rejected.lease, &apps[&group(2)])
        .unwrap_err();
    assert_eq!(rejected.reason, SnapshotRouteError::Overloaded);
    let stale = SnapshotWorkEvent {
        request: SnapshotWorkTicket {
            sequence: event.request.sequence + 99,
            ..event.request
        },
        visit: event.visit,
        result: Err(StorageError::Fenced),
    };
    assert_eq!(
        router.deliver(
            &mut owner,
            apps.get_mut(&group(1)).unwrap(),
            stale,
            MonoTime(0)
        ),
        Err(SnapshotRouteError::StaleCompletion)
    );
    assert_eq!(
        router.recovery_usage(),
        SnapshotRouterUsage {
            requests: 1,
            image_bytes: worker.allowance
        }
    );
    router
        .deliver(
            &mut owner,
            apps.get_mut(&group(1)).unwrap(),
            event,
            MonoTime(0),
        )
        .unwrap();
    assert_eq!(router.recovery_usage(), SnapshotRouterUsage::default());
    router
        .submit(&mut owner, &mut worker, *rejected.lease, &apps[&group(2)])
        .unwrap();
    finish(&mut owner, &mut wal, &mut worker, &mut router, &mut apps);
    for app in apps.values() {
        assert_eq!(app.read_applied(1), Ok(7));
    }
}

#[test]
fn count_quota_retains_credit_until_router_delivery_and_retries_exact_lease() {
    admission_history(1, 2);
}

#[test]
fn byte_quota_retains_credit_until_router_delivery_and_retries_exact_lease() {
    admission_history(2, 1);
}

#[test]
fn oversized_recovery_and_invalid_limits_are_rejected_before_preparation() {
    let (mut owner, _, mut worker, _) = setup(1, 4);
    for limits in [
        SnapshotRecoveryLimits {
            requests: 0,
            image_bytes: 1,
        },
        SnapshotRecoveryLimits {
            requests: 17,
            image_bytes: 1,
        },
        SnapshotRecoveryLimits {
            requests: 1,
            image_bytes: 0,
        },
    ] {
        assert!(matches!(
            SnapshotRouter::new_with_recovery_limits(
                owner.identity(),
                worker.binding(),
                SnapshotRouterLimits::default(),
                limits
            ),
            Err(SnapshotRouteError::InvalidLimits)
        ));
    }
    let mut router = SnapshotRouter::new_with_recovery_limits(
        owner.identity(),
        worker.binding(),
        SnapshotRouterLimits::default(),
        SnapshotRecoveryLimits {
            requests: 1,
            image_bytes: worker.allowance - 1,
        },
    )
    .unwrap();
    let lease = stage(&mut owner, 1);
    let reserved = owner.usage().reserved_bytes;
    let rejected = router
        .submit(&mut owner, &mut worker, lease, &Counter::new(20).unwrap())
        .unwrap_err();
    assert_eq!(rejected.reason, SnapshotRouteError::TooLarge);
    assert_eq!(owner.usage().reserved_bytes, reserved);
    assert_eq!(worker.sequence, 0);
    assert_eq!(router.recovery_usage(), SnapshotRouterUsage::default());
}

#[test]
fn provider_refusal_and_failed_owner_discard_release_only_their_credits() {
    let (mut owner, _, mut worker, _) = setup(2, 4);
    let mut router = limited(&owner, &worker, 2, 2);
    let mut app = Counter::new(20).unwrap();
    let first = stage(&mut owner, 1);
    worker.reject = true;
    let rejected = router
        .submit(&mut owner, &mut worker, first, &app)
        .unwrap_err();
    assert_eq!(
        rejected.reason,
        SnapshotRouteError::Worker(SnapshotWorkError::Overloaded)
    );
    assert_eq!(router.recovery_usage(), SnapshotRouterUsage::default());
    worker.reject = false;
    router
        .submit(&mut owner, &mut worker, *rejected.lease, &app)
        .unwrap();
    let second = stage(&mut owner, 2);
    router
        .submit(&mut owner, &mut worker, second, &app)
        .unwrap();
    let mut event = worker.poll(1).pop().unwrap();
    event.result = Err(StorageError::Uncertain("publication lost".into()));
    assert!(router
        .deliver(&mut owner, &mut app, event, MonoTime(0))
        .is_err());
    assert!(owner.is_failed());
    assert_eq!(
        router.recovery_usage(),
        SnapshotRouterUsage {
            requests: 1,
            image_bytes: worker.allowance
        }
    );
    router.discard_failed(&mut owner).unwrap();
    assert_eq!(router.recovery_usage(), SnapshotRouterUsage::default());
    assert_eq!(owner.usage().reserved_bytes, 0);
    assert_eq!(worker.usage().requests, 1);
    worker.close();
    worker.poll(1);
    assert!(worker.is_drained());
}

fn append(owner: &mut Owner, wal: &mut HostWorker, app: &mut Counter) {
    let sender = StoreBinding {
        identity: identity(1),
        session: StoreSession::new(1).unwrap(),
    };
    owner
        .admit(
            group(3),
            Event::Receive(Message {
                group: group(3),
                configuration: ConfigurationId::new(1).unwrap(),
                from: node(1),
                sender,
                to: node(2),
                term: 1,
                context: RequestContext {
                    origin: sender,
                    sequence: 1,
                },
                rpc: Rpc::Append {
                    previous_index: 0,
                    previous_term: 0,
                    leader_commit: 1,
                    entries: vec![LogEntry {
                        index: 1,
                        term: 1,
                        payload: EntryPayload::Command {
                            operation: OperationId::new(50).unwrap(),
                            bytes: 9i64.to_le_bytes().to_vec(),
                        },
                    }],
                },
            }),
        )
        .unwrap();
    for _ in 0..20 {
        for event in wal.poll(1) {
            owner.deliver_worker(event, MonoTime(0)).unwrap();
        }
        for result in owner.advance(MonoTime(0), 1).unwrap() {
            assert!(result.error.is_none());
        }
        while let Some(lease) = owner.take_effect().unwrap() {
            match &lease.effect {
                Effect::Persist(_) => {
                    owner
                        .submit_persists(wal, vec![lease], MonoTime(0))
                        .unwrap();
                }
                Effect::Committed(entries) => {
                    app.apply_batch(entries).unwrap();
                    owner
                        .release(lease, app.applied_index(), MonoTime(0))
                        .unwrap();
                }
                Effect::Send(_) => owner
                    .release(lease, app.applied_index(), MonoTime(0))
                    .unwrap(),
                _ => panic!("unexpected foreground effect"),
            }
        }
        if app.applied_index() == 1 {
            return;
        }
    }
    panic!("foreground append did not apply");
}

#[test]
fn checkpoint_publication_and_reconciliation_bypass_full_recovery_quota() {
    let (mut owner, mut wal, mut worker, _) = setup(3, 4);
    let mut router = limited(&owner, &worker, 1, 1);
    let mut apps = (1..=3)
        .map(|g| (group(g), Counter::new(20).unwrap()))
        .collect::<BTreeMap<_, _>>();
    let lease = stage(&mut owner, 1);
    router
        .submit(&mut owner, &mut worker, lease, &apps[&group(1)])
        .unwrap();
    append(&mut owner, &mut wal, apps.get_mut(&group(3)).unwrap());
    assert_eq!(apps[&group(3)].read_applied(1), Ok(9));
    owner.admit(group(3), Event::Checkpoint).unwrap();
    owner.advance(MonoTime(0), 1).unwrap();
    let checkpoint = owner.take_effect().unwrap().unwrap();
    assert!(matches!(
        checkpoint.effect,
        Effect::CheckpointRequired { .. }
    ));
    router
        .submit(&mut owner, &mut worker, checkpoint, &apps[&group(3)])
        .unwrap();
    assert_eq!(router.usage().requests, 2);
    assert_eq!(router.recovery_usage().requests, 1);
    // Deliver the checkpoint first, holding the recovery receipt through its WAL/reconcile dependencies.
    let recovery = worker.poll(1).pop().unwrap();
    let checkpoint = worker.poll(1).pop().unwrap();
    router
        .deliver(
            &mut owner,
            apps.get_mut(&group(3)).unwrap(),
            checkpoint,
            MonoTime(0),
        )
        .unwrap();
    let persist = owner.take_effect().unwrap().unwrap();
    owner
        .submit_persists(&mut wal, vec![persist], MonoTime(0))
        .unwrap();
    for event in wal.poll(1) {
        owner.deliver_worker(event, MonoTime(0)).unwrap();
    }
    for event in wal.poll(1) {
        owner.deliver_worker(event, MonoTime(0)).unwrap();
    }
    let reconcile = owner.take_effect().unwrap().unwrap();
    assert!(matches!(reconcile.effect, Effect::CheckpointCompacted(_)));
    router
        .submit(&mut owner, &mut worker, reconcile, &apps[&group(3)])
        .unwrap();
    assert_eq!(router.recovery_usage().requests, 1);
    router
        .deliver(
            &mut owner,
            apps.get_mut(&group(3)).unwrap(),
            worker.poll(1).pop().unwrap(),
            MonoTime(0),
        )
        .unwrap();
    assert_eq!(owner.core(group(3)).unwrap().state().base_index(), 1);
    router
        .deliver(
            &mut owner,
            apps.get_mut(&group(1)).unwrap(),
            recovery,
            MonoTime(0),
        )
        .unwrap();
    finish(&mut owner, &mut wal, &mut worker, &mut router, &mut apps);
}
