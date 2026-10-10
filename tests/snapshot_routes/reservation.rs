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

fn pressure_setup() -> (Owner, HostWorker, Worker, SnapshotRouter) {
    let (mut probe, _, _, _) = setup(1, 1);
    let _lease = stage(&mut probe, 1);
    let ceiling = 5 * probe.usage().reserved_bytes + 4096;
    let mut log = HostLogStore::new(2);
    let runtime = timed(2, &mut log, 5, 3);
    let wal = HostWorker::new(log);
    let owner = EffectOwner::new(
        runtime,
        wal.binding(),
        EffectOwnerLimits {
            reserved_bytes: ceiling,
            control_bytes: 1024,
            ..EffectOwnerLimits::default()
        },
    )
    .unwrap();
    let (mut worker, router) = for_owner(&owner, 1);
    worker.allowance = ceiling / 2;
    (owner, wal, worker, router)
}

#[test]
fn competing_snapshot_leases_release_speculation_and_all_finish() {
    let (mut owner, mut wal, mut worker, mut router) = pressure_setup();
    let mut apps = (1..=5)
        .map(|g| (group(g), Counter::new(20).unwrap()))
        .collect::<BTreeMap<_, _>>();
    let mut waiting = (1..=5)
        .map(|g| stage(&mut owner, g))
        .collect::<VecDeque<_>>();
    let original = owner.usage();
    assert_eq!(original.leased, 5);
    let mut saw_capacity_rejection = false;
    for _ in 0..waiting.len() {
        let lease = waiting.pop_front().unwrap();
        let ticket = lease.ticket;
        let app = &apps[&ticket.visit.group];
        if let Err(rejected) = router.submit(&mut owner, &mut worker, lease, app) {
            saw_capacity_rejection |=
                rejected.reason == SnapshotRouteError::Owner(EffectOwnerError::Overloaded);
            assert_eq!(rejected.lease.ticket, ticket);
            waiting.push_back(*rejected.lease);
        }
    }
    assert!(saw_capacity_rejection);
    assert_eq!(
        router.usage().requests,
        1,
        "one attempt escapes the capacity deadlock"
    );
    assert_eq!(owner.usage().leased, 5);
    assert!(owner.usage().reserved_bytes < original.reserved_bytes);
    drain(
        &mut owner,
        &mut wal,
        &mut worker,
        &mut router,
        &mut apps,
        waiting,
    );
    assert_eq!(owner.usage().reserved_bytes, 0);
    for app in apps.values() {
        assert_eq!(app.read_applied(1), Ok(7));
    }
}

fn drain(
    owner: &mut Owner,
    wal: &mut HostWorker,
    worker: &mut Worker,
    router: &mut SnapshotRouter,
    apps: &mut BTreeMap<GroupIdentity, Counter>,
    mut waiting: VecDeque<EffectLease>,
) {
    for _ in 0..100 {
        for event in worker.poll(1) {
            router
                .deliver(
                    owner,
                    apps.get_mut(&event.visit.group).unwrap(),
                    event,
                    MonoTime(0),
                )
                .unwrap();
        }
        for event in wal.poll(1) {
            owner.deliver_worker(event, MonoTime(0)).unwrap();
        }
        while let Some(lease) = owner.take_effect().unwrap() {
            waiting.push_back(lease);
        }
        for _ in 0..waiting.len() {
            let lease = waiting.pop_front().unwrap();
            let app = &apps[&lease.ticket.visit.group];
            match lease.effect {
                Effect::Persist(_) => {
                    owner
                        .submit_persists(wal, vec![lease], MonoTime(0))
                        .unwrap();
                }
                Effect::Send(_) => owner
                    .release(lease, app.applied_index(), MonoTime(0))
                    .unwrap(),
                _ => {
                    if let Err(rejected) = router.submit(owner, worker, lease, app) {
                        assert!(matches!(
                            rejected.reason,
                            SnapshotRouteError::Overloaded
                                | SnapshotRouteError::Owner(EffectOwnerError::Overloaded)
                        ));
                        waiting.push_back(*rejected.lease);
                    }
                }
            }
        }
        if owner.is_drained() && waiting.is_empty() {
            assert!(router.is_drained());
            assert!(worker.is_drained());
            return;
        }
    }
    panic!("competing snapshot leases failed to drain");
}

#[test]
fn rejected_worker_releases_only_speculation_and_retry_restores_budget() {
    let (mut owner, _, mut worker, mut router) = setup(1, 1);
    let lease = stage(&mut owner, 1);
    let ticket = lease.ticket;
    let original = owner.usage().reserved_bytes;
    worker.reject = true;
    let app = Counter::new(20).unwrap();
    let rejected = router
        .submit(&mut owner, &mut worker, lease, &app)
        .unwrap_err();
    assert_eq!(
        rejected.reason,
        SnapshotRouteError::Worker(SnapshotWorkError::Overloaded)
    );
    assert_eq!(rejected.lease.ticket, ticket);
    let retained = owner.usage().reserved_bytes;
    assert!(retained > 0 && retained < original);
    assert!(owner.core(group(1)).unwrap().has_pending_dependency());
    worker.reject = false;
    router
        .submit(&mut owner, &mut worker, *rejected.lease, &app)
        .unwrap();
    assert!(owner.usage().reserved_bytes >= original);
    assert_eq!(router.usage().requests, 1);
}
