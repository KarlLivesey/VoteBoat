// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

struct Fixture {
    owner: Owner,
    worker: HostWorker,
    clients: Clients,
    results: ApplicationRouter<CounterReceipt>,
    apps: BTreeMap<GroupIdentity, Counter>,
    single: usize,
    growth: usize,
}
fn fixture(groups: u128) -> Fixture {
    let mut store = HostLogStore::new(1);
    let runtime = timed_with_visit_items(1, &mut store, groups, 1, 64);
    let event_bytes = runtime.limits().max_event_bytes;
    let single = runtime
        .core(group(1))
        .unwrap()
        .effect_reservation(event_bytes)
        .unwrap()
        + std::mem::size_of::<LogEntry>();
    // Leave enough room for all four original writes' retained history. Only
    // batching's extra transient observation metadata must exceed this slack.
    let growth = 4 * (std::mem::size_of::<LogEntry>() + 8);
    assert!(4 * std::mem::size_of::<OwnerStep>() > growth);
    let mut worker = HostWorker::new(store);
    let mut owner = EffectOwner::new(
        runtime,
        worker.binding(),
        EffectOwnerLimits {
            active_visits: 4,
            control_visits: 1,
            reserved_bytes: groups as usize * single + growth + 1,
            control_bytes: 1,
        },
    )
    .unwrap();
    let mut apps = BTreeMap::new();
    pump(&mut owner, &mut worker, &mut apps, MonoTime(400));
    for id in 1..=groups {
        assert_eq!(owner.core(group(id)).unwrap().role(), Role::Leader);
        assert_eq!(owner.core(group(id)).unwrap().state().last_index(), 1);
        assert_eq!(
            owner
                .core(group(id))
                .unwrap()
                .effect_reservation(event_bytes),
            Some(single)
        );
    }
    assert_eq!(owner.usage().reserved_bytes, 0);
    let clients = Clients::new(
        ClientRouterBinding::new(
            owner.authorization(),
            ClientRouterGeneration::new(1).unwrap(),
        ),
        ClientRouterLimits::default(),
    )
    .unwrap();
    let results = ApplicationRouter::new(
        ApplicationRouterBinding::new(
            owner.authorization(),
            ApplicationRouterGeneration::new(1).unwrap(),
        ),
        ApplicationRouterLimits::default(),
    )
    .unwrap();
    Fixture {
        owner,
        worker,
        clients,
        results,
        apps,
        single,
        growth,
    }
}
fn submit(f: &mut Fixture, group_id: u128, first: u128, count: usize) -> Vec<ClientTicket> {
    (0..count)
        .map(|offset| {
            f.clients
                .submit(
                    &mut f.owner,
                    &f.apps[&group(group_id)],
                    request(group_id, first + offset as u128, 1),
                )
                .unwrap()
        })
        .collect()
}
fn advance(f: &mut Fixture, limit: usize) -> Vec<OwnerStep> {
    f.clients
        .advance(&mut f.owner, MonoTime(400), limit, |g| f.apps.get(&g))
        .unwrap()
}
fn persist_one(f: &mut Fixture, operation: OperationId) -> EffectLease {
    let lease = f.owner.take_effect().unwrap().unwrap();
    let Effect::Persist(update) = &lease.effect else {
        panic!("original persistence required")
    };
    let suffix = update.suffix.as_ref().unwrap();
    assert_eq!(suffix.entries.len(), 1);
    assert!(
        matches!(suffix.entries[0].payload, EntryPayload::Command { operation: id, .. } if id == operation)
    );
    lease
}
fn finish(f: &mut Fixture, leases: Vec<EffectLease>, tickets: &[ClientTicket]) {
    f.owner
        .submit_persists(&mut f.worker, leases, MonoTime(400))
        .unwrap();
    drain(f);
    let mut seen = Vec::new();
    while let Some(completion) = f.clients.poll() {
        let ticket = completion.ticket();
        assert!(tickets.contains(&ticket));
        assert!(!seen.contains(&ticket));
        let ClientOutcome::Applied { position, receipt } = f.clients.complete(completion).unwrap()
        else {
            panic!("each original request must still apply")
        };
        assert_eq!(receipt.operation, ticket.operation);
        assert_eq!(receipt.index, position.index);
        seen.push(ticket);
    }
    assert_eq!(seen.len(), tickets.len());
    assert!(f.clients.is_drained());
    assert!(f.owner.is_drained());
    assert!(!f.owner.is_failed());
}

fn drain(f: &mut Fixture) {
    let mut retained = Vec::new();
    for _ in 0..1000 {
        for event in f.worker.poll(1) {
            f.owner.deliver_worker(event, MonoTime(400)).unwrap();
        }
        let _ = advance(f, 3);
        while let Some(lease) = f.owner.take_effect().unwrap() {
            match &lease.effect {
                Effect::Persist(_) => retained.push(lease),
                Effect::Committed(_) => {
                    let group = lease.ticket.visit.group;
                    f.results
                        .submit(
                            &mut f.owner,
                            lease,
                            f.apps.get_mut(&group).unwrap(),
                            MonoTime(400),
                        )
                        .unwrap();
                    let output = f.results.poll().unwrap();
                    f.clients
                        .deliver(&mut f.owner, &mut f.results, output)
                        .unwrap();
                }
                _ => panic!("unexpected single-voter effect"),
            }
        }
        // This host fixture owns one worker request at a time. Independent
        // visits can produce their next original persist at different stages.
        // Retain those leases while its previous request is still in flight.
        if !retained.is_empty() && f.worker.is_drained() {
            f.owner
                .submit_persists(&mut f.worker, std::mem::take(&mut retained), MonoTime(400))
                .unwrap();
        }
        f.clients.reconcile(&f.owner, 64).unwrap();
        if retained.is_empty() && f.owner.is_drained() && f.worker.is_drained() {
            return;
        }
    }
    panic!("budget fixture did not drain");
}

#[test]
fn tight_bulk_output_budget_shrinks_ready_prefix_instead_of_fencing_or_starving() {
    let mut f = fixture(1);
    let tickets = submit(&mut f, 1, 100, 4);
    let before = f.owner.core(group(1)).unwrap().state().clone();
    let steps = advance(&mut f, 64);
    assert_eq!(steps.len(), 1);
    assert_eq!(steps[0].operation, Some(tickets[0].operation));
    assert!(steps[0].error.is_none());
    assert_eq!(f.owner.core(group(1)).unwrap().state(), &before);
    assert_eq!(f.clients.usage().requests, 4);
    assert!(f.clients.poll().is_none());
    assert!(!f.owner.is_failed());
    assert!(f.owner.usage().reserved_bytes <= f.single + f.growth);
    let persist = persist_one(&mut f, tickets[0].operation);
    assert!(f.owner.take_effect().unwrap().is_none());
    finish(&mut f, vec![persist], &tickets);
    assert_eq!(f.apps[&group(1)].read_applied(5), Ok(4));
}

#[test]
fn held_visit_reduces_current_global_or_bulk_room_without_blocking_a_fitting_singleton() {
    let mut idle = fixture(2);
    let tickets = submit(&mut idle, 2, 100, 4);
    assert_eq!(advance(&mut idle, 64).len(), 4);
    let persist = idle.owner.take_effect().unwrap().unwrap();
    finish(&mut idle, vec![persist], &tickets);
    for control in [false, true] {
        let mut f = fixture(2);
        let mut tickets = Vec::new();
        let held = if control {
            f.owner.admit_tracked(group(1), Event::Campaign).unwrap();
            let steps = f.owner.advance(MonoTime(400), 1).unwrap();
            assert_eq!(steps.len(), 1);
            assert!(steps[0].error.is_none());
            f.owner.take_effect().unwrap().unwrap()
        } else {
            tickets.extend(submit(&mut f, 1, 10, 1));
            assert_eq!(advance(&mut f, 1).len(), 1);
            persist_one(&mut f, tickets[0].operation)
        };
        assert_eq!(f.owner.usage().reserved_bytes, f.single);
        // Total configured bulk capacity permits a four-command batch. Its
        // current available room does not: the held control visit spends only
        // global room, while a held data visit also spends bulk room.
        let second = submit(&mut f, 2, 100, 4);
        let steps = advance(&mut f, 64);
        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0].visit.group, group(2));
        assert_eq!(steps[0].operation, Some(second[0].operation));
        assert!(steps[0].error.is_none());
        assert_eq!(f.owner.usage().active_visits, 2);
        assert!(f.owner.usage().reserved_bytes <= f.owner.limits().reserved_bytes);
        assert!(!f.owner.is_failed());
        let next = persist_one(&mut f, second[0].operation);
        tickets.extend(second);
        finish(&mut f, vec![held, next], &tickets);
        assert_eq!(f.apps[&group(2)].read_applied(5), Ok(4));
    }
}
