// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
mod budget;
mod context;
mod wrapper;

#[test]
fn ready_native_admissions_share_persist_but_keep_every_original_result() {
    let (mut owner, mut worker, mut clients, mut results, mut apps) =
        setup_with_visit_items(1, 2, 64);
    let tickets: Vec<_> = [(1, 7), (2, 2), (1, 9), (1, 7)]
        .into_iter()
        .map(|(id, delta)| {
            clients
                .submit(&mut owner, &apps[&group(1)], request(1, id, delta))
                .unwrap()
        })
        .collect();
    let steps = clients
        .advance(&mut owner, MonoTime(400), 8, |g| apps.get(&g))
        .unwrap();
    assert_eq!(steps.len(), 4);
    for (offset, step) in steps.iter().enumerate() {
        assert_eq!(step.visit, steps[0].visit);
        assert_eq!(step.operation, Some(tickets[offset].operation));
        assert_eq!(step.proposed.unwrap().index, offset as u64 + 2);
        assert!(step.error.is_none());
    }
    assert_eq!(owner.usage().active_visits, 1);
    assert_eq!(clients.usage().requests, 4);
    let persist = owner.take_effect().unwrap().unwrap();
    let Effect::Persist(update) = &persist.effect else {
        panic!("one persist required")
    };
    assert_eq!(update.suffix.as_ref().unwrap().entries.len(), 4);
    assert!(owner.take_effect().unwrap().is_none());
    owner
        .submit_persists(&mut worker, vec![persist], MonoTime(400))
        .unwrap();
    let written = worker.poll(1).pop().unwrap();
    assert!(matches!(written, WorkerEvent::Written { .. }));
    owner.deliver_worker(written, MonoTime(400)).unwrap();
    assert_eq!(owner.core(group(1)).unwrap().state().last_index(), 1);
    assert!(clients.poll().is_none());
    drive(
        &mut owner,
        &mut worker,
        &mut clients,
        &mut results,
        &mut apps,
    );
    for (offset, outcome) in [
        CounterOutcome::Value(7),
        CounterOutcome::Value(9),
        CounterOutcome::OperationConflict,
        CounterOutcome::Value(7),
    ]
    .into_iter()
    .enumerate()
    {
        let completion = clients.poll().unwrap();
        assert_eq!(completion.ticket(), tickets[offset]);
        let ClientOutcome::Applied { position, receipt } = clients.complete(completion).unwrap()
        else {
            panic!("original applied result required")
        };
        assert_eq!(position.index, offset as u64 + 2);
        assert_eq!(receipt.index, position.index);
        assert_eq!(receipt.outcome, outcome);
        assert_eq!(receipt.duplicate, offset >= 2);
    }
    assert!(clients.is_drained());
    assert!(owner.is_drained());
    assert_eq!(apps[&group(1)].read_applied(5), Ok(9));
}

#[test]
fn per_poll_original_step_budget_does_not_expand_with_batch_size() {
    let (mut owner, mut worker, mut clients, mut results, mut apps) =
        setup_with_visit_items(1, 8, 64);
    for id in 1..=6 {
        clients
            .submit(&mut owner, &apps[&group(1)], request(1, id, 1))
            .unwrap();
    }
    let first = clients
        .advance(&mut owner, MonoTime(400), 2, |g| apps.get(&g))
        .unwrap();
    assert_eq!(first.len(), 2);
    let lease = owner.take_effect().unwrap().unwrap();
    let Effect::Persist(update) = &lease.effect else {
        panic!()
    };
    assert_eq!(update.suffix.as_ref().unwrap().entries.len(), 2);
    owner
        .submit_persists(&mut worker, vec![lease], MonoTime(400))
        .unwrap();
    drive(
        &mut owner,
        &mut worker,
        &mut clients,
        &mut results,
        &mut apps,
    );
    for index in 2..=7 {
        let completion = clients.poll().unwrap();
        let outcome = clients.complete(completion).unwrap();
        let ClientOutcome::Applied { position, .. } = outcome else {
            panic!()
        };
        assert_eq!(position.index, index);
    }
    assert!(clients.is_drained());
}

#[test]
fn canceling_one_batched_observation_does_not_cancel_other_commands() {
    let (mut owner, mut worker, mut clients, mut results, mut apps) =
        setup_with_visit_items(1, 4, 64);
    let tickets: Vec<_> = (1..=4)
        .map(|id| {
            clients
                .submit(&mut owner, &apps[&group(1)], request(1, id, 1))
                .unwrap()
        })
        .collect();
    clients.cancel_wait(tickets[1]).unwrap();
    let canceled = clients.poll().unwrap();
    assert_eq!(canceled.ticket(), tickets[1]);
    assert!(matches!(
        clients.complete(canceled).unwrap(),
        ClientOutcome::Unknown(ClientUnknown::CancelledWait)
    ));
    assert_eq!(clients.usage().requests, 4);
    let steps = clients
        .advance(&mut owner, MonoTime(400), 4, |g| apps.get(&g))
        .unwrap();
    assert_eq!(steps.len(), 4);
    let lease = owner.take_effect().unwrap().unwrap();
    owner
        .submit_persists(&mut worker, vec![lease], MonoTime(400))
        .unwrap();
    drive(
        &mut owner,
        &mut worker,
        &mut clients,
        &mut results,
        &mut apps,
    );
    for ticket in [tickets[0], tickets[2], tickets[3]] {
        let completion = clients.poll().unwrap();
        assert_eq!(completion.ticket(), ticket);
        assert!(matches!(
            clients.complete(completion).unwrap(),
            ClientOutcome::Applied { .. }
        ));
    }
    assert_eq!(apps[&group(1)].read_applied(5), Ok(4));
    assert!(clients.is_drained());
}

#[test]
fn failure_after_written_leaves_all_batched_results_unknown_and_releases_on_observation() {
    let (mut owner, mut worker, mut clients, _, apps) = setup_with_visit_items(1, 4, 64);
    let tickets: Vec<_> = (1..=4)
        .map(|id| {
            clients
                .submit(&mut owner, &apps[&group(1)], request(1, id, 1))
                .unwrap()
        })
        .collect();
    let steps = clients
        .advance(&mut owner, MonoTime(400), 4, |g| apps.get(&g))
        .unwrap();
    assert_eq!(steps.len(), 4);
    let persist = owner.take_effect().unwrap().unwrap();
    owner
        .submit_persists(&mut worker, vec![persist], MonoTime(400))
        .unwrap();
    let written = worker.poll(1).pop().unwrap();
    owner.deliver_worker(written, MonoTime(400)).unwrap();
    assert!(clients.poll().is_none());
    worker.fail = true;
    let failed = worker.poll(1).pop().unwrap();
    assert!(owner.deliver_worker(failed, MonoTime(400)).is_err());
    clients.reconcile(&owner, 4).unwrap();
    for ticket in tickets {
        let completion = clients.poll().unwrap();
        assert_eq!(completion.ticket(), ticket);
        assert!(matches!(
            clients.complete(completion).unwrap(),
            ClientOutcome::Unknown(ClientUnknown::OwnerFailed)
        ));
    }
    assert!(clients.is_drained());
    assert_eq!(apps[&group(1)].applied_index(), 1);
}

#[test]
fn every_native_quantum_preserves_unknown_after_written_for_all_selected_originals() {
    for items in [1, 16, 32, 64] {
        let (mut owner, mut worker, mut clients, _, apps) = setup_with_visit_items(1, items, items);
        let tickets: Vec<_> = (1..=items)
            .map(|id| {
                clients
                    .submit(&mut owner, &apps[&group(1)], request(1, id as u128, 1))
                    .unwrap()
            })
            .collect();
        let steps = clients
            .advance(&mut owner, MonoTime(400), items, |g| apps.get(&g))
            .unwrap();
        assert_eq!(steps.len(), items);
        assert!(steps.iter().all(|step| step.visit == steps[0].visit));
        let persist = owner.take_effect().unwrap().unwrap();
        owner
            .submit_persists(&mut worker, vec![persist], MonoTime(400))
            .unwrap();
        let written = worker.poll(1).pop().unwrap();
        assert!(matches!(written, WorkerEvent::Written { .. }));
        owner.deliver_worker(written, MonoTime(400)).unwrap();
        assert!(clients.poll().is_none());
        worker.fail = true;
        assert!(owner
            .deliver_worker(worker.poll(1).pop().unwrap(), MonoTime(400))
            .is_err());
        clients.reconcile(&owner, items).unwrap();
        for ticket in tickets {
            let completion = clients.poll().unwrap();
            assert_eq!(completion.ticket(), ticket);
            assert!(matches!(
                clients.complete(completion).unwrap(),
                ClientOutcome::Unknown(ClientUnknown::OwnerFailed)
            ));
        }
        assert!(clients.is_drained());
        assert_eq!(apps[&group(1)].applied_index(), 1);
    }
}
