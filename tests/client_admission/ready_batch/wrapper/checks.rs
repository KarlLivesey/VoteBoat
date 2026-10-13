// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

#[test]
fn retained_capacity_refuses_novel_id_but_original_retry_keeps_exact_outcome() {
    let (mut owner, mut worker, mut clients, mut results, mut apps) = wrapped(1, false);
    let original = clients
        .submit(
            &mut owner,
            &apps[&group(1)],
            wrapped_request(&apps[&group(1)], 1, 7),
        )
        .unwrap();
    drive(
        &mut owner,
        &mut worker,
        &mut clients,
        &mut results,
        &mut apps,
    );
    applied(&mut clients, original, 2, CounterOutcome::Value(7), false);
    let refused = clients
        .submit(
            &mut owner,
            &apps[&group(1)],
            wrapped_request(&apps[&group(1)], 2, 1),
        )
        .unwrap_err();
    assert_eq!(
        refused.reason,
        ClientError::Application(ApplicationError::DedupCapacity)
    );
    let tickets: Vec<_> = [9, 7]
        .into_iter()
        .map(|delta| {
            clients
                .submit(
                    &mut owner,
                    &apps[&group(1)],
                    wrapped_request(&apps[&group(1)], 1, delta),
                )
                .unwrap()
        })
        .collect();
    let steps = clients
        .advance(&mut owner, MonoTime(400), 2, |g| apps.get(&g))
        .unwrap();
    assert_eq!(steps.len(), 2);
    assert_eq!(steps[0].visit, steps[1].visit);
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
    applied(
        &mut clients,
        tickets[0],
        3,
        CounterOutcome::OperationConflict,
        true,
    );
    applied(&mut clients, tickets[1], 4, CounterOutcome::Value(7), true);
    assert!(clients.is_drained());
    assert_eq!(
        apps[&group(1)].read_at(4, application::Query::Data(())),
        Ok(application::ReadResult::Data(7))
    );
}

#[test]
fn written_failure_keeps_every_wrapped_original_unknown() {
    let (mut owner, mut worker, mut clients, _, apps) = wrapped(4, false);
    let tickets: Vec<_> = (1..=4)
        .map(|id| {
            clients
                .submit(
                    &mut owner,
                    &apps[&group(1)],
                    wrapped_request(&apps[&group(1)], id, 1),
                )
                .unwrap()
        })
        .collect();
    let steps = clients
        .advance(&mut owner, MonoTime(400), 4, |g| apps.get(&g))
        .unwrap();
    assert_eq!(steps.len(), 4);
    let lease = owner.take_effect().unwrap().unwrap();
    owner
        .submit_persists(&mut worker, vec![lease], MonoTime(400))
        .unwrap();
    let written = worker.poll(1).pop().unwrap();
    owner.deliver_worker(written, MonoTime(400)).unwrap();
    assert!(clients.poll().is_none());
    worker.fail = true;
    let failure = worker.poll(1).pop().unwrap();
    assert!(owner.deliver_worker(failure, MonoTime(400)).is_err());
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
fn maintenance_wrapper_keeps_actual_capability_and_singleton_data_order() {
    let (mut owner, mut worker, mut clients, mut results, mut apps) = wrapped(2, true);
    let inner = apps[&group(1)].maintenance().unwrap();
    assert_eq!(
        apps[&group(1)].supports_ready_batch(),
        inner.supports_ready_batch()
    );
    assert!(!apps[&group(1)].supports_ready_batch());
    assert_eq!(apps[&group(1)].schema_version(), 2);
    let tickets: Vec<_> = [(1, 7), (1, 9), (1, 7)]
        .into_iter()
        .map(|(id, delta)| {
            clients
                .submit(
                    &mut owner,
                    &apps[&group(1)],
                    wrapped_request(&apps[&group(1)], id, delta),
                )
                .unwrap()
        })
        .collect();
    let steps = clients
        .advance(&mut owner, MonoTime(400), 3, |g| apps.get(&g))
        .unwrap();
    assert_eq!(steps.len(), 1);
    let lease = owner.take_effect().unwrap().unwrap();
    let Effect::Persist(update) = &lease.effect else {
        panic!()
    };
    assert_eq!(update.suffix.as_ref().unwrap().entries.len(), 1);
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
    applied(&mut clients, tickets[0], 2, CounterOutcome::Value(7), false);
    applied(
        &mut clients,
        tickets[1],
        3,
        CounterOutcome::OperationConflict,
        true,
    );
    applied(&mut clients, tickets[2], 4, CounterOutcome::Value(7), true);
    assert!(clients.is_drained());
    assert!(apps[&group(1)].maintenance().unwrap().pending().is_none());
    assert_eq!(
        apps[&group(1)].read_at(4, application::Query::Data(())),
        Ok(application::ReadResult::Data(7))
    );
}
