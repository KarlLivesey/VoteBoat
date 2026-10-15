// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Exercise the executable's actual wrapper through the existing owner fixture.
use super::*;
#[path = "../../../src/bin/support/counter_application.rs"]
mod application;
mod checks;
use application::{Application, Receipt};

// The executable source needs only these existing setup operations. Application
// admission, state, receipt shape and capability use the original module.
mod setup {
    pub type Failure = Box<dyn std::error::Error>;
    pub fn checked<T, E: std::fmt::Debug>(value: Result<T, E>) -> Result<T, Failure> {
        value.map_err(|error| format!("{error:?}").into())
    }
    pub fn group() -> voteboat::identity::GroupIdentity {
        super::group(1)
    }
}
type WrappedClients = ClientRouter<Receipt>;
type WrappedApps = BTreeMap<GroupIdentity, Application>;
type WrappedSetup = (
    Owner,
    HostWorker,
    WrappedClients,
    ApplicationRouter<Receipt>,
    WrappedApps,
);

fn wrapped(capacity: usize, maintenance: bool) -> WrappedSetup {
    let (owner, worker, _, _, _) = setup_with_visit_items(1, capacity, 64);
    let mut app = Application::new(Counter::new(capacity).unwrap(), maintenance).unwrap();
    app.apply_batch(&owner.core(group(1)).unwrap().replay_committed())
        .unwrap();
    let clients = ClientRouter::new(
        ClientRouterBinding::new(
            owner.authorization(),
            ClientRouterGeneration::new(2).unwrap(),
        ),
        ClientRouterLimits::default(),
    )
    .unwrap();
    let results = ApplicationRouter::new(
        ApplicationRouterBinding::new(
            owner.authorization(),
            ApplicationRouterGeneration::new(2).unwrap(),
        ),
        ApplicationRouterLimits::default(),
    )
    .unwrap();
    (
        owner,
        worker,
        clients,
        results,
        BTreeMap::from([(group(1), app)]),
    )
}
fn wrapped_request(app: &Application, id: u128, delta: i64) -> ClientRequest {
    ClientRequest {
        group: group(1),
        operation: OperationId::new(id).unwrap(),
        bytes: app.data(delta).unwrap(),
    }
}
fn applied(
    clients: &mut WrappedClients,
    ticket: ClientTicket,
    index: u64,
    expected: CounterOutcome,
    duplicate: bool,
) {
    let charge = clients.usage();
    let completion = clients.poll().unwrap();
    assert_eq!(completion.ticket(), ticket);
    assert_eq!(clients.usage(), charge);
    let ClientOutcome::Applied {
        position,
        receipt: Receipt::Data(receipt),
    } = clients.complete(completion).unwrap()
    else {
        panic!("individual data receipt required")
    };
    assert_eq!(position.index, index);
    assert_eq!(position.term, 1);
    assert_eq!(receipt.index, position.index);
    assert_eq!(receipt.operation, ticket.operation);
    assert_eq!(receipt.outcome, expected);
    assert_eq!(receipt.duplicate, duplicate);
}

#[test]
fn plain_service_wrapper_batches_originals_with_full_wrapper_receipt_bounds() {
    let (mut owner, mut worker, mut clients, mut results, mut apps) = wrapped(2, false);
    assert!(apps[&group(1)].supports_ready_batch());
    assert!(apps[&group(1)].maintenance().is_err());
    let tickets: Vec<_> = [(1, 7), (2, 2), (1, 9), (1, 7)]
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
    let refused = clients
        .submit(
            &mut owner,
            &apps[&group(1)],
            wrapped_request(&apps[&group(1)], 3, 1),
        )
        .unwrap_err();
    assert_eq!(
        refused.reason,
        ClientError::Application(ApplicationError::DedupCapacity)
    );
    let steps = clients
        .advance(&mut owner, MonoTime(400), 8, |g| apps.get(&g))
        .unwrap();
    assert_eq!(steps.len(), 4);
    for (offset, step) in steps.iter().enumerate() {
        assert!(step.admission.is_some());
        assert!(steps[..offset]
            .iter()
            .all(|earlier| earlier.admission != step.admission));
        assert_eq!(step.visit, steps[0].visit);
        assert_eq!(step.operation, Some(tickets[offset].operation));
        assert_eq!(step.proposed.unwrap().index, offset as u64 + 2);
        assert!(step.error.is_none());
    }
    let lease = owner.take_effect().unwrap().unwrap();
    let Effect::Persist(update) = &lease.effect else {
        panic!()
    };
    let entries = &update.suffix.as_ref().unwrap().entries;
    assert_eq!(entries.len(), 4);
    assert_eq!(
        apps[&group(1)].receipt_bytes_bound(entries),
        Ok(4 * std::mem::size_of::<Receipt>())
    );
    assert!(owner.take_effect().unwrap().is_none());
    owner
        .submit_persists(&mut worker, vec![lease], MonoTime(400))
        .unwrap();
    let written = worker.poll(1).pop().unwrap();
    assert!(matches!(written, WorkerEvent::Written { .. }));
    owner.deliver_worker(written, MonoTime(400)).unwrap();
    assert_eq!(owner.core(group(1)).unwrap().state().last_index(), 1);
    assert_eq!(apps[&group(1)].applied_index(), 1);
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
        applied(
            &mut clients,
            tickets[offset],
            offset as u64 + 2,
            outcome,
            offset >= 2,
        );
    }
    assert!(clients.is_drained());
    assert!(results.is_drained());
    assert!(owner.is_drained());
    assert_eq!(
        apps[&group(1)].read_at(5, application::Query::Data(())),
        Ok(application::ReadResult::Data(9))
    );
}
