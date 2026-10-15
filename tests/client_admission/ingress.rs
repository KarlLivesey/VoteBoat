// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Downstream ingress composition and legacy-result differential controls.
use super::*;
use std::{
    cell::{Cell, RefCell},
    collections::BTreeSet,
    panic::{catch_unwind, AssertUnwindSafe},
    rc::Rc,
};
use voteboat::application::{PreparedAdmission, ReadyAdmission};

#[derive(Debug, Eq, PartialEq)]
enum Decision {
    Accepted(Box<ClientTicket>),
    Rejected(ClientError, GroupIdentity, OperationId, Vec<u8>, usize),
}
fn decision(result: Result<ClientTicket, ClientRejected>) -> Decision {
    match result {
        Ok(ticket) => Decision::Accepted(Box::new(ticket)),
        Err(error) => Decision::Rejected(
            error.reason,
            error.request.group,
            error.request.operation,
            error.request.bytes.clone(),
            error.request.bytes.capacity(),
        ),
    }
}
#[derive(Debug, Eq, PartialEq)]
struct CounterRun {
    decisions: Vec<Decision>,
    usage: Vec<ClientUsage>,
    applied: Vec<(ClientTicket, ProposalPosition, CounterReceipt)>,
}
fn drive_counter_groups(
    owner: &mut Owner,
    worker: &mut HostWorker,
    clients: &mut Clients,
    results: &mut ApplicationRouter<CounterReceipt>,
    apps: &mut BTreeMap<GroupIdentity, Counter>,
) {
    let mut retained = Vec::new();
    for _ in 0..1000 {
        for event in worker.poll(1) {
            owner.deliver_worker(event, MonoTime(400)).unwrap();
        }
        clients
            .advance(owner, MonoTime(400), 3, |g| apps.get(&g))
            .unwrap();
        while let Some(lease) = owner.take_effect().unwrap() {
            match &lease.effect {
                Effect::Persist(_) => retained.push(lease),
                Effect::Committed(_) => {
                    let group = lease.ticket.visit.group;
                    results
                        .submit(owner, lease, apps.get_mut(&group).unwrap(), MonoTime(400))
                        .unwrap();
                    let output = results.poll().unwrap();
                    clients.deliver(owner, results, output).unwrap();
                }
                _ => panic!("unexpected single-voter effect"),
            }
        }
        if !retained.is_empty() {
            if let Err(rejected) =
                owner.submit_persists(worker, std::mem::take(&mut retained), MonoTime(400))
            {
                assert_eq!(
                    rejected.reason,
                    EffectOwnerError::Worker(WorkerError::Overloaded)
                );
                // HostWorker holds one request. Ownership of this returned
                // batch survives until its original predecessor completes.
                retained = rejected.leases;
            }
        }
        clients.reconcile(owner, 64).unwrap();
        if owner.is_drained() && worker.is_drained() && retained.is_empty() {
            return;
        }
    }
    panic!("two-group original histories did not drain");
}
fn counter_run(scoped: bool) -> CounterRun {
    let (mut owner, mut worker, mut clients, mut results, mut apps) =
        setup_with_visit_items(2, 2, 64);
    let mut invalid = request(1, 1, 1);
    invalid.bytes = vec![1];
    let mut oversized = request(1, 1, 1);
    oversized.bytes = vec![0; 65537];
    let requests = vec![
        request(1, 1, 7),
        request(1, 2, 2),
        request(1, 1, 9),
        request(1, 3, 5),
        invalid,
        request(2, 1, 4),
        request(2, 2, 3),
        request(1, 1, 7),
        request(3, 1, 1),
        oversized,
        request(1, 2, 9),
    ];
    let mut decisions = Vec::new();
    let mut usage = Vec::new();
    if scoped {
        clients.with_submission_batch(
            &mut owner,
            |g| apps.get(&g),
            |scope| {
                for request in requests {
                    decisions.push(decision(scope.submit(request)));
                }
            },
        );
    } else {
        for request in requests {
            let app = apps.get(&request.group).unwrap_or(&apps[&group(1)]);
            decisions.push(decision(clients.submit(&mut owner, app, request)));
        }
    }
    usage.push(clients.usage());
    assert_eq!(
        decisions[3],
        Decision::Rejected(
            ClientError::Application(ApplicationError::DedupCapacity),
            group(1),
            OperationId::new(3).unwrap(),
            5i64.to_le_bytes().to_vec(),
            8,
        )
    );
    assert!(matches!(
        decisions[4],
        Decision::Rejected(
            ClientError::Application(ApplicationError::InvalidCommand),
            ..
        )
    ));
    assert_eq!(clients.usage().requests, 7);
    drive_counter_groups(
        &mut owner,
        &mut worker,
        &mut clients,
        &mut results,
        &mut apps,
    );
    let mut applied = Vec::new();
    while let Some(output) = clients.poll() {
        let ticket = output.ticket();
        let charge = clients.usage();
        assert_ne!(charge, ClientUsage::default());
        usage.push(charge);
        let ClientOutcome::Applied { position, receipt } = clients.complete(output).unwrap() else {
            panic!("every accepted original must retain its result")
        };
        applied.push((ticket, position, receipt));
        usage.push(clients.usage());
    }
    assert_eq!(applied.len(), 7);
    assert_eq!(apps[&group(1)].read_applied(6), Ok(9));
    assert_eq!(apps[&group(2)].read_applied(3), Ok(7));
    assert!(clients.is_drained() && owner.is_drained());
    CounterRun {
        decisions,
        usage,
        applied,
    }
}
#[test]
fn ingress_counter_matches_legacy_tickets_credits_retries_conflicts_and_original_results() {
    assert_eq!(counter_run(false), counter_run(true));
}

type PendingImage = Vec<(OperationId, Vec<u8>)>;
#[derive(Default)]
struct Observed {
    factories: Cell<usize>,
    ready_factories: Cell<usize>,
    live: Cell<usize>,
    drops: Cell<usize>,
    reference: RefCell<Vec<u128>>,
    prepared: RefCell<Vec<u128>>,
    guards: RefCell<Vec<u128>>,
    pending: RefCell<Vec<PendingImage>>,
    bad_bound: Cell<Option<(u128, usize)>>,
    guard_reject: Cell<Option<u128>>,
    panic_prepared: Cell<Option<u128>>,
}
struct Lease(Rc<Observed>);
impl Drop for Lease {
    fn drop(&mut self) {
        self.0.live.set(self.0.live.get() - 1);
        self.0.drops.set(self.0.drops.get() + 1);
    }
}
struct HostApp {
    inner: Counter,
    observed: Rc<Observed>,
    enabled: bool,
}
impl StateMachine for HostApp {
    type Receipt = CounterReceipt;
    fn applied_index(&self) -> u64 {
        self.inner.applied_index()
    }
    fn apply_batch(
        &mut self,
        entries: &[LogEntry],
    ) -> Result<Vec<CounterReceipt>, ApplicationError> {
        assert_eq!(
            self.observed.live.get(),
            0,
            "ingress context escaped into application"
        );
        self.inner.apply_batch(entries)
    }
}
impl BoundedStateMachine for HostApp {
    fn receipt_bytes_bound(&self, entries: &[LogEntry]) -> Result<usize, ApplicationError> {
        self.inner.receipt_bytes_bound(entries)
    }
}
impl HostApp {
    fn validate(
        &self,
        operation: OperationId,
        bytes: &[u8],
        reserved: &mut BTreeSet<OperationId>,
    ) -> Result<usize, ApplicationError> {
        if bytes.len() != 8 {
            return Err(ApplicationError::InvalidCommand);
        }
        reserved.insert(operation);
        if reserved.len() > 64 {
            return Err(ApplicationError::DedupCapacity);
        }
        Ok(self
            .observed
            .bad_bound
            .get()
            .filter(|(id, _)| *id == operation.get())
            .map_or(std::mem::size_of::<CounterReceipt>(), |(_, bound)| bound))
    }
}
impl ProposalAdmission for HostApp {
    fn supports_ready_batch(&self) -> bool {
        true
    }
    fn validate_proposal_context(
        &self,
        _: &Raft,
        operation: OperationId,
        _: &[u8],
    ) -> Result<(), ApplicationError> {
        self.observed.guards.borrow_mut().push(operation.get());
        if self.observed.guard_reject.get() == Some(operation.get()) {
            Err(ApplicationError::NotApplied)
        } else {
            Ok(())
        }
    }
    fn validate_proposal<'p>(
        &self,
        operation: OperationId,
        bytes: &[u8],
        pending: impl Iterator<Item = (OperationId, &'p [u8])>,
    ) -> Result<usize, ApplicationError> {
        self.observed.reference.borrow_mut().push(operation.get());
        self.validate(operation, bytes, &mut pending.map(|(id, _)| id).collect())
    }
    fn prepare_proposal_admission<'p>(
        &self,
        pending: impl Iterator<Item = (OperationId, &'p [u8])>,
    ) -> Option<PreparedAdmission<'_>> {
        self.observed
            .factories
            .set(self.observed.factories.get() + 1);
        if !self.enabled {
            return None;
        }
        assert_eq!(
            self.observed.live.get(),
            0,
            "only one contiguous context may live"
        );
        let pending: PendingImage = pending.map(|(id, bytes)| (id, bytes.to_vec())).collect();
        let mut reserved = pending.iter().map(|(id, _)| *id).collect();
        self.observed.pending.borrow_mut().push(pending);
        self.observed.live.set(1);
        let lease = Lease(Rc::clone(&self.observed));
        Some(Box::new(move |operation, bytes| {
            let _retained = &lease;
            self.observed.prepared.borrow_mut().push(operation.get());
            let result = self.validate(operation, bytes, &mut reserved);
            if self.observed.panic_prepared.get() == Some(operation.get()) {
                self.observed.panic_prepared.set(None);
                panic!("downstream validator panics after reserving the candidate");
            }
            result
        }))
    }
}
struct Fixture {
    owner: Owner,
    worker: HostWorker,
    clients: Clients,
    results: ApplicationRouter<CounterReceipt>,
    apps: BTreeMap<GroupIdentity, HostApp>,
    observed: Rc<Observed>,
}
fn fixture(groups: u128, enabled: bool) -> Fixture {
    let (owner, worker, clients, results, apps) = setup_with_visit_items(groups, 64, 64);
    let observed = Rc::new(Observed::default());
    Fixture {
        owner,
        worker,
        clients,
        results,
        apps: apps
            .into_iter()
            .map(|(group, inner)| {
                (
                    group,
                    HostApp {
                        inner,
                        observed: Rc::clone(&observed),
                        enabled,
                    },
                )
            })
            .collect(),
        observed,
    }
}
#[test]
fn ingress_empty_singleton_lazy_second_and_cached_none_keep_reference_fallback() {
    for (count, enabled) in [(0, true), (1, true), (4, true), (4, false)] {
        let mut f = fixture(1, enabled);
        f.clients.with_submission_batch(
            &mut f.owner,
            |g| f.apps.get(&g),
            |scope| {
                assert!(scope.ready_leader(group(1)));
                assert!(!scope.ready_leader(group(2)));
                for id in 1..=count {
                    scope.submit(request(1, id, 1)).unwrap();
                }
            },
        );
        assert_eq!(f.observed.factories.get(), usize::from(count > 1));
        assert_eq!(
            f.observed.reference.borrow().len(),
            if enabled {
                usize::from(count > 0)
            } else {
                count as usize
            }
        );
        assert_eq!(
            f.observed.prepared.borrow().len(),
            if enabled {
                count.saturating_sub(1) as usize
            } else {
                0
            }
        );
        assert_eq!(f.observed.live.get(), 0);
        assert_eq!(f.observed.drops.get(), usize::from(enabled && count > 1));
        assert_eq!(f.clients.usage().requests, count as usize);
    }
}

struct ReadyOnly(HostApp);
impl StateMachine for ReadyOnly {
    type Receipt = CounterReceipt;
    fn applied_index(&self) -> u64 {
        self.0.applied_index()
    }
    fn apply_batch(
        &mut self,
        entries: &[LogEntry],
    ) -> Result<Vec<CounterReceipt>, ApplicationError> {
        self.0.apply_batch(entries)
    }
}
impl BoundedStateMachine for ReadyOnly {
    fn receipt_bytes_bound(&self, entries: &[LogEntry]) -> Result<usize, ApplicationError> {
        self.0.receipt_bytes_bound(entries)
    }
}
impl ProposalAdmission for ReadyOnly {
    fn validate_proposal<'p>(
        &self,
        operation: OperationId,
        bytes: &[u8],
        pending: impl Iterator<Item = (OperationId, &'p [u8])>,
    ) -> Result<usize, ApplicationError> {
        self.0.validate_proposal(operation, bytes, pending)
    }
    fn prepare_ready_admission<'p>(
        &self,
        _: impl Iterator<Item = (OperationId, &'p [u8])>,
    ) -> Option<ReadyAdmission<'_>> {
        self.0
            .observed
            .ready_factories
            .set(self.0.observed.ready_factories.get() + 1);
        panic!("ready-only provider was incorrectly opted into ingress")
    }
}
#[test]
fn ingress_does_not_opt_a_ready_only_downstream_provider_in() {
    let mut f = fixture(1, true);
    let app = ReadyOnly(f.apps.remove(&group(1)).unwrap());
    f.clients.with_submission_batch(
        &mut f.owner,
        |_| Some(&app),
        |scope| {
            for id in 1..=4 {
                scope.submit(request(1, id, 1)).unwrap();
            }
        },
    );
    assert_eq!(f.observed.ready_factories.get(), 0);
    assert_eq!(f.observed.factories.get(), 0);
    assert_eq!(&*f.observed.reference.borrow(), &[1, 2, 3, 4]);
    assert_eq!(f.clients.usage().requests, 4);
}

#[test]
fn ingress_receipt_and_context_guard_errors_discard_candidate_reservations() {
    let mut f = fixture(1, true);
    f.observed.bad_bound.set(Some((3, usize::MAX)));
    f.observed.guard_reject.set(Some(6));
    f.clients.with_submission_batch(
        &mut f.owner,
        |g| f.apps.get(&g),
        |scope| {
            for id in 1..=7 {
                let result = scope.submit(request(1, id, 1));
                if id == 3 {
                    assert_eq!(result.unwrap_err().reason, ClientError::RequestTooLarge);
                } else if id == 6 {
                    assert_eq!(
                        result.unwrap_err().reason,
                        ClientError::Application(ApplicationError::NotApplied)
                    );
                } else {
                    result.unwrap();
                }
            }
        },
    );
    assert_eq!(&*f.observed.reference.borrow(), &[1, 4, 7]);
    assert_eq!(&*f.observed.prepared.borrow(), &[2, 3, 5]);
    assert_eq!(&*f.observed.guards.borrow(), &[1, 2, 3, 4, 5, 6, 7]);
    assert_eq!(f.observed.factories.get(), 2);
    assert_eq!(f.observed.drops.get(), 2);
    assert_eq!(f.observed.live.get(), 0);
    let images: Vec<_> = f
        .observed
        .pending
        .borrow()
        .iter()
        .map(|p| p.iter().map(|(id, _)| id.get()).collect::<Vec<_>>())
        .collect();
    assert_eq!(images, vec![vec![1], vec![1, 2, 4]]);
    assert!(f
        .observed
        .pending
        .borrow()
        .iter()
        .flatten()
        .all(|(_, bytes)| bytes == &1i64.to_le_bytes()));
    assert_eq!(f.clients.usage().requests, 5);
}

#[test]
fn ingress_final_byte_charge_refusal_resets_prepared_state_and_preserves_successful_credits() {
    let (mut owner, _, mut clients, _, apps) = setup(1, 64);
    clients
        .submit(&mut owner, &apps[&group(1)], request(1, 1, 1))
        .unwrap();
    let unit = clients.usage().bytes;
    let mut f = fixture(1, true);
    f.clients = Clients::new(
        f.clients.binding(),
        ClientRouterLimits {
            bytes: 4 * unit,
            receipt_bytes: 3 * unit,
            ..ClientRouterLimits::default()
        },
    )
    .unwrap();
    f.observed.bad_bound.set(Some((3, 3 * unit)));
    f.clients.with_submission_batch(
        &mut f.owner,
        |g| f.apps.get(&g),
        |scope| {
            scope.submit(request(1, 1, 1)).unwrap();
            scope.submit(request(1, 2, 1)).unwrap();
            assert_eq!(
                scope.submit(request(1, 3, 1)).unwrap_err().reason,
                ClientError::Overloaded
            );
            scope.submit(request(1, 4, 1)).unwrap();
        },
    );
    assert_eq!(
        f.clients.usage(),
        ClientUsage {
            requests: 3,
            bytes: 3 * unit
        }
    );
    assert_eq!(&*f.observed.reference.borrow(), &[1, 4]);
    assert_eq!(&*f.observed.prepared.borrow(), &[2, 3]);
    assert_eq!(f.observed.drops.get(), 1);
    assert_eq!(f.observed.live.get(), 0);
}

#[test]
fn ingress_resets_on_group_and_borrowed_application_identity_changes() {
    let mut f = fixture(2, true);
    let alternative = HostApp {
        inner: f.apps[&group(1)].inner.clone(),
        observed: Rc::clone(&f.observed),
        enabled: true,
    };
    let alternate = Cell::new(false);
    f.clients.with_submission_batch(
        &mut f.owner,
        |g| {
            if g == group(1) && alternate.get() {
                Some(&alternative)
            } else {
                f.apps.get(&g)
            }
        },
        |scope| {
            for (g, ids) in [(1, [1, 2]), (2, [21, 22]), (1, [3, 4])] {
                for id in ids {
                    scope.submit(request(g, id, 1)).unwrap();
                }
            }
            alternate.set(true);
            for id in [5, 6] {
                scope.submit(request(1, id, 1)).unwrap();
            }
        },
    );
    assert_eq!(&*f.observed.reference.borrow(), &[1, 21, 3, 5]);
    assert_eq!(&*f.observed.prepared.borrow(), &[2, 22, 4, 6]);
    assert_eq!(f.observed.factories.get(), 4);
    assert_eq!(f.observed.drops.get(), 4);
    assert_eq!(f.observed.live.get(), 0);
    let images: Vec<_> = f
        .observed
        .pending
        .borrow()
        .iter()
        .map(|p| p.iter().map(|(id, _)| id.get()).collect::<Vec<_>>())
        .collect();
    assert_eq!(
        images,
        vec![vec![1], vec![21], vec![1, 2, 3], vec![1, 2, 3, 4, 5]]
    );
}

#[test]
fn ingress_runtime_refusal_after_preparation_preserves_accepted_prefix_and_resets() {
    let mut f = fixture(1, true);
    let limits = ShardLimits::default();
    let bulk = limits.group_items - limits.group_control_items;
    // Explicit host-owned untracked inputs spend the real shard admission
    // credits. Leave two slots for tracked originals, then fail after validation.
    for offset in 0..bulk - 2 {
        f.owner
            .admit(
                group(1),
                Event::Propose {
                    operation: OperationId::new(1000 + offset as u128).unwrap(),
                    bytes: 1i64.to_le_bytes().to_vec(),
                },
            )
            .unwrap();
    }
    f.clients.with_submission_batch(
        &mut f.owner,
        |g| f.apps.get(&g),
        |scope| {
            assert_eq!(scope.submit(request(1, 1, 1)).unwrap().sequence, 1);
            assert_eq!(scope.submit(request(1, 2, 1)).unwrap().sequence, 2);
            for id in [3, 4] {
                assert_eq!(
                    scope.submit(request(1, id, 1)).unwrap_err().reason,
                    ClientError::Runtime(RuntimeError::Overloaded)
                );
            }
        },
    );
    assert_eq!(f.clients.usage().requests, 2);
    assert_eq!(&*f.observed.reference.borrow(), &[1, 4]);
    assert_eq!(&*f.observed.prepared.borrow(), &[2, 3]);
    assert_eq!(f.observed.factories.get(), 1);
    assert_eq!(f.observed.drops.get(), 1);
    assert_eq!(f.observed.live.get(), 0);
    assert!(!f.owner.is_failed());
}

#[test]
fn ingress_caught_provider_panic_discards_context_before_continuing_the_same_scope() {
    let mut f = fixture(1, true);
    f.observed.panic_prepared.set(Some(3));
    f.clients.with_submission_batch(
        &mut f.owner,
        |g| f.apps.get(&g),
        |scope| {
            scope.submit(request(1, 1, 1)).unwrap();
            scope.submit(request(1, 2, 1)).unwrap();
            assert_eq!(f.observed.live.get(), 1);
            let failed = catch_unwind(AssertUnwindSafe(|| scope.submit(request(1, 3, 1))));
            assert!(failed.is_err());
            assert_eq!(
                f.observed.live.get(),
                0,
                "caught panic retained a poisoned reservation context"
            );
            assert!(scope.ready_leader(group(1)));
            assert_eq!(scope.submit(request(1, 4, 1)).unwrap().sequence, 3);
            assert_eq!(scope.submit(request(1, 5, 1)).unwrap().sequence, 4);
        },
    );
    assert_eq!(&*f.observed.reference.borrow(), &[1, 4]);
    assert_eq!(&*f.observed.prepared.borrow(), &[2, 3, 5]);
    assert_eq!(f.observed.factories.get(), 2);
    assert_eq!(f.observed.drops.get(), 2);
    assert_eq!(f.observed.live.get(), 0);
    let images: Vec<_> = f
        .observed
        .pending
        .borrow()
        .iter()
        .map(|p| p.iter().map(|(id, _)| id.get()).collect::<Vec<_>>())
        .collect();
    assert_eq!(images, vec![vec![1], vec![1, 2, 4]]);
    assert_eq!(f.clients.usage().requests, 4);
    drive(
        &mut f.owner,
        &mut f.worker,
        &mut f.clients,
        &mut f.results,
        &mut f.apps,
    );
    let mut ids = Vec::new();
    while let Some(output) = f.clients.poll() {
        let ClientOutcome::Applied { receipt, .. } = f.clients.complete(output).unwrap() else {
            panic!()
        };
        ids.push(receipt.operation.get());
    }
    assert_eq!(ids, vec![1, 2, 4, 5]);
    assert!(f.clients.is_drained());
}

#[test]
fn ingress_callback_unwind_retains_admitted_outcomes_cancel_reservations_and_retry_history() {
    let mut f = fixture(1, true);
    let mut tickets = Vec::new();
    let unwind = catch_unwind(AssertUnwindSafe(|| {
        f.clients.with_submission_batch(
            &mut f.owner,
            |g| f.apps.get(&g),
            |scope| {
                for id in 1..=2 {
                    tickets.push(scope.submit(request(1, id, 1)).unwrap());
                }
                assert_eq!(f.observed.live.get(), 1);
                panic!("caller leaves after partial acceptance")
            },
        );
    }));
    assert!(unwind.is_err());
    assert_eq!(f.observed.live.get(), 0);
    assert_eq!(f.observed.drops.get(), 1);
    assert!(!f.owner.is_failed());
    let charge = f.clients.usage();
    f.clients.cancel_wait(tickets[0]).unwrap();
    let canceled = f.clients.poll().unwrap();
    assert!(matches!(
        f.clients.complete(canceled).unwrap(),
        ClientOutcome::Unknown(ClientUnknown::CancelledWait)
    ));
    assert_eq!(f.clients.usage(), charge);
    drive(
        &mut f.owner,
        &mut f.worker,
        &mut f.clients,
        &mut f.results,
        &mut f.apps,
    );
    let output = f.clients.poll().unwrap();
    assert_eq!(output.ticket(), tickets[1]);
    assert!(matches!(
        f.clients.complete(output).unwrap(),
        ClientOutcome::Applied { .. }
    ));
    assert!(f.clients.poll().is_none() && f.clients.is_drained());
    assert_eq!(f.apps[&group(1)].inner.read_applied(3), Ok(2));
    f.clients.with_submission_batch(
        &mut f.owner,
        |g| f.apps.get(&g),
        |scope| {
            scope.submit(request(1, 1, 1)).unwrap();
        },
    );
    drive(
        &mut f.owner,
        &mut f.worker,
        &mut f.clients,
        &mut f.results,
        &mut f.apps,
    );
    let output = f.clients.poll().unwrap();
    let ClientOutcome::Applied { receipt, .. } = f.clients.complete(output).unwrap() else {
        panic!()
    };
    assert_eq!(receipt.outcome, CounterOutcome::Value(1));
    assert!(receipt.duplicate);
    assert!(f.clients.is_drained());
}
