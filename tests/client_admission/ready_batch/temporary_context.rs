// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Independent downstream implementation of the optional temporary capability.
use super::*;
use std::{
    cell::{Cell, RefCell},
    collections::BTreeSet,
    rc::Rc,
};
use voteboat::application::ReadyAdmission;

type PendingCommands = Vec<(u128, Vec<u8>)>;

#[derive(Default)]
struct Observations {
    factories: Cell<usize>,
    live: Cell<usize>,
    max_live: Cell<usize>,
    drops: Cell<usize>,
    reference: RefCell<Vec<u128>>,
    prepared: RefCell<Vec<u128>>,
    guards: RefCell<Vec<u128>>,
    pending: RefCell<Vec<PendingCommands>>,
    reject: Cell<Option<(u128, ApplicationError)>>,
    bad_bound: Cell<Option<u128>>,
    guard_reject: Cell<Option<u128>>,
}
impl Observations {
    fn clear_calls(&self) {
        self.reference.borrow_mut().clear();
        self.prepared.borrow_mut().clear();
        self.guards.borrow_mut().clear();
    }
}
struct TemporaryLease(Rc<Observations>);
impl Drop for TemporaryLease {
    fn drop(&mut self) {
        self.0.live.set(self.0.live.get() - 1);
        self.0.drops.set(self.0.drops.get() + 1);
    }
}
struct HostApplication {
    inner: Counter,
    observations: Rc<Observations>,
    enabled: bool,
}
impl StateMachine for HostApplication {
    type Receipt = CounterReceipt;
    fn applied_index(&self) -> u64 {
        self.inner.applied_index()
    }
    fn apply_batch(
        &mut self,
        entries: &[LogEntry],
    ) -> Result<Vec<CounterReceipt>, ApplicationError> {
        assert_eq!(
            self.observations.live.get(),
            0,
            "context survived into application execution"
        );
        self.inner.apply_batch(entries)
    }
}
impl BoundedStateMachine for HostApplication {
    fn receipt_bytes_bound(&self, entries: &[LogEntry]) -> Result<usize, ApplicationError> {
        self.inner.receipt_bytes_bound(entries)
    }
}
impl HostApplication {
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
        if let Some((id, error)) = self.observations.reject.get() {
            if id == operation.get() {
                return Err(error);
            }
        }
        Ok(std::mem::size_of::<CounterReceipt>()
            + usize::from(self.observations.bad_bound.get() == Some(operation.get())))
    }
}
impl ProposalAdmission for HostApplication {
    fn supports_ready_batch(&self) -> bool {
        true
    }
    fn validate_proposal_context(
        &self,
        _: &Raft,
        operation: OperationId,
        _: &[u8],
    ) -> Result<(), ApplicationError> {
        self.observations.guards.borrow_mut().push(operation.get());
        if self.observations.guard_reject.get() == Some(operation.get()) {
            Err(ApplicationError::NotApplied)
        } else {
            Ok(())
        }
    }
    fn validate_proposal<'pending>(
        &self,
        operation: OperationId,
        bytes: &[u8],
        pending: impl Iterator<Item = (OperationId, &'pending [u8])>,
    ) -> Result<usize, ApplicationError> {
        self.observations
            .reference
            .borrow_mut()
            .push(operation.get());
        let mut reserved = pending.map(|(id, _)| id).collect();
        self.validate(operation, bytes, &mut reserved)
    }
    fn prepare_ready_admission<'pending>(
        &self,
        pending: impl Iterator<Item = (OperationId, &'pending [u8])>,
    ) -> Option<ReadyAdmission<'_>> {
        self.observations
            .factories
            .set(self.observations.factories.get() + 1);
        if !self.enabled {
            return None;
        }
        // Own borrowed iterator content, independently of Counter's context.
        let pending: Vec<_> = pending.map(|(id, bytes)| (id, bytes.to_vec())).collect();
        self.observations.pending.borrow_mut().push(
            pending
                .iter()
                .map(|(id, bytes)| (id.get(), bytes.clone()))
                .collect(),
        );
        let mut reserved = pending.iter().map(|(id, _)| *id).collect();
        self.observations.live.set(self.observations.live.get() + 1);
        self.observations.max_live.set(
            self.observations
                .max_live
                .get()
                .max(self.observations.live.get()),
        );
        let lease = TemporaryLease(Rc::clone(&self.observations));
        Some(Box::new(move |operation, bytes| {
            let _retained = &lease;
            self.observations
                .prepared
                .borrow_mut()
                .push(operation.get());
            self.validate(operation, bytes, &mut reserved)
        }))
    }
}
type Fixture = (
    Owner,
    HostWorker,
    Clients,
    ApplicationRouter<CounterReceipt>,
    BTreeMap<GroupIdentity, HostApplication>,
    Rc<Observations>,
);
fn fixture(groups: u128, enabled: bool) -> Fixture {
    let (owner, worker, clients, results, apps) = setup_with_visit_items(groups, 64, 64);
    let observed = Rc::new(Observations::default());
    let apps = apps
        .into_iter()
        .map(|(id, inner)| {
            (
                id,
                HostApplication {
                    inner,
                    enabled,
                    observations: Rc::clone(&observed),
                },
            )
        })
        .collect();
    (owner, worker, clients, results, apps, observed)
}
fn submit(
    owner: &mut Owner,
    clients: &mut Clients,
    apps: &BTreeMap<GroupIdentity, HostApplication>,
    group_id: u128,
    ids: &[u128],
) -> Vec<ClientTicket> {
    ids.iter()
        .map(|id| {
            clients
                .submit(owner, &apps[&group(group_id)], request(group_id, *id, 1))
                .unwrap()
        })
        .collect()
}

#[test]
fn singleton_never_prepares_and_fallback_is_attempted_once_per_prefix() {
    for (count, enabled) in [(1, true), (4, false)] {
        let (mut owner, _, mut clients, _, apps, observed) = fixture(1, enabled);
        submit(
            &mut owner,
            &mut clients,
            &apps,
            1,
            &(1..=count).collect::<Vec<_>>(),
        );
        observed.clear_calls();
        let steps = clients
            .advance(&mut owner, MonoTime(400), 8, |g| apps.get(&g))
            .unwrap();
        assert_eq!(steps.len(), count as usize);
        assert_eq!(observed.factories.get(), usize::from(count > 1));
        assert_eq!(observed.reference.borrow().len(), count as usize);
        assert_eq!(observed.guards.borrow().len(), count as usize);
        assert!(observed.prepared.borrow().is_empty());
        assert_eq!(observed.live.get(), 0);
    }
}

#[test]
fn lazy_context_sees_all_pending_and_drops_before_application() {
    let (mut owner, mut worker, mut clients, mut results, mut apps, observed) = fixture(1, true);
    let tickets = submit(&mut owner, &mut clients, &apps, 1, &[1, 2, 3, 4]);
    observed.clear_calls();
    let steps = clients
        .advance(&mut owner, MonoTime(400), 2, |g| apps.get(&g))
        .unwrap();
    assert_eq!(steps.len(), 2);
    assert_eq!(&*observed.reference.borrow(), &[1]);
    assert_eq!(&*observed.prepared.borrow(), &[2]);
    assert_eq!(&*observed.guards.borrow(), &[1, 2]);
    assert_eq!(observed.factories.get(), 1);
    assert_eq!(observed.drops.get(), 1);
    assert_eq!(observed.live.get(), 0);
    let pending = observed.pending.borrow();
    assert_eq!(
        pending[0].iter().map(|(id, _)| *id).collect::<Vec<_>>(),
        vec![1, 2, 3, 4]
    );
    assert!(pending[0]
        .iter()
        .all(|(_, bytes)| bytes == &1i64.to_le_bytes()));
    drop(pending);
    drive(
        &mut owner,
        &mut worker,
        &mut clients,
        &mut results,
        &mut apps,
    );
    for (offset, ticket) in tickets.into_iter().enumerate() {
        let completion = clients.poll().unwrap();
        assert_eq!(completion.ticket(), ticket);
        let ClientOutcome::Applied { position, receipt } = clients.complete(completion).unwrap()
        else {
            panic!("each original must apply")
        };
        assert_eq!(position.index, offset as u64 + 2);
        assert_eq!(receipt.index, position.index);
        assert_eq!(receipt.operation, ticket.operation);
    }
    assert_eq!(observed.factories.get(), 2, "a later prefix must reprepare");
    assert_eq!(observed.drops.get(), 2);
    assert_eq!(observed.max_live.get(), 1);
    assert!(clients.is_drained() && owner.is_drained());
}

#[test]
fn contexts_reset_between_groups_without_cross_group_reservations() {
    let (mut owner, _, mut clients, _, apps, observed) = fixture(2, true);
    submit(&mut owner, &mut clients, &apps, 1, &[11, 12]);
    submit(&mut owner, &mut clients, &apps, 2, &[21, 22]);
    observed.clear_calls();
    let steps = clients
        .advance(&mut owner, MonoTime(400), 4, |g| apps.get(&g))
        .unwrap();
    assert_eq!(steps.len(), 4);
    assert_eq!(observed.factories.get(), 2);
    assert_eq!(observed.drops.get(), 2);
    assert_eq!(observed.max_live.get(), 1);
    let sets: BTreeSet<_> = observed
        .pending
        .borrow()
        .iter()
        .map(|p| p.iter().map(|(id, _)| *id).collect::<Vec<_>>())
        .collect();
    assert_eq!(sets, BTreeSet::from([vec![11, 12], vec![21, 22]]));
    for step in steps {
        assert_eq!(step.error, None);
    }
}

#[test]
fn later_capacity_or_receipt_refusal_keeps_successful_prefix_and_drops_context() {
    for receipt in [false, true] {
        let (mut owner, mut worker, mut clients, mut results, mut apps, observed) =
            fixture(1, true);
        let tickets = submit(&mut owner, &mut clients, &apps, 1, &[1, 2, 3, 4]);
        if receipt {
            observed.bad_bound.set(Some(3));
        } else {
            observed
                .reject
                .set(Some((3, ApplicationError::DedupCapacity)));
        }
        observed.clear_calls();
        let steps = clients
            .advance(&mut owner, MonoTime(400), 4, |g| apps.get(&g))
            .unwrap();
        assert_eq!(steps.len(), 2);
        assert_eq!(&*observed.reference.borrow(), &[1]);
        assert_eq!(&*observed.prepared.borrow(), &[2, 3]);
        assert_eq!(observed.drops.get(), 1);
        assert_eq!(observed.live.get(), 0);
        assert!(clients.poll().is_none());
        drive(
            &mut owner,
            &mut worker,
            &mut clients,
            &mut results,
            &mut apps,
        );
        let mut outcomes = BTreeMap::new();
        while let Some(completion) = clients.poll() {
            outcomes.insert(
                completion.ticket().sequence,
                clients.complete(completion).unwrap(),
            );
        }
        let expected = if receipt {
            ApplicationError::ReceiptBudget
        } else {
            ApplicationError::DedupCapacity
        };
        assert!(
            matches!(outcomes[&tickets[2].sequence], ClientOutcome::NotProposed(RaftError::Admission(error)) if error == expected)
        );
        for (i, ticket) in tickets.iter().enumerate().filter(|(i, _)| *i != 2) {
            let ClientOutcome::Applied { position, .. } = outcomes[&ticket.sequence] else {
                panic!("successful prefix/suffix must apply")
            };
            assert_eq!(position.index, if i == 3 { 4 } else { i as u64 + 2 });
        }
        assert!(clients.is_drained() && owner.is_drained());
        assert_eq!(observed.live.get(), 0);
    }
}

#[test]
fn per_original_context_guard_precedes_prepared_validation() {
    let (mut owner, _, mut clients, _, apps, observed) = fixture(1, true);
    submit(&mut owner, &mut clients, &apps, 1, &[1, 2, 3]);
    observed.guard_reject.set(Some(3));
    observed
        .reject
        .set(Some((3, ApplicationError::DedupCapacity)));
    observed.clear_calls();
    let steps = clients
        .advance(&mut owner, MonoTime(400), 3, |g| apps.get(&g))
        .unwrap();
    assert_eq!(steps.len(), 2);
    assert_eq!(&*observed.guards.borrow(), &[1, 2, 3]);
    assert_eq!(&*observed.prepared.borrow(), &[2]);
    assert_eq!(observed.live.get(), 0);
    assert_eq!(observed.drops.get(), 1);
}
