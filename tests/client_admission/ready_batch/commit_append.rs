// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Three-voter host histories; deterministic fixture tokens are not disk sync.
use super::*;

struct App {
    counter: Counter,
    combine: bool,
    reject: Option<OperationId>,
    expected_commit: Option<u64>,
}
impl StateMachine for App {
    type Receipt = CounterReceipt;
    fn applied_index(&self) -> u64 {
        self.counter.applied_index()
    }
    fn apply_batch(
        &mut self,
        entries: &[LogEntry],
    ) -> Result<Vec<CounterReceipt>, ApplicationError> {
        self.counter.apply_batch(entries)
    }
}
impl BoundedStateMachine for App {
    fn receipt_bytes_bound(&self, entries: &[LogEntry]) -> Result<usize, ApplicationError> {
        self.counter.receipt_bytes_bound(entries)
    }
}
impl ProposalAdmission for App {
    fn supports_ready_batch(&self) -> bool {
        true
    }
    fn supports_commit_append_batch(&self) -> bool {
        self.combine
    }
    fn validate_proposal_context(
        &self,
        core: &Raft,
        operation: OperationId,
        _: &[u8],
    ) -> Result<(), ApplicationError> {
        if self.reject == Some(operation)
            || self
                .expected_commit
                .is_some_and(|index| index != core.state().commit_index)
        {
            Err(ApplicationError::InvalidCommand)
        } else {
            Ok(())
        }
    }
    fn validate_proposal<'a>(
        &self,
        operation: OperationId,
        bytes: &[u8],
        pending: impl Iterator<Item = (OperationId, &'a [u8])>,
    ) -> Result<usize, ApplicationError> {
        self.counter.validate_proposal(operation, bytes, pending)
    }
}
struct Fixture {
    owner: Owner,
    worker: HostWorker,
    clients: Clients,
    results: ApplicationRouter<CounterReceipt>,
    apps: BTreeMap<GroupIdentity, App>,
}
fn fixture(combine: bool) -> Fixture {
    fixture_with_bulk_limit(combine, false)
}
fn fixture_with_bulk_limit(combine: bool, tight: bool) -> Fixture {
    let mut store = HostLogStore::new(1);
    let runtime = timed_with_visit_items(1, &mut store, 1, 3, 64);
    let worker = HostWorker::new(store);
    let mut limits = EffectOwnerLimits::default();
    if tight {
        let bulk = runtime
            .core(group(1))
            .unwrap()
            .effect_reservation(runtime.limits().max_event_bytes)
            .unwrap()
            + 4096;
        limits.control_bytes = limits.reserved_bytes - bulk;
    }
    let owner = EffectOwner::new(runtime, worker.binding(), limits).unwrap();
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
    let apps = BTreeMap::from([(
        group(1),
        App {
            counter: Counter::new(64).unwrap(),
            combine,
            reject: None,
            expected_commit: None,
        },
    )]);
    let mut f = Fixture {
        owner,
        worker,
        clients,
        results,
        apps,
    };
    drain(&mut f);
    assert_eq!(f.owner.core(group(1)).unwrap().role(), Role::Leader);
    assert_eq!(f.apps[&group(1)].applied_index(), 1);
    f
}
fn response(owner: &Owner, request: &Message) -> Option<Message> {
    if request.to != node(2) {
        return None;
    }
    let rpc = match &request.rpc {
        Rpc::Vote { .. } => Rpc::Voted { granted: true },
        Rpc::Append {
            previous_index,
            entries,
            ..
        } => Rpc::Appended {
            success: true,
            matching_index: entries.last().map_or(*previous_index, |entry| entry.index),
        },
        _ => return None,
    };
    Some(Message {
        group: request.group,
        configuration: request.configuration,
        from: request.to,
        to: request.from,
        term: request.term,
        context: request.context,
        sender: StoreBinding {
            identity: owner
                .core(group(1))
                .unwrap()
                .membership()
                .replica_store(request.to)
                .unwrap(),
            session: StoreSession::new(1).unwrap(),
        },
        rpc,
    })
}
fn route(f: &mut Fixture, lease: EffectLease) {
    match &lease.effect {
        Effect::Persist(_) => {
            f.owner
                .submit_persists(&mut f.worker, vec![lease], MonoTime(400))
                .unwrap();
        }
        Effect::Committed(_) => {
            f.results
                .submit(
                    &mut f.owner,
                    lease,
                    f.apps.get_mut(&group(1)).unwrap(),
                    MonoTime(400),
                )
                .unwrap();
            let output = f.results.poll().unwrap();
            f.clients
                .deliver(&mut f.owner, &mut f.results, output)
                .unwrap();
        }
        Effect::Send(message) => {
            if let Some(reply) = response(&f.owner, message) {
                f.owner.admit(group(1), Event::Receive(reply)).unwrap();
            }
            f.owner
                .release(lease, f.apps[&group(1)].applied_index(), MonoTime(400))
                .unwrap();
        }
        _ => panic!("unexpected ordinary fixture effect"),
    }
}
fn drain(f: &mut Fixture) {
    for _ in 0..1000 {
        for event in f.worker.poll(1) {
            f.owner.deliver_worker(event, MonoTime(400)).unwrap();
        }
        f.clients
            .advance(&mut f.owner, MonoTime(400), 64, |g| f.apps.get(&g))
            .unwrap();
        while let Some(lease) = f.owner.take_effect().unwrap() {
            route(f, lease);
        }
        f.clients.reconcile(&f.owner, 64).unwrap();
        if f.owner.is_drained() && f.worker.is_drained() {
            return;
        }
    }
    panic!("three-voter fixture did not drain");
}
fn submit(f: &mut Fixture, id: u128, delta: i64) -> ClientTicket {
    f.clients
        .submit(&mut f.owner, &f.apps[&group(1)], request(1, id, delta))
        .unwrap()
}
fn old_append(f: &mut Fixture) -> (Vec<ClientTicket>, Message) {
    let tickets = vec![submit(f, 10, 1), submit(f, 11, 1)];
    let steps = f
        .clients
        .advance(&mut f.owner, MonoTime(400), 8, |g| f.apps.get(&g))
        .unwrap();
    assert_eq!(steps.len(), 2);
    let lease = f.owner.take_effect().unwrap().unwrap();
    f.owner
        .submit_persists(&mut f.worker, vec![lease], MonoTime(400))
        .unwrap();
    for _ in 0..2 {
        let event = f.worker.poll(1).pop().unwrap();
        f.owner.deliver_worker(event, MonoTime(400)).unwrap();
    }
    let mut ack = None;
    while let Some(lease) = f.owner.take_effect().unwrap() {
        let Effect::Send(message) = &lease.effect else {
            panic!("append replication required")
        };
        ack = ack.or_else(|| response(&f.owner, message));
        f.owner.release(lease, 1, MonoTime(400)).unwrap();
    }
    assert_eq!(f.owner.core(group(1)).unwrap().state().last_index(), 3);
    assert_eq!(f.owner.core(group(1)).unwrap().state().commit_index, 1);
    (tickets, ack.unwrap())
}
fn pending_effect(f: &mut Fixture, ack: Message, limit: usize) -> (Vec<OwnerStep>, EffectLease) {
    f.owner.admit(group(1), Event::Receive(ack)).unwrap();
    let steps = f
        .clients
        .advance(&mut f.owner, MonoTime(400), limit, |g| f.apps.get(&g))
        .unwrap();
    let lease = f.owner.take_effect().unwrap().unwrap();
    assert!(matches!(lease.effect, Effect::Persist(_)));
    (steps, lease)
}

#[test]
fn fused_control_keeps_each_repeated_full_id_invocation_position_and_outcome() {
    let mut f = fixture(true);
    let (mut tickets, ack) = old_append(&mut f);
    for delta in [7, 9, 7] {
        tickets.push(submit(&mut f, u128::MAX - 1, delta));
    }
    let (steps, lease) = pending_effect(&mut f, ack, 8);
    assert_eq!(steps.len(), 4);
    assert!(steps[0].operation.is_none());
    for (offset, step) in steps[1..].iter().enumerate() {
        assert!(step.admission.is_some());
        assert_eq!(step.operation, Some(tickets[offset + 2].operation));
        assert_eq!(step.proposed.unwrap().index, offset as u64 + 4);
        assert!(step.error.is_none());
    }
    let Effect::Persist(update) = &lease.effect else {
        unreachable!()
    };
    assert_eq!(update.commit_index, 3);
    assert_eq!(update.suffix.as_ref().unwrap().entries.len(), 3);
    assert_eq!(f.apps[&group(1)].applied_index(), 1);
    assert!(f.clients.poll().is_none());
    route(&mut f, lease);
    drain(&mut f);
    let mut received = Vec::new();
    while let Some(completion) = f.clients.poll() {
        let ticket = completion.ticket();
        let ClientOutcome::Applied { position, receipt } = f.clients.complete(completion).unwrap()
        else {
            panic!("original result required")
        };
        assert_eq!(position.index, receipt.index);
        received.push((ticket, receipt));
    }
    assert_eq!(
        received
            .iter()
            .map(|(ticket, _)| *ticket)
            .collect::<Vec<_>>(),
        tickets
    );
    assert_eq!(received[2].1.outcome, CounterOutcome::Value(9));
    assert_eq!(received[3].1.outcome, CounterOutcome::OperationConflict);
    assert_eq!(received[4].1.outcome, CounterOutcome::Value(9));
    assert!(f.clients.is_drained());
}

#[test]
fn one_ready_original_can_share_commit_without_expanding_poll_step_limit() {
    for limit in [1, 2] {
        let mut f = fixture(true);
        let (_, ack) = old_append(&mut f);
        submit(&mut f, 12, 1);
        let (steps, lease) = pending_effect(&mut f, ack, limit);
        let Effect::Persist(update) = &lease.effect else {
            unreachable!()
        };
        assert_eq!(steps.len(), limit);
        assert_eq!(update.suffix.is_some(), limit == 2);
        route(&mut f, lease);
        drain(&mut f);
    }
}

#[test]
fn context_sensitive_opt_out_rechecks_after_commit_and_protects_original_outcome() {
    let mut f = fixture(false);
    let (_, ack) = old_append(&mut f);
    f.apps.get_mut(&group(1)).unwrap().expected_commit = Some(1);
    let ticket = submit(&mut f, 12, 1);
    let (steps, lease) = pending_effect(&mut f, ack, 8);
    assert_eq!(steps.len(), 1);
    assert!(matches!(&lease.effect, Effect::Persist(update) if update.suffix.is_none()));
    route(&mut f, lease);
    drain(&mut f);
    let mut outcome = None;
    while let Some(completion) = f.clients.poll() {
        let current = completion.ticket();
        let result = f.clients.complete(completion).unwrap();
        if current == ticket {
            outcome = Some(result);
        }
    }
    assert!(matches!(
        outcome,
        Some(ClientOutcome::NotProposed(RaftError::Admission(
            ApplicationError::InvalidCommand
        )))
    ));
    assert_eq!(f.owner.core(group(1)).unwrap().state().last_index(), 3);
}

#[test]
fn invalid_ready_prefix_never_consumes_rejected_tail_or_replaces_commit_on_first_error() {
    for rejected in [12, 13] {
        let mut f = fixture(true);
        let (_, ack) = old_append(&mut f);
        for id in [12, 13, 14] {
            submit(&mut f, id, 1);
        }
        f.apps.get_mut(&group(1)).unwrap().reject = OperationId::new(rejected);
        let (steps, lease) = pending_effect(&mut f, ack, 8);
        let Effect::Persist(update) = &lease.effect else {
            unreachable!()
        };
        let count = usize::from(rejected == 13);
        assert_eq!(steps.len(), 1 + count);
        assert_eq!(
            update
                .suffix
                .as_ref()
                .map_or(0, |suffix| suffix.entries.len()),
            count
        );
        assert_eq!(f.clients.usage().requests, 5);
        route(&mut f, lease);
        drain(&mut f);
        let mut not_proposed = 0;
        while let Some(completion) = f.clients.poll() {
            not_proposed += usize::from(matches!(
                f.clients.complete(completion).unwrap(),
                ClientOutcome::NotProposed(_)
            ));
        }
        assert_eq!(not_proposed, 1);
        assert!(f.clients.is_drained());
    }
}

#[test]
fn due_control_turn_keeps_commit_separate_from_ready_data() {
    let mut f = fixture(true);
    let (_, ack) = old_append(&mut f);
    submit(&mut f, 12, 1);
    f.owner.admit(group(1), Event::Receive(ack)).unwrap();
    f.owner.admit(group(1), Event::Heartbeat).unwrap();
    let steps = f
        .clients
        .advance(&mut f.owner, MonoTime(400), 8, |g| f.apps.get(&g))
        .unwrap();
    assert_eq!(steps.len(), 1);
    let lease = f.owner.take_effect().unwrap().unwrap();
    assert!(matches!(&lease.effect, Effect::Persist(update) if update.suffix.is_none()));
    assert_eq!(f.clients.usage().requests, 3);
    route(&mut f, lease);
    drain(&mut f);
    assert!(!f.owner.is_failed());
}

#[test]
fn protected_control_reserve_never_pays_for_a_bulk_fusion() {
    let mut f = fixture_with_bulk_limit(true, true);
    let (_, ack) = old_append(&mut f);
    for id in 12..28 {
        submit(&mut f, id, 1);
    }
    let (steps, lease) = pending_effect(&mut f, ack, 32);
    assert_eq!(steps.len(), 1);
    assert!(matches!(&lease.effect, Effect::Persist(update) if update.suffix.is_none()));
    assert_eq!(f.clients.usage().requests, 18);
    assert!(!f.owner.is_failed());
    route(&mut f, lease);
    drain(&mut f);
    let mut applied = 0;
    while let Some(completion) = f.clients.poll() {
        assert!(matches!(
            f.clients.complete(completion).unwrap(),
            ClientOutcome::Applied { .. }
        ));
        applied += 1;
    }
    assert_eq!(applied, 18);
}
