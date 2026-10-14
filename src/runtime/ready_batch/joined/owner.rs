// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Owner/worker ownership controls; this deterministic worker makes no disk claim.
use super::*;
use crate::{
    native::runtime::{DeadlineQueue, JitterEntropy},
    worker::*,
};
type Owner = EffectOwner<Ready, DeadlineQueue, JitterEntropy>;
struct Worker {
    binding: WorkerBinding,
    state: GroupLog,
    events: VecDeque<WorkerEvent>,
}
impl PersistenceWorker for Worker {
    fn binding(&self) -> WorkerBinding {
        self.binding
    }
    fn limits(&self) -> WorkerLimits {
        WorkerLimits::default()
    }
    fn usage(&self) -> WorkerUsage {
        WorkerUsage::default()
    }
    fn submit(&mut self, units: Vec<PersistUnit>) -> Result<WorkerTicket, WorkerRejected> {
        assert!(self.events.is_empty());
        assert_eq!(units.len(), 1);
        let unit = &units[0];
        let mut states = BTreeMap::from([(group(), self.state.clone())]);
        apply_batch(
            &mut states,
            &[LogMutation::Update(unit.update.clone())],
            LogLimits::default(),
        )
        .unwrap();
        self.state = states.remove(&group()).unwrap();
        let request = WorkerTicket {
            binding: self.binding,
            sequence: self.state.revision.get(),
        };
        let ticket = LogTicket {
            binding: self.binding.store,
            batch: self.state.revision.get(),
            group: group(),
            revision: self.state.revision,
            generation: self.state.generation,
            last_index: self.state.last_index(),
            term: self.state.hard_state.term,
        };
        self.events.push_back(WorkerEvent::Written {
            request,
            admissions: vec![(unit.visit, ticket)],
        });
        self.events.push_back(WorkerEvent::Durable {
            request,
            visits: vec![unit.visit],
            completion: DurableLog {
                tickets: vec![ticket],
            },
        });
        Ok(request)
    }
    fn poll(&mut self, limit: usize) -> Vec<WorkerEvent> {
        (0..limit).filter_map(|_| self.events.pop_front()).collect()
    }
    fn close(&mut self) {}
}
fn setup(limits: EffectOwnerLimits, batch: BatchLimits) -> (Owner, Worker, Vec<Message>) {
    let (shard, replies) = prepared(ShardLimits::default(), batch);
    let binding = WorkerBinding {
        store: shard.owner.store,
        generation: StorageWorkerGeneration::new(1).unwrap(),
    };
    let worker = Worker {
        binding,
        state: shard.core(group()).unwrap().state().clone(),
        events: VecDeque::new(),
    };
    let timers = DeadlineQueue::new(shard.owner, 1).unwrap();
    let timed = TimedShard::new(
        shard,
        timers,
        JitterEntropy::new(1),
        TimerConfig::default(),
        MonoTime(0),
    )
    .unwrap();
    (
        EffectOwner::new(timed, binding, limits).unwrap(),
        worker,
        replies,
    )
}
fn queue(owner: &mut Owner, replies: Vec<Message>) {
    for reply in replies {
        owner.admit(group(), Event::Receive(reply)).unwrap();
    }
    owner.admit_tracked(group(), command(u128::MAX)).unwrap();
    owner
        .admit_tracked(group(), command((1u128 << 96) + 3))
        .unwrap();
}
fn retained(owner: &Owner, expected: bool) {
    assert_eq!(owner.has_pending_peer_ingress(group(), node(2)), expected);
    assert_eq!(owner.has_pending_peer_ingress(group(), node(3)), expected);
}
#[test]
fn both_receive_sources_live_through_lease_written_durable_and_final_retirement() {
    let (mut owner, mut worker, replies) =
        setup(EffectOwnerLimits::default(), BatchLimits::default());
    queue(&mut owner, replies);
    let token = owner.deadline(group()).unwrap();
    let steps = owner
        .advance_batch_authorized(MonoTime(0), 4, |_| (true, true), |_, _, _| Ok(()))
        .unwrap();
    assert_eq!(steps.len(), 4);
    assert!(steps
        .iter()
        .all(|step| step.visit == steps[0].visit && step.error.is_none()));
    assert!(steps[..2].iter().all(|step| step.operation.is_none()));
    assert_eq!(steps[2].operation.unwrap().get(), u128::MAX);
    assert_eq!(steps[3].operation.unwrap().get(), (1u128 << 96) + 3);
    assert_eq!(steps[2].proposed.unwrap().index, 2);
    assert_eq!(steps[3].proposed.unwrap().index, 3);
    assert_eq!(owner.deadline(group()), Some(token));
    retained(&owner, true);
    let persist = owner.take_effect().unwrap().unwrap();
    retained(&owner, true);
    owner
        .submit_persists(&mut worker, vec![persist], MonoTime(0))
        .unwrap();
    retained(&owner, true);
    let written = worker.poll(1).pop().unwrap();
    assert!(matches!(written, WorkerEvent::Written { .. }));
    owner.deliver_worker(written, MonoTime(0)).unwrap();
    retained(&owner, true);
    // Early ordinary suffix sends may precede Durable, but cannot retire the
    // receive-source union while this same visit still waits on its barrier.
    while let Some(lease) = owner.take_effect().unwrap() {
        assert!(matches!(lease.effect, Effect::Send(_)));
        owner.release_transferred_send(lease.ticket).unwrap();
        retained(&owner, true);
    }
    owner
        .deliver_worker(worker.poll(1).pop().unwrap(), MonoTime(0))
        .unwrap();
    retained(&owner, true);
    let mut committed = 0;
    while let Some(lease) = owner.take_effect().unwrap() {
        match &lease.effect {
            Effect::Send(_) => owner.release_transferred_send(lease.ticket).unwrap(),
            Effect::Committed(entries) => {
                committed += entries.len();
                let applied = entries.last().unwrap().index;
                retained(&owner, true);
                owner.release(lease, applied, MonoTime(0)).unwrap();
            }
            other => panic!("unexpected joined output {other:?}"),
        }
    }
    assert_eq!(committed, 1);
    retained(&owner, false);
    assert_eq!(owner.usage().active_visits, 0);
}

#[test]
fn unavailable_bulk_or_capability_keeps_original_protected_control_and_queued_data() {
    for enabled in [false, true] {
        // Admit each ordinary proposal, but leave no bulk room for the
        // joined producer's additional staging/output/source reservation.
        let (core, _) = leader(BatchLimits::default());
        let ordinary = core
            .effect_reservation(ShardLimits::default().max_event_bytes)
            .unwrap();
        let limits = EffectOwnerLimits {
            reserved_bytes: 256 * 1024 * 1024,
            control_bytes: 256 * 1024 * 1024 - ordinary,
            ..EffectOwnerLimits::default()
        };
        let (mut owner, _, replies) = setup(limits, BatchLimits::default());
        queue(&mut owner, replies);
        let steps = owner
            .advance_batch_authorized(
                MonoTime(0),
                4,
                |_| (true, enabled),
                |_, _, _| panic!("bulk refusal precedes app checks"),
            )
            .unwrap();
        assert_eq!(steps.len(), 1);
        assert!(steps[0].operation.is_none());
        let persist = owner.take_effect().unwrap().unwrap();
        let Effect::Persist(update) = persist.effect else {
            panic!()
        };
        assert!(update.suffix.is_none());
        assert_eq!(update.commit_index, 1);
        assert_eq!(owner.core(group()).unwrap().state().last_index(), 1);
        assert_eq!(owner.usage().active_visits, 1);
        retained(&owner, true);
    }
}

#[test]
fn one_original_core_limit_does_not_hide_control_plus_original_output_room() {
    let (mut owner, _, mut replies) = setup(
        EffectOwnerLimits::default(),
        BatchLimits {
            max_proposal_entries: 1,
            ..BatchLimits::default()
        },
    );
    owner
        .admit(group(), Event::Receive(replies.remove(0)))
        .unwrap();
    owner.admit_tracked(group(), command(5)).unwrap();
    let steps = owner
        .advance_batch_authorized(MonoTime(0), 2, |_| (true, true), |_, _, _| Ok(()))
        .unwrap();
    assert_eq!(steps.len(), 2);
    assert!(steps[0].operation.is_none());
    assert_eq!(steps[1].operation.unwrap().get(), 5);
    let lease = owner.take_effect().unwrap().unwrap();
    let Effect::Persist(update) = lease.effect else {
        panic!()
    };
    assert_eq!(update.commit_index, 1);
    assert_eq!(update.suffix.unwrap().entries.len(), 1);
}

#[test]
fn closed_timer_owner_serializes_existing_controls_without_a_joined_refresh() {
    let (mut owner, _, replies) = setup(EffectOwnerLimits::default(), BatchLimits::default());
    queue(&mut owner, replies);
    owner.close_admission().unwrap();
    let steps = owner
        .advance_batch_authorized(
            MonoTime(0),
            4,
            |_| (true, true),
            |_, _, _| panic!("closed timer owner must serialize controls"),
        )
        .unwrap();
    assert_eq!(steps.len(), 1);
    assert!(steps[0].operation.is_none());
    assert!(owner.deadline(group()).is_none());
    let lease = owner.take_effect().unwrap().unwrap();
    let Effect::Persist(update) = lease.effect else {
        panic!()
    };
    assert!(update.suffix.is_none());
}

struct CommitContext(crate::application::Counter);
impl crate::application::StateMachine for CommitContext {
    type Receipt = crate::application::CounterReceipt;
    fn applied_index(&self) -> u64 {
        self.0.applied_index()
    }
    fn apply_batch(
        &mut self,
        entries: &[LogEntry],
    ) -> Result<Vec<Self::Receipt>, crate::application::ApplicationError> {
        self.0.apply_batch(entries)
    }
}
impl crate::application::BoundedStateMachine for CommitContext {
    fn receipt_bytes_bound(
        &self,
        entries: &[LogEntry],
    ) -> Result<usize, crate::application::ApplicationError> {
        self.0.receipt_bytes_bound(entries)
    }
}
impl crate::application::ProposalAdmission for CommitContext {
    fn supports_ready_batch(&self) -> bool {
        true
    }
    fn validate_proposal_context(
        &self,
        core: &Raft,
        _: OperationId,
        _: &[u8],
    ) -> Result<(), crate::application::ApplicationError> {
        if core.state().commit_index == 0 {
            Ok(())
        } else {
            Err(crate::application::ApplicationError::InvalidCommand)
        }
    }
    fn validate_proposal<'a>(
        &self,
        operation: OperationId,
        bytes: &[u8],
        pending: impl Iterator<Item = (OperationId, &'a [u8])>,
    ) -> Result<usize, crate::application::ApplicationError> {
        self.0.validate_proposal(operation, bytes, pending)
    }
}
fn apply_serialized_noop(owner: &mut Owner, app: &mut CommitContext) {
    while let Some(lease) = owner.take_effect().unwrap() {
        match &lease.effect {
            Effect::Send(_) => owner.release_transferred_send(lease.ticket).unwrap(),
            Effect::Committed(entries) => {
                let receipts = crate::application::StateMachine::apply_batch(app, entries).unwrap();
                assert!(
                    receipts.is_empty(),
                    "the committed prefix is the ordinary Noop"
                );
                let applied = crate::application::StateMachine::applied_index(app);
                assert_eq!(applied, entries.last().unwrap().index);
                owner.release(lease, applied, MonoTime(0)).unwrap();
            }
            other => panic!("unexpected serialized output {other:?}"),
        }
    }
}
#[test]
fn context_sensitive_batch_host_default_false_waits_for_original_commit() {
    use crate::application::{Counter, ProposalAdmission};
    let (mut owner, mut worker, replies) =
        setup(EffectOwnerLimits::default(), BatchLimits::default());
    let mut app = CommitContext(Counter::new(8).unwrap());
    assert!(app.supports_ready_batch());
    assert!(!app.supports_ready_commit_batch());
    let mut clients = ClientRouter::new(
        ClientRouterBinding::new(
            owner.authorization(),
            ClientRouterGeneration::new(1).unwrap(),
        ),
        ClientRouterLimits::default(),
    )
    .unwrap();
    let operation = OperationId::new(u128::MAX).unwrap();
    clients
        .submit(
            &mut owner,
            &app,
            ClientRequest {
                group: group(),
                operation,
                bytes: 1i64.to_le_bytes().to_vec(),
            },
        )
        .unwrap();
    for reply in replies {
        owner.admit(group(), Event::Receive(reply)).unwrap();
    }
    // The real Router must thread BOTH capabilities. A batch-only wrapper
    // observes the original durable commit before proposal execution.
    let steps = clients
        .advance(&mut owner, MonoTime(0), 4, |_| Some(&app))
        .unwrap();
    assert_eq!(steps.len(), 1);
    assert!(steps[0].operation.is_none());
    let persist = owner.take_effect().unwrap().unwrap();
    assert!(matches!(&persist.effect, Effect::Persist(update) if update.suffix.is_none()));
    owner
        .submit_persists(&mut worker, vec![persist], MonoTime(0))
        .unwrap();
    for event in worker.poll(2) {
        owner.deliver_worker(event, MonoTime(0)).unwrap();
    }
    apply_serialized_noop(&mut owner, &mut app);
    assert_eq!(crate::application::StateMachine::applied_index(&app), 1);
    let mut steps = Vec::new();
    let mut notifications = 0;
    // The still-queued second ACK owes its old commit notification. Its Send
    // owns that visit until handback; do not skip it to find the later Data.
    for _ in 0..4 {
        steps.extend(
            clients
                .advance(&mut owner, MonoTime(0), 4, |_| Some(&app))
                .unwrap(),
        );
        while let Some(lease) = owner.take_effect().unwrap() {
            assert!(
                matches!(&lease.effect, Effect::Send(message) if message.to == node(3)
                && matches!(&message.rpc, Rpc::Append { leader_commit: 1, .. }))
            );
            notifications += 1;
            owner.release_transferred_send(lease.ticket).unwrap();
        }
        if steps.iter().any(|step| step.operation == Some(operation)) {
            break;
        }
    }
    assert_eq!(notifications, 1);
    assert!(steps
        .iter()
        .filter(|step| step.operation.is_none())
        .all(|step| step.error.is_none()));
    let step = steps
        .iter()
        .find(|s| s.operation == Some(operation))
        .unwrap();
    assert_eq!(
        step.error,
        Some(RaftError::Admission(
            crate::application::ApplicationError::InvalidCommand
        ))
    );
    assert!(step.proposed.is_none());
    assert_eq!(owner.core(group()).unwrap().state().last_index(), 1);
}

#[test]
fn failed_join_checks_refund_to_identical_control_reservation_without_dequeue() {
    let mut charges = Vec::new();
    for enabled in [false, true] {
        let (mut owner, _, replies) = setup(EffectOwnerLimits::default(), BatchLimits::default());
        queue(&mut owner, replies);
        let mut checks = 0;
        let steps = owner
            .advance_batch_authorized(
                MonoTime(0),
                4,
                |_| (true, enabled),
                |_, _, _| {
                    checks += 1;
                    Err(RaftError::Busy)
                },
            )
            .unwrap();
        assert_eq!(checks, usize::from(enabled));
        assert_eq!(steps.len(), 1);
        assert!(steps[0].error.is_none());
        assert!(steps[0].operation.is_none());
        charges.push(owner.usage().reserved_bytes);
        let lease = owner.take_effect().unwrap().unwrap();
        assert!(matches!(lease.effect, Effect::Persist(update) if update.suffix.is_none()));
        assert_eq!(owner.core(group()).unwrap().state().last_index(), 1);
    }
    assert_eq!(charges[0], charges[1]);
}
