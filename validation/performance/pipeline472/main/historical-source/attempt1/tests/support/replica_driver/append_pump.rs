// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Deterministic public-driver controls; no threads, clocks, or native durability claims.
use super::*;
use std::{cell::RefCell, rc::Rc};

#[derive(Clone, Copy, PartialEq)]
enum Delivery {
    Held,
    Written,
    Durable,
    Failed,
}
struct Request {
    request: WorkerTicket,
    ids: Vec<PersistUnitId>,
    tickets: Vec<LogTicket>,
    written: bool,
}
struct PumpWorker {
    store: HostLogStore,
    depth: usize,
    sequence: u64,
    pending: VecDeque<Request>,
    delivery: Delivery,
    attempts: Vec<Vec<PersistUnitId>>,
    accepted: Vec<Vec<PersistUnitId>>,
    reject_attempt: Option<usize>,
    closed: bool,
}
impl PumpWorker {
    fn new(store: HostLogStore, depth: usize) -> Self {
        Self {
            store,
            depth,
            sequence: 0,
            pending: VecDeque::new(),
            delivery: Delivery::Durable,
            attempts: Vec::new(),
            accepted: Vec::new(),
            reject_attempt: None,
            closed: false,
        }
    }
}
impl PersistenceWorker for PumpWorker {
    fn binding(&self) -> WorkerBinding {
        WorkerBinding {
            store: self.store.binding(),
            generation: StorageWorkerGeneration::new(1).unwrap(),
        }
    }
    fn limits(&self) -> WorkerLimits {
        WorkerLimits::default()
    }
    fn max_outstanding_units_per_group(&self) -> usize {
        self.depth
    }
    fn usage(&self) -> WorkerUsage {
        WorkerUsage {
            requests: self.pending.len(),
            units: self.pending.iter().map(|r| r.ids.len()).sum(),
            bytes: 0,
        }
    }
    fn submit(&mut self, units: Vec<PersistUnit>) -> Result<WorkerTicket, WorkerRejected> {
        let ids: Vec<_> = units.iter().map(|u| u.id).collect();
        self.attempts.push(ids.clone());
        if self.closed || self.reject_attempt == Some(self.attempts.len()) {
            return Err(WorkerRejected {
                reason: if self.closed {
                    WorkerError::Closed
                } else {
                    WorkerError::Overloaded
                },
                units,
            });
        }
        if let Err(reason) = batch_cost(&units, units.capacity(), self.limits()) {
            return Err(WorkerRejected { reason, units });
        }
        assert!(ids.iter().all(|id| self
            .pending
            .iter()
            .flat_map(|r| &r.ids)
            .filter(|old| old.visit().group == id.visit().group)
            .count()
            < self.depth));
        self.sequence += 1;
        let request = WorkerTicket {
            binding: self.binding(),
            sequence: self.sequence,
        };
        let tickets = self
            .store
            .append_batch(
                units
                    .into_iter()
                    .map(|u| LogMutation::Update(u.update))
                    .collect(),
            )
            .unwrap();
        self.accepted.push(ids.clone());
        self.pending.push_back(Request {
            request,
            ids,
            tickets,
            written: false,
        });
        Ok(request)
    }
    fn poll(&mut self, limit: usize) -> Vec<WorkerEvent> {
        let mut events = Vec::new();
        while self.delivery != Delivery::Held && events.len() < limit {
            let Some(front) = self.pending.front_mut() else {
                break;
            };
            if !front.written {
                front.written = true;
                events.push(WorkerEvent::Written {
                    request: front.request,
                    admissions: front
                        .ids
                        .iter()
                        .copied()
                        .zip(front.tickets.iter().copied())
                        .collect(),
                });
            } else if self.delivery == Delivery::Written {
                break;
            } else {
                let front = self.pending.pop_front().unwrap();
                events.push(if self.delivery == Delivery::Failed {
                    WorkerEvent::Failed {
                        request: front.request,
                        units: front.ids,
                        error: voteboat::contracts::StorageError::Uncertain("pump failure"),
                    }
                } else {
                    WorkerEvent::Durable {
                        request: front.request,
                        units: front.ids,
                        completion: self.store.barrier(&front.tickets).unwrap(),
                    }
                });
            }
        }
        events
    }
    fn close(&mut self) {
        self.closed = true;
    }
}

#[derive(Clone)]
struct CheckedCounter {
    counter: Counter,
    checks: Rc<RefCell<BTreeMap<OperationId, usize>>>,
}
impl StateMachine for CheckedCounter {
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
impl BoundedStateMachine for CheckedCounter {
    fn receipt_bytes_bound(&self, entries: &[LogEntry]) -> Result<usize, ApplicationError> {
        self.counter.receipt_bytes_bound(entries)
    }
}
impl ProposalAdmission for CheckedCounter {
    fn supports_ready_batch(&self) -> bool {
        true
    }
    fn supports_append_window(&self) -> bool {
        true
    }
    fn validate_proposal_context(
        &self,
        core: &Raft,
        operation: OperationId,
        bytes: &[u8],
    ) -> Result<(), ApplicationError> {
        *self.checks.borrow_mut().entry(operation).or_default() += 1;
        self.counter
            .validate_proposal_context(core, operation, bytes)
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
impl ReadableStateMachine for CheckedCounter {
    type Query = ();
    type ReadResult = i64;
    fn read_at(&self, index: u64, query: ()) -> Result<i64, ApplicationError> {
        self.counter.read_at(index, query)
    }
}
impl BoundedReadableStateMachine for CheckedCounter {
    fn query_bytes(&self, query: &(), limit: usize) -> Result<usize, ApplicationError> {
        self.counter.query_bytes(query, limit)
    }
    fn read_result_bound(&self, query: &()) -> Result<usize, ApplicationError> {
        self.counter.read_result_bound(query)
    }
    fn read_result_bytes(&self, result: &i64, limit: usize) -> Result<usize, ApplicationError> {
        self.counter.read_result_bytes(result, limit)
    }
}
impl CheckpointStateMachine for CheckedCounter {
    fn schema_version(&self) -> u64 {
        self.counter.schema_version()
    }
    fn checkpoint(&self, limit: usize) -> Result<Vec<u8>, ApplicationError> {
        self.counter.checkpoint(limit)
    }
    fn restore_checkpoint(
        &mut self,
        schema: u64,
        index: u64,
        bytes: &[u8],
    ) -> Result<(), ApplicationError> {
        self.counter.restore_checkpoint(schema, index, bytes)
    }
}

struct PumpFixture {
    base: Fixture,
    worker: PumpWorker,
    apps: BTreeMap<GroupIdentity, CheckedCounter>,
}
impl PumpFixture {
    fn new(depth: usize, groups: u128) -> Self {
        let mut base = Fixture::new(groups);
        let store = std::mem::replace(&mut base.worker.store, HostLogStore::new(99));
        let identity = base.owner.identity();
        let mut shard = Shard::new(
            identity,
            ShardLimits {
                max_groups: 100,
                visit_items: 16,
                ..ShardLimits::default()
            },
            Ready(VecDeque::new()),
        )
        .unwrap();
        for g in 1..=groups {
            let mut core = Raft::recover(
                node(1),
                store.binding(),
                store.state(group(g)).unwrap(),
                store.limits(),
            )
            .unwrap();
            if depth > 1 {
                core.configure_append_window(Some(AppendWindowLimits {
                    max_units: depth,
                    max_originals: 64,
                    max_retained_bytes: 1024 * 1024,
                }))
                .unwrap();
            }
            shard.register(core).unwrap();
        }
        let timed = TimedShard::new(
            shard,
            Timers {
                owner: identity,
                sequence: 0,
                entries: BTreeMap::new(),
            },
            Entropy(17),
            TimerConfig::default(),
            MonoTime(0),
        )
        .unwrap();
        let worker = PumpWorker::new(store, depth);
        base.owner =
            EffectOwner::new(timed, worker.binding(), EffectOwnerLimits::default()).unwrap();
        base.driver = None;
        let apps = (1..=groups)
            .map(|g| {
                (
                    group(g),
                    CheckedCounter {
                        counter: Counter::new(256).unwrap(),
                        checks: Default::default(),
                    },
                )
            })
            .collect();
        let mut f = Self { base, worker, apps };
        f.parts(|d, p| {
            *d = Some(ReplicaDriver::new(p, ReplicaDriverLimits::default(), MonoTime(0)).unwrap())
        });
        f.drain();
        f.worker.delivery = Delivery::Held;
        f.worker.attempts.clear();
        f.worker.accepted.clear();
        f
    }
    fn parts<R>(
        &mut self,
        call: impl FnOnce(
            &mut Option<ReplicaDriver<CounterReceipt>>,
            &mut ReplicaParts<'_, Ready, Timers, Entropy, CheckedCounter, PumpWorker, HostOutbound>,
        ) -> R,
    ) -> R {
        let mut p = ReplicaParts {
            owner: &mut self.base.owner,
            persistence: &mut self.worker,
            applications: &mut self.apps,
            results: &mut self.base.results,
            clients: &mut self.base.clients,
            reads: &mut self.base.reads,
            outbound: &mut self.base.outbound,
            snapshots: None,
        };
        call(&mut self.base.driver, &mut p)
    }
    fn poll(&mut self, budget: ReplicaPollBudget) -> Result<ReplicaProgress, ReplicaError> {
        self.parts(|d, p| d.as_mut().unwrap().poll(p, MonoTime(400), budget))
    }
    fn drain(&mut self) {
        self.worker.delivery = Delivery::Durable;
        for _ in 0..300 {
            let p = self.poll(ReplicaPollBudget::default()).unwrap();
            assert!(p.steps.iter().all(|s| s.error.is_none()));
            if self.base.owner.is_drained()
                && self.worker.is_drained()
                && self.base.driver.as_ref().unwrap().is_drained()
            {
                return;
            }
        }
        panic!("driver did not drain");
    }
    fn queue(&mut self, g: u128, count: u128) -> Vec<ClientTicket> {
        (1..=count)
            .map(|id| {
                self.base
                    .clients
                    .submit(
                        &mut self.base.owner,
                        &self.apps[&group(g)],
                        ClientRequest {
                            group: group(g),
                            operation: OperationId::new((g << 96) | id).unwrap(),
                            bytes: 1i64.to_le_bytes().to_vec(),
                        },
                    )
                    .unwrap()
            })
            .collect()
    }
    fn assert_outcomes(&mut self, tickets: &[ClientTicket]) {
        let mut actual = Vec::new();
        while let Some(output) = self.base.clients.poll() {
            let ticket = output.ticket();
            let ClientOutcome::Applied { position, receipt } =
                self.base.clients.complete(output).unwrap()
            else {
                panic!("original not applied");
            };
            assert_eq!(receipt.operation, ticket.operation);
            assert_eq!(receipt.index, position.index);
            assert_eq!(
                receipt.outcome,
                CounterOutcome::Value(position.index as i64 - 1)
            );
            actual.push(ticket);
        }
        assert_eq!(actual, tickets);
        assert_eq!(self.base.clients.usage(), ClientUsage::default());
        assert_eq!(self.base.owner.usage().reserved_bytes, 0);
        assert_eq!(self.base.owner.usage().leased, 0);
    }
}

#[test]
fn selected_four_units_submit_in_one_poll_without_written_or_durable_success() {
    let mut f = PumpFixture::new(4, 1);
    let tickets = f.queue(1, 64);
    let before = f.base.owner.core(group(1)).unwrap().state().clone();
    let progress = f.poll(ReplicaPollBudget::default()).unwrap();
    assert_eq!(progress.persistence_batches, 4);
    assert_eq!(progress.steps.len(), 64);
    assert_eq!(progress.worker_events, 0);
    assert_eq!(f.worker.accepted.len(), 4);
    let ids: Vec<_> = f.worker.accepted.iter().flatten().copied().collect();
    assert_eq!(ids.len(), 4);
    assert_eq!(
        f.worker
            .pending
            .iter()
            .flat_map(|r| &r.tickets)
            .map(|t| t.last_index)
            .collect::<Vec<_>>(),
        [17, 33, 49, 65]
    );
    assert_eq!(
        progress
            .steps
            .iter()
            .map(|step| step.operation.unwrap())
            .collect::<Vec<_>>(),
        tickets.iter().map(|t| t.operation).collect::<Vec<_>>()
    );
    assert_eq!(
        progress
            .steps
            .iter()
            .map(|step| step.proposed.unwrap().index)
            .collect::<Vec<_>>(),
        (2..=65).collect::<Vec<_>>()
    );
    assert!(ids.iter().all(|id| id.append.is_some()));
    assert!(ids
        .windows(2)
        .all(|w| w[0] != w[1] && w[0].visit().sequence < w[1].visit().sequence));
    assert_eq!(f.base.owner.core(group(1)).unwrap().state(), &before);
    assert_eq!(f.apps[&group(1)].applied_index(), 1);
    assert_eq!(f.base.clients.usage().requests, 64);
    assert!(f.base.clients.poll().is_none());
    assert!(f.base.owner.usage().reserved_bytes > 0);
    assert_eq!(f.base.owner.usage().persistence_requests, 4);
    f.drain();
    f.assert_outcomes(&tickets);
    assert!(f.apps[&group(1)].checks.borrow().values().all(|n| *n == 2));
}

#[test]
fn default_one_retains_single_submission_and_does_not_recheck_unstepped_originals() {
    let mut f = PumpFixture::new(1, 1);
    let tickets = f.queue(1, 64);
    let progress = f.poll(ReplicaPollBudget::default()).unwrap();
    assert_eq!(progress.persistence_batches, 1);
    assert_eq!(f.worker.accepted.len(), 1);
    assert_eq!(progress.steps.len(), 16);
    assert!(f.worker.accepted[0].iter().all(|id| id.append.is_none()));
    assert_eq!(
        f.apps[&group(1)]
            .checks
            .borrow()
            .values()
            .filter(|n| **n == 2)
            .count(),
        16
    );
    f.drain();
    f.assert_outcomes(&tickets);
}

#[test]
fn residual_step_and_effect_allowances_do_not_multiply_across_passes() {
    for budget in [
        ReplicaPollBudget {
            steps: 17,
            ..ReplicaPollBudget::default()
        },
        ReplicaPollBudget {
            effects: 1,
            ..ReplicaPollBudget::default()
        },
    ] {
        let mut f = PumpFixture::new(4, 1);
        let tickets = f.queue(1, 64);
        let progress = f.poll(budget).unwrap();
        assert!(progress.steps.len() <= budget.steps);
        assert!(progress.effects <= budget.effects);
        assert!(progress.retries <= budget.retries);
        assert!(progress.worker_events <= budget.worker_events);
        assert!(progress.snapshot_events <= budget.snapshot_events);
        assert!(f.worker.attempts.len() <= budget.effects);
        assert!(f.worker.accepted.len() < 4);
        f.drain();
        f.assert_outcomes(&tickets);
    }
}

#[test]
fn available_written_then_durable_or_failure_precedes_the_next_ready_pass() {
    for delivery in [Delivery::Durable, Delivery::Failed] {
        let mut f = PumpFixture::new(4, 1);
        f.queue(1, 64);
        f.worker.delivery = delivery;
        let progress = f.poll(ReplicaPollBudget::default());
        if delivery == Delivery::Failed {
            assert!(progress.is_err());
            assert!(f.base.owner.is_failed());
        } else {
            let progress = progress.unwrap();
            assert_eq!(progress.worker_events, 2);
        }
        assert_eq!(
            f.worker
                .accepted
                .iter()
                .flatten()
                .filter(|id| id.append.is_some())
                .count(),
            1,
            "terminal must prevent staging B; its ordinary continuation may persist commit"
        );
    }
}

#[test]
fn exhausted_worker_events_stop_even_when_only_written_was_available() {
    let mut f = PumpFixture::new(4, 1);
    f.queue(1, 64);
    f.worker.delivery = Delivery::Written;
    let progress = f
        .poll(ReplicaPollBudget {
            worker_events: 1,
            ..ReplicaPollBudget::default()
        })
        .unwrap();
    assert_eq!(progress.worker_events, 1);
    assert_eq!(f.worker.accepted.len(), 1);
    assert_eq!(f.base.owner.core(group(1)).unwrap().state().commit_index, 1);
    assert_eq!(f.apps[&group(1)].applied_index(), 1);
}

#[test]
fn overload_restores_exact_b_front_once_without_rechecking_or_losing_originals() {
    let mut f = PumpFixture::new(4, 1);
    let tickets = f.queue(1, 64);
    f.worker.reject_attempt = Some(2);
    let progress = f.poll(ReplicaPollBudget::default()).unwrap();
    assert_eq!(progress.persistence_batches, 1);
    assert_eq!(f.worker.attempts.len(), 2);
    let rejected = f.worker.attempts[1].clone();
    let checks = f.apps[&group(1)].checks.borrow().clone();
    f.worker.reject_attempt = None;
    f.poll(ReplicaPollBudget {
        effects: 1,
        steps: 1,
        ..ReplicaPollBudget::default()
    })
    .unwrap();
    assert_eq!(f.worker.attempts[2], rejected);
    assert_eq!(*f.apps[&group(1)].checks.borrow(), checks);
    f.drain();
    f.assert_outcomes(&tickets);
}

#[test]
fn queued_control_seals_before_more_data_and_cold_group_keeps_fifo_priority() {
    let mut f = PumpFixture::new(4, 2);
    let hot = f.queue(1, 64);
    let cold = f.queue(2, 16);
    f.poll(ReplicaPollBudget {
        steps: 16,
        ..ReplicaPollBudget::default()
    })
    .unwrap();
    assert_eq!(f.worker.accepted.len(), 1);
    f.base.owner.admit(group(1), Event::Heartbeat).unwrap();
    f.poll(ReplicaPollBudget::default()).unwrap();
    assert_eq!(f.worker.accepted[1][0].visit().group, group(2));
    assert_eq!(
        f.worker
            .accepted
            .iter()
            .flatten()
            .filter(|id| id.visit().group == group(1))
            .count(),
        1
    );
    f.drain();
    let mut all = hot;
    all.extend(cold);
    // Cross-group application completion order is scheduler-defined; originals
    // still map one-to-one to the same public tickets and operations.
    let mut outputs = Vec::new();
    while let Some(output) = f.base.clients.poll() {
        outputs.push(output.ticket());
        assert!(matches!(
            f.base.clients.complete(output).unwrap(),
            ClientOutcome::Applied { .. }
        ));
    }
    assert_eq!(outputs.len(), all.len());
    for ticket in all {
        assert_eq!(outputs.iter().filter(|out| **out == ticket).count(), 1);
    }
    assert_eq!(f.base.owner.usage().reserved_bytes, 0);
}

#[test]
fn retry_allowance_is_shared_with_a_cold_groups_ready_read() {
    let mut f = PumpFixture::new(4, 2);
    let tickets = f.queue(1, 64);
    let read = f
        .base
        .reads
        .submit(&mut f.base.owner, &f.apps[&group(2)], group(2), ())
        .unwrap();
    let budget = ReplicaPollBudget {
        retries: 1,
        ..ReplicaPollBudget::default()
    };
    let progress = f.poll(budget).unwrap();
    assert_eq!(progress.retries, 1);
    assert_eq!(
        f.worker.accepted.len(),
        1,
        "consumed retry allowance cannot refresh for B"
    );
    assert!(progress.steps.len() <= budget.steps);
    assert!(progress.effects <= budget.effects);
    f.drain();
    f.assert_outcomes(&tickets);
    let output = f.base.reads.poll().unwrap();
    assert_eq!(output.ticket(), read);
    assert!(matches!(
        f.base.reads.complete(output).unwrap(),
        ReadOutcome::Read { result: Ok(0), .. }
    ));
}
