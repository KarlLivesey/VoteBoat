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
mod support;
use std::collections::{BTreeMap, VecDeque};
use support::outbound::HostOutbound;
use support::*;
use voteboat::outbound::{
    LocalSendResult, OutboundBatch, OutboundBinding, OutboundBudget, OutboundError, OutboundLimits,
    OutboundQueue, OutboundUsage,
};
use voteboat::transport::{PeerTransport, TransportError, TransportPollBudget};
use voteboat::wire::{WireCodec, WireScope};
use voteboat::{application::*, identity::*, log::*, raft::*, runtime::*, worker::*};

struct Ready(VecDeque<GroupIdentity>);
impl ReadyScheduler for Ready {
    fn capacity(&self) -> usize {
        100
    }
    fn enqueue(&mut self, g: GroupIdentity) -> Result<(), RuntimeError> {
        if !self.0.contains(&g) {
            self.0.push_back(g);
        }
        Ok(())
    }
    fn pop(&mut self) -> Option<GroupIdentity> {
        self.0.pop_front()
    }
    fn cancel(&mut self, g: GroupIdentity) {
        self.0.retain(|v| *v != g);
    }
    fn len(&self) -> usize {
        self.0.len()
    }
}
fn owner(store: StoreBinding) -> RuntimeOwner {
    RuntimeOwner {
        store,
        lane: ExecutionLaneId::new(1).unwrap(),
        generation: RuntimeGeneration::new(1).unwrap(),
    }
}
fn shard<L: LogStore>(store: &mut L, count: u128) -> Shard<Ready> {
    append(
        store,
        (1..=count)
            .map(|g| LogMutation::Create(bootstrap(g, 3)))
            .collect(),
    );
    let mut shard = Shard::new(
        owner(store.binding()),
        ShardLimits {
            max_groups: 100,
            visit_items: 1,
            ..ShardLimits::default()
        },
        Ready(VecDeque::new()),
    )
    .unwrap();
    for g in 1..=count {
        shard
            .register(
                Raft::recover(
                    node(1),
                    store.binding(),
                    store.state(group(g)).unwrap(),
                    store.limits(),
                )
                .unwrap(),
            )
            .unwrap();
    }
    shard
}
fn campaign<Q: ReadyScheduler>(shard: &mut Shard<Q>, g: u128) -> PersistUnit {
    shard.admit(group(g), Event::Campaign).unwrap();
    let visit = shard.poll(MonoTime(0)).unwrap().unwrap();
    let effects = shard
        .step_next(visit, MonoTime(0))
        .unwrap()
        .unwrap()
        .result
        .unwrap();
    let [Effect::Persist(update)] = effects.as_slice() else {
        panic!()
    };
    PersistUnit {
        visit,
        update: update.clone(),
    }
}
/// A public host replacement exposes one manual progress stage per poll. No
/// thread, native journal or private core state is needed.
struct HostWorker {
    store: HostLogStore,
    binding: WorkerBinding,
    sequence: u64,
    accepted: Option<(WorkerTicket, Vec<PersistUnit>)>,
    written: Option<(WorkerTicket, Vec<VisitTicket>, Vec<LogTicket>)>,
    usage: WorkerUsage,
    closed: bool,
}
impl HostWorker {
    fn new(store: HostLogStore) -> Self {
        let binding = WorkerBinding {
            store: store.binding(),
            generation: StorageWorkerGeneration::new(1).unwrap(),
        };
        Self {
            store,
            binding,
            sequence: 0,
            accepted: None,
            written: None,
            usage: WorkerUsage::default(),
            closed: false,
        }
    }
}
impl PersistenceWorker for HostWorker {
    fn binding(&self) -> WorkerBinding {
        self.binding
    }
    fn limits(&self) -> WorkerLimits {
        WorkerLimits::default()
    }
    fn usage(&self) -> WorkerUsage {
        self.usage
    }
    fn submit(&mut self, units: Vec<PersistUnit>) -> Result<WorkerTicket, WorkerRejected> {
        let error = if self.closed {
            Some(WorkerError::Closed)
        } else if self.usage.requests > 0 {
            Some(WorkerError::Overloaded)
        } else {
            None
        };
        if let Some(reason) = error {
            return Err(WorkerRejected { reason, units });
        }
        let (bytes, _) = match batch_cost(&units, units.capacity(), self.limits()) {
            Ok(v) => v,
            Err(reason) => return Err(WorkerRejected { reason, units }),
        };
        if units
            .iter()
            .any(|u| u.visit.owner.store != self.binding.store)
        {
            return Err(WorkerRejected {
                reason: WorkerError::WrongBinding,
                units,
            });
        }
        self.sequence += 1;
        let ticket = WorkerTicket {
            binding: self.binding,
            sequence: self.sequence,
        };
        self.usage = WorkerUsage {
            requests: 1,
            units: units.len(),
            bytes,
        };
        self.accepted = Some((ticket, units));
        Ok(ticket)
    }
    fn poll(&mut self, limit: usize) -> Vec<WorkerEvent> {
        if limit == 0 {
            return vec![];
        }
        if let Some((request, units)) = self.accepted.take() {
            let visits = units.iter().map(|u| u.visit).collect::<Vec<_>>();
            let tickets = self
                .store
                .append_batch(
                    units
                        .into_iter()
                        .map(|u| LogMutation::Update(u.update))
                        .collect(),
                )
                .unwrap();
            let admissions = visits
                .iter()
                .copied()
                .zip(tickets.iter().copied())
                .collect();
            self.written = Some((request, visits, tickets));
            vec![WorkerEvent::Written {
                request,
                admissions,
            }]
        } else if let Some((request, visits, tickets)) = self.written.take() {
            let completion = self.store.barrier(&tickets).unwrap();
            self.usage = WorkerUsage::default();
            vec![WorkerEvent::Durable {
                request,
                visits,
                completion,
            }]
        } else {
            vec![]
        }
    }
    fn close(&mut self) {
        self.closed = true;
    }
}
#[test]
fn host_worker_stages_exact_effects_and_durable_owner_delivery_without_native_features() {
    let mut store = HostLogStore::new(1);
    let mut shard = shard(&mut store, 2);
    let mut unit = campaign(&mut shard, 1);
    let visit = unit.visit;
    let original = unit.update.clone();
    let mut worker = HostWorker::new(store);
    unit.update.hard_state.term += 1;
    let rejected = submit_for_shard(&mut shard, &mut worker, vec![unit]).unwrap_err();
    assert_eq!(
        rejected.reason,
        WorkerError::Consensus(RaftError::WrongCompletion)
    );
    assert!(worker.is_drained());
    let mut units = rejected.units;
    units[0].update = original;
    let ticket = submit_for_shard(&mut shard, &mut worker, units).unwrap();
    assert_eq!(worker.usage().units, 1);
    assert_eq!(shard.finish(visit), Err(RuntimeError::DependencyPending));
    assert!(worker.poll(0).is_empty());
    let written = worker.poll(1).pop().unwrap();
    assert_eq!(written.request(), ticket);
    assert!(!written.terminal());
    assert!(apply_to_shard(&mut shard, written)[0]
        .result
        .as_ref()
        .unwrap()
        .is_empty());
    assert_eq!(worker.usage().requests, 1);
    assert_eq!(shard.finish(visit), Err(RuntimeError::DependencyPending));
    let durable = worker.poll(1).pop().unwrap();
    let replay = durable.clone();
    assert!(durable.terminal());
    let deliveries = apply_to_shard(&mut shard, durable);
    assert_eq!(deliveries[0].result.as_ref().unwrap().len(), 2);
    shard.finish(visit).unwrap();
    assert_eq!(worker.usage(), WorkerUsage::default());
    assert!(matches!(
        &apply_to_shard(&mut shard, replay)[0].result,
        Err(WorkerError::Runtime(RuntimeError::StaleTicket))
    ));
    worker.close();
    assert!(worker.is_drained());
    assert_eq!(
        worker.store.state(group(1)).unwrap().hard_state.voted_for,
        Some(node(1))
    );
}
#[test]
fn stopped_visit_does_not_discard_other_groups_in_a_shared_completion() {
    let mut store = HostLogStore::new(1);
    let mut shard = shard(&mut store, 2);
    let first = campaign(&mut shard, 1);
    let second = campaign(&mut shard, 2);
    let surviving = second.visit;
    let mut worker = HostWorker::new(store);
    submit_for_shard(&mut shard, &mut worker, vec![first, second]).unwrap();
    let event = worker.poll(1).pop().unwrap();
    apply_to_shard(&mut shard, event)
        .iter()
        .for_each(|d| assert!(d.result.is_ok()));
    shard.stop_group(group(1)).unwrap();
    let event = worker.poll(1).pop().unwrap();
    let delivered = apply_to_shard(&mut shard, event);
    assert!(matches!(
        delivered[0].result,
        Err(WorkerError::Runtime(RuntimeError::StaleTicket))
    ));
    assert_eq!(delivered[1].result.as_ref().unwrap().len(), 2);
    shard.finish(surviving).unwrap();
}

/// End-to-end assembly uses public owner/worker interfaces, with no synchronous
/// log access after construction. All retained test-driver queues have ceilings.
struct WorkerNode<W: PersistenceWorker> {
    shard: Shard<Ready>,
    worker: W,
    apps: BTreeMap<GroupIdentity, Counter>,
    pending: BTreeMap<GroupIdentity, PersistUnit>,
    messages: VecDeque<Message>,
    outbound: Box<dyn OutboundQueue>,
    rejected_sends: usize,
    wire: Option<Box<dyn WireCodec>>,
    transports: BTreeMap<NodeId, Box<dyn PeerTransport>>,
    staged_sends: VecDeque<OutboundBatch>,
    sent_frames: usize,
    received_frames: usize,
    held: Option<WorkerEvent>,
    hold: bool,
    results: Vec<CounterReceipt>,
    reads: Vec<i64>,
    largest_batch: usize,
}
impl<W: PersistenceWorker> WorkerNode<W> {
    fn new<L: LogStore>(
        id: u64,
        mut store: L,
        create: bool,
        worker: impl FnOnce(L) -> W,
        outbound: impl FnOnce(OutboundBinding) -> Box<dyn OutboundQueue>,
    ) -> Self {
        if create {
            append(
                &mut store,
                (1..=100)
                    .map(|g| LogMutation::Create(bootstrap(g, 3)))
                    .collect(),
            );
        }
        let mut shard = Shard::new(
            owner(store.binding()),
            ShardLimits {
                max_groups: 100,
                visit_items: 1,
                group_items: 6,
                group_control_items: 2,
                ..ShardLimits::default()
            },
            Ready(VecDeque::new()),
        )
        .unwrap();
        let mut apps = BTreeMap::new();
        for g in 1..=100 {
            let core = Raft::recover(
                node(id),
                store.binding(),
                store.state(group(g)).unwrap(),
                store.limits(),
            )
            .unwrap();
            let mut app = Counter::new(10).unwrap();
            app.apply_batch(core.replay_committed()).unwrap();
            apps.insert(group(g), app);
            shard.register(core).unwrap();
        }
        let outbound = outbound(OutboundBinding {
            node: node(id),
            store: store.binding(),
            generation: OutboundGeneration::new(1).unwrap(),
        });
        Self {
            shard,
            worker: worker(store),
            apps,
            pending: BTreeMap::new(),
            messages: VecDeque::new(),
            outbound,
            rejected_sends: 0,
            wire: None,
            transports: BTreeMap::new(),
            staged_sends: VecDeque::new(),
            sent_frames: 0,
            received_frames: 0,
            held: None,
            hold: false,
            results: vec![],
            reads: vec![],
            largest_batch: 0,
        }
    }
    fn effects(&mut self, visit: VisitTicket, effects: Vec<Effect>) {
        for effect in effects {
            match effect {
                Effect::Persist(update) => {
                    assert!(self.pending.len() < 100);
                    assert!(self
                        .pending
                        .insert(visit.group, PersistUnit { visit, update })
                        .is_none());
                }
                Effect::Send(message) => {
                    assert!(self.messages.len() < 8192);
                    self.messages.push_back(message);
                }
                Effect::Committed(entries) => {
                    let receipts = self
                        .apps
                        .get_mut(&visit.group)
                        .unwrap()
                        .apply_batch(&entries)
                        .unwrap();
                    assert!(self.results.len() + receipts.len() <= 8192);
                    self.results.extend(receipts);
                }
                Effect::ReadReady(barrier) => {
                    assert!(self.reads.len() < 1024);
                    let app = &self.apps[&visit.group];
                    self.reads.push(
                        self.shard
                            .with_core(visit, |core| read_at_barrier(core, &barrier, app, ()))
                            .unwrap()
                            .unwrap(),
                    );
                }
                _ => panic!("snapshot installation is outside this worker history"),
            }
        }
        if !self
            .shard
            .core(visit.group)
            .unwrap()
            .has_pending_dependency()
        {
            self.shard.finish(visit).unwrap();
        }
    }
    #[cfg(feature = "native")]
    fn with_wire(mut self, wire: impl WireCodec + 'static) -> Self {
        self.wire = Some(Box::new(wire));
        self
    }
    fn deliver(&mut self, mut event: WorkerEvent) {
        if self.hold {
            if let WorkerEvent::Durable {
                request,
                visits,
                completion,
            } = &mut event
            {
                if let Some(index) = visits.iter().position(|v| v.group == group(1)) {
                    assert!(self.held.is_none());
                    self.held = Some(WorkerEvent::Durable {
                        request: *request,
                        visits: vec![visits.remove(index)],
                        completion: completion.clone(),
                    });
                }
            }
        }
        for delivery in apply_to_shard(&mut self.shard, event) {
            self.effects(delivery.visit, delivery.result.unwrap());
        }
    }
    fn progress(&mut self) -> bool {
        let mut progress = false;
        for event in self.worker.poll(16) {
            self.deliver(event);
            progress = true;
        }
        for _ in 0..100 {
            let Some(visit) = self.shard.poll(MonoTime(0)).unwrap() else {
                break;
            };
            let effects = self
                .shard
                .step_next(visit, MonoTime(0))
                .unwrap()
                .unwrap()
                .result
                .unwrap();
            self.effects(visit, effects);
            progress = true;
        }
        if !self.pending.is_empty() {
            let units = std::mem::take(&mut self.pending)
                .into_values()
                .collect::<Vec<_>>();
            let count = units.len();
            match submit_for_shard(&mut self.shard, &mut self.worker, units) {
                Ok(_) => {
                    self.largest_batch = self.largest_batch.max(count);
                    progress = true;
                }
                Err(WorkerRejected {
                    reason: WorkerError::Overloaded,
                    units,
                }) => {
                    for unit in units {
                        self.pending.insert(unit.visit.group, unit);
                    }
                }
                Err(e) => panic!("worker rejected valid persistence: {e:?}"),
            }
        }
        // Keep rejected effects owned and bounded while admitted sends hold
        // node/peer credits. Inspect each retained effect once, so bulk overload
        // cannot hide a later control message from its reserved capacity.
        for _ in 0..self.messages.len() {
            let message = self.messages.pop_front().unwrap();
            match self.outbound.submit(vec![message]) {
                Ok(_) => progress = true,
                Err(rejected) if rejected.reason == OutboundError::Overloaded => {
                    self.rejected_sends += 1;
                    self.messages.extend(rejected.messages);
                }
                Err(e) => panic!("outbound rejected valid effects: {e:?}"),
            }
        }
        progress
    }
    fn values(&self, expected: i64) {
        for app in self.apps.values() {
            assert_eq!(app.read_applied(app.applied_index()), Ok(expected));
        }
    }
}
fn worker_pump<W: PersistenceWorker>(nodes: &mut [WorkerNode<W>], isolated: Option<NodeId>) {
    let end = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let mut network = VecDeque::new();
    let mut sequence = 0;
    loop {
        assert!(
            std::time::Instant::now() < end,
            "worker cluster did not converge"
        );
        let mut progress = false;
        for node in nodes.iter_mut() {
            progress |= node.progress();
            if !node.transports.is_empty() {
                if node.staged_sends.is_empty() {
                    node.staged_sends.extend(node.outbound.poll(32));
                }
                for _ in 0..node.staged_sends.len().min(32) {
                    let batch = node.staged_sends.pop_front().unwrap();
                    let transport = node.transports.get_mut(&batch.ticket.peer).unwrap();
                    match transport.submit(batch) {
                        Ok(()) => {
                            node.sent_frames += 1;
                            progress = true;
                        }
                        Err(rejected) if rejected.reason == TransportError::Overloaded => {
                            node.staged_sends.push_back(*rejected.batch)
                        }
                        Err(e) => panic!("transport rejected a valid outbound batch: {e:?}"),
                    }
                }
                assert!(node.staged_sends.len() <= 32);
                for transport in node.transports.values_mut() {
                    let p = transport
                        .poll(MonoTime(0), TransportPollBudget::default())
                        .unwrap();
                    progress |= p.read_bytes > 0
                        || p.written_bytes > 0
                        || p.session.read_bytes > 0
                        || p.session.written_bytes > 0
                        || p.sent
                        || p.received;
                    if let Some(done) = transport.take_send() {
                        assert_eq!(done.connection, transport.binding());
                        node.outbound.complete(done.batch, done.result).unwrap();
                        progress = true;
                    }
                    // Reserve a complete codec-sized batch in the bounded test
                    // network before transferring receive ownership.
                    if network.len() <= 8192 - 128 {
                        if let Some(received) = transport.take_received() {
                            assert_eq!(received.connection, transport.binding());
                            assert!(received.messages.len() <= 128);
                            network.extend(received.messages);
                            node.received_frames += 1;
                            progress = true;
                        }
                    }
                }
                continue;
            }
            for mut batch in node.outbound.poll(32) {
                assert!(network.len() + batch.messages.len() <= 8192);
                if let Some(codec) = &node.wire {
                    let binding = node.outbound.binding();
                    let scope = WireScope {
                        from: binding.node,
                        sender: binding.store,
                        to: batch.ticket.peer,
                    };
                    let encoded = codec.encode_batch(scope, &batch.messages).unwrap();
                    assert_eq!(
                        codec.frame_length(&encoded[..codec.header_bytes()]),
                        Ok(encoded.len())
                    );
                    // The simulator supplies the trusted scope. This exercises
                    // frame handling, not authentication of a real connection.
                    network.extend(codec.decode_batch(scope, &encoded).unwrap());
                } else {
                    network.extend(batch.messages.drain(..));
                }
                // A local send can complete before remote delivery (or loss).
                // Only actual received Raft messages can acknowledge a prefix.
                node.outbound
                    .complete(batch, LocalSendResult::Sent)
                    .unwrap();
                progress = true;
            }
        }
        // Bound each network visit and redeliver some packets. Rejected ingress
        // stays owned by this bounded driver rather than disappearing.
        for _ in 0..32 {
            let Some(message) = network.pop_front() else {
                break;
            };
            if isolated.is_some_and(|n| message.from == n || message.to == n) {
                progress = true;
                continue;
            }
            sequence += 1;
            let duplicate = (sequence % 17 == 0).then(|| message.clone());
            let target = message.to.get() as usize - 1;
            match nodes[target]
                .shard
                .admit(message.group, Event::Receive(message))
            {
                Ok(()) => {
                    progress = true;
                    if let Some(message) = duplicate {
                        assert!(network.len() < 8192);
                        network.push_back(message);
                    }
                }
                Err(rejected) if rejected.reason == RuntimeError::Overloaded => {
                    let Event::Receive(message) = *rejected.event else {
                        panic!()
                    };
                    network.push_back(message);
                }
                Err(e) => panic!("unexpected network rejection: {e:?}"),
            }
        }
        if !progress {
            if network.is_empty()
                && nodes.iter().map(|n| n.sent_frames).sum::<usize>()
                    == nodes.iter().map(|n| n.received_frames).sum::<usize>()
                && nodes.iter().all(|n| {
                    n.worker.is_drained()
                        && n.pending.is_empty()
                        && n.messages.is_empty()
                        && n.outbound.is_drained()
                        && n.staged_sends.is_empty()
                })
            {
                return;
            }
            // ThreadWake can coalesce notifications; the bounded poll is the
            // authority. Host replacements also make progress on the next poll.
            std::thread::park_timeout(std::time::Duration::from_millis(1));
        }
    }
}
fn propose(operation: u128, delta: i64) -> Event {
    Event::Propose {
        operation: OperationId::new(operation).unwrap(),
        bytes: delta.to_le_bytes().to_vec(),
    }
}
fn worker_cluster_history<W: PersistenceWorker>(nodes: &mut [WorkerNode<W>]) {
    for g in 1..=100 {
        nodes[0].shard.admit(group(g), Event::Campaign).unwrap();
    }
    worker_pump(nodes, None);
    assert_eq!(nodes[0].largest_batch, 100);
    for node in nodes.iter() {
        for g in 1..=100 {
            assert_eq!(node.shard.core(group(g)).unwrap().state().commit_index, 1);
        }
    }
    nodes[0].hold = true;
    for g in 1..=100 {
        nodes[0].shard.admit(group(g), propose(1, 7)).unwrap();
    }
    worker_pump(nodes, None);
    assert!(nodes[0].held.is_some());
    assert_eq!(nodes[0].apps[&group(1)].read_applied(1), Ok(0));
    for node in nodes.iter() {
        for g in 2..=100 {
            assert_eq!(node.apps[&group(g)].read_applied(2), Ok(7));
        }
    }
    let mut admitted = 0;
    while nodes[0].shard.admit(group(1), propose(1, 7)).is_ok() {
        admitted += 1;
        assert!(admitted < 6);
    }
    assert!(admitted > 0);
    nodes[0].shard.admit(group(1), Event::Heartbeat).unwrap();
    nodes[0]
        .shard
        .admit(
            group(100),
            Event::Read {
                request: ReadRequestId::new(1).unwrap(),
            },
        )
        .unwrap();
    worker_pump(nodes, None);
    assert_eq!(nodes[0].reads, [7]);
    nodes[0].hold = false;
    let event = nodes[0].held.take().unwrap();
    nodes[0].deliver(event);
    worker_pump(nodes, None);
    for g in 1..=100 {
        nodes[0].shard.admit(group(g), propose(1, 7)).unwrap();
    }
    worker_pump(nodes, None);
    for g in 1..=100 {
        nodes[0].shard.admit(group(g), propose(2, 3)).unwrap();
    }
    worker_pump(nodes, None);
    for node in nodes.iter() {
        node.values(10);
        assert!(node
            .results
            .iter()
            .any(|r| r.duplicate && r.outcome == CounterOutcome::Value(7)));
    }
    for g in 1..=100 {
        nodes[0]
            .shard
            .admit(
                group(g),
                Event::Read {
                    request: ReadRequestId::new(2).unwrap(),
                },
            )
            .unwrap();
    }
    worker_pump(nodes, None);
    assert_eq!(nodes[0].reads.len(), 101);
    assert!(nodes[0].reads[1..].iter().all(|v| *v == 10));
    // An isolated old leader cannot complete a fresh read. The other two
    // workers continue and elect a new leader without sharing a storage lane.
    nodes[0].shard.admit(group(100), propose(99, 1000)).unwrap();
    nodes[0]
        .shard
        .admit(
            group(100),
            Event::Read {
                request: ReadRequestId::new(3).unwrap(),
            },
        )
        .unwrap();
    for g in 1..=100 {
        nodes[1].shard.admit(group(g), Event::Campaign).unwrap();
    }
    worker_pump(nodes, Some(node(1)));
    for g in 1..=100 {
        nodes[1].shard.admit(group(g), propose(3, 5)).unwrap();
    }
    worker_pump(nodes, Some(node(1)));
    assert_eq!(nodes[0].reads.len(), 101);
    nodes[0].values(10);
    nodes[1].values(15);
    nodes[2].values(15);
    for g in 1..=100 {
        nodes[1].shard.admit(group(g), Event::Heartbeat).unwrap();
    }
    worker_pump(nodes, None);
    for node in nodes.iter() {
        node.values(15);
        assert!(node.shard.is_drained());
    }
    // Close one instance sharing the same wake resource. The surviving quorum
    // must still persist, apply and serve a fresh read.
    nodes[0].shard.close_admission();
    nodes[0].worker.close();
    nodes[0].outbound.close();
    nodes[1].shard.admit(group(100), propose(4, 1)).unwrap();
    worker_pump(nodes, Some(node(1)));
    nodes[1]
        .shard
        .admit(
            group(100),
            Event::Read {
                request: ReadRequestId::new(1).unwrap(),
            },
        )
        .unwrap();
    worker_pump(nodes, Some(node(1)));
    assert_eq!(nodes[1].reads, [16]);
    assert!(nodes
        .iter()
        .all(|n| n.worker.is_drained() && n.pending.is_empty() && n.outbound.is_drained()));
    assert!(nodes.iter().any(|n| n.rejected_sends > 0));
}

fn outbound_limits() -> OutboundLimits {
    let budget = OutboundBudget {
        max: OutboundUsage {
            batches: 16,
            messages: 32,
            bytes: 128 * 1024,
        },
        control: OutboundUsage {
            batches: 4,
            messages: 8,
            bytes: 16 * 1024,
        },
        background: OutboundUsage {
            batches: 2,
            messages: 4,
            bytes: 16 * 1024,
        },
    };
    OutboundLimits {
        max_peers: 2,
        node: OutboundBudget {
            max: OutboundUsage {
                batches: 32,
                messages: 64,
                bytes: 256 * 1024,
            },
            ..budget
        },
        peer: budget,
        batch_messages: 8,
        batch_bytes: 16 * 1024,
    }
}

#[test]
fn public_host_workers_replicate_read_retry_replace_leader_and_close_independently() {
    let mut nodes = (1..=3)
        .map(|id| {
            WorkerNode::new(
                id,
                HostLogStore::new(id as u128),
                true,
                HostWorker::new,
                |binding| Box::new(HostOutbound::new(binding, outbound_limits()).unwrap()),
            )
        })
        .collect::<Vec<_>>();
    worker_cluster_history(&mut nodes);
}

#[cfg(feature = "native")]
mod native {
    use super::*;
    use std::{
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc, Condvar, Mutex,
        },
        time::{Duration, Instant},
    };
    use voteboat::{
        contracts::{HardState, StorageError},
        native::{log_store::*, outbound::NativeOutbound, wire::NativeWireCodec, worker::*},
    };
    #[derive(Default)]
    struct Wake(AtomicUsize);
    impl WorkerWake for Wake {
        fn wake(&self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    fn event<W: PersistenceWorker>(worker: &mut W) -> WorkerEvent {
        let end = Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(event) = worker.poll(1).pop() {
                return event;
            }
            assert!(Instant::now() < end, "worker did not complete");
            std::thread::park_timeout(Duration::from_millis(1));
        }
    }
    fn reclaim<L: LogStore + Send + 'static>(
        worker: &mut NativeLogWorker<L>,
    ) -> Result<L, StorageError> {
        worker.close();
        let end = Instant::now() + Duration::from_secs(5);
        loop {
            match worker.try_reclaim() {
                Ok(Some(store)) => return Ok(store),
                Err(e) => return Err(e),
                Ok(None) => {
                    assert!(Instant::now() < end);
                    std::thread::park_timeout(Duration::from_millis(1));
                }
            }
        }
    }
    #[cfg(feature = "tls")]
    fn enable_tls_mesh<W: PersistenceWorker>(nodes: &mut [WorkerNode<W>]) {
        use voteboat::{
            native::transport::NativePeerTransport, secure::LocalIdentity,
            transport::TransportLimits, wire::WireLimits,
        };
        for a in 0..nodes.len() {
            for b in a + 1..nodes.len() {
                let local = |n: &WorkerNode<W>| LocalIdentity {
                    node: n.outbound.binding().node,
                    store: n.outbound.binding().store,
                };
                let (left, right) =
                    support::tls::pair(local(&nodes[a]), local(&nodes[b]), (a * 3 + b + 1) as u64);
                let left = NativePeerTransport::new(
                    left,
                    NativeWireCodec::new(WireLimits::default()).unwrap(),
                    &*nodes[a].outbound,
                    TransportLimits::default(),
                )
                .unwrap();
                let right = NativePeerTransport::new(
                    right,
                    NativeWireCodec::new(WireLimits::default()).unwrap(),
                    &*nodes[b].outbound,
                    TransportLimits::default(),
                )
                .unwrap();
                nodes[a]
                    .transports
                    .insert(node(b as u64 + 1), Box::new(left));
                nodes[b]
                    .transports
                    .insert(node(a as u64 + 1), Box::new(right));
            }
        }
    }
    #[test]
    fn three_native_wal_workers_replicate_and_recover_acknowledged_operations() {
        let root =
            std::env::temp_dir().join(format!("voteboat-worker-cluster-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let wake = Arc::new(ThreadWake::current());
        let mut nodes = (1..=3)
            .map(|id| {
                let store = NativeLogStore::create(
                    FileLogIo::create(root.join(id.to_string())).unwrap(),
                    identity(id as u128),
                    LogLimits::default(),
                )
                .unwrap();
                WorkerNode::new(
                    id,
                    store,
                    true,
                    |store| {
                        NativeLogWorker::spawn(
                            store,
                            StorageWorkerGeneration::new(1).unwrap(),
                            WorkerLimits::default(),
                            wake.clone(),
                        )
                        .unwrap()
                    },
                    |binding| Box::new(NativeOutbound::new(binding, outbound_limits()).unwrap()),
                )
                .with_wire(NativeWireCodec::new(voteboat::wire::WireLimits::default()).unwrap())
            })
            .collect::<Vec<_>>();
        let old_bindings = nodes
            .iter()
            .map(|n| n.worker.binding().store)
            .collect::<Vec<_>>();
        #[cfg(feature = "tls")]
        enable_tls_mesh(&mut nodes);
        worker_cluster_history(&mut nodes);
        for node in &mut nodes {
            node.shard.close_admission();
            drop(reclaim(&mut node.worker).unwrap());
        }
        drop(nodes);
        let mut nodes = (1..=3)
            .map(|id| {
                let store = NativeLogStore::recover(
                    FileLogIo::open(root.join(id.to_string())).unwrap(),
                    identity(id as u128),
                    LogLimits::default(),
                )
                .unwrap();
                assert_ne!(store.binding(), old_bindings[id as usize - 1]);
                WorkerNode::new(
                    id,
                    store,
                    false,
                    |store| {
                        NativeLogWorker::spawn(
                            store,
                            StorageWorkerGeneration::new(1).unwrap(),
                            WorkerLimits::default(),
                            wake.clone(),
                        )
                        .unwrap()
                    },
                    |binding| Box::new(NativeOutbound::new(binding, outbound_limits()).unwrap()),
                )
                .with_wire(NativeWireCodec::new(voteboat::wire::WireLimits::default()).unwrap())
            })
            .collect::<Vec<_>>();
        // The explicitly stopped follower missed operation 4; both surviving
        // durable voters recover its success before the follower catches up.
        #[cfg(feature = "tls")]
        enable_tls_mesh(&mut nodes);
        for (id, node) in nodes.iter().enumerate() {
            for g in 1..=100 {
                let app = &node.apps[&group(g)];
                assert_eq!(
                    app.read_applied(app.applied_index()),
                    Ok(if g == 100 && id != 0 { 16 } else { 15 })
                );
            }
        }
        for g in 1..=100 {
            nodes[1].shard.admit(group(g), Event::Campaign).unwrap();
        }
        worker_pump(&mut nodes, None);
        for operation in [1, 3, 5] {
            let delta = match operation {
                1 => 7,
                3 => 5,
                _ => 4,
            };
            for g in 1..=100 {
                nodes[1]
                    .shard
                    .admit(group(g), propose(operation, delta))
                    .unwrap();
            }
            worker_pump(&mut nodes, None);
        }
        for g in 1..=100 {
            nodes[1]
                .shard
                .admit(
                    group(g),
                    Event::Read {
                        request: ReadRequestId::new(1).unwrap(),
                    },
                )
                .unwrap();
        }
        worker_pump(&mut nodes, None);
        assert_eq!(nodes[1].reads.len(), 100);
        assert_eq!(nodes[1].reads.iter().filter(|v| **v == 20).count(), 1);
        assert_eq!(nodes[1].reads.iter().filter(|v| **v == 19).count(), 99);
        for node in &mut nodes {
            assert!(node
                .results
                .iter()
                .any(|r| r.duplicate && r.outcome == CounterOutcome::Value(7)));
            assert!(node
                .results
                .iter()
                .any(|r| r.duplicate && r.outcome == CounterOutcome::Value(15)));
            for g in 1..=100 {
                let app = &node.apps[&group(g)];
                assert_eq!(
                    app.read_applied(app.applied_index()),
                    Ok(if g == 100 { 20 } else { 19 })
                );
            }
            node.shard.close_admission();
            drop(reclaim(&mut node.worker).unwrap());
        }
        drop(nodes);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn automatic_timers_submit_and_receive_through_the_timed_worker_helpers() {
        use voteboat::native::runtime::{DeadlineQueue, JitterEntropy};
        let mut store = HostLogStore::new(1);
        let shard = shard(&mut store, 2);
        let timers = DeadlineQueue::new(owner(store.binding()), 100).unwrap();
        let mut timed = TimedShard::new(
            shard,
            timers,
            JitterEntropy::new(7),
            TimerConfig::default(),
            MonoTime(0),
        )
        .unwrap();
        let mut worker = NativeLogWorker::spawn(
            store,
            StorageWorkerGeneration::new(1).unwrap(),
            WorkerLimits::default(),
            Arc::new(ThreadWake::current()),
        )
        .unwrap();
        let now = MonoTime(300);
        let mut units = vec![];
        while let Some(visit) = timed.poll(now).unwrap() {
            let effects = timed
                .step_next(visit, now)
                .unwrap()
                .unwrap()
                .result
                .unwrap();
            let [Effect::Persist(update)] = effects.as_slice() else {
                panic!()
            };
            units.push(PersistUnit {
                visit,
                update: update.clone(),
            });
        }
        assert_eq!(units.len(), 2);
        submit_for_timed(&mut timed, &mut worker, units, now).unwrap();
        let written = event(&mut worker);
        assert!(apply_to_timed(&mut timed, written, now).iter().all(|d| d
            .result
            .as_ref()
            .unwrap()
            .is_empty()));
        assert!(timed.poll(MonoTime(301)).unwrap().is_none());
        let durable = event(&mut worker);
        let deliveries = apply_to_timed(&mut timed, durable, MonoTime(301));
        assert_eq!(deliveries.len(), 2);
        for delivery in deliveries {
            assert_eq!(delivery.result.unwrap().len(), 2);
            timed.finish(delivery.visit).unwrap();
        }
        assert!(timed.is_drained());
        assert!(timed.poll(MonoTime(302)).unwrap().is_none());
        timed.close_admission().unwrap();
        drop(reclaim(&mut worker).unwrap());
    }
    #[derive(Default)]
    struct Gate {
        released: bool,
        entered: bool,
        fail: bool,
        panic: bool,
    }
    struct Gated<L: LogStore> {
        store: L,
        gate: Arc<(Mutex<Gate>, Condvar)>,
    }
    impl<L: LogStore> LogStore for Gated<L> {
        fn binding(&self) -> StoreBinding {
            self.store.binding()
        }
        fn limits(&self) -> LogLimits {
            self.store.limits()
        }
        fn state(&self, g: GroupIdentity) -> Result<GroupLog, StorageError> {
            self.store.state(g)
        }
        fn append_batch(&mut self, u: Vec<LogMutation>) -> Result<Vec<LogTicket>, StorageError> {
            self.store.append_batch(u)
        }
        fn barrier(&mut self, t: &[LogTicket]) -> Result<DurableLog, StorageError> {
            let (lock, cv) = &*self.gate;
            let mut gate = lock.lock().unwrap();
            gate.entered = true;
            cv.notify_all();
            while !gate.released {
                gate = cv.wait(gate).unwrap();
            }
            let fail = gate.fail;
            let panic = gate.panic;
            drop(gate);
            assert!(!panic, "injected worker panic");
            if fail {
                Err(StorageError::Uncertain("injected barrier failure".into()))
            } else {
                self.store.barrier(t)
            }
        }
        fn fetch_range(
            &self,
            g: GroupIdentity,
            v: LogGeneration,
            f: u64,
            n: usize,
            b: usize,
        ) -> Result<Vec<LogEntry>, StorageError> {
            self.store.fetch_range(g, v, f, n, b)
        }
    }
    fn release(gate: &Arc<(Mutex<Gate>, Condvar)>) {
        let (lock, cv) = &**gate;
        lock.lock().unwrap().released = true;
        cv.notify_all();
    }
    #[test]
    fn admission_reserves_control_and_keeps_credits_until_terminal_delivery() {
        let mut store = HostLogStore::new(1);
        let mut shard = shard(&mut store, 4);
        let mut first = campaign(&mut shard, 1);
        let mut second = campaign(&mut shard, 2);
        let control = campaign(&mut shard, 3);
        let fourth = campaign(&mut shard, 4);
        for unit in [&mut first, &mut second] {
            unit.update.suffix = Some(Suffix {
                from: 1,
                entries: vec![entry(1, 1, 7)],
            });
        }
        let duplicate = PersistUnit {
            visit: first.visit,
            update: first.update.clone(),
        };
        let gate = Arc::new((Mutex::new(Gate::default()), Condvar::new()));
        let wake = Arc::new(Wake::default());
        let limits = WorkerLimits {
            max_requests: 3,
            control_requests: 1,
            max_units: 4,
            control_units: 1,
            batch_units: 3,
            ..WorkerLimits::default()
        };
        let mut worker = NativeLogWorker::spawn(
            Gated {
                store,
                gate: gate.clone(),
            },
            StorageWorkerGeneration::new(1).unwrap(),
            limits,
            wake.clone(),
        )
        .unwrap();
        worker.submit(vec![first]).unwrap();
        assert_eq!(
            worker.submit(vec![duplicate]).unwrap_err().reason,
            WorkerError::Overloaded
        );
        worker.submit(vec![second]).unwrap();
        let mut oversized = Vec::with_capacity(4);
        oversized.push(fourth);
        let rejected = worker.submit(oversized).unwrap_err();
        assert_eq!(rejected.reason, WorkerError::BatchTooLarge);
        assert_eq!(rejected.units.len(), 1);
        let mut fourth = rejected.units.into_iter().next().unwrap();
        fourth.update.suffix = Some(Suffix {
            from: 1,
            entries: vec![entry(1, 1, 9)],
        });
        assert_eq!(
            worker.submit(vec![fourth]).unwrap_err().reason,
            WorkerError::Overloaded
        );
        worker.submit(vec![control]).unwrap();
        assert_eq!(worker.usage().requests, 3);
        assert!(matches!(event(&mut worker), WorkerEvent::Written { .. }));
        worker.close();
        release(&gate);
        let end = Instant::now() + Duration::from_secs(5);
        while wake.0.load(Ordering::SeqCst) < 6 {
            assert!(Instant::now() < end);
            std::thread::park_timeout(Duration::from_millis(1));
        }
        // All physical barriers completed, but no terminal has been consumed.
        assert_eq!(worker.usage().requests, 3);
        assert!(worker.try_reclaim().unwrap().is_none());
        let mut durable = 0;
        for _ in 0..5 {
            if matches!(event(&mut worker), WorkerEvent::Durable { .. }) {
                durable += 1;
            }
        }
        assert_eq!(durable, 3);
        assert_eq!(worker.usage(), WorkerUsage::default());
        let store = reclaim(&mut worker).unwrap();
        assert_eq!(store.store.state(group(1)).unwrap().entries.len(), 1);
        assert_eq!(store.store.state(group(3)).unwrap().hard_state.term, 1);
    }

    #[test]
    fn hundred_groups_share_one_native_worker_and_recover_exact_votes() {
        let root = std::env::temp_dir().join(format!("voteboat-wal-worker-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let mut store = NativeLogStore::create(
            FileLogIo::create(&root).unwrap(),
            identity(1),
            LogLimits::default(),
        )
        .unwrap();
        let mut shard = shard(&mut store, 100);
        let old_binding = store.binding();
        let units = (1..=100)
            .map(|g| campaign(&mut shard, g))
            .collect::<Vec<_>>();
        let visits = units.iter().map(|u| u.visit).collect::<Vec<_>>();
        let mut worker = NativeLogWorker::spawn(
            store,
            StorageWorkerGeneration::new(1).unwrap(),
            WorkerLimits::default(),
            Arc::new(ThreadWake::current()),
        )
        .unwrap();
        submit_for_shard(&mut shard, &mut worker, units).unwrap();
        assert_eq!(worker.usage().units, 100);
        let written = apply_to_shard(&mut shard, event(&mut worker));
        assert_eq!(written.len(), 100);
        assert!(written
            .iter()
            .all(|d| d.result.as_ref().unwrap().is_empty()));
        let durable = apply_to_shard(&mut shard, event(&mut worker));
        assert_eq!(durable.len(), 100);
        assert!(durable
            .iter()
            .all(|d| d.result.as_ref().unwrap().len() == 2));
        for visit in visits {
            shard.finish(visit).unwrap();
        }
        drop(reclaim(&mut worker).unwrap());
        let store = NativeLogStore::recover(
            FileLogIo::open(&root).unwrap(),
            identity(1),
            LogLimits::default(),
        )
        .unwrap();
        assert_ne!(store.binding(), old_binding);
        for g in 1..=100 {
            let state = store.state(group(g)).unwrap();
            assert_eq!(
                state.hard_state,
                HardState {
                    term: 1,
                    voted_for: Some(node(1))
                }
            );
            Raft::recover(node(1), store.binding(), state, store.limits()).unwrap();
        }
        drop(store);
        std::fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn native_written_stage_never_releases_vote_and_another_group_runs_while_sync_blocks() {
        let mut store = HostLogStore::new(1);
        let mut shard = shard(&mut store, 2);
        let unit = campaign(&mut shard, 1);
        let visit = unit.visit;
        let gate = Arc::new((Mutex::new(Gate::default()), Condvar::new()));
        let wake = Arc::new(Wake::default());
        let mut worker = NativeLogWorker::spawn(
            Gated {
                store,
                gate: gate.clone(),
            },
            StorageWorkerGeneration::new(1).unwrap(),
            WorkerLimits::default(),
            wake.clone(),
        )
        .unwrap();
        submit_for_shard(&mut shard, &mut worker, vec![unit]).unwrap();
        let written = event(&mut worker);
        assert!(matches!(written, WorkerEvent::Written { .. }));
        assert!(apply_to_shard(&mut shard, written)[0]
            .result
            .as_ref()
            .unwrap()
            .is_empty());
        assert_eq!(worker.usage().requests, 1);
        assert!(worker.poll(10).is_empty());
        assert_eq!(shard.finish(visit), Err(RuntimeError::DependencyPending));
        shard.admit(group(2), Event::Heartbeat).unwrap();
        let other = shard.poll(MonoTime(1)).unwrap().unwrap();
        assert_eq!(other.group, group(2));
        assert!(shard
            .step_next(other, MonoTime(1))
            .unwrap()
            .unwrap()
            .result
            .unwrap()
            .is_empty());
        shard.finish(other).unwrap();
        worker.close();
        release(&gate);
        let durable = event(&mut worker);
        assert!(matches!(durable, WorkerEvent::Durable { .. }));
        assert_eq!(
            apply_to_shard(&mut shard, durable)[0]
                .result
                .as_ref()
                .unwrap()
                .len(),
            2
        );
        shard.finish(visit).unwrap();
        assert!(wake.0.load(Ordering::SeqCst) >= 1);
        let returned = reclaim(&mut worker).unwrap();
        assert_eq!(returned.store.state(group(1)).unwrap().hard_state.term, 1);
    }
    #[test]
    fn fatal_barrier_and_panicked_worker_fail_every_accepted_request_without_durable_evidence() {
        for panic in [false, true] {
            let mut store = HostLogStore::new(1);
            let mut shard = shard(&mut store, 2);
            let first = campaign(&mut shard, 1);
            let second = campaign(&mut shard, 2);
            let gate = Arc::new((
                Mutex::new(Gate {
                    fail: !panic,
                    panic,
                    ..Gate::default()
                }),
                Condvar::new(),
            ));
            let mut worker = NativeLogWorker::spawn(
                Gated {
                    store,
                    gate: gate.clone(),
                },
                StorageWorkerGeneration::new(1).unwrap(),
                WorkerLimits::default(),
                Arc::new(ThreadWake::current()),
            )
            .unwrap();
            submit_for_shard(&mut shard, &mut worker, vec![first]).unwrap();
            submit_for_shard(&mut shard, &mut worker, vec![second]).unwrap();
            let written = event(&mut worker);
            assert!(matches!(written, WorkerEvent::Written { .. }));
            apply_to_shard(&mut shard, written);
            release(&gate);
            let mut failures = std::collections::BTreeSet::new();
            while failures.len() < 2 {
                let output = event(&mut worker);
                match output {
                    WorkerEvent::Written { .. } => assert!(apply_to_shard(&mut shard, output)
                        .iter()
                        .all(|d| d.result.as_ref().unwrap().is_empty())),
                    WorkerEvent::Failed { request, .. } => {
                        assert!(failures.insert(request.sequence));
                        assert!(apply_to_shard(&mut shard, output)
                            .iter()
                            .all(|d| d.result.is_err()));
                    }
                    WorkerEvent::Durable { .. } => panic!("failed barrier released durability"),
                }
            }
            assert!(shard.core(group(1)).unwrap().is_fenced());
            assert!(shard.core(group(2)).unwrap().is_fenced());
            assert!(worker.is_drained());
            let returned = reclaim(&mut worker);
            assert_eq!(returned.is_err(), panic);
        }
    }
}
