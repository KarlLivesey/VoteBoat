// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use crate::Maintenance;
use std::{collections::VecDeque, mem::size_of, time::Instant};
use voteboat::{application::StateMachine, snapshot::*};

pub(super) struct Parts {
    pub slot: usize,
    pub core: Raft,
    pub store: MemoryStore,
    pub snapshots: MemorySnapshots,
    pub application: Noop,
    pub receiver: mpsc::Receiver<Request>,
    pub peer_receiver: mpsc::Receiver<Message>,
    pub peers: Vec<mpsc::Sender<Message>>,
    pub stopped: watch::Receiver<bool>,
    pub stopping: Arc<AtomicBool>,
    pub stats: Arc<[stats::Stats; 3]>,
    #[cfg(test)]
    pub paused: Arc<[AtomicBool; 3]>,
    #[cfg(test)]
    pub paused_proposals: Arc<AtomicBool>,
    pub frontiers: Arc<[AtomicU64; 3]>,
    pub settings: Settings,
}
pub(super) struct Actor {
    parts: Parts,
    pending: BTreeMap<OperationId, Reply>,
    deferred: Option<Request>,
    last_snapshot_at: u64,
    last_contact: Instant,
}
impl Actor {
    pub fn new(parts: Parts) -> Self {
        Self {
            parts,
            pending: BTreeMap::new(),
            deferred: None,
            last_snapshot_at: 0,
            last_contact: Instant::now(),
        }
    }
    pub async fn run(mut self) -> Result<(), Failure> {
        let mut tick = tokio::time::interval(Duration::from_millis(50));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        if self.parts.slot == 0 {
            self.step(Event::Campaign)?;
        }
        loop {
            // Keep stop and already-delivered peer progress ahead of more data.
            if let Some(request) = self.deferred.take() {
                self.requests(request)?;
            } else {
                tokio::select! {
                    biased;
                    _ = self.parts.stopped.changed() => return Ok(()),
                    message = self.parts.peer_receiver.recv(), if self.accepts_peer() => {
                        let message = message.ok_or("peer actor input closed")?;
                        self.last_contact = Instant::now();
                        self.step(Event::Receive(message))?;
                    }
                    request = self.parts.receiver.recv(), if self.accepts_proposals() => self.requests(request.ok_or("client actor input closed")?)?,
                    _ = tick.tick() => self.tick()?,
                }
            }
            let stats = &self.parts.stats[self.parts.slot];
            let retained = self.parts.core.state().entries.len();
            stats.peak_retained.fetch_max(retained, Ordering::Relaxed);
            stats
                .peak_client_queue
                .fetch_max(self.parts.receiver.len(), Ordering::Relaxed);
            stats
                .peak_peer_queue
                .fetch_max(self.parts.peer_receiver.len(), Ordering::Relaxed);
            self.maintain()?;
            self.parts.stats[self.parts.slot]
                .retained
                .store(self.parts.core.state().entries.len(), Ordering::Relaxed);
            self.parts.frontiers[self.parts.slot]
                .store(self.parts.application.applied, Ordering::Release);
        }
    }
    fn accepts_peer(&self) -> bool {
        #[cfg(test)]
        {
            !self.parts.paused[self.parts.slot].load(Ordering::Acquire)
        }
        #[cfg(not(test))]
        {
            true
        }
    }
    fn accepts_proposals(&self) -> bool {
        #[cfg(test)]
        {
            !self.parts.paused_proposals.load(Ordering::Acquire)
        }
        #[cfg(not(test))]
        {
            true
        }
    }
    fn tick(&mut self) -> Result<(), Failure> {
        let event = if self.parts.core.role() == Role::Leader {
            Some(Event::Heartbeat)
        } else if self.last_contact.elapsed() >= Duration::from_millis(2000) {
            self.last_contact = Instant::now();
            Some(Event::Campaign)
        } else {
            None
        };
        if let Some(event) = event {
            self.step(event)?;
        }
        Ok(())
    }
    fn reserve(&self, input: usize) -> Result<(), Failure> {
        if self
            .parts
            .core
            .effect_reservation(input)
            .is_none_or(|bytes| bytes > u32::MAX as usize)
        {
            return Err("core effect reservation exceeds declared4GiB actor budget".into());
        }
        Ok(())
    }
    fn step(&mut self, event: Event) -> Result<(), Failure> {
        let input = match &event {
            Event::Receive(message) => {
                voteboat::outbound::message_cost(message, u32::MAX as usize)
                    .map_err(debug_error)?
                    .1
                    + size_of::<Event>()
            }
            _ => size_of::<Event>(),
        };
        self.reserve(input)?;
        let effects = self.parts.core.step(event).map_err(debug_error)?;
        self.effects(effects)
    }
    fn requests(&mut self, first: Request) -> Result<(), Failure> {
        let capacity = self.parts.core.batch_limits().max_proposal_entries;
        let mut proposals = Vec::with_capacity(first.proposals.len());
        self.include(first, &mut proposals)?;
        while proposals.len() < capacity && self.parts.peer_receiver.is_empty() {
            match self.parts.receiver.try_recv() {
                Ok(request) if proposals.len() + request.proposals.len() <= capacity => {
                    self.include(request, &mut proposals)?;
                }
                Ok(request) => {
                    self.deferred = Some(request);
                    break;
                }
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    return Err("actor disconnected".into())
                }
            }
        }
        self.reserve(size_of::<Vec<Proposal>>() + proposals.capacity() * size_of::<Proposal>())?;
        let width = proposals.len();
        let effects = if width == 1 {
            let proposal = proposals.pop().unwrap();
            self.parts.core.step(Event::Propose {
                operation: proposal.operation,
                bytes: proposal.bytes,
            })
        } else {
            self.parts.core.propose_batch(proposals)
        }
        .map_err(debug_error)?;
        self.parts.stats[self.parts.slot].record_proposals(width)?;
        self.effects(effects)
    }
    fn include(&mut self, request: Request, proposals: &mut Vec<Proposal>) -> Result<(), Failure> {
        if request.proposals.is_empty()
            || request.proposals.len() > self.parts.core.batch_limits().max_proposal_entries
            || request.proposals.len() != request.replies.len()
        {
            return Err("original proposal/reply shape differs".into());
        }
        if self.pending.len() + request.proposals.len() > self.parts.settings.clients * 4 + 1 {
            return Err("original result budget exceeded".into());
        }
        for (proposal, reply) in request.proposals.into_iter().zip(request.replies) {
            if self.pending.insert(proposal.operation, reply).is_some() {
                return Err("duplicate original admission".into());
            }
            proposals.push(proposal);
        }
        Ok(())
    }
    fn effects(&mut self, initial: Vec<Effect>) -> Result<(), Failure> {
        let mut effects = VecDeque::from(initial);
        while let Some(effect) = effects.pop_front() {
            match effect {
                Effect::Persist(update) => {
                    self.parts.stats[self.parts.slot]
                        .persistence_units
                        .fetch_add(1, Ordering::Relaxed);
                    effects.extend(
                        persist_effect(&mut self.parts.core, &mut self.parts.store, update)
                            .map_err(debug_error)?,
                    );
                }
                Effect::Send(message) => {
                    let slot = usize::try_from(message.to.get() - 1)?;
                    let sent = self
                        .parts
                        .peers
                        .get(slot)
                        .ok_or("unknown recipient")?
                        .try_send(message);
                    if sent.is_err() && !self.parts.stopping.load(Ordering::Acquire) {
                        return Err("peer queue overflow/closed".into());
                    }
                }
                Effect::Committed(entries) => {
                    let results = self
                        .parts
                        .application
                        .apply_batch(&entries)
                        .map_err(debug_error)?;
                    if self.parts.slot == 0 {
                        for operation in results {
                            let reply = self
                                .pending
                                .remove(&operation)
                                .ok_or("missing/duplicate original applied result")?;
                            reply
                                .send(operation)
                                .map_err(|_| "original client dropped result wait")?;
                        }
                    }
                }
                Effect::SnapshotRequired {
                    to,
                    context,
                    reference,
                } => effects.extend(
                    supply_snapshot(
                        &self.parts.core,
                        &mut self.parts.snapshots,
                        to,
                        context,
                        reference,
                    )
                    .map_err(debug_error)?,
                ),
                Effect::StageSnapshot(message) => effects.extend(
                    stage_snapshot_effect(
                        &mut self.parts.core,
                        &mut self.parts.store,
                        &mut self.parts.snapshots,
                        &self.parts.application,
                        message,
                    )
                    .map_err(debug_error)?,
                ),
                Effect::SnapshotInstalled(reference) => {
                    self.parts.stats[self.parts.slot]
                        .snapshot_installs
                        .fetch_add(1, Ordering::Relaxed);
                    effects.extend(
                        finish_snapshot_install(
                            &mut self.parts.core,
                            &self.parts.store,
                            &mut self.parts.snapshots,
                            &mut self.parts.application,
                            reference,
                        )
                        .map_err(debug_error)?,
                    );
                }
                Effect::CheckpointCompacted(reference) => {
                    self.parts.snapshots.reconcile_log(Some(reference))?;
                }
                _ => return Err("unexpected read/configuration effect in framework run".into()),
            }
        }
        Ok(())
    }
    fn maintain(&mut self) -> Result<(), Failure> {
        if self.parts.settings.maintenance != Maintenance::DefaultRetention {
            return Ok(());
        }
        let applied = self.parts.application.applied;
        if applied.saturating_sub(self.last_snapshot_at) < 5000 {
            return Ok(());
        }
        let boundary = applied.saturating_sub(1000);
        if boundary.saturating_sub(self.parts.core.state().base_index()) < 1024 {
            return Ok(());
        }
        // The no-op historical state is exactly known: only its prefix matters.
        // Preserve the selected1000-entry ordered suffix, without replacing cores.
        let historical = Noop {
            group: self.parts.application.group,
            applied: boundary,
        };
        self.reserve(size_of::<SnapshotMetadata>() + 8)?;
        let receipt =
            checkpoint_application(&self.parts.core, &historical, &mut self.parts.snapshots)
                .map_err(debug_error)?;
        let effects = compact_replica(
            &mut self.parts.core,
            &mut self.parts.store,
            &mut self.parts.snapshots,
            &self.parts.application,
            receipt.reference(),
        )
        .map_err(debug_error)?;
        self.effects(effects)?;
        self.last_snapshot_at = applied;
        self.parts.stats[self.parts.slot]
            .checkpoint_count
            .fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}
