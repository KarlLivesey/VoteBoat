// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use crate::{
    attribution::{Attribution, Phase, Recorder},
    Maintenance,
};
use std::{collections::VecDeque, mem::size_of, time::Instant};
use voteboat::{
    application::{ApplicationError, StateMachine},
    snapshot::*,
};

#[cfg(test)]
mod tests;

mod pending_replies;
use pending_replies::PendingReplies;
mod applied;
use applied::Applied;

/// One collection pass's originals.
enum Collected {
    /// A lone original, proposed with `Event::Propose`.
    One(OperationId, Vec<u8>),
    /// Two or more originals, staged once in the core's builder.
    Batch(ProposalBatch),
}
impl Collected {
    fn len(&self) -> usize {
        match self {
            Self::One(..) => 1,
            Self::Batch(batch) => batch.len(),
        }
    }
    #[cfg(test)]
    fn into_batch(self) -> ProposalBatch {
        match self {
            Self::One(..) => panic!("a lone original is not staged"),
            Self::Batch(batch) => batch,
        }
    }
}

pub(super) struct Parts<const TRACE: bool = false> {
    pub slot: usize,
    pub domain: WriteDomain<OperationId>,
    pub core: Raft,
    pub store: VolatileStore<TRACE>,
    pub snapshots: MemorySnapshots<TRACE>,
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
    #[cfg(test)]
    pub proposals_pause_observed: Arc<[AtomicBool; 3]>,
    pub frontiers: Arc<[AtomicU64; 3]>,
    pub settings: Settings,
}
pub(super) struct Actor<const TRACE: bool = false> {
    parts: Parts<TRACE>,
    attribution: Recorder<TRACE>,
    pending: PendingReplies<Reply>,
    deferred: Option<Request>,
    /// The one effect queue the core fills in place. Empty between successful
    /// calls: every drain leaves it empty, and any failure that skips the
    /// drain (such as `record_proposals` after a core call) ends the actor.
    queue: VecDeque<Effect>,
    last_snapshot_at: u64,
    unbound: Option<(u64, usize)>,
    last_contact: Instant,
    #[cfg(test)]
    tick_paused: bool,
    /// Test-only: runs once, right after a pass reads its ready count.
    #[cfg(test)]
    after_ready: Option<Box<dyn FnOnce() + Send>>,
}
impl<const TRACE: bool> Actor<TRACE> {
    pub fn new(parts: Parts<TRACE>) -> Self {
        let pending = PendingReplies::new(parts.settings.clients * 4 + 1);
        Self {
            parts,
            attribution: Recorder::default(),
            pending,
            deferred: None,
            queue: VecDeque::new(),
            last_snapshot_at: 0,
            unbound: None,
            last_contact: Instant::now(),
            #[cfg(test)]
            tick_paused: false,
            #[cfg(test)]
            after_ready: None,
        }
    }
    pub async fn run(mut self) -> Result<Option<Attribution>, Failure> {
        let started = Recorder::<TRACE>::start();
        let result = self.run_inner().await;
        self.attribution
            .finish(Phase::ActorLifetime, started, result.is_err(), 0);
        result?;
        self.attribution.report(
            self.parts.slot,
            &self.parts.store.attribution,
            &self.parts.snapshots.attribution,
        )
    }
    async fn run_inner(&mut self) -> Result<(), Failure> {
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
                let started = Recorder::<TRACE>::start();
                tokio::select! {
                    biased;
                    _ = self.parts.stopped.changed() => {
                        self.attribution.finish(Phase::HostWait, started, false, 0);
                        return Ok(());
                    }
                    message = self.parts.peer_receiver.recv(), if self.accepts_peer() => {
                        self.attribution.finish(Phase::HostWait, started, false, 0);
                        let message = message.ok_or("peer actor input closed")?;
                        self.last_contact = Instant::now();
                        self.step(Event::Receive(message))?;
                    }
                    request = self.parts.receiver.recv(), if self.accepts_proposals() => {
                        self.attribution.finish(Phase::HostWait, started, false, 0);
                        self.requests(request.ok_or("client actor input closed")?)?;
                    }
                    _ = tick.tick() => {
                        self.attribution.finish(Phase::HostWait, started, false, 0);
                        self.tick()?;
                    }
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
            let paused = self.parts.paused_proposals.load(Ordering::Acquire);
            self.parts.proposals_pause_observed[self.parts.slot].store(paused, Ordering::Release);
            !paused
        }
        #[cfg(not(test))]
        {
            true
        }
    }
    fn tick(&mut self) -> Result<(), Failure> {
        #[cfg(test)]
        {
            if self.parts.paused[self.parts.slot].load(Ordering::Acquire) {
                self.tick_paused = true;
                return Ok(());
            }
            if std::mem::take(&mut self.tick_paused) {
                self.last_contact = Instant::now();
            }
        }
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
        let started = Recorder::<TRACE>::start();
        let result = self.parts.core.step_into(event, &mut self.queue);
        self.attribution
            .finish(Phase::Core, started, result.is_err(), 0);
        result.map_err(debug_error)?;
        self.drain()
    }
    fn requests(&mut self, first: Request) -> Result<(), Failure> {
        let started = Recorder::<TRACE>::start();
        let collected = self.collect(first);
        self.attribution.finish(
            Phase::HostAdmission,
            started,
            collected.is_err(),
            collected.as_ref().map_or(0, Collected::len),
        );
        let collected = collected?;
        let width = collected.len();
        let first = self
            .parts
            .core
            .state()
            .last_index()
            .checked_add(1)
            .ok_or("proposal index exhausted")?;
        self.unbound = Some((first, width));
        let started = Recorder::<TRACE>::start();
        let result = match collected {
            Collected::One(operation, bytes) => self
                .parts
                .core
                .step_into(Event::Propose { operation, bytes }, &mut self.queue),
            Collected::Batch(batch) => self.parts.core.propose_entries_into(batch, &mut self.queue),
        };
        self.attribution
            .finish(Phase::Core, started, result.is_err(), width);
        result.map_err(debug_error)?;
        self.parts.stats[self.parts.slot].record_proposals(width)?;
        self.drain()
    }
    fn collect(&mut self, first: Request) -> Result<Collected, Failure> {
        let capacity = self.parts.core.batch_limits().max_proposal_entries;
        // Only requests already queued when the pass starts. Peer progress that
        // arrives during the pass is handled before the next pass, not by cutting
        // this one, so batch width does not track follower reply latency.
        let mut ready = self.parts.receiver.len();
        #[cfg(test)]
        {
            if let Some(arrival) = self.after_ready.take() {
                arrival();
            }
        }
        // A pass of one original takes the core's single-proposal event, as
        // before the builder; the builder stages wider passes once.
        if ready == 0 && first.len() == 1 {
            let (operation, bytes) = self.include_one(first)?;
            self.reserve(size_of::<Event>() + bytes.capacity())?;
            return Ok(Collected::One(operation, bytes));
        }
        // An oversized request still reaches the shape check in `include`.
        let mut batch = self
            .parts
            .core
            .proposal_batch(first.len().min(capacity))
            .map_err(debug_error)?;
        self.include(first, &mut batch)?;
        while batch.len() < capacity && ready > 0 {
            ready -= 1;
            match self.parts.receiver.try_recv() {
                Ok(request) if batch.len() + request.len() <= capacity => {
                    self.include(request, &mut batch)?;
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
        self.reserve(batch.retained_bytes())?;
        Ok(Collected::Batch(batch))
    }
    fn admissible(&self, request: &Request) -> Result<(), Failure> {
        if request.is_empty() || request.len() > self.parts.core.batch_limits().max_proposal_entries
        {
            return Err("original proposal/reply shape differs".into());
        }
        if self.pending.len() + request.len() > self.parts.settings.clients * 4 + 1 {
            return Err("original result budget exceeded".into());
        }
        Ok(())
    }
    fn include(&mut self, request: Request, batch: &mut ProposalBatch) -> Result<(), Failure> {
        self.admissible(&request)?;
        request.validate(&self.parts.domain)?;
        for original in request.into_originals() {
            let Original {
                operation,
                bytes,
                reply,
            } = original?;
            if self.pending.insert(operation, reply).is_some() {
                return Err("duplicate original admission".into());
            }
            batch.push(operation, bytes).map_err(debug_error)?;
        }
        Ok(())
    }
    /// `include` for a request of exactly one original, without a builder.
    fn include_one(&mut self, request: Request) -> Result<(OperationId, Vec<u8>), Failure> {
        self.admissible(&request)?;
        request.validate(&self.parts.domain)?;
        let Some(Original {
            operation,
            bytes,
            reply,
        }) = request.into_originals().next().transpose()?
        else {
            return Err("original proposal/reply shape differs".into());
        };
        if self.pending.insert(operation, reply).is_some() {
            return Err("duplicate original admission".into());
        }
        Ok((operation, bytes))
    }
    fn bind_installed(&mut self) -> Result<(), Failure> {
        let Some((first, count)) = self.unbound else {
            return Ok(());
        };
        for ordinal in 0..count {
            let index = first
                .checked_add(ordinal as u64)
                .ok_or("proposal index exhausted")?;
            let entry = self
                .parts
                .core
                .state()
                .entry_at(index)
                .ok_or("actual proposal not installed")?;
            let EntryPayload::Command { operation, .. } = &entry.payload else {
                return Err("actual proposal is not a command".into());
            };
            self.pending
                .get_mut(operation)
                .ok_or("missing original proposal binding")?
                .bind(&self.parts.domain, entry)?;
        }
        self.unbound = None;
        Ok(())
    }
    /// Handle owned effects from a helper that returns them.
    fn effects(&mut self, initial: Vec<Effect>) -> Result<(), Failure> {
        self.queue.extend(initial);
        self.drain()
    }
    /// Handle every queued effect in order. Completions append to the same
    /// queue, which is put back empty so its allocation is reused.
    fn drain(&mut self) -> Result<(), Failure> {
        let mut effects = std::mem::take(&mut self.queue);
        let result = self.handle(&mut effects);
        effects.clear();
        self.queue = effects;
        result
    }
    fn handle(&mut self, effects: &mut VecDeque<Effect>) -> Result<(), Failure> {
        while let Some(effect) = effects.pop_front() {
            match effect {
                Effect::PersistVolatile(persist) => self.persist(persist, effects)?,
                Effect::Persist(_) => {
                    return Err("volatile replica emitted durable persistence".into())
                }
                Effect::Send(message) => self.send(message)?,
                Effect::Committed(entries) => self.apply(entries)?,
                snapshot @ (Effect::SnapshotRequired { .. }
                | Effect::StageSnapshot(_)
                | Effect::SnapshotInstalled(_)
                | Effect::CheckpointCompacted(_)) => {
                    let started = Recorder::<TRACE>::start();
                    let result = self.snapshot(snapshot);
                    self.attribution.finish(
                        Phase::SnapshotMaintenance,
                        started,
                        result.is_err(),
                        0,
                    );
                    effects.extend(result?);
                }
                _ => return Err("unexpected read/configuration effect in framework run".into()),
            }
        }
        Ok(())
    }
    fn persist(
        &mut self,
        persist: VolatilePersist,
        effects: &mut VecDeque<Effect>,
    ) -> Result<(), Failure> {
        self.parts.stats[self.parts.slot]
            .persistence_units
            .fetch_add(1, Ordering::Relaxed);
        let started = Recorder::<TRACE>::start();
        let queued = effects.len();
        let result = persist_volatile_into(
            &mut self.parts.core,
            &self.parts.store.log,
            persist,
            effects,
        );
        if queued == 0 {
            self.attribution
                .finish(Phase::PersistHelper, started, result.is_err(), 0);
            // The following effects wait in the queue until this interval
            // is applied.
            match result.map_err(debug_error)? {
                Some(entries) => Self::apply_entries(
                    &mut self.parts.application,
                    &mut self.pending,
                    &self.parts.domain,
                    &self.attribution,
                    self.parts.slot,
                    Applied::Borrowed(entries),
                    &self.parts.stats[self.parts.slot],
                )?,
                None => self.bind_installed()?,
            }
        } else {
            // Existing FIFO work precedes every newly completed effect.
            let result = result.map(|committed| {
                if let Some(entries) = committed {
                    effects.insert(queued, Effect::Committed(entries.to_vec()));
                }
            });
            self.attribution
                .finish(Phase::PersistHelper, started, result.is_err(), 0);
            result.map_err(debug_error)?;
            self.bind_installed()?;
        }
        Ok(())
    }
    fn send(&mut self, message: Message) -> Result<(), Failure> {
        let started = Recorder::<TRACE>::start();
        let result = self.send_inner(message);
        self.attribution
            .finish(Phase::Send, started, result.is_err(), 1);
        result
    }
    fn send_inner(&self, message: Message) -> Result<(), Failure> {
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
        Ok(())
    }
    fn apply(&mut self, entries: Vec<LogEntry>) -> Result<(), Failure> {
        Self::apply_entries(
            &mut self.parts.application,
            &mut self.pending,
            &self.parts.domain,
            &self.attribution,
            self.parts.slot,
            Applied::Owned(&entries),
            &self.parts.stats[self.parts.slot],
        )
    }
    fn apply_entries(
        application: &mut Noop,
        pending: &mut PendingReplies<Reply>,
        domain: &WriteDomain<OperationId>,
        attribution: &Recorder<TRACE>,
        slot: usize,
        entries: Applied<'_>,
        stats: &stats::Stats,
    ) -> Result<(), Failure> {
        let started = Recorder::<TRACE>::start();
        let results = entries.apply(application);
        attribution.finish(
            Phase::Application,
            started,
            results.is_err(),
            results.as_ref().map_or(0, Vec::len),
        );
        let results = results.map_err(debug_error)?;
        #[cfg(test)]
        {
            stats
                .application_receipts
                .fetch_add(results.len() as u64, Ordering::Relaxed);
            stats
                .application_intervals
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .push((
                    entries
                        .iter()
                        .next()
                        .map_or(application.applied, |e| e.index),
                    entries
                        .iter()
                        .last()
                        .map_or(application.applied, |e| e.index),
                    results.len(),
                ));
        }
        #[cfg(not(test))]
        let _ = stats;
        // Noop's ordinary owned Vec/spare capacity is retained and charged.
        let expected = entries.commands().count();
        let capacity_bound = expected
            .max(4)
            .checked_mul(2)
            .and_then(|n| n.checked_mul(size_of::<OperationId>()))
            .ok_or("receipt capacity overflow")?;
        if results
            .capacity()
            .checked_mul(size_of::<OperationId>())
            .is_none_or(|n| n > capacity_bound)
        {
            return Err("owned receipt capacity exceeds reservation".into());
        }
        if slot == 0 {
            let started = Recorder::<TRACE>::start();
            let count = results.len();
            let result = Self::deliver_results(pending, domain, entries, results);
            attribution.finish(Phase::Results, started, result.is_err(), count);
            result?;
        } else {
            let mut commands = entries.commands();
            for operation in results {
                let entry = commands.next().ok_or("extra follower original result")?;
                if !matches!(entry.payload,EntryPayload::Command { operation:actual,.. } if actual==operation)
                {
                    return Err("follower original applied result differs".into());
                }
            }
            if commands.next().is_some() {
                return Err("missing follower original result".into());
            }
        }
        Ok(())
    }
    #[cfg(test)]
    fn deliver(&mut self, results: Vec<OperationId>) -> Result<(), Failure> {
        // Baseline owned-helper controls deliberately have no real committed
        // context. Public-port controls must exercise the real consumer above.
        for operation in results {
            self.pending
                .remove(&operation)
                .ok_or("missing/duplicate original applied result")?
                .legacy_send(operation)?;
        }
        Ok(())
    }
    fn deliver_results(
        pending: &mut PendingReplies<Reply>,
        domain: &WriteDomain<OperationId>,
        entries: Applied<'_>,
        results: Vec<OperationId>,
    ) -> Result<(), Failure> {
        // Validate the WHOLE owned receipt set against actual committed commands
        // before removing any producer or publishing any current-interval result.
        let mut commands = entries.commands();
        for &operation in &results {
            let entry = commands.next().ok_or("extra original applied result")?;
            if !matches!(entry.payload,EntryPayload::Command { operation:actual,.. } if actual==operation)
            {
                return Err("original applied result differs".into());
            }
        }
        if commands.next().is_some() {
            return Err("missing original applied result".into());
        }
        // Delivery can still fail after a valid whole application. Preserve its
        // successful prefix and leave later producers pending for normal cleanup.
        for (entry, operation) in entries.commands().zip(results) {
            let reply = pending
                .remove(&operation)
                .ok_or("missing/duplicate original applied result")?;
            reply.publish(domain, entry, operation)?;
        }
        Ok(())
    }
    fn snapshot(&mut self, effect: Effect) -> Result<Vec<Effect>, Failure> {
        match effect {
            Effect::SnapshotRequired {
                to,
                context,
                reference,
            } => supply_snapshot(
                &self.parts.core,
                &mut self.parts.snapshots,
                to,
                context,
                reference,
            )
            .map_err(debug_error),
            Effect::StageSnapshot(message) => stage_volatile_snapshot_effect(
                &mut self.parts.core,
                &self.parts.store.log,
                &mut self.parts.snapshots,
                &self.parts.application,
                message,
            )
            .map_err(debug_error),
            Effect::SnapshotInstalled(reference) => {
                self.parts.stats[self.parts.slot]
                    .snapshot_installs
                    .fetch_add(1, Ordering::Relaxed);
                finish_volatile_snapshot_install(
                    &mut self.parts.core,
                    &self.parts.store.log,
                    &mut self.parts.snapshots,
                    &mut self.parts.application,
                    reference,
                )
                .map_err(debug_error)
            }
            Effect::CheckpointCompacted(reference) => {
                self.parts.snapshots.reconcile_log(Some(reference))?;
                Ok(Vec::new())
            }
            _ => Err("not a snapshot effect".into()),
        }
    }
    fn maintain(&mut self) -> Result<(), Failure> {
        let started = Recorder::<TRACE>::start();
        let result = self.maintain_inner();
        self.attribution
            .finish(Phase::Maintenance, started, result.is_err(), 0);
        result
    }
    fn provider_snapshot(&self) -> Option<crate::attribution::ProviderSnapshot> {
        Recorder::<TRACE>::provider_snapshot(
            &self.parts.store.attribution,
            &self.parts.snapshots.attribution,
        )
    }
    fn finish_checkpoint(
        &self,
        phase: Phase,
        started: Option<Instant>,
        before: Option<crate::attribution::ProviderSnapshot>,
        failed: bool,
    ) {
        self.attribution.finish(phase, started, failed, 0);
        if matches!(phase, Phase::CheckpointCompact) {
            self.attribution.finish_compact_gaps(
                before.as_ref(),
                &self.parts.store.attribution,
                &self.parts.snapshots.attribution,
                failed,
            );
        }
        self.attribution.finish_scope(
            phase,
            before,
            &self.parts.store.attribution,
            &self.parts.snapshots.attribution,
        );
    }
    fn maintain_inner(&mut self) -> Result<(), Failure> {
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
        let started = Recorder::<TRACE>::start();
        let before = self.provider_snapshot();
        let reserved = self.reserve(size_of::<SnapshotMetadata>() + 8);
        self.finish_checkpoint(Phase::CheckpointReserve, started, before, reserved.is_err());
        reserved?;
        let started = Recorder::<TRACE>::start();
        let before = self.provider_snapshot();
        let receipt =
            checkpoint_application(&self.parts.core, &historical, &mut self.parts.snapshots);
        self.finish_checkpoint(Phase::CheckpointPublish, started, before, receipt.is_err());
        let receipt = receipt.map_err(debug_error)?;
        let started = Recorder::<TRACE>::start();
        let before = self.provider_snapshot();
        let effects = compact_volatile_replica(
            &mut self.parts.core,
            &self.parts.store.log,
            &mut self.parts.snapshots,
            &self.parts.application,
            receipt.reference(),
        );
        self.finish_checkpoint(Phase::CheckpointCompact, started, before, effects.is_err());
        let effects = effects.map_err(debug_error)?;
        let started = Recorder::<TRACE>::start();
        let before = self.provider_snapshot();
        let delivered = self.effects(effects);
        self.finish_checkpoint(
            Phase::CheckpointEffects,
            started,
            before,
            delivered.is_err(),
        );
        delivered?;
        self.last_snapshot_at = applied;
        self.parts.stats[self.parts.slot]
            .checkpoint_count
            .fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

#[cfg(test)]
#[path = "../../../../validation/performance/pillar1-cpu4/adapter/ports.rs"]
mod r3_ports;
#[cfg(test)]
#[path = "../../../../validation/performance/pillar1-cpu4/adapter/steps.rs"]
mod r3_steps;
