// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use crate::{
    attribution::{Attribution, Phase, Recorder},
    Maintenance,
};
use std::{collections::VecDeque, mem::size_of, time::Instant};
use voteboat::{application::StateMachine, snapshot::*};

#[cfg(test)]
mod tests;

mod pending_replies;
use pending_replies::PendingReplies;

pub(super) struct Parts<const TRACE: bool = false> {
    pub slot: usize,
    pub core: Raft,
    pub store: MemoryStore<TRACE>,
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
    last_snapshot_at: u64,
    last_contact: Instant,
    #[cfg(test)]
    tick_paused: bool,
}
impl<const TRACE: bool> Actor<TRACE> {
    pub fn new(parts: Parts<TRACE>) -> Self {
        let pending = PendingReplies::new(parts.settings.clients * 4 + 1);
        Self {
            parts,
            attribution: Recorder::default(),
            pending,
            deferred: None,
            last_snapshot_at: 0,
            last_contact: Instant::now(),
            #[cfg(test)]
            tick_paused: false,
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
        let result = self.parts.core.step(event);
        self.attribution
            .finish(Phase::Core, started, result.is_err(), 0);
        let effects = result.map_err(debug_error)?;
        self.effects(effects)
    }
    fn requests(&mut self, first: Request) -> Result<(), Failure> {
        let started = Recorder::<TRACE>::start();
        let proposals = self.collect(first);
        self.attribution.finish(
            Phase::HostAdmission,
            started,
            proposals.is_err(),
            proposals.as_ref().map_or(0, Vec::len),
        );
        let mut proposals = proposals?;
        let width = proposals.len();
        let started = Recorder::<TRACE>::start();
        let result = if width == 1 {
            let proposal = proposals.pop().unwrap();
            self.parts.core.step(Event::Propose {
                operation: proposal.operation,
                bytes: proposal.bytes,
            })
        } else {
            self.parts.core.propose_batch(proposals)
        };
        self.attribution
            .finish(Phase::Core, started, result.is_err(), width);
        let effects = result.map_err(debug_error)?;
        self.parts.stats[self.parts.slot].record_proposals(width)?;
        self.effects(effects)
    }
    fn collect(&mut self, first: Request) -> Result<Vec<Proposal>, Failure> {
        let capacity = self.parts.core.batch_limits().max_proposal_entries;
        let mut proposals = Vec::with_capacity(first.originals.len());
        self.include(first, &mut proposals)?;
        while proposals.len() < capacity && self.parts.peer_receiver.is_empty() {
            match self.parts.receiver.try_recv() {
                Ok(request) if proposals.len() + request.originals.len() <= capacity => {
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
        Ok(proposals)
    }
    fn include(&mut self, request: Request, proposals: &mut Vec<Proposal>) -> Result<(), Failure> {
        if request.originals.is_empty()
            || request.originals.len() > self.parts.core.batch_limits().max_proposal_entries
        {
            return Err("original proposal/reply shape differs".into());
        }
        if self.pending.len() + request.originals.len() > self.parts.settings.clients * 4 + 1 {
            return Err("original result budget exceeded".into());
        }
        for Original {
            operation,
            bytes,
            reply,
        } in request.originals
        {
            if self.pending.insert(operation, reply).is_some() {
                return Err("duplicate original admission".into());
            }
            proposals.push(Proposal { operation, bytes });
        }
        Ok(())
    }
    fn effects(&mut self, initial: Vec<Effect>) -> Result<(), Failure> {
        let mut effects = VecDeque::from(initial);
        while let Some(effect) = effects.pop_front() {
            match effect {
                Effect::Persist(update) => self.persist(update, &mut effects)?,
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
        update: LogUpdate,
        effects: &mut VecDeque<Effect>,
    ) -> Result<(), Failure> {
        self.parts.stats[self.parts.slot]
            .persistence_units
            .fetch_add(1, Ordering::Relaxed);
        let started = Recorder::<TRACE>::start();
        if effects.is_empty() {
            let result =
                persist_effect_borrowed(&mut self.parts.core, &mut self.parts.store, update);
            self.attribution
                .finish(Phase::PersistHelper, started, result.is_err(), 0);
            let (committed, following) = result.map_err(debug_error)?.into_parts();
            if let Some(entries) = committed {
                Self::apply_entries(
                    &mut self.parts.application,
                    &mut self.pending,
                    &self.attribution,
                    self.parts.slot,
                    entries,
                )?;
            }
            effects.extend(following);
        } else {
            // Existing FIFO work precedes every newly completed effect.
            let result = persist_effect(&mut self.parts.core, &mut self.parts.store, update);
            self.attribution
                .finish(Phase::PersistHelper, started, result.is_err(), 0);
            effects.extend(result.map_err(debug_error)?);
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
            &self.attribution,
            self.parts.slot,
            &entries,
        )
    }
    fn apply_entries(
        application: &mut Noop,
        pending: &mut PendingReplies<Reply>,
        attribution: &Recorder<TRACE>,
        slot: usize,
        entries: &[LogEntry],
    ) -> Result<(), Failure> {
        let started = Recorder::<TRACE>::start();
        let results = application.apply_batch(entries);
        attribution.finish(
            Phase::Application,
            started,
            results.is_err(),
            results.as_ref().map_or(0, Vec::len),
        );
        let results = results.map_err(debug_error)?;
        if slot == 0 {
            let started = Recorder::<TRACE>::start();
            let count = results.len();
            let result = Self::deliver_results(pending, results);
            attribution.finish(Phase::Results, started, result.is_err(), count);
            result?;
        }
        Ok(())
    }
    #[cfg(test)]
    fn deliver(&mut self, results: Vec<OperationId>) -> Result<(), Failure> {
        Self::deliver_results(&mut self.pending, results)
    }
    fn deliver_results(
        pending: &mut PendingReplies<Reply>,
        results: Vec<OperationId>,
    ) -> Result<(), Failure> {
        for operation in results {
            let reply = pending
                .remove(&operation)
                .ok_or("missing/duplicate original applied result")?;
            reply
                .send(operation)
                .map_err(|_| "original client dropped result wait")?;
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
            Effect::StageSnapshot(message) => stage_snapshot_effect(
                &mut self.parts.core,
                &mut self.parts.store,
                &mut self.parts.snapshots,
                &self.parts.application,
                message,
            )
            .map_err(debug_error),
            Effect::SnapshotInstalled(reference) => {
                self.parts.stats[self.parts.slot]
                    .snapshot_installs
                    .fetch_add(1, Ordering::Relaxed);
                finish_snapshot_install(
                    &mut self.parts.core,
                    &self.parts.store,
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
        let effects = compact_replica(
            &mut self.parts.core,
            &mut self.parts.store,
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
