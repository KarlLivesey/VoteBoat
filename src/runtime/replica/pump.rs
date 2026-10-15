// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Bounded selected append work within one existing driver poll.
use super::*;

#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) enum PersistSubmission {
    Stopped,
    Submitted,
    ParkedAppend,
}
impl PersistSubmission {
    pub(super) fn accepted(self) -> bool {
        self != Self::Stopped
    }
}

struct PumpBudget {
    limits: ReplicaPollBudget,
    ready: ReadyAdvanceBudget,
    data_attempts: usize,
    control_attempted: bool,
}
impl PumpBudget {
    fn new(limits: ReplicaPollBudget) -> Self {
        Self {
            limits,
            ready: ReadyAdvanceBudget::new(limits.steps),
            data_attempts: limits.effects,
            control_attempted: false,
        }
    }
    fn available(&self, out: &ReplicaProgress) -> bool {
        self.ready.available()
            && self.data_attempts != 0
            && out.effects < self.limits.effects
            && out.retries < self.limits.retries
            && out.worker_events < self.limits.worker_events
            && out.snapshot_events < self.limits.snapshot_events
    }
}

impl<R: ApplicationReceipt> ReplicaDriver<R> {
    pub(super) fn poll_worker_events<
        S: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
        W: PersistenceWorker,
    >(
        owner: &mut EffectOwner<S, T, E>,
        worker: &mut W,
        now: MonoTime,
        limit: usize,
        out: &mut ReplicaProgress,
    ) -> Result<bool, ReplicaError> {
        let events = worker.poll(limit);
        if events.len() > limit {
            return Err(ReplicaError::ProviderContract);
        }
        let mut terminal = false;
        for event in events {
            terminal |= event.terminal();
            out.worker_events += 1;
            owner
                .deliver_worker(event, now)
                .map_err(ReplicaError::Owner)?;
        }
        Ok(terminal)
    }

    pub(super) fn pump_ready<
        S: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
        A: ProposalAdmission<Receipt = R> + BoundedReadableStateMachine + CheckpointStateMachine,
        W: PersistenceWorker,
        O: OutboundQueue,
    >(
        &mut self,
        p: &mut ReplicaParts<'_, S, T, E, A, W, O>,
        now: MonoTime,
        limits: ReplicaPollBudget,
        mut configuration: impl FnMut(&Raft, &Event) -> Result<(), RaftError>,
        out: &mut ReplicaProgress,
    ) -> Result<(), ReplicaError> {
        let mut budget = PumpBudget::new(limits);
        loop {
            let steps = p
                .clients
                .advance_budgeted(
                    p.owner,
                    now,
                    &mut budget.ready,
                    |g| p.applications.get(&g),
                    &mut configuration,
                )
                .map_err(ReplicaError::Client)?;
            p.reads
                .observe_steps(p.owner, &steps)
                .map_err(ReplicaError::Read)?;
            if out.steps.is_empty() {
                out.steps = steps;
            } else {
                out.steps.extend(steps);
            }
            self.collect_effects(p.owner, limits.effects - out.effects, out)?;
            self.retry_effects(p, now, limits.retries - out.retries, out)?;
            if !budget.control_attempted {
                budget.control_attempted = true;
                self.submit_batch(p.owner, p.persistence, now, true, out)?;
            } else if !self.control.is_empty() {
                break;
            }
            let mut parked = false;
            while budget.data_attempts != 0 && !self.data.is_empty() {
                budget.data_attempts -= 1;
                let submitted = self.submit_batch(p.owner, p.persistence, now, false, out)?;
                if !submitted.accepted() {
                    break;
                }
                parked |= submitted == PersistSubmission::ParkedAppend;
            }
            if !parked
                || !self.control.is_empty()
                || !self.data.is_empty()
                || !budget.available(out)
                || Self::poll_worker_events(
                    p.owner,
                    p.persistence,
                    now,
                    limits.worker_events - out.worker_events,
                    out,
                )?
                || !budget.available(out)
            {
                break;
            }
        }
        Ok(())
    }
}
