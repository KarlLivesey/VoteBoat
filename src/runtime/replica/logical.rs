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
//! Named logical polling retains every original result behind capsule authority.
use super::*;
impl<R: ApplicationReceipt> ReplicaDriver<R> {
    pub(crate) fn poll_logical<
        S: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
        A: ProposalAdmission<Receipt = R> + LogicalResponseCodec + CheckpointStateMachine,
        W: PersistenceWorker,
        O: LogicalOutboundQueue,
    >(
        &mut self,
        parts: &mut ReplicaParts<'_, S, T, E, A, W, O>,
        release: &mut LogicalReleaseCoordinator<R, A::ReadResult>,
        now: MonoTime,
        budget: ReplicaPollBudget,
        configuration: impl FnMut(&Raft, &Event) -> Result<(), RaftError>,
    ) -> Result<ReplicaProgress, ReplicaError> {
        if !parts.owner.authorization().is_logical()
            || release.authorization() != parts.owner.authorization()
            || Self::bindings(parts)? != self.binding
        {
            return Err(ReplicaError::WrongBinding);
        }
        if !budget.valid() {
            return Err(ReplicaError::InvalidLimits);
        }
        if now < self.now {
            return Err(ReplicaError::ClockRegressed);
        }
        if self.failed.is_some() {
            return Err(ReplicaError::Fenced);
        }
        self.now = now;
        let result = self.poll_logical_inner(parts, release, now, budget, configuration);
        if let Err(reason) = &result {
            let _ = parts.owner.fail::<()>(EffectOwnerError::ProviderContract);
            self.failed = Some(reason.clone());
        }
        result
    }
    fn poll_logical_inner<
        S: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
        A: ProposalAdmission<Receipt = R> + LogicalResponseCodec + CheckpointStateMachine,
        W: PersistenceWorker,
        O: LogicalOutboundQueue,
    >(
        &mut self,
        p: &mut ReplicaParts<'_, S, T, E, A, W, O>,
        release: &mut LogicalReleaseCoordinator<R, A::ReadResult>,
        now: MonoTime,
        b: ReplicaPollBudget,
        configuration: impl FnMut(&Raft, &Event) -> Result<(), RaftError>,
    ) -> Result<ReplicaProgress, ReplicaError> {
        let mut out = ReplicaProgress::default();
        release
            .publish(p, now, b.retries)
            .map_err(ReplicaError::Logical)?;
        self.poll_reclaims(p.persistence)?;
        self.poll_snapshots(p, now, b.snapshot_events, &mut out)?;
        let events = p.persistence.poll(b.worker_events);
        if events.len() > b.worker_events {
            return Err(ReplicaError::ProviderContract);
        }
        for event in events {
            out.worker_events += 1;
            p.owner
                .deliver_worker(event, now)
                .map_err(ReplicaError::Owner)?;
        }
        out.steps = p
            .clients
            .advance_authorized(
                p.owner,
                now,
                b.steps,
                |g| p.applications.get(&g),
                configuration,
            )
            .map_err(ReplicaError::Client)?;
        p.reads
            .observe_steps(p.owner, &out.steps)
            .map_err(ReplicaError::Read)?;
        self.collect_effects(p.owner, b.effects, &mut out)?;
        self.retry_logical(p, release, now, b.retries, &mut out)?;
        self.submit_batch(p.owner, p.persistence, now, true, &mut out)?;
        self.submit_batch(p.owner, p.persistence, now, false, &mut out)?;
        p.clients
            .reconcile(p.owner, b.reconcile)
            .map_err(ReplicaError::Client)?;
        p.reads
            .reconcile(p.owner, b.reconcile)
            .map_err(ReplicaError::Read)?;
        Ok(out)
    }
    fn retry_logical<
        S: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
        A: ProposalAdmission<Receipt = R> + LogicalResponseCodec + CheckpointStateMachine,
        W: PersistenceWorker,
        O: LogicalOutboundQueue,
    >(
        &mut self,
        p: &mut ReplicaParts<'_, S, T, E, A, W, O>,
        release: &mut LogicalReleaseCoordinator<R, A::ReadResult>,
        now: MonoTime,
        limit: usize,
        out: &mut ReplicaProgress,
    ) -> Result<(), ReplicaError> {
        for _ in 0..self.retries.len().min(limit) {
            let lease = self.retries.pop_front().unwrap();
            out.retries += 1;
            if !matches!(
                lease.effect,
                Effect::Send(_) | Effect::Committed(_) | Effect::ReadReady(_)
            ) {
                self.retry_snapshot(p, lease)?;
                continue;
            }
            match release.capture(p, lease, now) {
                Ok(()) => (),
                Err(LogicalCaptureError::Rejected { reason, lease }) => {
                    self.retries.push_back(*lease);
                    if reason != LogicalReleaseError::Overloaded {
                        return Err(ReplicaError::Logical(reason));
                    }
                }
                Err(LogicalCaptureError::Failed { reason }) => {
                    return Err(ReplicaError::Logical(reason))
                }
            }
        }
        Ok(())
    }
}
