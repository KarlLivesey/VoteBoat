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
//! One-use snapshot admissions tied to live effect leases and one runtime owner.
use super::*;
use crate::{
    application::CheckpointStateMachine, contracts::StorageError, snapshot::CheckpointError,
    snapshot_worker::*,
};

#[derive(Clone, Copy, Debug)]
pub struct SnapshotRouterLimits {
    pub requests: usize,
    pub max_image_bytes: usize,
}
impl Default for SnapshotRouterLimits {
    fn default() -> Self {
        Self {
            requests: 16,
            max_image_bytes: 128 * 1024 * 1024,
        }
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SnapshotRouteError {
    InvalidLimits,
    WrongOwner,
    WrongWorker,
    Overloaded,
    TooLarge,
    NotSnapshot,
    StaleCompletion,
    ProviderContract,
    Owner(EffectOwnerError),
    Worker(SnapshotWorkError),
    Checkpoint(CheckpointError),
}
#[derive(Debug)]
pub struct SnapshotRouteRejected {
    pub reason: SnapshotRouteError,
    /// Original lease is returned even if a broken provider accepted work under
    /// an invalid ticket. In that case the owner is fenced; discard this lease
    /// explicitly and drain/recover unknown accepted worker work.
    pub lease: Box<EffectLease>,
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SnapshotRouterUsage {
    pub requests: usize,
    pub image_bytes: usize,
}
struct Pending {
    lease: EffectLease,
    request: SnapshotWorkTicket,
    allowance: usize,
}
/// Fixed bounded coordination, with a construction-selected public worker. Owns
/// original effect leases through accepted snapshot work. It creates no worker,
/// I/O resource or application, and never treats publication as WAL durability.
/// Drain the owner first, then close providers; installation can require another
/// snapshot request after a WAL completion during shutdown.
pub struct SnapshotRouter {
    owner: RuntimeOwner,
    worker: SnapshotWorkerBinding,
    limits: SnapshotRouterLimits,
    pending: BTreeMap<u64, Pending>,
    last_admission: u64,
}
impl SnapshotRouter {
    pub fn new(
        owner: RuntimeOwner,
        worker: SnapshotWorkerBinding,
        limits: SnapshotRouterLimits,
    ) -> Result<Self, SnapshotRouteError> {
        if owner.store != worker.store {
            return Err(SnapshotRouteError::WrongWorker);
        }
        if limits.requests == 0
            || limits.requests > 65536
            || limits.max_image_bytes == 0
            || limits.max_image_bytes as u128 > 4 * 1024 * 1024 * 1024u128
        {
            return Err(SnapshotRouteError::InvalidLimits);
        }
        Ok(Self {
            owner,
            worker,
            limits,
            pending: BTreeMap::new(),
            last_admission: 0,
        })
    }
    pub fn usage(&self) -> SnapshotRouterUsage {
        SnapshotRouterUsage {
            requests: self.pending.len(),
            image_bytes: self.pending.values().map(|p| p.allowance).sum(),
        }
    }
    pub fn owner(&self) -> RuntimeOwner {
        self.owner
    }
    pub fn worker_binding(&self) -> SnapshotWorkerBinding {
        self.worker
    }
    pub fn is_drained(&self) -> bool {
        self.pending.is_empty()
    }
    fn prepare_work<
        Q: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
        W: SnapshotWorker + ?Sized,
        A: CheckpointStateMachine,
    >(
        &self,
        owner: &mut EffectOwner<Q, T, E>,
        worker: &W,
        lease: &EffectLease,
        application: &A,
    ) -> Result<(usize, SnapshotWork), SnapshotRouteError> {
        if owner.identity() != self.owner {
            return Err(SnapshotRouteError::WrongOwner);
        }
        if worker.binding() != self.worker {
            return Err(SnapshotRouteError::WrongWorker);
        }
        owner
            .validate_lease(lease)
            .map_err(SnapshotRouteError::Owner)?;
        application
            .validate_group(lease.ticket.visit.group)
            .map_err(|e| SnapshotRouteError::Checkpoint(CheckpointError::Application(e)))?;
        if !matches!(
            lease.effect,
            Effect::VerifyLearnerReadiness(_)
                | Effect::StageSnapshot(_)
                | Effect::SnapshotRequired { .. }
                | Effect::SnapshotInstalled(_)
                | Effect::CheckpointRequired { .. }
                | Effect::CheckpointCompacted(_)
        ) {
            return Err(SnapshotRouteError::NotSnapshot);
        }
        if self.pending.len() >= self.limits.requests {
            return Err(SnapshotRouteError::Overloaded);
        }
        if self
            .pending
            .values()
            .any(|p| p.lease.ticket.visit.group == lease.ticket.visit.group)
        {
            return Err(SnapshotRouteError::Owner(EffectOwnerError::StaleEffect));
        }
        let allowance = worker
            .load_reservation(lease.ticket.visit.group)
            .ok_or(SnapshotRouteError::WrongWorker)?;
        if allowance == 0 || allowance > self.limits.max_image_bytes {
            return Err(SnapshotRouteError::TooLarge);
        }
        if let Effect::StageSnapshot(message) = &lease.effect {
            if let Rpc::Snapshot { snapshot } = &message.rpc {
                if snapshot_image_bytes(snapshot).is_none_or(|n| n > allowance) {
                    return Err(SnapshotRouteError::TooLarge);
                }
            }
        }
        // Reserve before cloning a Publish image or polling a Load result.
        // The minimum avoids repeatedly charging a rejected lease on retry.
        // Readiness can retain the loaded anchor and a live application
        // checkpoint simultaneously during deterministic owner validation.
        let extra = allowance
            .checked_mul(
                if matches!(lease.effect, Effect::VerifyLearnerReadiness(_)) {
                    2
                } else {
                    1
                },
            )
            .ok_or(SnapshotRouteError::TooLarge)?
            .checked_add(4096)
            .ok_or(SnapshotRouteError::TooLarge)?;
        owner
            .reserve_snapshot(lease, extra)
            .map_err(SnapshotRouteError::Owner)?;
        let core = owner.core(lease.ticket.visit.group).unwrap();
        let work = if let Effect::CheckpointRequired { context } = &lease.effect {
            let max_bytes = worker
                .checkpoint_bytes(lease.ticket.visit.group)
                .filter(|n| *n > 0 && *n <= allowance)
                .ok_or(SnapshotRouteError::TooLarge)?;
            let work = prepare_local_checkpoint_work(
                core,
                application,
                lease.ticket.visit,
                *context,
                self.worker,
                max_bytes,
            )
            .map_err(SnapshotRouteError::Checkpoint)?;
            if let SnapshotJob::Publish { snapshot, .. } = &work.job {
                if snapshot_image_bytes(snapshot).is_none_or(|n| n > allowance) {
                    return Err(SnapshotRouteError::TooLarge);
                }
            }
            work
        } else {
            prepare_snapshot_work(
                core,
                application,
                lease.ticket.visit,
                &lease.effect,
                self.worker,
            )
            .map_err(SnapshotRouteError::Checkpoint)?
        };
        Ok((allowance, work))
    }
    pub fn submit<
        Q: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
        W: SnapshotWorker + ?Sized,
        A: CheckpointStateMachine,
    >(
        &mut self,
        owner: &mut EffectOwner<Q, T, E>,
        worker: &mut W,
        lease: EffectLease,
        application: &A,
    ) -> Result<SnapshotWorkTicket, SnapshotRouteRejected> {
        let checked = self.prepare_work(owner, worker, &lease, application);
        let (allowance, work) = match checked {
            Ok(v) => v,
            Err(reason) => {
                return Err(SnapshotRouteRejected {
                    reason,
                    lease: Box::new(lease),
                })
            }
        };
        let request = match worker.submit(work) {
            Ok(t) => t,
            Err(rejected) => {
                return Err(SnapshotRouteRejected {
                    reason: SnapshotRouteError::Worker(rejected.reason),
                    lease: Box::new(lease),
                })
            }
        };
        if request.binding != self.worker || request.sequence <= self.last_admission {
            let _ = owner.fail::<()>(EffectOwnerError::ProviderContract);
            return Err(SnapshotRouteRejected {
                reason: SnapshotRouteError::ProviderContract,
                lease: Box::new(lease),
            });
        }
        self.last_admission = request.sequence;
        self.pending.insert(
            request.sequence,
            Pending {
                lease,
                request,
                allowance,
            },
        );
        Ok(request)
    }
    pub fn deliver<
        Q: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
        A: CheckpointStateMachine,
    >(
        &mut self,
        owner: &mut EffectOwner<Q, T, E>,
        application: &mut A,
        event: SnapshotWorkEvent,
        now: MonoTime,
    ) -> Result<(), SnapshotRouteError> {
        if owner.identity() != self.owner {
            return Err(SnapshotRouteError::WrongOwner);
        }
        if event.request.binding != self.worker {
            return Err(SnapshotRouteError::StaleCompletion);
        }
        let Some(p) = self.pending.get(&event.request.sequence) else {
            return Err(SnapshotRouteError::StaleCompletion);
        };
        if event.request != p.request || event.visit != p.lease.ticket.visit {
            return Err(SnapshotRouteError::StaleCompletion);
        }
        owner
            .validate_lease(&p.lease)
            .map_err(SnapshotRouteError::Owner)?;
        let p = self.pending.remove(&event.request.sequence).unwrap();
        if let Ok(SnapshotOutput::Readiness { limits, .. }) = &event.result {
            if crate::snapshot_worker::snapshot_load_reservation(*limits)
                .is_none_or(|n| n > p.allowance)
            {
                let _ = owner.fail::<()>(EffectOwnerError::ProviderContract);
                owner
                    .discard_failed(p.lease)
                    .map_err(|e| SnapshotRouteError::Owner(e.reason))?;
                return Err(SnapshotRouteError::ProviderContract);
            }
        }
        if let Ok(
            SnapshotOutput::Loaded { snapshot, .. }
            | SnapshotOutput::Readiness {
                snapshot: Some(snapshot),
                ..
            },
        ) = &event.result
        {
            if snapshot_image_bytes(snapshot).is_none_or(|n| n > p.allowance) {
                let _ = owner.fail::<()>(EffectOwnerError::ProviderContract);
                owner
                    .discard_failed(p.lease)
                    .map_err(|e| SnapshotRouteError::Owner(e.reason))?;
                return Err(SnapshotRouteError::ProviderContract);
            }
        }
        let visit = p.lease.ticket.visit;
        let mut checkpoint_error = None;
        let result = owner.complete_effect_with(
            p.lease,
            application.applied_index(),
            now,
            |core, effect| match complete_snapshot_work(
                core,
                application,
                effect,
                p.request,
                visit,
                event,
            ) {
                Ok(effects) => Ok((effects, ())),
                Err(error) => {
                    checkpoint_error = Some(error);
                    Err(RaftError::Storage(StorageError::Corrupt(
                        "snapshot worker completion failed",
                    )))
                }
            },
        );
        if let Err(rejected) = result {
            let reason = checkpoint_error.map_or(
                SnapshotRouteError::Owner(rejected.reason),
                SnapshotRouteError::Checkpoint,
            );
            // This lease is router-owned, so dropping its payload is explicit
            // and its failed-owner reservation can be released now. Other
            // accepted leases remain charged until discard_failed is called.
            if owner.is_failed() {
                owner
                    .discard_failed(*rejected.lease)
                    .map_err(|e| SnapshotRouteError::Owner(e.reason))?;
            } else {
                self.pending.insert(
                    p.request.sequence,
                    Pending {
                        lease: *rejected.lease,
                        request: p.request,
                        allowance: p.allowance,
                    },
                );
            }
            return Err(reason);
        }
        Ok(())
    }
    /// Drop router-owned payloads only after the matching owner is fenced.
    /// Accepted provider work still needs drain/recovery; this is not rollback.
    pub fn discard_failed<Q: ReadyScheduler, T: TimerService, E: ElectionEntropy>(
        &mut self,
        owner: &mut EffectOwner<Q, T, E>,
    ) -> Result<(), SnapshotRouteError> {
        if owner.identity() != self.owner {
            return Err(SnapshotRouteError::WrongOwner);
        }
        if !owner.is_failed() {
            return Err(SnapshotRouteError::ProviderContract);
        }
        for (_, p) in std::mem::take(&mut self.pending) {
            owner
                .discard_failed(p.lease)
                .map_err(|e| SnapshotRouteError::Owner(e.reason))?;
        }
        Ok(())
    }
}
