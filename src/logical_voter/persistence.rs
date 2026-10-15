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
//! One-group asynchronous persistence with explicit host-driven child progress.
use super::*;
use crate::{contracts::StorageError, worker::*};
use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver, SyncSender, TryRecvError, TrySendError},
        Arc,
    },
};
mod admission;
mod progress;
#[cfg(test)]
mod tests;
pub struct LogicalPersistenceWorker {
    binding: WorkerBinding,
    limits: WorkerLimits,
    mapping: LogicalStoreMapping,
    sender: Option<SyncSender<PersistBatch>>,
    events: Receiver<WorkerEvent>,
    retained: Option<RetainedPersist>,
    sequence: u64,
    fenced: bool,
    outstanding: Arc<AtomicBool>,
}
pub struct LogicalPersistencePort {
    binding: WorkerBinding,
    mapping: LogicalStoreMapping,
    representative: RepresentativeBinding,
    limits: LogLimits,
    parent: GroupLog,
    requests: Receiver<PersistBatch>,
    events: SyncSender<WorkerEvent>,
    pending: Option<PendingPersist>,
    held: VecDeque<WorkerEvent>,
    outstanding: Arc<AtomicBool>,
    fenced: bool,
}
struct PersistBatch {
    ticket: WorkerTicket,
    units: Vec<PersistUnit>,
}
struct RetainedPersist {
    ticket: WorkerTicket,
    units: Vec<PersistUnitId>,
    usage: WorkerUsage,
    written: Option<LogTicket>,
}
struct PendingPersist {
    batch: PersistBatch,
    next: GroupLog,
    operation: Option<OperationId>,
}
/// Construction allocates bounded channels only. Every mutation is pumped by
/// the host through the actual borrowed child Nodes. There is no parent WAL.
pub fn logical_persistence(
    image: &VerifiedLogicalImage,
    generation: StorageWorkerGeneration,
    limits: WorkerLimits,
) -> Result<(LogicalPersistenceWorker, LogicalPersistencePort), WorkerError> {
    let limits = limits.validate()?;
    if limits.batch_units > image.limits.parent_log.max_batch_units
        || limits.batch_bytes > image.limits.parent_log.max_batch_bytes
    {
        return Err(WorkerError::InvalidLimits);
    }
    let binding = image.worker_binding(generation);
    let (sender, requests) = mpsc::sync_channel(1);
    let (events, outputs) = mpsc::sync_channel(2);
    let outstanding = Arc::new(AtomicBool::new(false));
    let worker = LogicalPersistenceWorker {
        binding,
        limits,
        mapping: image.mapping(),
        sender: Some(sender),
        events: outputs,
        retained: None,
        sequence: 0,
        fenced: false,
        outstanding: outstanding.clone(),
    };
    let port = LogicalPersistencePort {
        binding,
        mapping: image.mapping(),
        representative: image.representative(),
        limits: image.limits.parent_log,
        parent: image.image().parent.clone(),
        requests,
        events,
        pending: None,
        held: VecDeque::with_capacity(2),
        outstanding,
        fenced: false,
    };
    Ok((worker, port))
}
impl LogicalPersistencePort {
    pub(super) fn current_parent(&self) -> Option<&GroupLog> {
        (self.pending.is_none()
            && self.held.is_empty()
            && !self.outstanding.load(Ordering::Acquire))
        .then_some(&self.parent)
    }
    pub(super) fn scope(&self) -> (LogicalStoreMapping, RepresentativeBinding) {
        (self.mapping, self.representative)
    }
    pub(super) fn fenced(&self) -> bool {
        self.fenced
    }
    pub(super) fn submitted(&self) -> bool {
        self.pending.as_ref().is_some_and(|p| p.operation.is_some())
    }
    pub(super) fn command(&self) -> Option<CommandBody> {
        let pending = self.pending.as_ref()?;
        let unit = &pending.batch.units[0];
        Some(CommandBody::Mutate {
            expected_generation: self.parent.generation,
            update: unit.update.clone(),
            origin: PersistOrigin {
                request: pending.batch.ticket,
                visit: unit.id.visit(),
            },
        })
    }
    pub(super) fn mark_submitted(&mut self, operation: OperationId) {
        self.pending
            .as_mut()
            .expect("original pending work")
            .operation = Some(operation);
    }
    pub fn binding(&self) -> WorkerBinding {
        self.binding
    }
    pub fn is_drained(&self) -> bool {
        self.pending.is_none() && self.held.is_empty() && !self.outstanding.load(Ordering::Acquire)
    }
}
