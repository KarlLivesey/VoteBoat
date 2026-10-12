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
//! Existing snapshot-worker contract with explicit consumed-child progress.
use super::*;
use crate::{
    contracts::StorageError, runtime::VisitTicket, snapshot_worker::*, worker::WorkerBinding,
};
use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver, SyncSender, TryRecvError, TrySendError},
        Arc,
    },
};
mod admission;
mod port;
mod progress;
#[cfg(test)]
mod tests;
mod worker;

pub struct LogicalSnapshotWorker {
    binding: SnapshotWorkerBinding,
    mapping: LogicalStoreMapping,
    limits: SnapshotWorkLimits,
    images: SnapshotLimits,
    allowance: usize,
    sender: Option<SyncSender<AcceptedSnapshot>>,
    events: Receiver<SnapshotWorkEvent>,
    retained: Option<RetainedSnapshot>,
    sequence: u64,
    fenced: bool,
    outstanding: Arc<AtomicBool>,
}
pub struct LogicalSnapshotPort {
    binding: SnapshotWorkerBinding,
    persistence: WorkerBinding,
    mapping: LogicalStoreMapping,
    representative: RepresentativeBinding,
    images: SnapshotLimits,
    allowance: usize,
    snapshots: Vec<(SnapshotRef, Snapshot)>,
    requests: Receiver<AcceptedSnapshot>,
    events: SyncSender<SnapshotWorkEvent>,
    pending: Option<PendingSnapshot>,
    held: VecDeque<SnapshotWorkEvent>,
    outstanding: Arc<AtomicBool>,
    fenced: bool,
}
struct AcceptedSnapshot {
    request: SnapshotWorkTicket,
    work: SnapshotWork,
}
struct PendingSnapshot {
    accepted: AcceptedSnapshot,
    revision: LogRevision,
    operation: Option<OperationId>,
}
struct RetainedSnapshot {
    request: SnapshotWorkTicket,
    visit: VisitTicket,
    bytes: usize,
    expected: ExpectedSnapshot,
}
enum ExpectedSnapshot {
    Readiness(Option<SnapshotRef>),
    Published {
        index: u64,
        term: u64,
        schema: u64,
        configuration: ConfigurationId,
    },
    Loaded {
        reference: SnapshotRef,
        install: bool,
    },
    Reconciled(SnapshotRef),
}
/// Explicit matching storage handles; construction performs no I/O or spawning.
/// Cache capacity is two complete selected snapshot images, separate from the
/// original worker request/completion reservations.
pub fn logical_snapshots(
    image: &VerifiedLogicalImage,
    persistence: &LogicalPersistencePort,
    generation: SnapshotWorkerGeneration,
    limits: SnapshotWorkLimits,
) -> Result<(LogicalSnapshotWorker, LogicalSnapshotPort), SnapshotWorkError> {
    let limits = limits.validate()?;
    if persistence.scope() != (image.mapping(), image.representative())
        || persistence.binding().store != image.binding()
    {
        return Err(SnapshotWorkError::WrongBinding);
    }
    let images = image.limits.parent_snapshots;
    let allowance = snapshot_load_reservation(images).ok_or(SnapshotWorkError::InvalidLimits)?;
    if allowance
        .checked_mul(2)
        .and_then(|n| n.checked_add(4096))
        .is_none_or(|n| n > limits.max_bytes)
        || images
            .max_file_bytes()
            .checked_add(1024)
            .is_none_or(|n| n > image.limits.max_command_bytes)
        || image.snapshots.len() > 2
        || image
            .snapshots
            .iter()
            .any(|(r, s)| !r.matches(s) || snapshot_image_bytes(s).is_none_or(|n| n > allowance))
    {
        return Err(SnapshotWorkError::InvalidLimits);
    }
    let binding = image.snapshot_worker_binding(generation);
    let (sender, requests) = mpsc::sync_channel(1);
    let (events, outputs) = mpsc::sync_channel(1);
    let outstanding = Arc::new(AtomicBool::new(false));
    Ok((
        LogicalSnapshotWorker {
            binding,
            mapping: image.mapping(),
            limits,
            images,
            allowance,
            sender: Some(sender),
            events: outputs,
            retained: None,
            sequence: 0,
            fenced: false,
            outstanding: outstanding.clone(),
        },
        LogicalSnapshotPort {
            binding,
            persistence: persistence.binding(),
            mapping: image.mapping(),
            representative: image.representative(),
            images,
            allowance,
            snapshots: image.snapshots.clone(),
            requests,
            events,
            pending: None,
            held: VecDeque::with_capacity(1),
            outstanding,
            fenced: false,
        },
    ))
}
