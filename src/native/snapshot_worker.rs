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
//! One explicit blocking snapshot thread shared by selected group handles.
//! It never owns a log store or mutates an application or consensus core.
use crate::{
    contracts::StorageError, identity::*, snapshot::*, snapshot_worker::*, worker::WorkerWake,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        mpsc::{self, Receiver, SyncSender, TryRecvError, TrySendError},
        Arc,
    },
    thread::{self, JoinHandle},
};

struct Accepted {
    request: SnapshotWorkTicket,
    work: SnapshotWork,
}
struct Retained {
    visit: crate::runtime::VisitTicket,
    bytes: usize,
    control: bool,
}
pub struct NativeSnapshotWorker<S: SnapshotRetention + Send + 'static> {
    binding: SnapshotWorkerBinding,
    limits: SnapshotWorkLimits,
    stores: BTreeMap<GroupIdentity, SnapshotLimits>,
    sender: Option<SyncSender<Accepted>>,
    events: Receiver<SnapshotWorkEvent>,
    thread: Option<JoinHandle<BTreeMap<GroupIdentity, S>>>,
    retained: BTreeMap<u64, Retained>,
    groups: BTreeSet<GroupIdentity>,
    usage: SnapshotWorkUsage,
    bulk: SnapshotWorkUsage,
    sequence: u64,
    fenced: bool,
}
impl<S: SnapshotRetention + Send + 'static> NativeSnapshotWorker<S> {
    /// Snapshot handles have independent recovered sessions. Their identities
    /// must match the supplied authoritative WAL binding and group keys. The
    /// generation must be fresh within that WAL session. Construction creates
    /// no thread until all identities, limits and output allowances validate.
    pub fn spawn(
        stores: BTreeMap<GroupIdentity, S>,
        binding: SnapshotWorkerBinding,
        limits: SnapshotWorkLimits,
        wake: Arc<dyn WorkerWake>,
    ) -> Result<Self, SnapshotWorkError> {
        let limits = limits.validate()?;
        if stores.is_empty() || stores.len() > limits.max_groups {
            return Err(SnapshotWorkError::InvalidLimits);
        }
        let mut selected = BTreeMap::new();
        for (group, store) in &stores {
            if store.identity()
                != (SnapshotIdentity {
                    store: binding.store.identity,
                    group: *group,
                })
                || store.binding().identity != binding.store.identity
            {
                return Err(SnapshotWorkError::WrongBinding);
            }
            let l = store
                .limits()
                .validate()
                .map_err(|_| SnapshotWorkError::InvalidLimits)?;
            if snapshot_load_reservation(l).is_none_or(|n| n > limits.max_bytes / 2) {
                return Err(SnapshotWorkError::InvalidLimits);
            }
            selected.insert(*group, l);
        }
        let (sender, input) = mpsc::sync_channel::<Accepted>(limits.max_requests);
        let (output, events) = mpsc::sync_channel(limits.max_requests);
        let thread = thread::Builder::new()
            .name("voteboat-snapshot".into())
            .spawn(move || {
                let mut stores = stores;
                let mut fenced = false;
                while let Ok(accepted) = input.recv() {
                    let visit = accepted.work.visit;
                    let result = if fenced {
                        Err(StorageError::Fenced)
                    } else {
                        perform(stores.get_mut(&visit.group).unwrap(), accepted.work.job)
                    };
                    // Every accepted error conservatively stops further snapshot
                    // writes. Already accepted requests receive explicit failure.
                    fenced |= result.is_err();
                    let _ = output.send(SnapshotWorkEvent {
                        request: accepted.request,
                        visit,
                        result,
                    });
                    wake.wake();
                }
                stores
            })
            .map_err(|_| SnapshotWorkError::Exhausted)?;
        Ok(Self {
            binding,
            limits,
            stores: selected,
            sender: Some(sender),
            events,
            thread: Some(thread),
            retained: BTreeMap::new(),
            groups: BTreeSet::new(),
            usage: SnapshotWorkUsage::default(),
            bulk: SnapshotWorkUsage::default(),
            sequence: 0,
            fenced: false,
        })
    }
    fn admission(&self, work: &SnapshotWork) -> Result<(usize, bool), SnapshotWorkError> {
        if self.fenced {
            return Err(SnapshotWorkError::Fenced);
        }
        if self.sender.is_none() {
            return Err(SnapshotWorkError::Closed);
        }
        if work.visit.owner.store != self.binding.store {
            return Err(SnapshotWorkError::WrongBinding);
        }
        let l = *self
            .stores
            .get(&work.visit.group)
            .ok_or(SnapshotWorkError::WrongBinding)?;
        let check_ref =
            |r: SnapshotRef| r.store == self.binding.store.identity && r.group == work.visit.group;
        let (input, control) = match &work.job {
            SnapshotJob::Publish { snapshot, durable } => {
                if snapshot.metadata.bootstrap.group != work.visit.group
                    || durable.is_some_and(|r| !check_ref(r))
                {
                    return Err(SnapshotWorkError::WrongBinding);
                }
                snapshot
                    .metadata
                    .validate()
                    .map_err(|_| SnapshotWorkError::TooLarge)?;
                if snapshot.application.len() > l.max_application_bytes {
                    return Err(SnapshotWorkError::TooLarge);
                }
                (
                    snapshot_image_bytes(snapshot).ok_or(SnapshotWorkError::TooLarge)?,
                    true,
                )
            }
            SnapshotJob::Load { reference, install } => {
                if !check_ref(*reference) {
                    return Err(SnapshotWorkError::WrongBinding);
                }
                (0, *install)
            }
        };
        let bytes = snapshot_load_reservation(l)
            .and_then(|n| n.checked_add(input))
            .and_then(|n| n.checked_add(4096))
            .ok_or(SnapshotWorkError::TooLarge)?;
        if bytes > self.limits.max_bytes {
            return Err(SnapshotWorkError::TooLarge);
        }
        if self.groups.contains(&work.visit.group)
            || self.usage.requests >= self.limits.max_requests
            || bytes > self.limits.max_bytes - self.usage.bytes
            || (!control
                && (self.bulk.requests >= self.limits.max_requests - self.limits.control_requests
                    || bytes
                        > (self.limits.max_bytes - self.limits.control_bytes)
                            .saturating_sub(self.bulk.bytes)))
        {
            return Err(SnapshotWorkError::Overloaded);
        }
        Ok((bytes, control))
    }
    fn release(&mut self, sequence: u64) {
        if let Some(r) = self.retained.remove(&sequence) {
            self.groups.remove(&r.visit.group);
            self.usage.requests -= 1;
            self.usage.bytes -= r.bytes;
            if !r.control {
                self.bulk.requests -= 1;
                self.bulk.bytes -= r.bytes;
            }
        }
    }
    /// Close and consume all terminal events first. A failed store still needs
    /// recovery. Dropping the handle abandons observation, not accepted writes.
    pub fn try_reclaim(&mut self) -> Result<Option<BTreeMap<GroupIdentity, S>>, StorageError> {
        if self.sender.is_some() {
            return Err(StorageError::Rejected(
                "close snapshot worker before reclaim",
            ));
        }
        if !self.retained.is_empty() {
            return Ok(None);
        }
        let thread = self
            .thread
            .as_ref()
            .ok_or(StorageError::Rejected("snapshot worker already reclaimed"))?;
        if !thread.is_finished() {
            return Ok(None);
        }
        self.thread.take().unwrap().join().map(Some).map_err(|_| {
            StorageError::Uncertain("snapshot worker panicked; recover accepted work".into())
        })
    }
}
impl<S: SnapshotRetention + Send + 'static> SnapshotWorker for NativeSnapshotWorker<S> {
    fn binding(&self) -> SnapshotWorkerBinding {
        self.binding
    }
    fn limits(&self) -> SnapshotWorkLimits {
        self.limits
    }
    fn usage(&self) -> SnapshotWorkUsage {
        self.usage
    }
    fn submit(&mut self, work: SnapshotWork) -> Result<SnapshotWorkTicket, SnapshotWorkRejected> {
        let (bytes, control) = match self.admission(&work) {
            Ok(v) => v,
            Err(reason) => {
                return Err(SnapshotWorkRejected {
                    reason,
                    work: Box::new(work),
                })
            }
        };
        let Some(sequence) = self.sequence.checked_add(1) else {
            return Err(SnapshotWorkRejected {
                reason: SnapshotWorkError::Exhausted,
                work: Box::new(work),
            });
        };
        let request = SnapshotWorkTicket {
            binding: self.binding,
            sequence,
        };
        let visit = work.visit;
        match self
            .sender
            .as_ref()
            .unwrap()
            .try_send(Accepted { request, work })
        {
            Ok(()) => {
                self.sequence = sequence;
                self.groups.insert(visit.group);
                self.retained.insert(
                    sequence,
                    Retained {
                        visit,
                        bytes,
                        control,
                    },
                );
                self.usage.requests += 1;
                self.usage.bytes += bytes;
                if !control {
                    self.bulk.requests += 1;
                    self.bulk.bytes += bytes;
                }
                Ok(request)
            }
            Err(TrySendError::Full(a)) => Err(SnapshotWorkRejected {
                reason: SnapshotWorkError::Overloaded,
                work: Box::new(a.work),
            }),
            Err(TrySendError::Disconnected(a)) => {
                self.fenced = true;
                self.sender.take();
                Err(SnapshotWorkRejected {
                    reason: SnapshotWorkError::Fenced,
                    work: Box::new(a.work),
                })
            }
        }
    }
    fn poll(&mut self, limit: usize) -> Vec<SnapshotWorkEvent> {
        let mut events = Vec::new();
        for _ in 0..limit.min(self.limits.max_requests) {
            let event = match self.events.try_recv() {
                Ok(e) => e,
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    let Some((&sequence, r)) = self.retained.first_key_value() else {
                        break;
                    };
                    self.fenced = true;
                    self.sender.take();
                    SnapshotWorkEvent {
                        request: SnapshotWorkTicket {
                            binding: self.binding,
                            sequence,
                        },
                        visit: r.visit,
                        result: Err(StorageError::Uncertain(
                            "snapshot worker disconnected; recover accepted work".into(),
                        )),
                    }
                }
            };
            if event.result.is_err() {
                self.fenced = true;
                self.sender.take();
            }
            self.release(event.request.sequence);
            events.push(event);
        }
        events
    }
    fn close(&mut self) {
        self.sender.take();
    }
}
fn perform<S: SnapshotRetention>(
    store: &mut S,
    job: SnapshotJob,
) -> Result<SnapshotOutput, StorageError> {
    let allowance = snapshot_load_reservation(store.limits())
        .ok_or(StorageError::Rejected("snapshot worker limits"))?;
    match job {
        SnapshotJob::Publish { snapshot, durable } => {
            store.reconcile_log(durable)?;
            let ticket = store.begin(snapshot.metadata.clone(), snapshot.application.len())?;
            if ticket.binding != store.binding()
                || ticket.group != snapshot.metadata.bootstrap.group
            {
                return Err(StorageError::StaleTicket);
            }
            let size = store.limits().max_chunk_bytes;
            for (i, chunk) in snapshot.application.chunks(size).enumerate() {
                store.write_chunk(ticket, i * size, chunk)?;
            }
            let sealed = store.seal(ticket)?;
            if sealed.ticket != ticket {
                return Err(StorageError::StaleTicket);
            }
            let receipt = store.publish(sealed)?;
            if receipt.sealed != sealed || receipt.metadata != snapshot.metadata {
                return Err(StorageError::StaleTicket);
            }
            let reference = receipt.reference();
            store.pin_for_log(reference)?;
            let verified = store.load_pinned(reference)?;
            if verified != snapshot || snapshot_image_bytes(&verified).is_none_or(|n| n > allowance)
            {
                return Err(StorageError::Corrupt(
                    "published snapshot verification/budget",
                ));
            }
            Ok(SnapshotOutput::Published(reference))
        }
        SnapshotJob::Load { reference, install } => {
            let snapshot = store.load_pinned(reference)?;
            if !reference.matches(&snapshot)
                || snapshot_image_bytes(&snapshot).is_none_or(|n| n > allowance)
            {
                return Err(StorageError::Corrupt("loaded snapshot verification/budget"));
            }
            if install {
                store.reconcile_log(Some(reference))?;
            }
            Ok(SnapshotOutput::Loaded {
                reference,
                snapshot,
                reconciled: install,
            })
        }
    }
}
