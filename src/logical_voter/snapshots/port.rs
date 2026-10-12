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
use super::*;
impl LogicalSnapshotPort {
    pub(in crate::logical_voter) fn scope(
        &self,
    ) -> (LogicalStoreMapping, RepresentativeBinding, WorkerBinding) {
        (self.mapping, self.representative, self.persistence)
    }
    pub(in crate::logical_voter) fn fenced(&self) -> bool {
        self.fenced
    }
    pub(in crate::logical_voter) fn submitted(&self) -> bool {
        self.pending.as_ref().is_some_and(|p| p.operation.is_some())
    }
    pub fn binding(&self) -> SnapshotWorkerBinding {
        self.binding
    }
    pub fn pending_request(&self) -> Option<SnapshotWorkTicket> {
        self.pending.as_ref().map(|p| p.accepted.request)
    }
    pub fn operation(&self) -> Option<OperationId> {
        self.pending.as_ref().and_then(|p| p.operation)
    }
    pub fn retained_references(&self) -> impl Iterator<Item = SnapshotRef> + '_ {
        self.snapshots.iter().map(|(r, _)| *r)
    }
    pub fn cache_bytes(&self) -> usize {
        self.snapshots
            .iter()
            .map(|(_, s)| snapshot_image_bytes(s).expect("validated cache"))
            .sum()
    }
    pub fn is_drained(&self) -> bool {
        self.pending.is_none() && self.held.is_empty() && !self.outstanding.load(Ordering::Acquire)
    }
    pub(in crate::logical_voter) fn flush(&mut self) {
        if let Some(event) = self.held.pop_front() {
            match self.events.try_send(event) {
                Ok(()) => {}
                Err(TrySendError::Full(event)) => self.held.push_front(event),
                Err(TrySendError::Disconnected(event)) => {
                    self.held.push_front(event);
                    self.fenced = true;
                }
            }
        }
    }
    pub(in crate::logical_voter) fn receive(
        &mut self,
        parent: &GroupLog,
    ) -> Result<(), StorageError> {
        if self.pending.is_some() || !self.held.is_empty() {
            return Ok(());
        }
        let accepted = match self.requests.try_recv() {
            Ok(a) => a,
            Err(TryRecvError::Empty | TryRecvError::Disconnected) => return Ok(()),
        };
        let outcome = self.local_job(&accepted.work.job, parent);
        match outcome {
            Ok(Some(output)) => self.output(accepted, Ok(output)),
            Ok(None) => {
                self.pending = Some(PendingSnapshot {
                    accepted,
                    revision: parent.revision,
                    operation: None,
                })
            }
            Err(error) => {
                self.fenced = true;
                self.output(accepted, Err(error.clone()));
                return Err(error);
            }
        }
        Ok(())
    }
    fn local_job(
        &self,
        job: &SnapshotJob,
        parent: &GroupLog,
    ) -> Result<Option<SnapshotOutput>, StorageError> {
        match job {
            SnapshotJob::Readiness { reference } => {
                if *reference != parent.snapshot {
                    return Err(StorageError::StaleTicket);
                }
                let snapshot = reference.map(|r| self.load(r)).transpose()?;
                Ok(Some(SnapshotOutput::Readiness {
                    snapshot,
                    limits: self.images,
                }))
            }
            SnapshotJob::Load {
                reference,
                install: false,
            } => Ok(Some(SnapshotOutput::Loaded {
                reference: *reference,
                snapshot: self.load(*reference)?,
                reconciled: false,
            })),
            SnapshotJob::Publish { durable, .. } => {
                if *durable != parent.snapshot {
                    return Err(StorageError::StaleTicket);
                }
                Ok(None)
            }
            SnapshotJob::Load {
                reference,
                install: true,
            }
            | SnapshotJob::Reconcile { reference } => {
                if Some(*reference) != parent.snapshot {
                    return Err(StorageError::StaleTicket);
                }
                self.load(*reference)?;
                Ok(None)
            }
        }
    }
    pub(super) fn load(&self, reference: SnapshotRef) -> Result<Snapshot, StorageError> {
        let snapshot = &self
            .snapshots
            .iter()
            .find(|(r, _)| *r == reference)
            .ok_or(StorageError::StaleTicket)?
            .1;
        if !reference.matches(snapshot)
            || snapshot_image_bytes(snapshot).is_none_or(|n| n > self.allowance)
        {
            return Err(StorageError::Corrupt("logical retained snapshot mismatch"));
        }
        Ok(snapshot.clone())
    }
    pub(super) fn output(
        &mut self,
        accepted: AcceptedSnapshot,
        result: Result<SnapshotOutput, StorageError>,
    ) {
        self.held.push_back(SnapshotWorkEvent {
            request: accepted.request,
            visit: accepted.work.visit,
            result,
        });
    }
    pub(in crate::logical_voter) fn command(&self) -> Option<CommandBody> {
        let pending = self.pending.as_ref()?;
        let origin = SnapshotOrigin {
            request: pending.accepted.request,
            visit: pending.accepted.work.visit,
        };
        Some(match &pending.accepted.work.job {
            SnapshotJob::Publish { snapshot, durable } => CommandBody::Publish {
                expected_revision: pending.revision,
                durable: *durable,
                snapshot: snapshot.clone(),
                origin,
            },
            SnapshotJob::Load {
                reference,
                install: true,
            }
            | SnapshotJob::Reconcile { reference } => CommandBody::Reconcile {
                expected_revision: pending.revision,
                reference: *reference,
                origin,
            },
            _ => unreachable!("local snapshot jobs already complete"),
        })
    }
    pub(in crate::logical_voter) fn mark_submitted(&mut self, operation: OperationId) {
        self.pending
            .as_mut()
            .expect("original pending snapshot")
            .operation = Some(operation);
    }
}
