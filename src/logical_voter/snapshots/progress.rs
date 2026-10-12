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
    pub(in crate::logical_voter) fn finish(
        &mut self,
        observation: &bridge::CommittedLogicalAction,
    ) -> Result<(), StorageError> {
        let pending = self.pending.as_ref().ok_or(StorageError::StaleTicket)?;
        if observation.mapping() != self.mapping
            || observation.representative() != self.representative
            || pending.operation != Some(observation.receipt().operation)
            || self.command().as_ref() != Some(observation.command_body())
        {
            return Err(StorageError::StaleTicket);
        }
        let pending = self.pending.take().expect("checked original snapshot");
        let result = match observation.receipt().outcome {
            LogicalOutcome::SnapshotPublished(reference) => self.published(&pending, reference),
            LogicalOutcome::SnapshotReconciled => self.reconciled(&pending),
            LogicalOutcome::Refused(_) => {
                Err(StorageError::Rejected("committed logical snapshot refusal"))
            }
            _ => {
                self.pending = Some(pending);
                return Err(StorageError::Corrupt("logical snapshot outcome mismatch"));
            }
        };
        self.fenced |= result.is_err();
        self.output(pending.accepted, result);
        Ok(())
    }
    fn published(
        &mut self,
        pending: &PendingSnapshot,
        reference: SnapshotRef,
    ) -> Result<SnapshotOutput, StorageError> {
        let SnapshotJob::Publish { snapshot, durable } = &pending.accepted.work.job else {
            return Err(StorageError::StaleTicket);
        };
        if reference.store != self.mapping.store
            || !reference.matches(snapshot)
            || reference.file_bytes > self.images.max_file_bytes() as u64
            || snapshot_image_bytes(snapshot).is_none_or(|n| n > self.allowance)
        {
            return Err(StorageError::Corrupt(
                "logical published snapshot scope/budget",
            ));
        }
        if let Some(anchor) = durable {
            self.load(*anchor)?;
        }
        self.snapshots
            .retain(|(r, _)| Some(*r) == *durable || *r == reference);
        if !self.snapshots.iter().any(|(r, _)| *r == reference) {
            self.snapshots.push((reference, snapshot.clone()));
        }
        if self.snapshots.len() > 2 {
            return Err(StorageError::Corrupt("logical snapshot pin count"));
        }
        Ok(SnapshotOutput::Published(reference))
    }
    fn reconciled(&mut self, pending: &PendingSnapshot) -> Result<SnapshotOutput, StorageError> {
        let (reference, install) = match pending.accepted.work.job {
            SnapshotJob::Load {
                reference,
                install: true,
            } => (reference, true),
            SnapshotJob::Reconcile { reference } => (reference, false),
            _ => return Err(StorageError::StaleTicket),
        };
        let snapshot = self.load(reference)?;
        self.snapshots.retain(|(r, _)| *r == reference);
        Ok(if install {
            SnapshotOutput::Loaded {
                reference,
                snapshot,
                reconciled: true,
            }
        } else {
            SnapshotOutput::Reconciled(reference)
        })
    }
}
