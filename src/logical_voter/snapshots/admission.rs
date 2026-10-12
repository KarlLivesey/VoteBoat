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
impl LogicalSnapshotWorker {
    pub(super) fn admission(
        &self,
        work: &SnapshotWork,
    ) -> Result<(usize, ExpectedSnapshot), SnapshotWorkError> {
        if self.fenced {
            return Err(SnapshotWorkError::Fenced);
        }
        if self.sender.is_none() {
            return Err(SnapshotWorkError::Closed);
        }
        if self.retained.is_some() {
            return Err(SnapshotWorkError::Overloaded);
        }
        if work.visit.group != self.mapping.parent
            || work.visit.owner.store != self.binding.store
            || work.visit.sequence == 0
        {
            return Err(SnapshotWorkError::WrongBinding);
        }
        let scoped = |r: SnapshotRef| {
            r.store == self.binding.store.identity && r.group == self.mapping.parent
        };
        let (input, control, expected) = match &work.job {
            SnapshotJob::Readiness { reference } => {
                if reference.is_some_and(|r| !scoped(r)) {
                    return Err(SnapshotWorkError::WrongBinding);
                }
                (0, false, ExpectedSnapshot::Readiness(*reference))
            }
            SnapshotJob::Publish { snapshot, durable } => {
                if snapshot.metadata.bootstrap.group != self.mapping.parent
                    || durable.is_some_and(|r| !scoped(r))
                {
                    return Err(SnapshotWorkError::WrongBinding);
                }
                snapshot
                    .metadata
                    .validate()
                    .map_err(|_| SnapshotWorkError::TooLarge)?;
                let cost = snapshot_image_bytes(snapshot).ok_or(SnapshotWorkError::TooLarge)?;
                if snapshot.application.is_empty()
                    || snapshot.application.len() > self.images.max_application_bytes
                    || cost > self.allowance
                {
                    return Err(SnapshotWorkError::TooLarge);
                }
                (
                    cost,
                    true,
                    ExpectedSnapshot::Published {
                        index: snapshot.metadata.index,
                        term: snapshot.metadata.term,
                        schema: snapshot.metadata.application_schema,
                        configuration: snapshot.metadata.configuration(),
                    },
                )
            }
            SnapshotJob::Load { reference, install } => {
                if !scoped(*reference) {
                    return Err(SnapshotWorkError::WrongBinding);
                }
                (
                    0,
                    *install,
                    ExpectedSnapshot::Loaded {
                        reference: *reference,
                        install: *install,
                    },
                )
            }
            SnapshotJob::Reconcile { reference } => {
                if !scoped(*reference) {
                    return Err(SnapshotWorkError::WrongBinding);
                }
                (0, true, ExpectedSnapshot::Reconciled(*reference))
            }
        };
        let bytes = input
            .checked_add(self.allowance)
            .and_then(|n| n.checked_add(4096))
            .ok_or(SnapshotWorkError::TooLarge)?;
        let ceiling = if control {
            self.limits.max_bytes
        } else {
            self.limits.max_bytes - self.limits.control_bytes
        };
        if bytes > ceiling {
            return Err(SnapshotWorkError::Overloaded);
        }
        Ok((bytes, expected))
    }
    pub(super) fn consume(&mut self, event: SnapshotWorkEvent) -> SnapshotWorkEvent {
        let original = self.retained.as_ref().expect("accepted original snapshot");
        let valid = event.request == original.request
            && event.visit == original.visit
            && event
                .result
                .as_ref()
                .is_ok_and(|out| self.valid_output(&original.expected, out));
        if event.result.is_ok() && !valid
            || event.request != original.request
            || event.visit != original.visit
        {
            return self.failure("logical snapshot completion mismatch");
        }
        self.fenced |= event.result.is_err();
        self.retained = None;
        self.outstanding.store(false, Ordering::Release);
        event
    }
    fn valid_output(&self, expected: &ExpectedSnapshot, out: &SnapshotOutput) -> bool {
        let image = |reference: SnapshotRef, snapshot: &Snapshot| {
            reference.matches(snapshot)
                && reference.store == self.binding.store.identity
                && reference.group == self.mapping.parent
                && snapshot_image_bytes(snapshot).is_some_and(|n| n <= self.allowance)
        };
        match (expected, out) {
            (
                ExpectedSnapshot::Readiness(reference),
                SnapshotOutput::Readiness { snapshot, limits },
            ) => {
                limits.max_application_bytes == self.images.max_application_bytes
                    && limits.max_metadata_bytes == self.images.max_metadata_bytes
                    && limits.max_chunk_bytes == self.images.max_chunk_bytes
                    && match (reference, snapshot) {
                        (None, None) => true,
                        (Some(r), Some(s)) => image(*r, s),
                        _ => false,
                    }
            }
            (
                ExpectedSnapshot::Published {
                    index,
                    term,
                    schema,
                    configuration,
                },
                SnapshotOutput::Published(r),
            ) => {
                r.store == self.binding.store.identity
                    && r.group == self.mapping.parent
                    && r.index == *index
                    && r.term == *term
                    && r.application_schema == *schema
                    && r.configuration == *configuration
                    && r.file_bytes <= self.images.max_file_bytes() as u64
            }
            (
                ExpectedSnapshot::Loaded { reference, install },
                SnapshotOutput::Loaded {
                    reference: r,
                    snapshot,
                    reconciled,
                },
            ) => reference == r && install == reconciled && image(*r, snapshot),
            (ExpectedSnapshot::Reconciled(reference), SnapshotOutput::Reconciled(r)) => {
                reference == r
            }
            _ => false,
        }
    }
    pub(super) fn failure(&mut self, message: &str) -> SnapshotWorkEvent {
        let original = self
            .retained
            .take()
            .expect("original accepted snapshot work");
        self.fenced = true;
        self.outstanding.store(false, Ordering::Release);
        SnapshotWorkEvent {
            request: original.request,
            visit: original.visit,
            result: Err(StorageError::Uncertain(message.to_owned())),
        }
    }
}
