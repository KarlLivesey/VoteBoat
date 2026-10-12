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
impl SnapshotWorker for LogicalSnapshotWorker {
    fn binding(&self) -> SnapshotWorkerBinding {
        self.binding
    }
    fn limits(&self) -> SnapshotWorkLimits {
        self.limits
    }
    fn usage(&self) -> SnapshotWorkUsage {
        self.retained
            .as_ref()
            .map_or(SnapshotWorkUsage::default(), |r| SnapshotWorkUsage {
                requests: 1,
                bytes: r.bytes,
            })
    }
    fn load_reservation(&self, group: GroupIdentity) -> Option<usize> {
        (group == self.mapping.parent).then_some(self.allowance)
    }
    fn checkpoint_bytes(&self, group: GroupIdentity) -> Option<usize> {
        (group == self.mapping.parent).then_some(self.images.max_application_bytes)
    }
    fn submit(&mut self, work: SnapshotWork) -> Result<SnapshotWorkTicket, SnapshotWorkRejected> {
        let (bytes, expected) = match self.admission(&work) {
            Ok(r) => r,
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
        self.outstanding.store(true, Ordering::Release);
        match self
            .sender
            .as_ref()
            .expect("checked open snapshot worker")
            .try_send(AcceptedSnapshot { request, work })
        {
            Ok(()) => {
                self.sequence = sequence;
                self.retained = Some(RetainedSnapshot {
                    request,
                    visit,
                    bytes,
                    expected,
                });
                Ok(request)
            }
            Err(error) => {
                self.outstanding.store(false, Ordering::Release);
                let (reason, accepted) = match error {
                    TrySendError::Full(a) => (SnapshotWorkError::Overloaded, a),
                    TrySendError::Disconnected(a) => {
                        self.fenced = true;
                        (SnapshotWorkError::Fenced, a)
                    }
                };
                Err(SnapshotWorkRejected {
                    reason,
                    work: Box::new(accepted.work),
                })
            }
        }
    }
    fn poll(&mut self, limit: usize) -> Vec<SnapshotWorkEvent> {
        if limit == 0 || self.retained.is_none() {
            return Vec::new();
        }
        match self.events.try_recv() {
            Ok(event) => vec![self.consume(event)],
            Err(TryRecvError::Empty) => Vec::new(),
            Err(TryRecvError::Disconnected) => vec![self.failure("logical snapshot bridge lost")],
        }
    }
    fn close(&mut self) {
        self.sender = None;
    }
}
