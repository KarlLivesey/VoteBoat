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
impl SnapshotRouter {
    /// Replace the ordinary worker consumer only after the exact owner failed.
    /// Processes every transferred event even if one provider envelope is wrong.
    /// Accepted loads are never rolled back and externally held outputs survive.
    pub fn drain_failed_exports<
        Q: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
        W: SnapshotWorker + ?Sized,
    >(
        &mut self,
        owner: &mut EffectOwner<Q, T, E>,
        worker: &mut W,
        limit: usize,
    ) -> Result<usize, SnapshotRouteError> {
        if owner.identity() != self.owner {
            return Err(SnapshotRouteError::WrongOwner);
        }
        if worker.binding() != self.worker {
            return Err(SnapshotRouteError::WrongWorker);
        }
        if !owner.is_failed() {
            return Err(SnapshotRouteError::ProviderContract);
        }
        self.close_exports();
        let events = worker.poll(limit);
        let count = events.len();
        let mut error = (count > limit).then_some(SnapshotRouteError::ProviderContract);
        for event in events {
            let result = self.failed_export_event(owner, event);
            if let Err(reason) = result {
                error.get_or_insert(reason);
            }
        }
        if worker.is_drained() {
            self.discard_failed(owner)?;
            self.exports.failed.clear();
            self.exports.pending.retain(|_, p| {
                if p.cancelled {
                    return false;
                }
                if !p.exported && p.outcome.is_none() {
                    p.outcome = Some(SnapshotExportOutcome::Failed(StorageError::Uncertain(
                        "snapshot export worker drained without original completion".into(),
                    )));
                }
                true
            });
        }
        error.map_or(Ok(count), Err)
    }
    fn failed_export_event<Q: ReadyScheduler, T: TimerService, E: ElectionEntropy>(
        &mut self,
        owner: &mut EffectOwner<Q, T, E>,
        event: SnapshotWorkEvent,
    ) -> Result<(), SnapshotRouteError> {
        if self.exports.has_request(event.request.sequence) {
            return self.deliver_export(owner, event);
        }
        if let Some(p) = self.pending.get(&event.request.sequence) {
            if p.request != event.request || p.lease.ticket.visit != event.visit {
                return Err(SnapshotRouteError::StaleCompletion);
            }
            let p = self.pending.remove(&event.request.sequence).unwrap();
            return owner
                .discard_failed(p.lease)
                .map_err(|e| SnapshotRouteError::Owner(e.reason));
        }
        if self.exports.failed.get(&event.request.sequence) == Some(&(event.request, event.visit)) {
            self.exports.failed.remove(&event.request.sequence);
            return Ok(());
        }
        Err(SnapshotRouteError::StaleCompletion)
    }
}
