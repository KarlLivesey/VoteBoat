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
    /// Whether original checkpoint-export observations and their failed cleanup
    /// are empty. Healthy ordinary snapshot jobs have independent ownership;
    /// this is neither whole-router quiescence nor readiness/quorum authority.
    pub fn checkpoint_exports_are_drained(&self) -> bool {
        self.exports.is_drained()
    }

    /// Stop only new exports; ordinary snapshot dependencies may still drain.
    pub fn close_exports(&mut self) {
        self.exports.closed = true;
    }
    pub fn poll_export(&mut self) -> Option<SnapshotExportCompletion> {
        let p = self
            .exports
            .pending
            .values_mut()
            .find(|p| !p.exported && p.outcome.is_some())?;
        p.exported = true;
        Some(SnapshotExportCompletion {
            ticket: p.ticket,
            outcome: p.outcome.take().unwrap(),
        })
    }
    /// Transfer the original image/failure to the host and release router credits.
    pub fn complete_export(
        &mut self,
        completion: SnapshotExportCompletion,
    ) -> Result<SnapshotExportOutcome, SnapshotExportRejected> {
        let key = completion.ticket.visit.sequence;
        if completion.ticket.owner != self.owner
            || self
                .exports
                .pending
                .get(&key)
                .is_none_or(|p| p.ticket != completion.ticket || !p.exported)
        {
            return Err(SnapshotExportRejected {
                reason: SnapshotRouteError::StaleCompletion,
                completion: Box::new(completion),
            });
        }
        self.exports.pending.remove(&key);
        Ok(completion.outcome)
    }
    /// Cancel only an unexported observation. Pending worker work remains owned.
    /// An externally held completion must still be completed exactly once.
    pub fn cancel_export(
        &mut self,
        ticket: SnapshotExportTicket,
    ) -> Result<(), SnapshotRouteError> {
        let key = ticket.visit.sequence;
        let p = self
            .exports
            .pending
            .get_mut(&key)
            .ok_or(SnapshotRouteError::StaleCompletion)?;
        if p.ticket != ticket || p.exported {
            return Err(SnapshotRouteError::StaleCompletion);
        }
        if p.outcome.is_some() {
            self.exports.pending.remove(&key);
        } else {
            p.cancelled = true;
        }
        Ok(())
    }
}
