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
//! Delegate the sole original Node snapshot-export consumer through its borrow.
use super::*;
impl<N: LogicalActivationBackingChild> LogicalBackingBridge<'_, N> {
    /// Load the original durable child anchor. Data is not activation authority.
    pub fn export_checkpoint_backing(
        &mut self,
        node: usize,
    ) -> Result<SnapshotExportTicket, SnapshotRouteError> {
        if self.closed {
            return Err(SnapshotRouteError::Closed);
        }
        if self.failure.is_some() {
            return Err(SnapshotRouteError::Closed);
        }
        self.nodes
            .get_mut(node)
            .ok_or(SnapshotRouteError::UnknownGroup)?
            .export_checkpoint(self.mapping.child)
    }
    pub fn poll_checkpoint_export_backing(
        &mut self,
        node: usize,
    ) -> Option<SnapshotExportCompletion> {
        self.nodes.get_mut(node)?.poll_checkpoint_export()
    }
    /// Wrong endpoint rejection retains the complete original opaque result.
    pub fn complete_checkpoint_export_backing(
        &mut self,
        node: usize,
        output: SnapshotExportCompletion,
    ) -> Result<SnapshotExportOutcome, SnapshotExportRejected> {
        let Some(endpoint) = self.nodes.get_mut(node).filter(|n| {
            n.local().owner.identity() == output.ticket().owner
                && output.ticket().visit.group == self.mapping.child
        }) else {
            return Err(SnapshotExportRejected {
                reason: SnapshotRouteError::WrongOwner,
                completion: Box::new(output),
            });
        };
        endpoint.complete_checkpoint_export(output)
    }
    pub fn cancel_checkpoint_export_backing(
        &mut self,
        node: usize,
        ticket: SnapshotExportTicket,
    ) -> Result<(), SnapshotRouteError> {
        let endpoint = self
            .nodes
            .get_mut(node)
            .ok_or(SnapshotRouteError::WrongOwner)?;
        if endpoint.local().owner.identity() != ticket.owner
            || ticket.visit.group != self.mapping.child
        {
            return Err(SnapshotRouteError::WrongOwner);
        }
        endpoint.cancel_checkpoint_export(ticket)
    }
}
