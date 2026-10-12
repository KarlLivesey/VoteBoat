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
impl<N: LogicalBackingChild> LogicalBackingBridge<'_, N> {
    /// Pump the constructor-bound release port. This accepts no caller receipt,
    /// readiness flag or reconstructed result. The coordinator retains each
    /// original parent lease/result while this bridge owns its child command.
    pub fn pump_release<R, Vr>(
        &mut self,
        coordinator: &mut LogicalReleaseCoordinator<R, Vr>,
    ) -> Result<bool, LogicalBridgeError> {
        let logical = coordinator
            .authorization()
            .logical()
            .ok_or(LogicalBridgeError::StaleRepresentative)?;
        if logical.mapping != self.mapping || Some(logical.representative) != self.initialized {
            return Err(LogicalBridgeError::StaleRepresentative);
        }
        if let Some(ticket) = self.release_ticket {
            if ticket.authorization != coordinator.authorization() {
                return Err(LogicalBridgeError::StaleRepresentative);
            }
            if let Some(observation) = self.take_committed_action() {
                match coordinator.accept_committed(ticket, observation) {
                    Ok(()) => {
                        self.release_ticket = None;
                        return Ok(true);
                    }
                    Err(rejected) => {
                        self.action = Some(*rejected.observation);
                        // Refusal fences production; no original parent output
                        // is published or reinterpreted as a successful permit.
                        let error = LogicalBridgeError::Release(rejected.reason);
                        self.failure.get_or_insert(error.clone());
                        return Err(error);
                    }
                }
            }
            return Ok(false);
        }
        if self.work.is_some() || self.image.is_some() || self.action.is_some() {
            return Ok(false);
        }
        let Some(request) = coordinator.front_request() else {
            return Ok(false);
        };
        if request.authorization != coordinator.authorization() {
            return Err(LogicalBridgeError::StaleRepresentative);
        }
        let ticket = request.ticket;
        self.submit_body(CommandBody::Authorize(request.capsule.clone()))?;
        self.release_ticket = Some(ticket);
        if let Err(error) = coordinator.mark_submitted(ticket) {
            let error = LogicalBridgeError::Release(error);
            self.failure.get_or_insert(error.clone());
            return Err(error);
        }
        Ok(true)
    }
}
