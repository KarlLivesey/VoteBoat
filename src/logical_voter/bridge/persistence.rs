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
    /// Conservative Written and Durable both wait for consumed child commitment.
    pub fn pump_persistence(
        &mut self,
        port: &mut LogicalPersistencePort,
    ) -> Result<bool, LogicalBridgeError> {
        let (mapping, representative) = port.scope();
        if mapping != self.mapping || Some(representative) != self.initialized {
            return Err(LogicalBridgeError::StaleRepresentative);
        }
        port.flush();
        if port.fenced() {
            return Err(LogicalBridgeError::Storage(StorageError::Fenced));
        }
        port.receive().map_err(LogicalBridgeError::Storage)?;
        if port.submitted() {
            if self.release_ticket.is_some() {
                return Ok(false);
            }
            if let Some(observation) = self.take_committed_action() {
                if let Err(error) = port.finish(&observation) {
                    self.action = Some(observation);
                    return Err(LogicalBridgeError::Storage(error));
                }
                port.flush();
                return Ok(true);
            }
            return Ok(false);
        }
        if self.work.is_some()
            || self.image.is_some()
            || self.action.is_some()
            || self.release_ticket.is_some()
        {
            return Ok(false);
        }
        let Some(body) = port.command() else {
            return Ok(false);
        };
        let operation = self.submit_body(body)?;
        port.mark_submitted(operation);
        Ok(true)
    }
}
