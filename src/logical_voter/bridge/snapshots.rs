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
    /// Host-pumped snapshot progress uses the matching consumed parent image;
    /// inspection data or a caller-provided receipt cannot finish accepted work.
    pub fn pump_snapshots(
        &mut self,
        port: &mut LogicalSnapshotPort,
        persistence: &LogicalPersistencePort,
    ) -> Result<bool, LogicalBridgeError> {
        let (mapping, representative, binding) = port.scope();
        if mapping != self.mapping
            || Some(representative) != self.initialized
            || persistence.scope() != (mapping, representative)
            || binding != persistence.binding()
        {
            return Err(LogicalBridgeError::StaleRepresentative);
        }
        port.flush();
        if port.fenced() {
            return Err(LogicalBridgeError::Storage(StorageError::Fenced));
        }
        if port.submitted() {
            if self.release_ticket.is_some() {
                return Ok(false);
            }
            let Some(observation) = self.take_committed_action() else {
                return Ok(false);
            };
            let result = self.verify_publication(&observation).and_then(|()| {
                port.finish(&observation)
                    .map_err(LogicalBridgeError::Storage)
            });
            if let Err(error) = result {
                self.action = Some(observation);
                self.failure.get_or_insert(error.clone());
                return Err(error);
            }
            port.flush();
            return Ok(true);
        }
        let Some(parent) = persistence.current_parent() else {
            return Ok(false);
        };
        port.receive(parent).map_err(LogicalBridgeError::Storage)?;
        port.flush();
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
    fn verify_publication(
        &self,
        observation: &CommittedLogicalAction,
    ) -> Result<(), LogicalBridgeError> {
        if let LogicalOutcome::SnapshotPublished(reference) = observation.receipt().outcome {
            let CommandBody::Publish { snapshot, .. } = observation.command_body() else {
                return Err(LogicalBridgeError::ProviderViolation);
            };
            self.nodes[0].local().applications[&self.mapping.child]
                .verify_snapshot(reference, snapshot)
                .map_err(LogicalBridgeError::Application)?;
        }
        Ok(())
    }
}
