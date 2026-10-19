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
    pub(super) fn read_result(
        &mut self,
        work: &mut BridgeWork,
        node: usize,
        ticket: ReadInvocationTicket,
        query: LogicalQuery,
        barrier: ReadBarrier,
        result: LogicalRead,
    ) -> Result<bool, LogicalBridgeError> {
        match (&work.stage, query, result) {
            (
                BridgeStage::ActivationHistory(operation),
                LogicalQuery::ConfigurationOperation(requested),
                LogicalRead::ConfigurationOperation(history),
            ) if *operation == requested => {
                self.finish_activation_history(node, ticket, requested, barrier, *history)
            }
            (
                BridgeStage::ActivationRepresentative(operation),
                LogicalQuery::Metadata,
                LogicalRead::Metadata(metadata),
            ) => {
                if metadata.mapping != self.mapping
                    || metadata.representative != self.initialized
                    || self
                        .initialized_at
                        .is_none_or(|previous| barrier.index() < previous.index())
                {
                    return Err(LogicalBridgeError::StaleRepresentative);
                }
                self.activation_verified = true;
                work.stage = BridgeStage::ActivationHistory(*operation);
                Ok(false)
            }
            (BridgeStage::BeforeClaim(operation), LogicalQuery::Image, read) => {
                self.prepare_claim(work, *operation, node, read)?;
                Ok(false)
            }
            (BridgeStage::BeforeEnrollment { .. }, LogicalQuery::Image, read) => {
                self.prepare_enrollment(work, node, read)?;
                Ok(false)
            }
            (
                BridgeStage::Enrollment { .. },
                LogicalQuery::Enrollment,
                LogicalRead::Enrollment(view),
            ) => self.finish_enrollment_read(work, node, (ticket, barrier, *view)),
            (
                BridgeStage::AfterClaim {
                    claim,
                    representative,
                },
                LogicalQuery::Image,
                LogicalRead::Image(image),
            ) => {
                let (claim, representative) = (**claim, *representative);
                self.finish_claim_read(work, node, *image, claim, barrier, representative)
            }
            (
                BridgeStage::Resolving { command, bytes },
                LogicalQuery::Outcome(operation),
                LogicalRead::Outcome(lookup),
            ) if operation == command.operation => {
                if let LogicalOutcomeLookup::Retained(status) = lookup {
                    if status.operation != operation
                        || status.command != *bytes
                        || status.first_index != status.receipt.first_index
                    {
                        return Err(LogicalBridgeError::ProviderViolation);
                    }
                    if status.first_index > barrier.index() || status.first_term > barrier.term() {
                        // read_at can expose newer applied retained state than
                        // the original read barrier. Obtain a covering barrier;
                        // this observation cannot yet authorize the outcome.
                        return Ok(false);
                    }
                    self.finish_action(
                        work,
                        status.receipt,
                        self.nodes[node].local().owner.identity(),
                    )
                } else {
                    // Unknown is not "not committed" and Expired is not
                    // "committed": repropose the SAME immutable bytes. The
                    // child deduplicates a retained copy and commits a
                    // terminal `Refused(Expired)` for a retired one, which
                    // `finish_action` turns into a sticky failure.
                    work.stage = BridgeStage::Applying {
                        command: command.clone(),
                        bytes: bytes.clone(),
                    };
                    Ok(false)
                }
            }
            (
                BridgeStage::Snapshots(_),
                LogicalQuery::Snapshot(reference),
                LogicalRead::Snapshot(snapshot),
            ) => self.finish_snapshot_read(work, node, reference, barrier, *snapshot),
            _ => Err(LogicalBridgeError::ProviderViolation),
        }
    }
    fn finish_snapshot_read(
        &mut self,
        work: &mut BridgeWork,
        node: usize,
        reference: SnapshotRef,
        barrier: ReadBarrier,
        snapshot: Snapshot,
    ) -> Result<bool, LogicalBridgeError> {
        let BridgeStage::Snapshots(reads) = &work.stage else {
            unreachable!()
        };
        if reads.references.get(reads.snapshots.len()) != Some(&reference)
            || barrier.index() < reads.barrier.index()
        {
            return Err(LogicalBridgeError::ProviderViolation);
        }
        self.nodes[node].local().applications[&self.mapping.child]
            .verify_snapshot(reference, &snapshot)
            .map_err(LogicalBridgeError::Application)?;
        let BridgeStage::Snapshots(reads) = &mut work.stage else {
            unreachable!()
        };
        reads.snapshots.push((reference, snapshot));
        if reads.snapshots.len() == reads.references.len() {
            let complete = SnapshotReads {
                image: reads.image.clone(),
                claim: reads.claim,
                barrier: reads.barrier,
                references: reads.references.clone(),
                snapshots: std::mem::take(&mut reads.snapshots),
                enrollment: reads.enrollment.take(),
            };
            self.finish_image(complete)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }
    fn finish_activation_history(
        &mut self,
        node: usize,
        ticket: ReadInvocationTicket,
        requested: OperationId,
        barrier: ReadBarrier,
        history: LogicalConfigurationOperation,
    ) -> Result<bool, LogicalBridgeError> {
        if history.child != self.mapping.child
            || history.operation != requested
            || history
                .entries
                .iter()
                .any(|e| e.index > history.child_applied)
        {
            return Err(LogicalBridgeError::ProviderViolation);
        }
        if history
            .entries
            .iter()
            .any(|e| e.index > barrier.index() || e.term > barrier.term())
        {
            return Ok(false);
        }
        self.activation_history = Some(Box::new(
            super::super::activation::ObservedLogicalConfiguration {
                history,
                ticket,
                barrier,
                owner: self.nodes[node].local().owner.identity(),
            },
        ));
        Ok(true)
    }
    fn finish_claim_read(
        &mut self,
        work: &mut BridgeWork,
        node: usize,
        image: LogicalImage,
        claim: LogicalReceipt,
        barrier: ReadBarrier,
        representative: RepresentativeBinding,
    ) -> Result<bool, LogicalBridgeError> {
        let app = &self.nodes[node].local().applications[&self.mapping.child];
        if image.mapping != self.mapping {
            return Err(LogicalBridgeError::ProviderViolation);
        }
        super::super::super::startup::validate_parent_image(
            &image,
            claim,
            barrier,
            representative,
            app.limits(),
            &app.log_codec,
            (app.initialization(), work.enrollment.as_deref()),
        )
        .map_err(LogicalBridgeError::Storage)?;
        let mut references = image.pins.clone();
        if let Some(published) = image.published {
            if !references.contains(&published) {
                references.push(published);
            }
        }
        if references.len() > 2 {
            return Err(LogicalBridgeError::ProviderViolation);
        }
        let reads = SnapshotReads {
            image,
            claim,
            barrier,
            references,
            snapshots: Vec::new(),
            enrollment: work.enrollment.take(),
        };
        if reads.references.is_empty() {
            self.finish_image(reads)?;
            Ok(true)
        } else {
            work.stage = BridgeStage::Snapshots(Box::new(reads));
            Ok(false)
        }
    }
    pub(in crate::logical_voter::bridge) fn prepare_claim(
        &self,
        work: &mut BridgeWork,
        operation: OperationId,
        node: usize,
        read: LogicalRead,
    ) -> Result<(), LogicalBridgeError> {
        let previous = match read {
            LogicalRead::Uninitialized => None,
            LogicalRead::Image(image) if image.mapping == self.mapping => {
                Some(image.representative)
            }
            _ => return Err(LogicalBridgeError::ProviderViolation),
        };
        let epoch = previous
            .map_or(Some(1), |r| r.epoch.get().checked_add(1))
            .and_then(RepresentativeEpoch::new)
            .ok_or(LogicalBridgeError::Exhausted)?;
        let logical_session = previous
            .map_or(Some(1), |r| r.logical_session.get().checked_add(1))
            .and_then(StoreSession::new)
            .ok_or(LogicalBridgeError::Exhausted)?;
        let selected = &self.nodes[self.representative_node].local().owner;
        let selected_core = selected
            .core(self.mapping.child)
            .ok_or(LogicalBridgeError::InvalidAssembly)?;
        let physical = crate::secure::PeerIdentity {
            node: selected_core.local_node(),
            store: selected.identity().store.identity,
        };
        let core = self.nodes[node]
            .local()
            .owner
            .core(self.mapping.child)
            .ok_or(LogicalBridgeError::InvalidAssembly)?;
        if core.membership().voter_store(physical.node) != Some(physical.store) {
            return Err(LogicalBridgeError::StaleRepresentative);
        }
        let representative = RepresentativeBinding {
            epoch,
            physical,
            physical_session: selected.identity().store.session,
            logical_session,
        };
        let command = Command {
            operation,
            origin_configuration: core.membership().id(),
            representative,
            consumed_through: 0,
            body: CommandBody::Claim {
                expected_epoch: previous.map(|r| r.epoch),
            },
        };
        let app = &self.nodes[node].local().applications[&self.mapping.child];
        let bytes = app
            .encode_command(&command, app.limits().max_command_bytes)
            .map_err(LogicalBridgeError::Application)?;
        work.stage = BridgeStage::Applying {
            command: Box::new(command),
            bytes,
        };
        Ok(())
    }
}
