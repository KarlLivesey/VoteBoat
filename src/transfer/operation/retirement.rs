// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use crate::retirement::{RetentionRelease, RetirementProof, TargetActivationEvidence};

impl TransferOperation {
    /// Build a source retirement proof from fresh, authenticated completed reads.
    /// The caller explicitly releases external retention promises for this cut.
    /// This neither proposes retirement nor releases any storage on its own.
    pub fn retirement_proof(
        &self,
        observations: &[TransferObservation],
        source: GroupIdentity,
        release: OperationId,
    ) -> Result<RetirementProof, TransferOperationError> {
        let state = State::new(self, observations)?;
        state.validate(self)?;
        if self.next(observations, &[])? != TransferAction::Complete {
            return Err(TransferOperationError::Inconsistent);
        }
        let evidence = state
            .sources
            .iter()
            .find(|s| s.read.group == source)
            .and_then(|s| s.source())
            .ok_or(TransferOperationError::Inconsistent)?;
        let targets = state
            .targets
            .iter()
            .map(|t| {
                TargetActivationEvidence::from_status(t.configuration, t.target().clone())
                    .map_err(|e| TransferOperationError::Application(e.0))
            })
            .collect::<Result<_, _>>()?;
        let proof = RetirementProof {
            metadata_configuration: state.publication_read.configuration,
            decision: state
                .publication()
                .ok_or(TransferOperationError::Inconsistent)?
                .clone(),
            targets,
            release: RetentionRelease {
                source,
                operation: self.operation,
                fence_index: evidence.fence.index,
                release,
            },
        };
        // Also enforce the retirement contract's stricter activation provenance.
        proof
            .source_status()
            .map_err(TransferOperationError::Application)?;
        Ok(proof)
    }
}
