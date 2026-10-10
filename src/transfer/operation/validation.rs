// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
pub(super) struct State<'a> {
    intent_read: &'a TransferObservation,
    pub publication_read: &'a TransferObservation,
    pub sources: Vec<&'a TransferObservation>,
    pub targets: Vec<&'a TransferObservation>,
}
impl<'a> State<'a> {
    pub fn new(
        plan: &TransferOperation,
        reads: &'a [TransferObservation],
    ) -> Result<Self, TransferOperationError> {
        let expected = plan.reads();
        if reads.len() > expected.len() {
            return Err(TransferOperationError::DuplicateRead);
        }
        let mut unique = BTreeSet::new();
        for r in reads {
            if !expected.contains(&r.read) {
                return Err(TransferOperationError::UnexpectedRead);
            }
            if !unique.insert(r.read) {
                return Err(TransferOperationError::DuplicateRead);
            }
        }
        let lookup = |read: TransferRead| {
            reads
                .iter()
                .find(|r| r.read == read)
                .ok_or(TransferOperationError::MissingRead(read))
        };
        Ok(Self {
            intent_read: lookup(expected[0])?,
            publication_read: lookup(expected[1])?,
            sources: expected
                .iter()
                .skip(2)
                .filter(|r| r.kind == TransferReadKind::Source)
                .map(|r| lookup(*r))
                .collect::<Result<_, _>>()?,
            targets: expected
                .iter()
                .skip(2)
                .filter(|r| r.kind == TransferReadKind::Target)
                .map(|r| lookup(*r))
                .collect::<Result<_, _>>()?,
        })
    }
    pub fn intent(&self) -> Option<&TransferIntentStatus> {
        self.intent_read.intent()
    }
    pub fn publication(&self) -> Option<&TransferPublicationStatus> {
        self.publication_read.publication()
    }
    pub fn validate(&self, plan: &TransferOperation) -> Result<(), TransferOperationError> {
        if self
            .intent()
            .is_some_and(|v| v.operation != plan.operation || v.intent != plan.intent)
            || self.publication().is_some_and(|v| {
                v.publication_operation != plan.publication_operation
                    || v.publication.operation() != plan.operation
                    || v.publication.intent() != &plan.intent
            })
        {
            return Err(TransferOperationError::Inconsistent);
        }
        if self.intent().is_none()
            && (self.publication().is_some()
                || self.sources.iter().any(|s| s.source().is_some())
                || self.targets.iter().any(|t| t.target().imported.is_some()))
        {
            return Err(TransferOperationError::Inconsistent);
        }
        for source in &self.sources {
            self.source(plan, source)?;
        }
        for target in &self.targets {
            self.target(plan, target)?;
        }
        if let Some(publication) = self.publication() {
            self.published(publication)?;
        }
        Ok(())
    }
    fn source(
        &self,
        plan: &TransferOperation,
        source: &TransferObservation,
    ) -> Result<(), TransferOperationError> {
        let Some(proof) = source.source() else {
            return Ok(());
        };
        let route = plan
            .sources
            .iter()
            .find(|r| route_group(r) == source.read.group)
            .unwrap();
        let digest = ContentDigest::sha256(
            &plan
                .intent
                .encode(MAX_TRANSFER_INTENT_BYTES)
                .map_err(TransferOperationError::Application)?,
        );
        if proof.fence.operation != plan.operation
            || proof.fence.group != source.read.group
            || proof.fence.responsibility != plan.intent.before().input().responsibility
            || proof.fence.epoch != plan.intent.before().input().epoch
            || proof.intent_digest != digest
            || proof.scope
                != if plan.intent.is_retained_insertion() {
                    Some(route.scope)
                } else {
                    None
                }
            || self
                .targets
                .iter()
                .any(|t| t.target().staged_index.is_none())
        {
            return Err(TransferOperationError::Inconsistent);
        }
        let expected = plan
            .targets
            .iter()
            .filter_map(|t| intersect(route.scope, t.scope).map(|s| (route_group(t), s)))
            .collect::<Vec<_>>();
        if proof.exports.len() != expected.len()
            || proof
                .exports
                .iter()
                .zip(expected)
                .any(|(e, (g, s))| e.target != g || e.scope != s)
        {
            return Err(TransferOperationError::Inconsistent);
        }
        Ok(())
    }
    fn target(
        &self,
        plan: &TransferOperation,
        target: &TransferObservation,
    ) -> Result<(), TransferOperationError> {
        let status = target.target();
        if status.operation != plan.operation || status.group != target.read.group {
            return Err(TransferOperationError::Inconsistent);
        }
        if let Some(imported) = &status.imported {
            if status.staged_index.is_none_or(|i| imported.index <= i) {
                return Err(TransferOperationError::Inconsistent);
            }
            let route = plan
                .targets
                .iter()
                .find(|r| route_group(r) == target.read.group)
                .unwrap();
            let expected = self
                .sources
                .iter()
                .zip(&plan.sources)
                .filter_map(|(s, r)| intersect(r.scope, route.scope).map(|scope| (s, scope)))
                .collect::<Vec<_>>();
            if expected.len() != imported.sources.len() {
                return Err(TransferOperationError::Inconsistent);
            }
            for (actual, (source, scope)) in imported.sources.iter().zip(expected) {
                let proof = source
                    .source()
                    .ok_or(TransferOperationError::Inconsistent)?;
                let export = proof
                    .exports
                    .iter()
                    .find(|e| e.target == target.read.group)
                    .ok_or(TransferOperationError::Inconsistent)?;
                if actual.fence != proof.fence
                    || actual.scope != scope
                    || actual.digest != export.digest
                    || (self.publication().is_none()
                        && actual.configuration != source.configuration)
                {
                    return Err(TransferOperationError::Inconsistent);
                }
            }
        }
        if let Some(active) = status.activated {
            let publication = self
                .publication()
                .ok_or(TransferOperationError::Inconsistent)?;
            if status
                .imported
                .as_ref()
                .is_none_or(|i| active.index <= i.index)
                || active.publication_operation != publication.publication_operation
                || active.publication_index != publication.index
            {
                return Err(TransferOperationError::Inconsistent);
            }
        }
        Ok(())
    }
    fn published(&self, status: &TransferPublicationStatus) -> Result<(), TransferOperationError> {
        let publication = &status.publication;
        if self.intent().is_none_or(|i| status.index <= i.index) {
            return Err(TransferOperationError::Inconsistent);
        }
        for source in &self.sources {
            let current = source
                .source()
                .ok_or(TransferOperationError::Inconsistent)?;
            let original = publication
                .sources()
                .iter()
                .find(|s| s.fence.group == source.read.group)
                .ok_or(TransferOperationError::Inconsistent)?;
            if current.fence != original.fence
                || current.exports != original.exports
                || current.intent_digest != original.intent_digest
                || current.scope != original.scope
            {
                return Err(TransferOperationError::Inconsistent);
            }
        }
        for target in &self.targets {
            let current = target.target();
            let original = publication
                .targets()
                .iter()
                .find(|t| t.group == target.read.group)
                .ok_or(TransferOperationError::Inconsistent)?;
            if current.staged_index != Some(original.staged_index)
                || current.imported.as_ref() != Some(&original.imported)
            {
                return Err(TransferOperationError::Inconsistent);
            }
        }
        Ok(())
    }
}
