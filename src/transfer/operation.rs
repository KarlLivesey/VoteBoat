// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Restartable operator decisions over completed quorum reads, without I/O.
use super::*;
use crate::{scope::ScopeImage, transfer_target::*};
mod observation;
pub use observation::*;
mod retirement;
mod validation;
use validation::State;

pub const TRANSFER_OPERATION_CONTRACT_VERSION: u32 = 1;
pub const MAX_OPERATION_IMAGES: usize = 512;
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum TransferReadKind {
    Intent,
    Publication,
    Source,
    Target,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct TransferRead {
    pub group: GroupIdentity,
    pub kind: TransferReadKind,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TransferOperationError {
    InvalidPlan,
    MissingRead(TransferRead),
    DuplicateRead,
    UnexpectedRead,
    NotRead,
    WrongReadType,
    Inconsistent,
    WrongImage,
    Budget,
    Application(ApplicationError),
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TransferExport {
    pub source: GroupIdentity,
    pub target: GroupIdentity,
    pub fence: crate::routed::OwnershipFence,
    pub scope: BucketRange,
    pub digest: ContentDigest,
}
pub struct TransferImage<'a> {
    pub source: GroupIdentity,
    pub target: GroupIdentity,
    pub image: &'a ScopeImage,
}
/// Execute through the existing application guard and authorized Node API.
/// Stage/Fence are encoded by the selected actual target/source provider.
/// Complete means the original activation was recorded, not current ownership
/// after later transfers. Every proposal can still return an unknown outcome.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TransferAction {
    RecordIntent,
    Stage(GroupIdentity),
    Fence(GroupIdentity),
    Export(TransferExport),
    Import(TargetImport),
    Publish(TransferPublication),
    Activate {
        target: GroupIdentity,
        activation: TargetActivation,
    },
    Complete,
}
/// Immutable original operation, not a new coordinator or durability owner.
/// Reconstruct from the same trusted intent and IDs after any interruption.
/// Obtain fresh completed reads for reads(), call next(), execute one action,
/// then discard observations and repeat. No local phase flag authorizes progress.
#[derive(Clone, Debug)]
pub struct TransferOperation {
    intent: TransferIntent,
    operation: OperationId,
    publication_operation: OperationId,
    sources: Vec<RouteEntry>,
    targets: Vec<RouteEntry>,
}
impl TransferOperation {
    #[allow(clippy::result_large_err)] // Preserve the original owned intent on rejection.
    pub fn new(
        intent: TransferIntent,
        operation: OperationId,
        publication_operation: OperationId,
    ) -> Result<Self, (TransferOperationError, TransferIntent)> {
        if operation == publication_operation
            || !intent.permits_operation(operation)
            || !intent.permits_operation(publication_operation)
            || intent.encode(MAX_TRANSFER_INTENT_BYTES).is_err()
        {
            return Err((TransferOperationError::InvalidPlan, intent));
        }
        Ok(Self {
            sources: intent.sources(),
            targets: intent.targets(),
            intent,
            operation,
            publication_operation,
        })
    }
    pub fn intent(&self) -> &TransferIntent {
        &self.intent
    }
    pub fn operation(&self) -> OperationId {
        self.operation
    }
    pub fn publication_operation(&self) -> OperationId {
        self.publication_operation
    }
    pub fn reads(&self) -> Vec<TransferRead> {
        let authority = self.intent.before().input().authority;
        let mut reads = vec![
            TransferRead {
                group: authority,
                kind: TransferReadKind::Intent,
            },
            TransferRead {
                group: authority,
                kind: TransferReadKind::Publication,
            },
        ];
        reads.extend(self.sources.iter().map(|r| TransferRead {
            group: route_group(r),
            kind: TransferReadKind::Source,
        }));
        reads.extend(self.targets.iter().map(|r| TransferRead {
            group: route_group(r),
            kind: TransferReadKind::Target,
        }));
        reads
    }
    pub fn next(
        &self,
        observations: &[TransferObservation],
        images: &[TransferImage<'_>],
    ) -> Result<TransferAction, TransferOperationError> {
        let state = State::new(self, observations)?;
        state.validate(self)?;
        if state.intent().is_none() {
            return Ok(TransferAction::RecordIntent);
        }
        for target in &state.targets {
            if target.target().staged_index.is_none() {
                return Ok(TransferAction::Stage(target.read.group));
            }
        }
        for source in &state.sources {
            if source.source().is_none() {
                return Ok(TransferAction::Fence(source.read.group));
            }
        }
        for target in &state.targets {
            if target.target().imported.is_none() {
                return self.import(target.read.group, &state, images);
            }
        }
        let Some(publication) = state.publication() else {
            let sources = state
                .sources
                .iter()
                .map(|s| s.source().unwrap().clone())
                .collect();
            let targets = state
                .targets
                .iter()
                .map(|t| {
                    crate::transfer_publication::TargetReadyEvidence::from_status(
                        t.configuration,
                        t.target().clone(),
                    )
                    .map_err(|e| TransferOperationError::Application(e.0))
                })
                .collect::<Result<_, _>>()?;
            let publication =
                TransferPublication::new(self.operation, self.intent.clone(), sources, targets)
                    .map_err(|e| TransferOperationError::Application(e.0))?;
            return Ok(TransferAction::Publish(publication));
        };
        for target in &state.targets {
            if target.target().activated.is_none() {
                return Ok(TransferAction::Activate {
                    target: target.read.group,
                    activation: TargetActivation {
                        metadata_configuration: state.publication_read.configuration,
                        decision: publication.clone(),
                    },
                });
            }
        }
        Ok(TransferAction::Complete)
    }
    fn import(
        &self,
        target: GroupIdentity,
        state: &State<'_>,
        images: &[TransferImage<'_>],
    ) -> Result<TransferAction, TransferOperationError> {
        validate_images(images)?;
        let mut selected = Vec::new();
        let target_scope = self
            .targets
            .iter()
            .find(|r| route_group(r) == target)
            .unwrap()
            .scope;
        let mut bytes = 0usize;
        for source in &state.sources {
            let proof = source.source().unwrap();
            let Some(export) = proof.exports.iter().find(|e| e.target == target) else {
                continue;
            };
            let scope = intersect(
                target_scope,
                self.sources
                    .iter()
                    .find(|r| route_group(r) == source.read.group)
                    .unwrap()
                    .scope,
            )
            .ok_or(TransferOperationError::Inconsistent)?;
            let request = TransferExport {
                source: source.read.group,
                target,
                fence: proof.fence,
                scope,
                digest: export.digest,
            };
            let Some(image) = images
                .iter()
                .find(|i| i.source == request.source && i.target == target)
            else {
                return Ok(TransferAction::Export(request));
            };
            if image.image.scope() != scope
                || image.image.source_applied() != proof.fence.index
                || image.image.scheme() != self.intent.before().input().scheme
                || ContentDigest::scope_image(image.image) != export.digest
            {
                return Err(TransferOperationError::WrongImage);
            }
            bytes = bytes
                .checked_add(image.image.payload_capacity())
                .ok_or(TransferOperationError::Budget)?;
            if bytes > MAX_INLINE_IMPORT_BYTES {
                return Err(TransferOperationError::Budget);
            }
            selected.push(SourceImport {
                fence: proof.fence,
                configuration: source.configuration,
                image: image.image.clone(),
                digest: export.digest,
            });
        }
        let request = TargetImport::new(self.operation, self.intent.clone(), target, selected)
            .map_err(|e| TransferOperationError::Application(e.0))?;
        request
            .encode(MAX_INLINE_IMPORT_BYTES)
            .map_err(TransferOperationError::Application)?;
        Ok(TransferAction::Import(request))
    }
}
fn validate_images(images: &[TransferImage<'_>]) -> Result<(), TransferOperationError> {
    if images.len() > MAX_OPERATION_IMAGES {
        return Err(TransferOperationError::Budget);
    }
    let mut seen = BTreeSet::new();
    for image in images {
        if !seen.insert((image.source, image.target)) {
            return Err(TransferOperationError::WrongImage);
        }
    }
    Ok(())
}
fn route_group(route: &RouteEntry) -> GroupIdentity {
    let RouteTarget::Group(group) = route.target else {
        unreachable!("checked concrete transfer")
    };
    group
}
fn intersect(a: BucketRange, b: BucketRange) -> Option<BucketRange> {
    BucketRange::new(a.start().max(b.start()), a.end().min(b.end())).ok()
}
