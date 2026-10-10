// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use crate::{
    raft::ReadBarrier, runtime::ReadOutcome, scoped_source::ScopedSourceRead,
    transfer_publication::SourceFenceEvidence, transfer_source::SourceRead,
};
#[derive(Clone, Debug)]
pub(super) enum ObservationValue {
    Intent(Option<TransferIntentStatus>),
    Publication(Option<TransferPublicationStatus>),
    Source(Option<SourceFenceEvidence>),
    Target(TargetStatus),
}
/// Derived from a successful completed Node read outcome. Pass the outcome from
/// Node::complete_read, not a local query or cached liveness hint. The caller
/// authenticates remote transport and serializes read ownership. Observations
/// must be refreshed after an action/unknown outcome, not saved as certificates.
#[derive(Clone, Debug)]
pub struct TransferObservation {
    pub(super) read: TransferRead,
    pub(super) configuration: ConfigurationId,
    pub(super) index: u64,
    pub(super) value: ObservationValue,
}
fn read<R>(outcome: &ReadOutcome<R>) -> Result<(&ReadBarrier, &R), TransferOperationError> {
    match outcome {
        ReadOutcome::Read {
            barrier,
            result: Ok(value),
        } if barrier.index() > 0 && barrier.term() > 0 => Ok((barrier, value)),
        _ => Err(TransferOperationError::NotRead),
    }
}
impl TransferObservation {
    fn new(barrier: &ReadBarrier, kind: TransferReadKind, value: ObservationValue) -> Self {
        Self {
            read: TransferRead {
                group: barrier.group(),
                kind,
            },
            configuration: barrier.configuration(),
            index: barrier.index(),
            value,
        }
    }
    pub fn intent_read(
        outcome: &ReadOutcome<DirectoryRead>,
    ) -> Result<Self, TransferOperationError> {
        let (b, value) = read(outcome)?;
        let DirectoryRead::Transfer(value) = value else {
            return Err(TransferOperationError::WrongReadType);
        };
        if let Some(v) = value {
            if v.index == 0 || v.index > b.index() {
                return Err(TransferOperationError::Inconsistent);
            }
            v.intent
                .encode(MAX_TRANSFER_INTENT_BYTES)
                .map_err(TransferOperationError::Application)?;
        }
        Ok(Self::new(
            b,
            TransferReadKind::Intent,
            ObservationValue::Intent(value.clone()),
        ))
    }
    pub fn publication_read(
        outcome: &ReadOutcome<DirectoryRead>,
    ) -> Result<Self, TransferOperationError> {
        let (b, value) = read(outcome)?;
        let DirectoryRead::Publication(value) = value else {
            return Err(TransferOperationError::WrongReadType);
        };
        if let Some(v) = value {
            if v.index > b.index() {
                return Err(TransferOperationError::Inconsistent);
            }
            v.encode(crate::transfer_publication::MAX_TRANSFER_PUBLICATION_BYTES)
                .map_err(TransferOperationError::Application)?;
        }
        Ok(Self::new(
            b,
            TransferReadKind::Publication,
            ObservationValue::Publication(value.clone()),
        ))
    }
    pub fn source_read<R>(
        outcome: &ReadOutcome<SourceRead<R>>,
    ) -> Result<Self, TransferOperationError> {
        let (b, value) = read(outcome)?;
        let SourceRead::Freeze(value) = value else {
            return Err(TransferOperationError::WrongReadType);
        };
        Self::source_status(b, value.as_ref())
    }
    pub fn target_source_read<R>(
        outcome: &ReadOutcome<TargetRead<R>>,
    ) -> Result<Self, TransferOperationError> {
        let (b, value) = read(outcome)?;
        let TargetRead::Freeze(value) = value else {
            return Err(TransferOperationError::WrongReadType);
        };
        Self::source_status(b, value.as_ref())
    }
    fn source_status(
        b: &ReadBarrier,
        value: Option<&crate::transfer_source::SourceFreezeStatus>,
    ) -> Result<Self, TransferOperationError> {
        let evidence = value
            .map(|v| {
                if v.fence.group != b.group()
                    || v.fence.index == 0
                    || v.fence.index > b.index()
                    || v.exports.capacity() > MAX_MANIFEST_ROUTES
                {
                    return Err(TransferOperationError::Inconsistent);
                }
                v.intent
                    .encode(MAX_TRANSFER_INTENT_BYTES)
                    .map_err(TransferOperationError::Application)?;
                SourceFenceEvidence::from_status(b.configuration(), v.clone())
                    .map_err(|e| TransferOperationError::Application(e.0))
            })
            .transpose()?;
        Ok(Self::new(
            b,
            TransferReadKind::Source,
            ObservationValue::Source(evidence),
        ))
    }
    pub fn scoped_source_read<R>(
        outcome: &ReadOutcome<ScopedSourceRead<R>>,
        operation: &TransferOperation,
    ) -> Result<Self, TransferOperationError> {
        let (b, value) = read(outcome)?;
        let ScopedSourceRead::Frozen(value) = value else {
            return Err(TransferOperationError::WrongReadType);
        };
        let evidence = value
            .map(|v| {
                if v.fence.fence.group != b.group()
                    || v.fence.fence.index == 0
                    || v.fence.fence.index > b.index()
                {
                    return Err(TransferOperationError::Inconsistent);
                }
                SourceFenceEvidence::from_scoped_status(b.configuration(), v, operation.intent())
                    .map_err(|e| TransferOperationError::Application(e.0))
            })
            .transpose()?;
        Ok(Self::new(
            b,
            TransferReadKind::Source,
            ObservationValue::Source(evidence),
        ))
    }
    pub fn target_read<R>(
        outcome: &ReadOutcome<TargetRead<R>>,
    ) -> Result<Self, TransferOperationError> {
        let (b, value) = read(outcome)?;
        let TargetRead::Status(value) = value else {
            return Err(TransferOperationError::WrongReadType);
        };
        if value.group != b.group()
            || value.staged_index.is_some_and(|i| i == 0 || i > b.index())
            || value
                .imported
                .as_ref()
                .is_some_and(|i| i.index > b.index() || i.sources.capacity() > MAX_MANIFEST_ROUTES)
            || value.activated.is_some_and(|i| i.index > b.index())
        {
            return Err(TransferOperationError::Inconsistent);
        }
        Ok(Self::new(
            b,
            TransferReadKind::Target,
            ObservationValue::Target(value.clone()),
        ))
    }
    pub fn read(&self) -> TransferRead {
        self.read
    }
    pub fn configuration(&self) -> ConfigurationId {
        self.configuration
    }
    pub fn index(&self) -> u64 {
        self.index
    }
    pub(super) fn intent(&self) -> Option<&TransferIntentStatus> {
        let ObservationValue::Intent(v) = &self.value else {
            unreachable!("checked read kind")
        };
        v.as_ref()
    }
    pub(super) fn publication(&self) -> Option<&TransferPublicationStatus> {
        let ObservationValue::Publication(v) = &self.value else {
            unreachable!("checked read kind")
        };
        v.as_ref()
    }
    pub(super) fn source(&self) -> Option<&SourceFenceEvidence> {
        let ObservationValue::Source(v) = &self.value else {
            unreachable!("checked read kind")
        };
        v.as_ref()
    }
    pub(super) fn target(&self) -> &TargetStatus {
        let ObservationValue::Target(v) = &self.value else {
            unreachable!("checked read kind")
        };
        v
    }
}
