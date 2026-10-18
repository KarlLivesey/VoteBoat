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
//! Versioned checkpoint v3: the materialised child state, restored by direct
//! decode and verified by exact canonical re-encoding (no record replay).
use super::*;
use codec::Encoder;
mod restore;
/// Ordinary checkpoint v3 (application schema 3).
const CHECKPOINT: &[u8; 8] = b"VBLOGC03";
/// Enrollment-candidate checkpoint v3 (application schema 4).
const CANDIDATE_CHECKPOINT: &[u8; 8] = b"VBLOGC04";
impl<C: LogCodec + Clone, S: SnapshotCodec + Clone, W: WireCodec + Clone>
    LogicalApplication<C, S, W>
{
    fn checkpoint_magic(&self) -> &'static [u8; 8] {
        if self.candidate() {
            CANDIDATE_CHECKPOINT
        } else {
            CHECKPOINT
        }
    }
    pub(super) fn encode_state(&self, max: usize) -> Result<Vec<u8>, ApplicationError> {
        #[cfg(test)]
        application::count_encode();
        let mut e = Encoder::new(max.min(self.limits.max_child_checkpoint_bytes));
        e.put(self.checkpoint_magic())?;
        e.blob(&self.configuration)?;
        e.u64(self.state.applied)?;
        e.u64(self.state.applied_term)?;
        e.u128(self.state.retired_below)?;
        e.u64(self.state.snapshot_generation)?;
        e.u64(self.state.records.len() as u64)?;
        for r in self.state.records.values() {
            e.u128(r.operation.get())?;
            e.u64(r.first_index)?;
            e.u64(r.first_term)?;
            e.u64(r.preceding_configuration.get())?;
            e.blob(&r.command)?;
            self.encode_outcome(&mut e, r.receipt.outcome)?;
            e.u64(r.receipt.parent_revision.map_or(0, LogRevision::get))?;
            e.u64(r.receipt.parent_generation.map_or(0, LogGeneration::get))?;
            e.u64(r.receipt.epoch.map_or(0, RepresentativeEpoch::get))?;
        }
        e.u64(self.state.configurations.len() as u64)?;
        for entry in &self.state.configurations {
            e.blob(&self.entry_bytes(entry)?)?;
        }
        self.materialized(&mut e)?;
        Ok(e.bytes)
    }
    fn materialized(&self, e: &mut Encoder) -> Result<(), ApplicationError> {
        if self.candidate() {
            e.u8(u8::from(self.state.enrollment.is_some()))?;
            if let Some(record) = &self.state.enrollment {
                self.encode_enrollment(e, record)?;
            }
        }
        e.u8(u8::from(self.state.representative.is_some()))?;
        if let Some(r) = self.state.representative {
            e.representative(r)?;
        }
        e.u8(u8::from(self.state.parent.is_some()))?;
        if let Some(parent) = &self.state.parent {
            e.blob(&self.parent_bytes(parent)?)?;
        }
        e.blob(
            &NativeLogCodec::encode_membership(
                &self.state.membership,
                self.limits.max_child_checkpoint_bytes,
            )
            .map_err(|_| ApplicationError::InvalidCheckpoint)?,
        )?;
        e.optional_reference(self.state.published)?;
        e.u64(self.state.snapshots.len() as u64)?;
        for (reference, snapshot) in &self.state.snapshots {
            e.reference(*reference)?;
            e.blob(&self.snapshot_bytes(snapshot)?)?;
        }
        e.u64(self.state.bytes as u64)
    }
    /// Canonical parent image bytes, verified to decode back to the same image.
    fn parent_bytes(&self, parent: &GroupLog) -> Result<Vec<u8>, ApplicationError> {
        let groups = BTreeMap::from([(self.mapping.parent, parent.clone())]);
        let bytes = self
            .log_codec
            .encode_checkpoint(
                parent.revision.get(),
                &groups,
                self.limits.parent_log,
                self.limits.max_parent_checkpoint_bytes,
            )
            .map_err(|_| ApplicationError::InvalidCheckpoint)?;
        let (_, decoded, consumed) = self
            .log_codec
            .decode_checkpoint(&bytes, self.limits.parent_log)
            .map_err(|_| ApplicationError::InvalidCheckpoint)?;
        if consumed != bytes.len() || decoded != groups {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        Ok(bytes)
    }
    fn encode_outcome(
        &self,
        e: &mut Encoder,
        outcome: LogicalOutcome,
    ) -> Result<(), ApplicationError> {
        match outcome {
            LogicalOutcome::Claimed => e.u8(0),
            LogicalOutcome::Mutated => e.u8(1),
            LogicalOutcome::SnapshotPublished(r) => {
                e.u8(2)?;
                e.reference(r)
            }
            LogicalOutcome::SnapshotReconciled => e.u8(3),
            LogicalOutcome::CapsuleAuthorized => e.u8(4),
            LogicalOutcome::Refused(r) => {
                e.u8(5)?;
                e.u8(match r {
                    LogicalRefusal::StaleRepresentative => 0,
                    LogicalRefusal::StaleImage => 1,
                    LogicalRefusal::Ineligible => 2,
                    LogicalRefusal::MissingSnapshot => 3,
                    LogicalRefusal::SnapshotConflict => 4,
                    LogicalRefusal::InvalidResponse => 5,
                    LogicalRefusal::OperationConflict => 6,
                })
            }
        }
    }
    fn entry_bytes(&self, entry: &LogEntry) -> Result<Vec<u8>, ApplicationError> {
        self.log_codec
            .encode_batch(
                1,
                &[LogMutation::Update(LogUpdate {
                    group: self.mapping.child,
                    expected_revision: LogRevision::new(1).unwrap(),
                    hard_state: crate::contracts::HardState::default(),
                    commit_index: 0,
                    suffix: Some(Suffix {
                        from: entry.index,
                        entries: vec![entry.clone()],
                    }),
                    snapshot: None,
                    snapshot_membership: None,
                })],
                self.journal_limits(),
            )
            .map_err(|_| ApplicationError::InvalidCheckpoint)
    }
    /// The generation floor implied by retained state: every generation named
    /// by a retained outcome, snapshot, published reference or enrollment
    /// target. Before retirement this equals the high-water mark exactly.
    pub(super) fn retained_generation_floor(state: &application::State) -> u64 {
        state
            .records
            .values()
            .filter_map(|s| match s.receipt.outcome {
                LogicalOutcome::SnapshotPublished(r) => Some(r.generation.get()),
                _ => None,
            })
            .chain(state.snapshots.iter().map(|(r, _)| r.generation.get()))
            .chain(state.published.iter().map(|r| r.generation.get()))
            .chain(
                state
                    .enrollment
                    .iter()
                    .map(|r| r.target_reference.generation.get()),
            )
            .max()
            .unwrap_or(0)
    }
}
impl<C: LogCodec + Clone, S: SnapshotCodec + Clone, W: WireCodec + Clone> CheckpointStateMachine
    for LogicalApplication<C, S, W>
{
    fn schema_version(&self) -> u64 {
        self.application_schema()
    }
    fn checkpoint(&self, max_bytes: usize) -> Result<Vec<u8>, ApplicationError> {
        self.encode_state(max_bytes)
            // Exported ownership must fit the advertised image allocation bound.
            .map(|bytes| bytes.into_boxed_slice().into_vec())
            .map_err(|_| ApplicationError::InvalidCheckpoint)
    }
    fn restore_checkpoint(
        &mut self,
        schema: u64,
        index: u64,
        bytes: &[u8],
    ) -> Result<(), ApplicationError> {
        if schema != self.application_schema() {
            return Err(ApplicationError::UnsupportedSchema);
        }
        self.restore(index, bytes)
            .map_err(|_| ApplicationError::InvalidCheckpoint)
    }
}
