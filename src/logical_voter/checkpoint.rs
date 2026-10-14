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
//! Versioned original-position replay plus exact materialized-state comparison.
use super::*;
use codec::{Decoder, Encoder};
const CHECKPOINT: &[u8; 8] = b"VBLOGC01";
impl<C: LogCodec + Clone, S: SnapshotCodec + Clone, W: WireCodec + Clone>
    LogicalApplication<C, S, W>
{
    pub(super) fn encode_state(&self, max: usize) -> Result<Vec<u8>, ApplicationError> {
        let mut e = Encoder::new(max.min(self.limits.max_child_checkpoint_bytes));
        e.put(if self.candidate() {
            b"VBLOGC02"
        } else {
            CHECKPOINT
        })?;
        e.blob(&self.configuration)?;
        e.u64(self.state.applied)?;
        e.u64(self.state.applied_term)?;
        e.u64(self.state.records.len() as u64)?;
        let mut records: Vec<_> = self.state.records.values().collect();
        records.sort_by_key(|r| r.first_index);
        for r in records {
            e.u64(r.first_index)?;
            e.u64(r.first_term)?;
            e.blob(&r.command)?;
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
            e.blob(&bytes)?;
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
        e.u64(self.state.bytes as u64)?;
        for record in self.state.records.values() {
            e.u128(record.operation.get())?;
            e.u64(record.preceding_configuration.get())?;
            self.encode_outcome(e, record.receipt.outcome)?;
            e.u64(record.receipt.parent_revision.map_or(0, LogRevision::get))?;
            e.u64(
                record
                    .receipt
                    .parent_generation
                    .map_or(0, LogGeneration::get),
            )?;
            e.u64(record.receipt.epoch.map_or(0, RepresentativeEpoch::get))?;
        }
        Ok(())
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
    fn decode_history(&self, d: &mut Decoder<'_>) -> Result<Vec<LogEntry>, ApplicationError> {
        let count = usize::try_from(d.u64()?)
            .ok()
            .filter(|n| *n <= self.limits.max_actions)
            .ok_or(ApplicationError::InvalidCheckpoint)?;
        let mut entries = BTreeMap::new();
        for _ in 0..count {
            let index = d.u64()?;
            let term = d.u64()?;
            let bytes = d.blob(self.limits.max_command_bytes)?.to_vec();
            let c = self.decode_command(&bytes)?;
            if entries
                .insert(
                    index,
                    LogEntry {
                        index,
                        term,
                        payload: EntryPayload::Command {
                            operation: c.operation,
                            bytes: bytes.into(),
                        },
                    },
                )
                .is_some()
            {
                return Err(ApplicationError::InvalidCheckpoint);
            }
        }
        let count = usize::try_from(d.u64()?)
            .ok()
            .filter(|n| *n <= 3 * crate::membership::MAX_CONFIGURATION_OPERATIONS)
            .ok_or(ApplicationError::InvalidCheckpoint)?;
        for _ in 0..count {
            let bytes = d.blob(self.journal_limits().max_batch_bytes)?;
            let (sequence, mut mutations) = self
                .log_codec
                .decode_batch(bytes, self.journal_limits())
                .map_err(|_| ApplicationError::InvalidCheckpoint)?;
            if sequence != 1 || mutations.len() != 1 {
                return Err(ApplicationError::InvalidCheckpoint);
            }
            let LogMutation::Update(update) = mutations.remove(0) else {
                return Err(ApplicationError::InvalidCheckpoint);
            };
            let Some(mut suffix) = update.suffix else {
                return Err(ApplicationError::InvalidCheckpoint);
            };
            if update.group != self.mapping.child || suffix.entries.len() != 1 {
                return Err(ApplicationError::InvalidCheckpoint);
            }
            let entry = suffix.entries.remove(0);
            if !matches!(entry.payload, EntryPayload::Configuration(_))
                || entries.insert(entry.index, entry).is_some()
            {
                return Err(ApplicationError::InvalidCheckpoint);
            }
        }
        Ok(entries.into_values().collect())
    }
    fn restore(&mut self, index: u64, bytes: &[u8]) -> Result<(), ApplicationError> {
        if bytes.len() > self.limits.max_child_checkpoint_bytes {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let mut d = Decoder::new(bytes);
        if d.take(8)?
            != if self.candidate() {
                b"VBLOGC02"
            } else {
                CHECKPOINT
            }
            || d.blob(self.limits.max_child_checkpoint_bytes)? != self.configuration
            || d.u64()? != index
        {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let final_term = d.u64()?;
        if (index == 0) != (final_term == 0) {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let entries = self.decode_history(&mut d)?;
        let mut next = self.clone();
        next.state = application::State::new(&self.child_bootstrap)?;
        for entry in entries {
            if entry.index == 0 || entry.index > index || entry.term == 0 {
                return Err(ApplicationError::InvalidCheckpoint);
            }
            next.state.applied = entry.index - 1;
            next.apply_entries(&[entry])?;
        }
        if final_term < next.state.applied_term {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        next.state.applied = index;
        next.state.applied_term = final_term;
        if next.encode_state(self.limits.max_child_checkpoint_bytes)? != bytes {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        self.state = next.state;
        Ok(())
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
