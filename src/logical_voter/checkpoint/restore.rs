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
//! Direct decode of checkpoint v3. Every field is decoded once, checked
//! structurally, and the rebuilt state must re-encode to the exact input.
//! No retained record is replayed, so restore is linear in the checkpoint.
use super::*;
use codec::Decoder;
/// At most the durable and the newly published parent snapshot are retained.
const MAX_SNAPSHOTS: usize = 2;
impl<C: LogCodec + Clone, S: SnapshotCodec + Clone, W: WireCodec + Clone>
    LogicalApplication<C, S, W>
{
    pub(super) fn restore(&mut self, index: u64, bytes: &[u8]) -> Result<(), ApplicationError> {
        if bytes.len() > self.limits.max_child_checkpoint_bytes {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let mut d = Decoder::new(bytes);
        if d.take(8)? != self.checkpoint_magic()
            || d.blob(self.limits.max_child_checkpoint_bytes)? != self.configuration
            || d.u64()? != index
        {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let term = d.u64()?;
        if (index == 0) != (term == 0) {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let mut state = application::State::new(&self.child_bootstrap)?;
        state.applied = index;
        state.applied_term = term;
        state.retired_below = d.u128()?;
        state.snapshot_generation = d.u64()?;
        // Log positions of every retained entry, to check index/term order.
        let mut positions = BTreeMap::new();
        self.decode_records(&mut d, &mut state, &mut positions)?;
        self.decode_configurations(&mut d, &mut state, &mut positions)?;
        if positions
            .values()
            .zip(positions.values().skip(1))
            .any(|(a, b)| a > b)
        {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        self.decode_materialized(&mut d, &mut state)?;
        d.finish()?;
        self.validate_restored(&state)?;
        let mut next = self.clone();
        next.state = state;
        // The retained set must fit every budget admission enforces (G6):
        // reserving nothing re-runs the exact admission check on it.
        next.reserve(&[])
            .map_err(|_| ApplicationError::InvalidCheckpoint)?;
        next.validate_read_envelopes()?;
        if next.encode_state(self.limits.max_child_checkpoint_bytes)? != bytes {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        self.state = next.state;
        Ok(())
    }
    /// Records in ascending operation ID with their original receipts.
    fn decode_records(
        &self,
        d: &mut Decoder<'_>,
        state: &mut application::State,
        positions: &mut BTreeMap<u64, u64>,
    ) -> Result<(), ApplicationError> {
        let count = usize::try_from(d.u64()?)
            .ok()
            .filter(|n| *n <= self.limits.max_actions)
            .ok_or(ApplicationError::InvalidCheckpoint)?;
        for _ in 0..count {
            let operation = d.id128(OperationId::new)?;
            let first_index = d.u64()?;
            let first_term = d.u64()?;
            let preceding_configuration = d.id(ConfigurationId::new)?;
            let command = d.blob(self.limits.max_command_bytes)?.to_vec();
            if self.decode_command(&command)?.operation != operation
                || state.records.keys().next_back() >= Some(&operation)
                || !(1..=state.applied).contains(&first_index)
                || !(1..=state.applied_term).contains(&first_term)
                || positions.insert(first_index, first_term).is_some()
            {
                return Err(ApplicationError::InvalidCheckpoint);
            }
            let receipt = LogicalReceipt {
                index: first_index,
                operation,
                first_index,
                outcome: Self::decode_outcome(d)?,
                parent_revision: LogRevision::new(d.u64()?),
                parent_generation: LogGeneration::new(d.u64()?),
                epoch: RepresentativeEpoch::new(d.u64()?),
                duplicate: false,
            };
            state.bytes = state
                .bytes
                .checked_add(command.len())
                .ok_or(ApplicationError::InvalidCheckpoint)?;
            state.records.insert(
                operation,
                LogicalActionStatus {
                    operation,
                    first_index,
                    first_term,
                    preceding_configuration,
                    command,
                    receipt,
                },
            );
        }
        Ok(())
    }
    /// Configuration entries in log order; membership is rebuilt from them
    /// exactly as apply does, and each record's preceding configuration must
    /// be the membership in force immediately before its index.
    fn decode_configurations(
        &self,
        d: &mut Decoder<'_>,
        state: &mut application::State,
        positions: &mut BTreeMap<u64, u64>,
    ) -> Result<(), ApplicationError> {
        let count = usize::try_from(d.u64()?)
            .ok()
            .filter(|n| *n <= 3 * crate::membership::MAX_CONFIGURATION_OPERATIONS)
            .ok_or(ApplicationError::InvalidCheckpoint)?;
        let mut history = vec![(0, state.membership.id())];
        for _ in 0..count {
            let entry = self.configuration_entry(d.blob(self.journal_limits().max_batch_bytes)?)?;
            let EntryPayload::Configuration(record) = &entry.payload else {
                return Err(ApplicationError::InvalidCheckpoint);
            };
            if state.configurations.last().map_or(0, |e| e.index) >= entry.index
                || entry.index > state.applied
                || !(1..=state.applied_term).contains(&entry.term)
                || positions.insert(entry.index, entry.term).is_some()
            {
                return Err(ApplicationError::InvalidCheckpoint);
            }
            state.membership = state
                .membership
                .preview_next(entry.index, record, entry.index - 1)
                .map_err(|_| ApplicationError::InvalidCheckpoint)?;
            history.push((entry.index, state.membership.id()));
            state.configurations.push(entry);
        }
        for r in state.records.values() {
            let before = history.partition_point(|(index, _)| *index < r.first_index);
            if history[before - 1].1 != r.preceding_configuration {
                return Err(ApplicationError::InvalidCheckpoint);
            }
        }
        Ok(())
    }
    fn configuration_entry(&self, bytes: &[u8]) -> Result<LogEntry, ApplicationError> {
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
        Ok(suffix.entries.remove(0))
    }
    fn decode_materialized(
        &self,
        d: &mut Decoder<'_>,
        state: &mut application::State,
    ) -> Result<(), ApplicationError> {
        if self.candidate() && d.flag()? {
            let record = self.decode_enrollment(d)?;
            if self.initialization != LogicalInitialization::EnrollmentCandidate(record.plan) {
                return Err(ApplicationError::InvalidCheckpoint);
            }
            state.enrollment = Some(Box::new(record));
        }
        if d.flag()? {
            state.representative = Some(d.representative()?);
        }
        if d.flag()? {
            state.parent =
                Some(self.decode_parent(d.blob(self.limits.max_parent_checkpoint_bytes)?)?);
        }
        // The rebuilt membership is authoritative; the exact re-encode checks
        // that this blob is its canonical encoding.
        d.blob(self.limits.max_child_checkpoint_bytes)?;
        state.published = d.optional_reference()?;
        let count = usize::try_from(d.u64()?)
            .ok()
            .filter(|n| *n <= MAX_SNAPSHOTS)
            .ok_or(ApplicationError::InvalidCheckpoint)?;
        for _ in 0..count {
            let reference = d.reference()?;
            let snapshot = self
                .snapshot_codec
                .decode(
                    d.blob(self.limits.max_child_checkpoint_bytes)?,
                    self.limits.parent_snapshots,
                )
                .map_err(|_| ApplicationError::InvalidCheckpoint)?;
            self.verify_snapshot(reference, &snapshot)?;
            if state.snapshots.iter().any(|(r, _)| *r == reference) {
                return Err(ApplicationError::InvalidCheckpoint);
            }
            state.snapshots.push((reference, snapshot));
        }
        if d.u64()? != state.bytes as u64 {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        Ok(())
    }
    fn decode_parent(&self, bytes: &[u8]) -> Result<GroupLog, ApplicationError> {
        let (revision, mut groups, consumed) = self
            .log_codec
            .decode_checkpoint(bytes, self.limits.parent_log)
            .map_err(|_| ApplicationError::InvalidCheckpoint)?;
        let parent = groups
            .remove(&self.mapping.parent)
            .filter(|_| groups.is_empty() && consumed == bytes.len())
            .ok_or(ApplicationError::InvalidCheckpoint)?;
        if parent.revision.get() != revision {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        Ok(parent)
    }
    fn decode_outcome(d: &mut Decoder<'_>) -> Result<LogicalOutcome, ApplicationError> {
        Ok(match d.u8()? {
            0 => LogicalOutcome::Claimed,
            1 => LogicalOutcome::Mutated,
            2 => LogicalOutcome::SnapshotPublished(d.reference()?),
            3 => LogicalOutcome::SnapshotReconciled,
            4 => LogicalOutcome::CapsuleAuthorized,
            5 => LogicalOutcome::Refused(match d.u8()? {
                0 => LogicalRefusal::StaleRepresentative,
                1 => LogicalRefusal::StaleImage,
                2 => LogicalRefusal::Ineligible,
                3 => LogicalRefusal::MissingSnapshot,
                4 => LogicalRefusal::SnapshotConflict,
                5 => LogicalRefusal::InvalidResponse,
                6 => LogicalRefusal::OperationConflict,
                _ => return Err(ApplicationError::InvalidCheckpoint),
            }),
            _ => return Err(ApplicationError::InvalidCheckpoint),
        })
    }
    /// Cross-field rules. The last two are LV1 reachable-state rules: nothing
    /// is retired yet, so the watermark is 0 and the generation high-water
    /// mark equals the floor derived from retained records. Retirement (LV2)
    /// relaxes the latter to `>=`.
    fn validate_restored(&self, state: &application::State) -> Result<(), ApplicationError> {
        let epoch = state.representative.map_or(0, |r| r.epoch.get());
        let retained = |reference: Option<SnapshotRef>| {
            reference.is_none_or(|r| state.snapshots.iter().any(|(s, _)| *s == r))
        };
        if state.representative.is_some() != state.parent.is_some()
            || (self.candidate() && state.representative.is_some() != state.enrollment.is_some())
            || !retained(state.published)
            || !retained(state.parent.as_ref().and_then(|p| p.snapshot))
            || state
                .records
                .values()
                .any(|r| r.receipt.epoch.map_or(0, RepresentativeEpoch::get) > epoch)
            || state.retired_below != 0
            || state.snapshot_generation != Self::retained_generation_floor(state)
        {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        match &state.enrollment {
            Some(e) => self.validate_enrollment(state, e),
            None => Ok(()),
        }
    }
    /// The enrollment record is the materialised result of its retained
    /// `EnrollmentClaim` record. LV1 retains that record; LV2 may retire it
    /// and then checks only the fields the record itself carries.
    fn validate_enrollment(
        &self,
        state: &application::State,
        e: &LogicalEnrollmentRecord,
    ) -> Result<(), ApplicationError> {
        let record = state
            .records
            .get(&e.operation)
            .ok_or(ApplicationError::InvalidCheckpoint)?;
        let CommandBody::EnrollmentClaim(import) = self.decode_command(&record.command)?.body
        else {
            return Err(ApplicationError::InvalidCheckpoint);
        };
        if record.first_index != e.first_index
            || record.first_term != e.first_term
            || record.receipt.outcome != LogicalOutcome::Claimed
            || import.plan != e.plan
            || import.origin != e.origin
            || import.learners != e.learners
            || self.snapshot_reference(&import.snapshot, SnapshotGeneration::new(1).unwrap())?
                != e.target_reference
        {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        Ok(())
    }
}
