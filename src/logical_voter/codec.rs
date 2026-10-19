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
//! Named bounded framing around the selected existing consensus formats.
use super::*;
mod atoms;
mod commands;
mod enrollment;
mod origins;
pub(super) use atoms::{Decoder, Encoder};
pub const LOGICAL_APPLICATION_SCHEMA: u64 = 5;
/// Command format v3 (ordinary): carries `consumed_through`.
const MAGIC: &[u8; 8] = b"VBLOGA03";
/// Command format v3 (enrollment candidate).
const CANDIDATE_MAGIC: &[u8; 8] = b"VBLOGA04";
impl<C: LogCodec + Clone, S: SnapshotCodec + Clone, W: WireCodec + Clone>
    LogicalApplication<C, S, W>
{
    pub(crate) fn encode_command(
        &self,
        c: &Command,
        max_bytes: usize,
    ) -> Result<Vec<u8>, ApplicationError> {
        let bytes = self.command_bytes(c, max_bytes)?;
        if self.decode_command(&bytes)? != *c {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(bytes)
    }
    /// Encoding without the decode check, so tests can build commands that
    /// violate the partition rules.
    pub(super) fn command_bytes(
        &self,
        c: &Command,
        max_bytes: usize,
    ) -> Result<Vec<u8>, ApplicationError> {
        let mut e = Encoder::new(max_bytes.min(self.limits.max_command_bytes));
        e.put(if self.candidate() {
            CANDIDATE_MAGIC
        } else {
            MAGIC
        })?;
        e.mapping(self.mapping)?;
        e.u128(c.operation.get())?;
        e.u64(c.origin_configuration.get())?;
        e.representative(c.representative)?;
        e.u64(c.consumed_through)?;
        self.encode_body(&mut e, &c.body)?;
        Ok(e.bytes)
    }
    pub(crate) fn decode_command(&self, bytes: &[u8]) -> Result<Command, ApplicationError> {
        if bytes.len() > self.limits.max_command_bytes {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut d = Decoder::new(bytes);
        if d.take(8)?
            != if self.candidate() {
                CANDIDATE_MAGIC
            } else {
                MAGIC
            }
            || d.mapping()? != self.mapping
        {
            return Err(ApplicationError::InvalidCommand);
        }
        let operation = d.id128(OperationId::new)?;
        let origin_configuration = d.id(ConfigurationId::new)?;
        let representative = d.representative()?;
        let consumed_through = d.u64()?;
        let body = self.decode_body(&mut d, representative)?;
        d.finish()?;
        let command = Command {
            operation,
            origin_configuration,
            representative,
            consumed_through,
            body,
        };
        // Every holder of command bytes (admission, apply, restore, bridge)
        // sees only partitioned commands.
        if !command.partitioned() {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(command)
    }
    pub(crate) fn snapshot_bytes(&self, snapshot: &Snapshot) -> Result<Vec<u8>, ApplicationError> {
        let mut bytes = self
            .snapshot_codec
            .prefix(
                &snapshot.metadata,
                snapshot.application.len(),
                self.limits.parent_snapshots,
            )
            .map_err(|_| ApplicationError::InvalidCommand)?;
        bytes.extend_from_slice(&snapshot.application);
        let checksum = self
            .snapshot_codec
            .finish(&bytes, self.limits.parent_snapshots)
            .map_err(|_| ApplicationError::InvalidCommand)?;
        bytes.extend_from_slice(&checksum);
        if self
            .snapshot_codec
            .decode(&bytes, self.limits.parent_snapshots)
            .map_err(|_| ApplicationError::InvalidCommand)?
            != *snapshot
        {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(bytes)
    }
    pub(crate) fn snapshot_reference(
        &self,
        snapshot: &Snapshot,
        generation: SnapshotGeneration,
    ) -> Result<SnapshotRef, ApplicationError> {
        let bytes = self.snapshot_bytes(snapshot)?;
        let checksum = u32::from_le_bytes(bytes[bytes.len() - 4..].try_into().unwrap());
        Ok(SnapshotRef {
            store: self.mapping.store,
            group: snapshot.metadata.bootstrap.group,
            generation,
            configuration: snapshot.metadata.configuration(),
            index: snapshot.metadata.index,
            term: snapshot.metadata.term,
            application_schema: snapshot.metadata.application_schema,
            file_bytes: bytes.len() as u64,
            checksum,
        })
    }
    pub(crate) fn verify_snapshot(
        &self,
        reference: SnapshotRef,
        snapshot: &Snapshot,
    ) -> Result<(), ApplicationError> {
        if reference.store != self.mapping.store
            || reference.group != self.mapping.parent
            || self.snapshot_reference(snapshot, reference.generation)? != reference
        {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        Ok(())
    }
    pub(super) fn journal_limits(&self) -> LogLimits {
        let max_batch_bytes = self
            .limits
            .max_child_checkpoint_bytes
            .min(self.limits.parent_log.max_wal_bytes);
        LogLimits {
            max_batch_bytes,
            max_command_bytes: max_batch_bytes,
            ..self.limits.parent_log
        }
    }
    pub(super) fn configuration_bytes(&self) -> Result<Vec<u8>, ApplicationError> {
        let mut e = Encoder::new(self.limits.max_child_checkpoint_bytes);
        if let LogicalInitialization::EnrollmentCandidate(plan) = self.initialization {
            e.put(b"VBENRL02")?;
            e.enrollment_plan(plan)?;
        }
        e.mapping(self.mapping)?;
        for (bootstrap, limits) in [
            (&self.child_bootstrap, self.journal_limits()),
            (&self.parent_bootstrap, self.limits.parent_log),
        ] {
            let bytes = self
                .log_codec
                .encode_batch(1, &[LogMutation::Create(bootstrap.clone())], limits)
                .map_err(|_| ApplicationError::InvalidCommand)?;
            e.blob(&bytes)?;
        }
        let l = self.limits;
        let p = l.parent_log;
        let s = l.parent_snapshots;
        for n in [
            p.max_groups,
            p.max_entries_per_group,
            p.max_command_bytes,
            p.max_snapshot_bytes,
            p.max_batch_bytes,
            p.max_batch_units,
            p.max_pending_units,
            p.max_wal_bytes,
            p.control_reserve_bytes,
            s.max_application_bytes,
            s.max_metadata_bytes,
            s.max_chunk_bytes,
            l.max_command_bytes,
            l.max_actions,
            l.max_retained_action_bytes,
            l.control_actions,
            l.control_action_bytes,
            l.max_capsule_bytes,
            l.max_parent_checkpoint_bytes,
            l.max_child_checkpoint_bytes,
        ] {
            e.u64(n as u64)?;
        }
        e.u64(self.log_codec.format_version() as u64)?;
        e.u64(self.snapshot_codec.format_version() as u64)?;
        e.u64(self.wire_codec.format_version() as u64)?;
        let w = self.wire_codec.limits();
        for n in [
            w.max_frame_bytes,
            w.max_messages,
            w.max_entries_per_message,
            w.max_command_bytes,
            w.max_snapshot_bytes,
            w.max_decoded_bytes,
            w.policy.max_depth,
            w.policy.max_voters,
            w.policy.max_tree_nodes,
        ] {
            e.u64(n as u64)?;
        }
        if let Some(profile) = &self.control_profile {
            e.put(b"VBACTP02")?;
            e.blob(&profile.configuration_bytes(self.limits.max_child_checkpoint_bytes)?)?;
        }
        Ok(e.bytes)
    }
}
