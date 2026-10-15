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
use codec::Decoder;
impl<C: LogCodec + Clone, S: SnapshotCodec + Clone, W: WireCodec + Clone>
    LogicalApplication<C, S, W>
{
    pub(super) fn response_configuration_operation(
        &self,
        e: &mut Encoder,
        h: &LogicalConfigurationOperation,
    ) -> Result<(), ApplicationError> {
        self.validate_configuration_operation(h)?;
        e.u8(5)?;
        e.group(h.child)?;
        e.u128(h.operation.get())?;
        e.u64(h.child_applied)?;
        e.u64(h.entries.len() as u64)?;
        e.u64(u64::from(self.log_codec.format_version()))?;
        for entry in &h.entries {
            e.blob(&self.configuration_entry_bytes(entry)?)?;
        }
        Ok(())
    }
    fn configuration_update(&self, entry: &LogEntry) -> LogMutation {
        LogMutation::Update(LogUpdate {
            group: self.mapping.child,
            expected_revision: LogRevision::new(1).unwrap(),
            hard_state: crate::contracts::HardState::default(),
            commit_index: 0,
            suffix: Some(Suffix {
                from: entry.index,
                entries: vec![entry.clone()].into(),
            }),
            snapshot: None,
            snapshot_membership: None,
        })
    }
    fn configuration_codec_limits(&self) -> LogLimits {
        LogLimits {
            max_batch_bytes: self
                .journal_limits()
                .max_batch_bytes
                .min(self.limits.max_parent_checkpoint_bytes),
            ..self.journal_limits()
        }
    }
    fn configuration_entry_bytes(&self, entry: &LogEntry) -> Result<Vec<u8>, ApplicationError> {
        let mutation = self.configuration_update(entry);
        let bytes = self
            .log_codec
            .encode_batch(
                1,
                std::slice::from_ref(&mutation),
                self.configuration_codec_limits(),
            )
            .map_err(|_| ApplicationError::InvalidCommand)?;
        if bytes.len() > self.configuration_codec_limits().max_batch_bytes {
            return Err(ApplicationError::ReceiptBudget);
        }
        let decoded = self.decode_configuration_entry(&bytes)?;
        if decoded != *entry {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(bytes)
    }
    fn decode_configuration_entry(&self, bytes: &[u8]) -> Result<LogEntry, ApplicationError> {
        let (sequence, mut mutations) = self
            .log_codec
            .decode_batch(bytes, self.configuration_codec_limits())
            .map_err(|_| ApplicationError::InvalidCommand)?;
        if sequence != 1 || mutations.len() != 1 {
            return Err(ApplicationError::InvalidCommand);
        }
        let LogMutation::Update(update) = mutations.remove(0) else {
            return Err(ApplicationError::InvalidCommand);
        };
        let Some(suffix) = &update.suffix else {
            return Err(ApplicationError::InvalidCommand);
        };
        if suffix.entries.len() != 1 {
            return Err(ApplicationError::InvalidCommand);
        }
        let entry = suffix.entries[0].clone();
        if LogMutation::Update(update) != self.configuration_update(&entry) {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(entry)
    }
    /// Decode a bounded retained-history response. This returns inspection data,
    /// never a quorum-read observation or configuration authorization.
    pub fn decode_configuration_operation(
        &self,
        bytes: &[u8],
    ) -> Result<LogicalConfigurationOperation, ApplicationError> {
        if bytes.len() > self.limits.max_parent_checkpoint_bytes {
            return Err(ApplicationError::ReceiptBudget);
        }
        let mut d = Decoder::new(bytes);
        if d.take(8)? != MAGIC || d.u8()? != 2 || d.u8()? != 5 {
            return Err(ApplicationError::InvalidCommand);
        }
        let child = d.group()?;
        let operation = d.id128(OperationId::new)?;
        let child_applied = d.u64()?;
        let count = usize::try_from(d.u64()?)
            .ok()
            .filter(|n| *n <= 2)
            .ok_or(ApplicationError::InvalidCommand)?;
        if d.u64()? != u64::from(self.log_codec.format_version()) {
            return Err(ApplicationError::UnsupportedSchema);
        }
        let mut entries = Vec::with_capacity(count);
        for _ in 0..count {
            entries.push(self.decode_configuration_entry(
                d.blob(self.configuration_codec_limits().max_batch_bytes)?,
            )?);
        }
        d.finish()?;
        let h = LogicalConfigurationOperation {
            child,
            operation,
            child_applied,
            entries,
        };
        self.validate_configuration_operation(&h)?;
        Ok(h)
    }
}
