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
impl<C: LogCodec + Clone, S: SnapshotCodec + Clone, W: WireCodec + Clone>
    LogicalApplication<C, S, W>
{
    pub(super) fn response_value(
        &self,
        e: &mut Encoder,
        value: &LogicalRead,
    ) -> Result<(), ApplicationError> {
        match value {
            LogicalRead::Enrollment(view) => {
                if !self.candidate() {
                    return Err(ApplicationError::UnsupportedSchema);
                }
                e.u8(6)?;
                e.enrollment_plan(view.plan)?;
                e.u8(u8::from(view.record.is_some()))?;
                if let Some(record) = &view.record {
                    self.encode_enrollment(e, record)?;
                }
                Ok(())
            }
            LogicalRead::Image(i) => {
                e.u8(0)?;
                self.response_image(e, i)
            }
            LogicalRead::Outcome(status) => {
                e.u8(1)?;
                e.u8(u8::from(status.is_some()))?;
                if let Some(s) = status {
                    if s.command.len() > self.limits.max_command_bytes {
                        return Err(ApplicationError::ReceiptBudget);
                    }
                    e.u128(s.operation.get())?;
                    e.u64(s.first_index)?;
                    e.u64(s.first_term)?;
                    e.u64(s.preceding_configuration.get())?;
                    e.blob(&s.command)?;
                    atoms::receipt(e, &s.receipt)?;
                }
                Ok(())
            }
            LogicalRead::Snapshot(s) => {
                e.u8(2)?;
                self.response_snapshot(e, s)
            }
            LogicalRead::Metadata(m) => {
                e.u8(3)?;
                atoms::metadata(e, m)
            }
            LogicalRead::Uninitialized => e.u8(4),
            LogicalRead::ConfigurationOperation(h) => self.response_configuration_operation(e, h),
        }
    }
    fn response_image(&self, e: &mut Encoder, i: &LogicalImage) -> Result<(), ApplicationError> {
        if i.pins.len() > 2 {
            return Err(ApplicationError::ReceiptBudget);
        }
        e.mapping(i.mapping)?;
        e.representative(i.representative)?;
        let groups = BTreeMap::from([(i.mapping.parent, i.parent.clone())]);
        let bytes = self
            .log_codec
            .encode_checkpoint(
                i.parent.revision.get(),
                &groups,
                self.limits.parent_log,
                self.limits.max_parent_checkpoint_bytes,
            )
            .map_err(|_| ApplicationError::InvalidCheckpoint)?;
        if bytes.len() > self.limits.max_parent_checkpoint_bytes {
            return Err(ApplicationError::ReceiptBudget);
        }
        let (sequence, decoded, consumed) = self
            .log_codec
            .decode_checkpoint(&bytes, self.limits.parent_log)
            .map_err(|_| ApplicationError::InvalidCheckpoint)?;
        if sequence != i.parent.revision.get() || consumed != bytes.len() || decoded != groups {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        e.u64(u64::from(self.log_codec.format_version()))?;
        e.blob(&bytes)?;
        let bytes = NativeLogCodec::encode_membership(
            &i.child_membership,
            self.limits.max_parent_checkpoint_bytes,
        )
        .map_err(|_| ApplicationError::InvalidCheckpoint)?;
        if NativeLogCodec::decode_membership(&bytes, i.child_applied)
            .map_err(|_| ApplicationError::InvalidCheckpoint)?
            != i.child_membership
        {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        e.u64(u64::from(NativeLogCodec.format_version()))?;
        e.blob(&bytes)?;
        e.u64(i.child_applied)?;
        e.optional_reference(i.published)?;
        e.u64(i.pins.len() as u64)?;
        for r in &i.pins {
            e.reference(*r)?;
        }
        Ok(())
    }
    pub(super) fn response_snapshot(
        &self,
        e: &mut Encoder,
        s: &Snapshot,
    ) -> Result<(), ApplicationError> {
        if s.application.len() > self.limits.parent_snapshots.max_application_bytes {
            return Err(ApplicationError::ReceiptBudget);
        }
        let bytes = self.snapshot_bytes(s)?;
        if bytes.len() > self.limits.parent_snapshots.max_file_bytes()
            || bytes.len() > self.limits.max_parent_checkpoint_bytes
        {
            return Err(ApplicationError::ReceiptBudget);
        }
        e.u64(u64::from(self.snapshot_codec.format_version()))?;
        e.blob(&bytes)
    }
}
