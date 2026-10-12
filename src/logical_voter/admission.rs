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
//! Admission reserves original content including pending commands and control credit.
use super::*;
impl<C: LogCodec + Clone, S: SnapshotCodec + Clone, W: WireCodec + Clone>
    LogicalApplication<C, S, W>
{
    pub(super) fn reserve(
        &self,
        commands: &[(OperationId, &[u8], bool)],
    ) -> Result<(), ApplicationError> {
        let mut distinct = BTreeMap::new();
        let mut ordinary = 0usize;
        let mut ordinary_bytes = 0usize;
        for old in self.state.records.values() {
            if !self
                .decode_command(&old.command)?
                .control(self.control_profile.as_ref())
            {
                ordinary += 1;
                ordinary_bytes += old.command.len();
            }
        }
        for (id, bytes, control) in commands {
            if let Some(old) = self.state.records.get(id) {
                if old.command != *bytes {
                    return Err(ApplicationError::OperationConflict);
                }
                continue;
            }
            if let Some((old, _)) = distinct.insert(*id, (*bytes, *control)) {
                if old != *bytes {
                    return Err(ApplicationError::OperationConflict);
                }
            }
        }
        let mut total = self.state.bytes;
        for (bytes, control) in distinct.values() {
            total = total
                .checked_add(bytes.len())
                .ok_or(ApplicationError::DedupCapacity)?;
            if !control {
                ordinary += 1;
                ordinary_bytes = ordinary_bytes
                    .checked_add(bytes.len())
                    .ok_or(ApplicationError::DedupCapacity)?;
            }
        }
        if self.state.records.len() + distinct.len() > self.limits.max_actions
            || total > self.limits.max_retained_action_bytes
            || ordinary > self.limits.max_actions - self.limits.control_actions
            || ordinary_bytes
                > self.limits.max_retained_action_bytes - self.limits.control_action_bytes
        {
            return Err(ApplicationError::DedupCapacity);
        }
        Ok(())
    }
}
impl<C: LogCodec + Clone, S: SnapshotCodec + Clone, W: WireCodec + Clone> ProposalAdmission
    for LogicalApplication<C, S, W>
{
    fn validate_proposal_context(
        &self,
        core: &crate::raft::Raft,
        operation: OperationId,
        bytes: &[u8],
    ) -> Result<(), ApplicationError> {
        let c = self.decode_command(bytes)?;
        if c.operation != operation || core.state().bootstrap.group != self.mapping.child {
            return Err(ApplicationError::InvalidCommand);
        }
        if let Some(old) = self.state.records.get(&operation) {
            return if old.command == bytes {
                Ok(())
            } else {
                Err(ApplicationError::OperationConflict)
            };
        }
        if core
            .membership()
            .voter_store(c.representative.physical.node)
            != Some(c.representative.physical.store)
        {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(())
    }
    fn validate_proposal<'a>(
        &self,
        operation: OperationId,
        bytes: &[u8],
        pending: impl Iterator<Item = (OperationId, &'a [u8])>,
    ) -> Result<usize, ApplicationError> {
        let c = self.decode_command(bytes)?;
        if c.operation != operation {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut distinct = BTreeMap::new();
        for (id, bytes) in pending {
            let c = self.decode_command(bytes)?;
            if c.operation != id {
                return Err(ApplicationError::InvalidCommand);
            }
            if let Some(old) = self.state.records.get(&id) {
                if old.command != bytes {
                    return Err(ApplicationError::OperationConflict);
                }
                continue;
            }
            if let Some((old, _)) =
                distinct.insert(id, (bytes, c.control(self.control_profile.as_ref())))
            {
                if old != bytes {
                    return Err(ApplicationError::OperationConflict);
                }
            }
            if distinct.len() > self.limits.max_actions {
                return Err(ApplicationError::DedupCapacity);
            }
        }
        let mut commands: Vec<_> = distinct
            .into_iter()
            .map(|(id, (bytes, control))| (id, bytes, control))
            .collect();
        commands.push((operation, bytes, c.control(self.control_profile.as_ref())));
        self.reserve(&commands)?;
        Ok(std::mem::size_of::<LogicalReceipt>())
    }
}
