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
use crate::{
    application::{ProposalAdmission, StateMachine},
    identity::OperationId,
    raft::Raft,
};
use std::{collections::BTreeMap, mem::size_of};

impl TransferApplication {
    pub(super) fn check_binding(
        &self,
        spec: TransferSpec,
        existing: impl Iterator<Item = TransferSpec>,
    ) -> Result<(), ApplicationError> {
        for old in existing {
            if old.id() == spec.id() {
                if old != spec {
                    return Err(ApplicationError::OperationConflict);
                }
                continue;
            }
            let groups = match self.role {
                TransferRole::Coordinator => vec![
                    self.profile.coordinator(),
                    self.profile.participants()[0],
                    self.profile.participants()[1],
                ],
                TransferRole::Participant(_) => vec![self.group],
            };
            for group in groups {
                if ids(spec, group)
                    .iter()
                    .any(|id| ids(old, group).contains(id))
                {
                    return Err(ApplicationError::OperationConflict);
                }
            }
        }
        Ok(())
    }
    fn reserve(
        &self,
        command: Command,
        operation: OperationId,
        reserved: &mut BTreeMap<TransactionId, TransferSpec>,
        phases: &mut BTreeMap<OperationId, Command>,
    ) -> Result<(), ApplicationError> {
        self.check_command(operation, command)?;
        self.check_binding(command.spec, reserved.values().copied())?;
        if !reserved.contains_key(&command.spec.id())
            && !matches!(
                (self.role, command.step, command.decision),
                (TransferRole::Coordinator, TransferStep::Start, _)
                    | (TransferRole::Participant(_), TransferStep::Prepare, _)
                    | (
                        TransferRole::Participant(_),
                        TransferStep::ApplyDecision,
                        Some(TransferDecision::Abort)
                    )
            )
        {
            return Err(ApplicationError::InvalidCommand);
        }
        if let Some(original) = phases.get(&operation) {
            if *original != command {
                return Err(ApplicationError::OperationConflict);
            }
        }
        if let Some(record) = self.records.get(&command.spec.id()) {
            if let Some(original) = record.phases[command.slot()] {
                if original.command != command {
                    return Err(ApplicationError::OperationConflict);
                }
            }
        }
        reserved.insert(command.spec.id(), command.spec);
        if reserved.len() > self.limits.transactions() {
            return Err(ApplicationError::DedupCapacity);
        }
        phases.insert(operation, command);
        Ok(())
    }
}
fn ids(spec: TransferSpec, group: GroupIdentity) -> Vec<OperationId> {
    if group == spec.profile().coordinator() {
        spec.phases().coordinator().to_vec()
    } else {
        spec.phases().participants()[spec.profile().participant(group).expect("bound profile")]
            .to_vec()
    }
}
impl ProposalAdmission for TransferApplication {
    fn validate_proposal_context(
        &self,
        core: &Raft,
        operation: OperationId,
        bytes: &[u8],
    ) -> Result<(), ApplicationError> {
        self.validate_group(core.state().bootstrap.group)?;
        self.check_command(operation, codec::decode(bytes)?)
    }
    fn validate_proposal<'a>(
        &self,
        operation: OperationId,
        bytes: &[u8],
        pending: impl Iterator<Item = (OperationId, &'a [u8])>,
    ) -> Result<usize, ApplicationError> {
        let mut reserved: BTreeMap<_, _> = self
            .records
            .values()
            .map(|r| (r.spec.id(), r.spec))
            .collect();
        let mut phases = BTreeMap::new();
        for (operation, bytes) in pending {
            self.reserve(codec::decode(bytes)?, operation, &mut reserved, &mut phases)?;
        }
        self.reserve(codec::decode(bytes)?, operation, &mut reserved, &mut phases)?;
        Ok(size_of::<TransferReceipt>())
    }
}
