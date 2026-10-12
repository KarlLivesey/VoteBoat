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
use crate::{membership::Configuration, quorum::Tree};
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LogicalActivationLimits {
    pub intents: usize,
    pub business_operations: usize,
    pub command_bytes: usize,
    pub checkpoint_bytes: usize,
}
impl Default for LogicalActivationLimits {
    fn default() -> Self {
        Self {
            intents: 64,
            business_operations: 16384,
            command_bytes: 64 * 1024,
            checkpoint_bytes: 16 * 1024 * 1024,
        }
    }
}
impl LogicalActivationLimits {
    pub fn validate(self) -> Result<Self, ApplicationError> {
        if self.business_operations == 0
            || self.business_operations > 65536
            || self.intents == 0
            || self.intents > 16384
            || self.command_bytes < CONTROL_BYTES
            || self.command_bytes > 16 * 1024 * 1024
            || self.checkpoint_bytes < 1024
            || self.checkpoint_bytes > 256 * 1024 * 1024
            || self.control_checkpoint_bound()? > self.checkpoint_bytes
        {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(self)
    }
    pub(super) fn control_checkpoint_bound(self) -> Result<usize, ApplicationError> {
        self.intents
            .checked_mul(3 * (CONTROL_BYTES + 16) + 128)
            .and_then(|v| v.checked_add(self.business_operations * 16))
            .and_then(|v| v.checked_add(2048))
            .ok_or(ApplicationError::ReceiptBudget)
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ChildActivationOperations {
    pub begin: OperationId,
    pub learners: OperationId,
    /// Joint and Final retain this SAME operation identity.
    pub membership: OperationId,
    pub outcome: OperationId,
    pub release: OperationId,
}
impl ChildActivationOperations {
    pub(super) fn ids(self) -> [OperationId; 5] {
        [
            self.begin,
            self.learners,
            self.membership,
            self.outcome,
            self.release,
        ]
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ActivationVoter {
    pub node: NodeId,
    pub store: StoreIdentity,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChildActivationIntent {
    pub(super) mapping: LogicalStoreMapping,
    pub(super) operations: ChildActivationOperations,
    pub(super) old_configuration: ConfigurationId,
    pub(super) learners_configuration: ConfigurationId,
    pub(super) joint_configuration: ConfigurationId,
    pub(super) target_configuration: ConfigurationId,
    pub(super) old: [ActivationVoter; 3],
    pub(super) target: [ActivationVoter; 3],
}
impl ChildActivationIntent {
    pub fn new(
        mapping: LogicalStoreMapping,
        operations: ChildActivationOperations,
        old: &Configuration,
        learners_configuration: ConfigurationId,
        joint_configuration: ConfigurationId,
        target: &Configuration,
    ) -> Result<Self, ApplicationError> {
        let intent = Self {
            mapping,
            operations,
            old_configuration: old.id(),
            learners_configuration,
            joint_configuration,
            target_configuration: target.id(),
            old: Self::voters(old)?,
            target: Self::voters(target)?,
        };
        intent.validate()?;
        Ok(intent)
    }
    fn voters(c: &Configuration) -> Result<[ActivationVoter; 3], ApplicationError> {
        let voters: Vec<_> = c
            .voter_stores()
            .iter()
            .map(|(&node, &store)| ActivationVoter { node, store })
            .collect();
        if !c.learners().is_empty()
            || c.policy().tree()
                != &Tree::Majority(voters.iter().map(|v| Tree::Voter(v.node)).collect())
        {
            return Err(ApplicationError::InvalidCommand);
        }
        voters
            .try_into()
            .map_err(|_| ApplicationError::InvalidCommand)
    }
    pub(super) fn validate(&self) -> Result<(), ApplicationError> {
        LogicalStoreMapping::new(
            self.mapping.parent,
            self.mapping.voter,
            self.mapping.store,
            self.mapping.child,
        )?;
        let ids = self.operations.ids();
        if ids.iter().enumerate().any(|(i, id)| ids[..i].contains(id))
            || !(self.old_configuration < self.learners_configuration
                && self.learners_configuration < self.joint_configuration
                && self.joint_configuration < self.target_configuration)
        {
            return Err(ApplicationError::InvalidCommand);
        }
        for list in [&self.old, &self.target] {
            if list.windows(2).any(|p| p[0].node >= p[1].node)
                || list
                    .iter()
                    .enumerate()
                    .any(|(i, v)| list[..i].iter().any(|p| p.store == v.store))
            {
                return Err(ApplicationError::InvalidCommand);
            }
        }
        let shared = self.old.iter().filter(|v| self.target.contains(v)).count();
        if shared != 2
            || self.old.iter().any(|v| {
                self.target
                    .iter()
                    .any(|n| n.node == v.node && n.store != v.store)
            })
            || self.old.iter().any(|v| {
                self.target
                    .iter()
                    .any(|n| n.store == v.store && n.node != v.node)
            })
        {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(())
    }
    pub fn mapping(&self) -> LogicalStoreMapping {
        self.mapping
    }
    pub fn operations(&self) -> ChildActivationOperations {
        self.operations
    }
    pub fn old_configuration(&self) -> ConfigurationId {
        self.old_configuration
    }
    pub fn learners_configuration(&self) -> ConfigurationId {
        self.learners_configuration
    }
    pub fn joint_configuration(&self) -> ConfigurationId {
        self.joint_configuration
    }
    pub fn target_configuration(&self) -> ConfigurationId {
        self.target_configuration
    }
    pub fn old_voters(&self) -> &[ActivationVoter; 3] {
        &self.old
    }
    pub fn target_voters(&self) -> &[ActivationVoter; 3] {
        &self.target
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ActivationPosition {
    pub index: u64,
    pub term: u64,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChildActivationOutcome {
    Begun,
    Busy(OperationId),
    Completed,
    Released,
    OperationConflict,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ChildActivationReceipt {
    pub index: u64,
    pub operation: OperationId,
    pub first: ActivationPosition,
    pub outcome: ChildActivationOutcome,
    pub duplicate: bool,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChildActivationStatus {
    pub intent: ChildActivationIntent,
    pub begin: ChildActivationReceipt,
    pub outcome: Option<ChildActivationReceipt>,
    pub release: Option<ChildActivationReceipt>,
    /// Trusted replicated assertion; this scalar is not a child proof.
    pub child_final: Option<ActivationPosition>,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ActivationReceipt<R> {
    Business(R),
    Control(ChildActivationReceipt),
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ActivationQuery<Q> {
    Business(Q),
    Status(OperationId),
    Active,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ActivationRead<R> {
    Business(R),
    Status(Option<Box<ChildActivationStatus>>),
    Active(Option<OperationId>),
}
