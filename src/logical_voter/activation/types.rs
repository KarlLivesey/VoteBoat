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
    pub command_bytes: usize,
    pub checkpoint_bytes: usize,
}
impl Default for LogicalActivationLimits {
    fn default() -> Self {
        Self {
            intents: 64,
            command_bytes: 64 * 1024,
            checkpoint_bytes: 16 * 1024 * 1024,
        }
    }
}
impl LogicalActivationLimits {
    pub fn validate(self) -> Result<Self, ApplicationError> {
        if self.intents == 0
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
            .and_then(|v| v.checked_add(2048))
            .ok_or(ApplicationError::ReceiptBudget)
    }
}
/// High 64 bits of every activation phase `OperationId` ("VBACTPH1").
///
/// The parent's operation-ID space is split: every phase ID of every activation
/// intent carries this tag, and no business operation may. The two kinds can
/// therefore never collide, and the wrapper keeps no business-ID history.
pub const ACTIVATION_OPERATION_TAG: u64 = 0x5642_4143_5450_4831;
/// Activation numbers `n` are below this bound, so `8n + 7` fits in 64 bits.
const ACTIVATION_NUMBERS: u64 = 1 << 61;
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
    /// Phase IDs of activation number `n` (`n < 2^61`): `(TAG << 64) | (8n + k)`
    /// with `k` = 0 begin, 1 learners, 2 membership, 3 outcome, 4 release.
    /// Every intent must use exactly this layout.
    pub fn reserved(n: u64) -> Result<Self, ApplicationError> {
        if n >= ACTIVATION_NUMBERS {
            return Err(ApplicationError::InvalidCommand);
        }
        let phase = |k: u64| {
            OperationId::new((u128::from(ACTIVATION_OPERATION_TAG) << 64) | u128::from(8 * n + k))
                .ok_or(ApplicationError::InvalidCommand)
        };
        Ok(Self {
            begin: phase(0)?,
            learners: phase(1)?,
            membership: phase(2)?,
            outcome: phase(3)?,
            release: phase(4)?,
        })
    }
    /// True iff `id` lies in the reserved activation space. Such an ID is never
    /// a valid business operation, whether or not any intent uses it.
    pub fn is_reserved(id: OperationId) -> bool {
        id.get() >> 64 == u128::from(ACTIVATION_OPERATION_TAG)
    }
    /// The complete reserved layout whose begin phase is `begin`, if any.
    pub(super) fn of_begin(begin: OperationId) -> Option<Self> {
        let low = begin.get() as u64;
        if !Self::is_reserved(begin) || !low.is_multiple_of(8) {
            return None;
        }
        Self::reserved(low / 8).ok()
    }
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
        // The exact reserved layout implies five distinct tagged phase IDs.
        if ChildActivationOperations::of_begin(self.operations.begin) != Some(self.operations)
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
