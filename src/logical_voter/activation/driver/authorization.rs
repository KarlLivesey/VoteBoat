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
//! Original consumed parent intent grants only these immutable child records.
use super::*;
use crate::{
    membership::{Configuration, ConfigurationChange, ConfigurationRecord},
    quorum::{Limits, Policy, Tree},
    raft::{ConfigurationProposal, ConfigurationProposalError, Raft, ReadinessRequirements},
};
use std::collections::BTreeMap;
/// Scoped administrative capability. No public constructor or clone exists;
/// only the owning activation driver can mint it from an original fresh read.
pub struct LogicalActivationAuthorization {
    observed: ObservedActivationIntent,
    bindings: (RuntimeOwner, ClientRouterBinding, ReadInvocationBinding),
    profile: LogicalActivationControlProfile,
    records: [ConfigurationRecord; 3],
    requirements: ReadinessRequirements,
}
impl LogicalActivationAuthorization {
    pub fn intent(&self) -> &ChildActivationIntent {
        &self.observed.intent
    }
    pub fn parent_owner(&self) -> RuntimeOwner {
        self.bindings.0
    }
    pub fn profile(&self) -> &LogicalActivationControlProfile {
        &self.profile
    }
    pub(super) fn mint(
        observed: ObservedActivationIntent,
        bindings: (RuntimeOwner, ClientRouterBinding, ReadInvocationBinding),
        profile: LogicalActivationControlProfile,
        requirements: ReadinessRequirements,
    ) -> Result<Self, ChildActivationDriverError> {
        if !observed.matches(bindings, &observed.intent)
            || profile.parent() != observed.intent.mapping.parent
            || !profile.mappings().contains(&observed.intent.mapping)
        {
            return Err(ChildActivationDriverError::WrongBinding);
        }
        let records = records(&observed.intent).map_err(ChildActivationDriverError::Application)?;
        Ok(Self {
            observed,
            bindings,
            profile,
            records,
            requirements,
        })
    }
    pub(super) fn retained_bound(
        intent: &ChildActivationIntent,
        profile: &LogicalActivationControlProfile,
    ) -> Result<usize, ApplicationError> {
        records(intent)?.iter().try_fold(
            profile
                .mappings()
                .len()
                .saturating_mul(std::mem::size_of::<crate::logical_voter::LogicalStoreMapping>()),
            |n, record| {
                n.checked_add(record.retained_bytes())
                    .ok_or(ApplicationError::ReceiptBudget)
            },
        )
    }
    pub(crate) fn learner(&self) -> ActivationVoter {
        *self
            .intent()
            .target_voters()
            .iter()
            .find(|v| !self.intent().old_voters().contains(v))
            .expect("checked one replacement")
    }
    pub(crate) fn record(&self, phase: usize) -> &ConfigurationRecord {
        &self.records[phase]
    }
    pub(crate) fn requirements(&self) -> ReadinessRequirements {
        self.requirements
    }
    pub(crate) fn authorize(
        &self,
        core: &Raft,
        proposal: &ConfigurationProposal,
    ) -> Result<(), ConfigurationProposalError> {
        if core.state().bootstrap.group != self.intent().mapping.child
            || !self.observed.matches(self.bindings, self.intent())
            || proposal.requirements != self.requirements
            || !self.records.contains(&proposal.record)
        {
            return Err(ConfigurationProposalError::AuthenticationRequired);
        }
        // Ordinary Node and Raft execute-time journal, readiness, transport,
        // exact current peer bindings remain mandatory.
        Ok(())
    }
}
fn configuration(
    id: ConfigurationId,
    voters: &[ActivationVoter; 3],
    learners: BTreeMap<NodeId, StoreIdentity>,
) -> Result<Configuration, ApplicationError> {
    Configuration::new(
        id,
        Policy::new(
            Tree::Majority(voters.iter().map(|v| Tree::Voter(v.node)).collect()),
            Limits::default(),
        )
        .map_err(|_| ApplicationError::InvalidCommand)?,
        voters.iter().map(|v| (v.node, v.store)).collect(),
        learners,
    )
    .map_err(|_| ApplicationError::InvalidCommand)
}
fn records(i: &ChildActivationIntent) -> Result<[ConfigurationRecord; 3], ApplicationError> {
    let new = i
        .target
        .iter()
        .find(|v| !i.old.contains(v))
        .ok_or(ApplicationError::InvalidCommand)?;
    let learners = configuration(
        i.learners_configuration,
        &i.old,
        BTreeMap::from([(new.node, new.store)]),
    )?;
    let target = configuration(i.target_configuration, &i.target, BTreeMap::new())?;
    Ok([
        ConfigurationRecord {
            operation: i.operations.learners,
            expected: i.old_configuration,
            change: ConfigurationChange::Learners(learners),
        },
        ConfigurationRecord {
            operation: i.operations.membership,
            expected: i.learners_configuration,
            change: ConfigurationChange::Joint {
                id: i.joint_configuration,
                next: target,
            },
        },
        ConfigurationRecord {
            operation: i.operations.membership,
            expected: i.joint_configuration,
            change: ConfigurationChange::Final {
                id: i.target_configuration,
            },
        },
    ])
}
