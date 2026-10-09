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
//! Constrained recommendations, consumed through ordinary membership admission.
use super::*;
use crate::runtime::MonoTime;
use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroU64,
};
pub const PLACEMENT_PLANNING_CONTRACT_VERSION: u32 = 1;
pub const MAX_PLACEMENT_CANDIDATES: usize = 4096;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PlacementSampleGeneration(NonZeroU64);
impl PlacementSampleGeneration {
    pub fn new(value: u64) -> Option<Self> {
        NonZeroU64::new(value).map(Self)
    }
    pub fn get(self) -> u64 {
        self.0.get()
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PlacementCandidate {
    pub node: NodeId,
    pub placement: ReplicaPlacement,
    pub enabled: bool,
    pub free_bytes: u64,
    pub free_replica_slots: u32,
    pub load_permille: u16,
}
#[derive(Clone, Copy, Debug)]
pub struct PlacementSnapshot<'a> {
    pub group: GroupIdentity,
    pub configuration: ConfigurationId,
    pub generation: PlacementSampleGeneration,
    pub observed_at: MonoTime,
    pub expires_at: MonoTime,
    pub candidates: &'a [PlacementCandidate],
}
#[derive(Clone, Copy, Debug)]
pub struct PlacementRequest<'a> {
    pub snapshot: PlacementSnapshot<'a>,
    pub current: &'a Membership,
    pub now: MonoTime,
    pub minimum_free_bytes: u64,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PlacementPlanningError {
    InvalidSample,
    StaleConfiguration,
    Expired,
    TimeWentBack,
    TransitionActive,
    NoCandidate,
    ProviderViolation,
    Exhausted,
    Membership(MembershipError),
    Authorization(PlacementError),
}
impl PlacementRequest<'_> {
    pub fn validate(&self) -> Result<(), PlacementPlanningError> {
        let s = self.snapshot;
        if s.configuration != self.current.id() {
            return Err(PlacementPlanningError::StaleConfiguration);
        }
        if self.current.joint().is_some() {
            return Err(PlacementPlanningError::TransitionActive);
        }
        if self.now < s.observed_at {
            return Err(PlacementPlanningError::TimeWentBack);
        }
        if self.now >= s.expires_at || s.expires_at <= s.observed_at {
            return Err(PlacementPlanningError::Expired);
        }
        if s.candidates.is_empty() || s.candidates.len() > MAX_PLACEMENT_CANDIDATES {
            return Err(PlacementPlanningError::InvalidSample);
        }
        let mut nodes = BTreeMap::new();
        let mut stores = BTreeSet::new();
        let mut domains = BTreeSet::new();
        for c in s.candidates {
            if c.load_permille > 1000
                || nodes.insert(c.node, c.placement).is_some()
                || !stores.insert((c.placement.store.id, c.placement.store.incarnation))
            {
                return Err(PlacementPlanningError::InvalidSample);
            }
            domains.insert(c.placement.domain);
        }
        if domains.len() > 64 {
            return Err(PlacementPlanningError::InvalidSample);
        }
        for (node, store) in self
            .current
            .stable()
            .voter_stores()
            .iter()
            .chain(self.current.stable().learners())
        {
            if nodes.get(node).is_none_or(|p| p.store != *store) {
                return Err(PlacementPlanningError::InvalidSample);
            }
        }
        Ok(())
    }
    pub fn eligible(&self, c: &PlacementCandidate) -> bool {
        c.enabled
            && c.free_replica_slots > 0
            && c.free_bytes >= self.minimum_free_bytes
            && !self.current.stable().voter_stores().contains_key(&c.node)
            && !self.current.stable().learners().contains_key(&c.node)
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PlacementRecommendation {
    pub group: GroupIdentity,
    pub configuration: ConfigurationId,
    pub sample: PlacementSampleGeneration,
    pub node: NodeId,
    pub store: StoreIdentity,
}
/// Pure bounded nonblocking recommendations. No grant of configuration,
/// credentials, readiness, resource reservations or ownership authority.
pub trait PlacementPlanner {
    fn select(
        &self,
        request: PlacementRequest<'_>,
    ) -> Result<PlacementRecommendation, PlacementPlanningError>;
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlannedLearner {
    pub recommendation: PlacementRecommendation,
    pub record: ConfigurationRecord,
}
pub fn plan_learner<P: PlacementPlanner, A: PlacementAuthorizer>(
    planner: &P,
    authorizer: &A,
    request: PlacementRequest<'_>,
    operation: OperationId,
) -> Result<PlannedLearner, PlacementPlanningError> {
    request.validate()?;
    if request.current.operations().contains(&operation) {
        return Err(PlacementPlanningError::Membership(
            MembershipError::ReusedOperation,
        ));
    }
    if request.current.operations().len() >= MAX_CONFIGURATION_OPERATIONS {
        return Err(PlacementPlanningError::Membership(
            MembershipError::HistoryFull,
        ));
    }
    if request.current.stable().voter_stores().len() + request.current.stable().learners().len()
        >= crate::quorum::Limits::default().max_voters
    {
        return Err(PlacementPlanningError::Membership(
            MembershipError::InvalidConfiguration,
        ));
    }
    let next = request
        .current
        .id()
        .get()
        .checked_add(1)
        .and_then(ConfigurationId::new)
        .ok_or(PlacementPlanningError::Exhausted)?;
    let recommendation = planner.select(request)?;
    if recommendation.group != request.snapshot.group
        || recommendation.configuration != request.current.id()
        || recommendation.sample != request.snapshot.generation
    {
        return Err(PlacementPlanningError::ProviderViolation);
    }
    let candidate = request
        .snapshot
        .candidates
        .iter()
        .find(|c| c.node == recommendation.node && c.placement.store == recommendation.store)
        .ok_or(PlacementPlanningError::ProviderViolation)?;
    if !request.eligible(candidate) {
        return Err(PlacementPlanningError::ProviderViolation);
    }
    let stable = request.current.stable();
    let mut learners = stable.learners().clone();
    learners.insert(candidate.node, candidate.placement.store);
    let configuration = Configuration::new(
        next,
        stable.policy().clone(),
        stable.voter_stores().clone(),
        learners,
    )
    .map_err(PlacementPlanningError::Membership)?;
    let record = ConfigurationRecord {
        operation,
        expected: request.current.id(),
        change: ConfigurationChange::Learners(configuration),
    };
    authorizer
        .authorize(request.snapshot.group, request.current, &record)
        .map_err(PlacementPlanningError::Authorization)?;
    Ok(PlannedLearner {
        recommendation,
        record,
    })
}
