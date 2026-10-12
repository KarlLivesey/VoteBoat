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
#[derive(Clone)]
pub struct LogicalPeerDescriptor<C = NativeLogCodec, S = NativeSnapshotCodec, W = NativeWireCodec> {
    pub(super) mapping: LogicalStoreMapping,
    pub(super) representative: RepresentativeBinding,
    pub(super) membership: Membership,
    pub(super) claim_index: u64,
    pub(super) application: LogicalApplication<C, S, W>,
    pub(super) endpoint: LogicalEndpointChain,
}
impl<C: LogCodec + Clone, S: SnapshotCodec + Clone, W: WireCodec + Clone>
    LogicalPeerDescriptor<C, S, W>
{
    /// Only a consumed Claim and fresh child quorum image can provision this descriptor.
    /// Current membership remains an execution-time core contract.
    pub fn from_verified(
        v: &VerifiedLogicalImage,
        a: &LogicalApplication<C, S, W>,
    ) -> Result<Self, LogicalNetworkError> {
        if a.activation_control_profile() != v.activation_control_profile() {
            return Err(LogicalNetworkError::WrongBinding);
        }
        if a.initialization() != v.initialization {
            return Err(LogicalNetworkError::WrongBinding);
        }
        let copy = match v.initialization {
            LogicalInitialization::BootstrapVoter => LogicalApplication::new(
                a.mapping,
                a.child_bootstrap.clone(),
                a.parent_bootstrap.clone(),
                v.limits,
                a.log_codec.clone(),
                a.snapshot_codec.clone(),
                a.wire_codec.clone(),
            ),
            LogicalInitialization::EnrollmentCandidate(plan) => {
                LogicalApplication::new_enrollment_candidate(
                    a.mapping,
                    a.child_bootstrap.clone(),
                    a.parent_bootstrap.clone(),
                    v.limits,
                    (
                        a.log_codec.clone(),
                        a.snapshot_codec.clone(),
                        a.wire_codec.clone(),
                    ),
                    plan,
                )
            }
        }
        .map_err(|_| LogicalNetworkError::WrongBinding)?;
        let copy = match v.activation_control_profile() {
            Some(profile) => copy
                .with_activation_control_profile(profile.clone())
                .map_err(|_| LogicalNetworkError::WrongBinding)?,
            None => copy,
        };
        if a.mapping != v.mapping()
            || !a.same_configuration(&copy)
            || a.parent_bootstrap != v.image().parent.bootstrap
        {
            return Err(LogicalNetworkError::WrongBinding);
        }
        if a.candidate() {
            startup::validate_initialization(
                v.image(),
                v.child_barrier(),
                (v.initialization, v.enrollment.as_deref()),
            )
            .map_err(|_| LogicalNetworkError::WrongBinding)?;
        }
        Ok(Self {
            mapping: v.mapping(),
            representative: v.representative(),
            membership: v.image().child_membership.clone(),
            claim_index: v.claim().first_index,
            application: copy,
            endpoint: v.endpoint_chain().clone(),
        })
    }
    pub fn mapping(&self) -> LogicalStoreMapping {
        self.mapping
    }
    pub fn representative(&self) -> RepresentativeBinding {
        self.representative
    }
    pub fn physical(&self) -> LocalIdentity {
        self.endpoint.leaf()
    }
    pub fn endpoint_chain(&self) -> &LogicalEndpointChain {
        &self.endpoint
    }
    pub(super) fn logical(&self) -> StoreBinding {
        StoreBinding {
            identity: self.mapping.store,
            session: self.representative.logical_session,
        }
    }
}
#[derive(Clone)]
pub struct LogicalPeerPlan<C = NativeLogCodec, S = NativeSnapshotCodec, W = NativeWireCodec> {
    pub(super) local: LogicalPeerDescriptor<C, S, W>,
    pub(super) peers: Vec<LogicalPeerDescriptor<C, S, W>>,
}
impl<C: LogCodec + Clone, S: SnapshotCodec + Clone, W: WireCodec + Clone> LogicalPeerPlan<C, S, W> {
    pub fn new(
        local: LogicalPeerDescriptor<C, S, W>,
        peers: Vec<LogicalPeerDescriptor<C, S, W>>,
    ) -> Result<Self, LogicalNetworkError> {
        if peers.is_empty() || peers.len() > 1024 {
            return Err(LogicalNetworkError::InvalidLimits);
        }
        for (i, p) in peers.iter().enumerate() {
            if p.application.parent_bootstrap != local.application.parent_bootstrap
                || p.mapping.parent != local.mapping.parent
                || p.mapping.voter == local.mapping.voter
                || p.mapping.store == local.mapping.store
                || p.mapping.child == local.mapping.child
                || p.physical().node == local.physical().node
                || p.physical().store.identity == local.physical().store.identity
                || peers[..i].iter().any(|old| {
                    old.mapping.voter == p.mapping.voter
                        || old.mapping.store == p.mapping.store
                        || old.mapping.child == p.mapping.child
                        || old.physical().node == p.physical().node
                        || old.physical().store.identity == p.physical().store.identity
                })
            {
                return Err(LogicalNetworkError::WrongPeer);
            }
        }
        let descriptors = std::iter::once(&local).chain(&peers);
        let bootstrap = &local.application.parent_bootstrap.voter_stores;
        let enrolled = descriptors.clone().any(|p| {
            matches!(
                p.application.initialization(),
                LogicalInitialization::EnrollmentCandidate(_)
            )
        });
        if (!enrolled
            && bootstrap.iter().any(|(node, store)| {
                !descriptors
                    .clone()
                    .any(|p| p.mapping.voter == *node && p.mapping.store == *store)
            }))
            || descriptors.clone().any(|p| {
                bootstrap
                    .get(&p.mapping.voter)
                    .is_some_and(|store| *store != p.mapping.store)
            })
            || descriptors
                .filter(|p| !bootstrap.contains_key(&p.mapping.voter))
                .any(|p| {
                    !matches!(
                        p.application.initialization(),
                        LogicalInitialization::EnrollmentCandidate(_)
                    )
                })
        {
            return Err(LogicalNetworkError::WrongPeer);
        }
        Ok(Self { local, peers })
    }
    pub fn local(&self) -> &LogicalPeerDescriptor<C, S, W> {
        &self.local
    }
    pub fn peers(&self) -> &[LogicalPeerDescriptor<C, S, W>] {
        &self.peers
    }
    pub(super) fn authorization(&self, a: RuntimeAuthorization) -> bool {
        self.local.endpoint.matches_authorization(a) && a.owner().store == self.local.logical()
    }
    pub(super) fn peer(&self, physical: LocalIdentity) -> Option<&LogicalPeerDescriptor<C, S, W>> {
        self.peers.iter().find(|p| p.physical() == physical)
    }
    pub(super) fn context(&self, binding: StoreBinding) -> bool {
        binding == self.local.logical() || self.peers.iter().any(|p| p.logical() == binding)
    }
}
