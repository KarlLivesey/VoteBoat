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
//! Selected memory classification is never read or release authority.
use super::*;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::runtime) enum ReadCapacityClass {
    Ordinary,
    Activation,
}
impl<R> ReadRouter<R> {
    pub(in crate::runtime) fn window_bytes<Q>(limits: ReadRouterLimits) -> Option<usize> {
        limits
            .query_bytes
            .checked_add(size_of::<Q>())
            .and_then(|n| n.checked_add(limits.result_bytes))
            .and_then(|n| n.checked_add(size_of::<Held<R>>()))
    }
    pub(in crate::runtime) fn select_activation<Q>(
        mut self,
        profile: LogicalActivationControlProfile,
    ) -> Result<Self, ReadRouteError> {
        let l = self.limits;
        let worst = Self::window_bytes::<Q>(l).ok_or(ReadRouteError::InvalidLimits)?;
        if l.control_results < 2
            || l.group_control_results < 2
            || l.group_control_results > l.control_results
            || l.control_results >= l.results
            || l.group_control_results >= l.group_results
            || worst
                .checked_mul(l.control_results)
                .is_none_or(|n| n > l.control_bytes)
            || l.bytes
                .checked_sub(l.control_bytes)
                .is_none_or(|n| n < worst)
        {
            return Err(ReadRouteError::InvalidLimits);
        }
        self.control_profile = Some(profile);
        Ok(self)
    }
    pub(in crate::runtime) fn activation_profile(
        &self,
    ) -> Option<&LogicalActivationControlProfile> {
        self.control_profile.as_ref()
    }
    pub(in crate::runtime) fn fits_activation<Q>(
        &self,
        profile: &LogicalActivationControlProfile,
        limits: ReadInvocationLimits,
    ) -> bool {
        self.control_profile.as_ref() == Some(profile)
            && self.fits_single::<Q>(limits.query_bytes, limits.result_bytes)
            && self.limits.control_results >= limits.control_requests
            && self.limits.group_control_results >= limits.group_control_requests
    }
    pub(in crate::runtime) fn check_capacity(
        &self,
        group: GroupIdentity,
        bytes: usize,
        class: ReadCapacityClass,
    ) -> Result<(), ReadRouteError> {
        let l = self.limits;
        if class == ReadCapacityClass::Activation
            && self
                .control_profile
                .as_ref()
                .is_none_or(|p| p.parent() != group)
        {
            return Err(ReadRouteError::WrongBinding);
        }
        if self.usage.results >= l.results
            || self.groups.get(&group).copied().unwrap_or(0) >= l.group_results
            || bytes > l.bytes.saturating_sub(self.usage.bytes)
            || (self.control_profile.is_some()
                && class == ReadCapacityClass::Ordinary
                && (self.bulk.results >= l.results - l.control_results
                    || self.bulk_groups.get(&group).copied().unwrap_or(0)
                        >= l.group_results - l.group_control_results
                    || bytes > (l.bytes - l.control_bytes).saturating_sub(self.bulk.bytes)))
        {
            return Err(ReadRouteError::Overloaded);
        }
        Ok(())
    }
}
