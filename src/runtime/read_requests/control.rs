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
//! Stable whole original invocation windows and derived capacity classification.
use super::*;
pub(super) struct EncodedControlQuery {
    bytes: [u8; 17],
    used: usize,
}
impl EncodedControlQuery {
    pub(super) fn bytes(&self) -> &[u8] {
        &self.bytes[..self.used]
    }
}
impl<Q, R> ReadRequests<Q, R> {
    pub(super) fn window_bytes(limits: ReadInvocationLimits) -> Option<usize> {
        limits
            .query_bytes
            .checked_add(limits.result_bytes)
            .and_then(|n| n.checked_add(size_of::<Pending<Q, R>>()))
    }
    pub(super) fn check_selection(
        binding: ReadInvocationBinding,
        limits: ReadInvocationLimits,
        execution: &ReadRouter<R>,
        profile: Option<&LogicalActivationControlProfile>,
    ) -> Result<(), ReadInvocationError> {
        if execution.activation_profile() != profile {
            return Err(ReadInvocationError::WrongBinding);
        }
        let Some(profile) = profile else {
            return Ok(());
        };
        if binding
            .authorization
            .logical()
            .is_none_or(|a| a.mapping.parent != profile.parent())
        {
            return Err(ReadInvocationError::WrongBinding);
        }
        Self::check_windows(limits)?;
        if !execution.fits_activation::<Q>(profile, limits) {
            return Err(ReadInvocationError::InvalidLimits);
        }
        Ok(())
    }
    pub(super) fn check_windows(l: ReadInvocationLimits) -> Result<(), ReadInvocationError> {
        let worst = Self::window_bytes(l).ok_or(ReadInvocationError::InvalidLimits)?;
        if l.control_requests < 2
            || l.group_control_requests < 2
            || l.group_control_requests > l.control_requests
            || l.control_requests >= l.requests
            || l.group_control_requests >= l.group_requests
            || worst
                .checked_mul(l.control_requests)
                .is_none_or(|n| n > l.control_bytes)
            || l.bytes
                .checked_sub(l.control_bytes)
                .is_none_or(|n| n < worst)
        {
            return Err(ReadInvocationError::InvalidLimits);
        }
        Ok(())
    }
    pub(super) fn query_control<A: LogicalResponseCodec<Query = Q, ReadResult = R>>(
        &self,
        application: &A,
        group: GroupIdentity,
        query: &Q,
    ) -> Result<Option<EncodedControlQuery>, ReadInvocationError>
    where
        A::Receipt: crate::application::ApplicationReceipt,
    {
        let Some(profile) = self.control_profile.as_ref() else {
            return Ok(None);
        };
        if application.logical_activation_control_profile().as_ref() != Some(profile)
            || application.logical_response_schema() != profile.response_schema()
            || group != profile.parent()
        {
            return Err(ReadInvocationError::WrongBinding);
        }
        Ok(application
            .query_encoding_bound(query)
            .ok()
            .and_then(|bound| {
                self.encoded_control_query(bound, |out| application.encode_query(query, out))
            }))
    }
    pub(super) fn encoded_control_query(
        &self,
        bound: usize,
        encode: impl FnOnce(&mut [u8]) -> Result<usize, ApplicationError>,
    ) -> Option<EncodedControlQuery> {
        let profile = self.control_profile.as_ref()?;
        let mut bytes = [0; 17];
        if bound > bytes.len() {
            return None;
        }
        let used = encode(&mut bytes[..bound]).ok()?;
        (used <= bound && profile.is_control_query(&bytes[..used]))
            .then_some(EncodedControlQuery { bytes, used })
    }
    pub(super) fn check_capacity(
        &self,
        group: GroupIdentity,
        bytes: usize,
        class: ReadCapacityClass,
    ) -> Result<(), ReadInvocationError> {
        let l = self.limits;
        if class == ReadCapacityClass::Activation
            && self
                .control_profile
                .as_ref()
                .is_none_or(|p| p.parent() != group)
        {
            return Err(ReadInvocationError::WrongBinding);
        }
        if self.usage.requests >= l.requests
            || self.groups.get(&group).copied().unwrap_or(0) >= l.group_requests
            || bytes > l.bytes.saturating_sub(self.usage.bytes)
            || (self.control_profile.is_some()
                && class == ReadCapacityClass::Ordinary
                && (self.bulk.requests >= l.requests - l.control_requests
                    || self.bulk_groups.get(&group).copied().unwrap_or(0)
                        >= l.group_requests - l.group_control_requests
                    || bytes > (l.bytes - l.control_bytes).saturating_sub(self.bulk.bytes)))
        {
            return Err(ReadInvocationError::Overloaded);
        }
        Ok(())
    }
}
