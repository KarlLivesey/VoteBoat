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
//! Selected capacity only; original protocol/application checks remain mandatory.
use super::*;
use crate::logical_voter::LogicalActivationCommandClass;
impl<R: ApplicationReceipt> ClientRouter<R> {
    pub(super) fn select_activation(
        mut self,
        profile: LogicalActivationControlProfile,
    ) -> Result<Self, ClientError> {
        let l = self.limits;
        let worst = Self::window_bytes(l).ok_or(ClientError::InvalidLimits)?;
        if l.control_requests < 2
            || l.group_control_requests < 2
            || l.group_control_requests > l.control_requests
            || l.control_requests >= l.requests
            || l.group_control_requests >= l.group_requests
            || l.command_bytes < profile.limits().command_bytes
            || worst
                .checked_mul(l.control_requests)
                .is_none_or(|n| n > l.control_bytes)
            || l.bytes
                .checked_sub(l.control_bytes)
                .is_none_or(|n| n < worst)
        {
            return Err(ClientError::InvalidLimits);
        }
        self.control_profile = Some(profile);
        Ok(self)
    }
    pub(super) fn window_bytes(limits: ClientRouterLimits) -> Option<usize> {
        limits
            .command_bytes
            .checked_mul(2)
            .and_then(|n| n.checked_add(limits.receipt_bytes))
            .and_then(|n| n.checked_add(size_of::<Pending<R>>()))
    }
    pub(super) fn request_control(&self, request: &ClientRequest) -> Result<bool, ClientError> {
        let control = self.control_profile.as_ref().is_some_and(|profile| {
            request.group == profile.parent()
                && profile.classify_command(request.operation, &request.bytes)
                    == Ok(LogicalActivationCommandClass::Control)
        });
        if control && request.bytes.capacity() > self.limits.command_bytes {
            return Err(ClientError::RequestTooLarge);
        }
        Ok(control)
    }
    pub(super) fn check_capacity(
        &self,
        group: GroupIdentity,
        bytes: usize,
        control: bool,
    ) -> Result<(), ClientError> {
        let l = self.limits;
        if self.usage.requests >= l.requests
            || self.groups.get(&group).copied().unwrap_or(0) >= l.group_requests
            || bytes > l.bytes.saturating_sub(self.usage.bytes)
            || (self.control_profile.is_some()
                && !control
                && (self.bulk.requests >= l.requests - l.control_requests
                    || self.bulk_groups.get(&group).copied().unwrap_or(0)
                        >= l.group_requests - l.group_control_requests
                    || bytes > (l.bytes - l.control_bytes).saturating_sub(self.bulk.bytes)))
        {
            return Err(ClientError::Overloaded);
        }
        Ok(())
    }
}
