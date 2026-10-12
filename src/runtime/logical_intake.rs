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
//! Verified immutable capacity selection; it grants no command authority.
use super::*;
use crate::logical_voter::{LogicalActivationCommandClass, LogicalActivationControlProfile};
#[cfg(test)]
mod tests;
#[derive(Clone, Copy)]
pub(super) struct LogicalReadInput<'a> {
    pub(super) request: ReadRequestId,
    pub(super) profile: &'a LogicalActivationControlProfile,
    pub(super) query: &'a [u8],
}
impl<Q: ReadyScheduler> Shard<Q> {
    pub(super) fn input_class(
        &self,
        group: GroupIdentity,
        event: &Event,
        ordinary: Class,
        read: Option<LogicalReadInput<'_>>,
    ) -> Result<Class, RuntimeError> {
        let Some(read) = read else {
            return self.proposal_class(group, event, ordinary);
        };
        if self.control_profile.as_ref() != Some(read.profile)
            || read.profile.parent() != group
            || !matches!(event, Event::Read { request } if *request == read.request)
        {
            return Err(RuntimeError::WrongOwner);
        }
        Ok(if read.profile.is_control_query(read.query) {
            Class::Control
        } else {
            ordinary
        })
    }
    pub(super) fn validate_control_profile(
        &self,
        profile: &LogicalActivationControlProfile,
    ) -> Result<(), RuntimeError> {
        let l = self.limits;
        let window = profile
            .limits()
            .command_bytes
            .checked_add(size_of::<Event>())
            .and_then(|n| n.checked_add(size_of::<AdmissionTicket>()))
            .ok_or(RuntimeError::InvalidLimits)?;
        if self.control_profile.is_some()
            || self.groups.len() != 1
            || !self.groups.contains_key(&profile.parent())
        {
            return Err(RuntimeError::WrongOwner);
        }
        if l.control_items < 2
            || l.group_control_items < 2
            || window > l.max_event_bytes
            || window
                .checked_mul(l.control_items)
                .is_none_or(|n| n > l.control_bytes)
            || window
                .checked_mul(l.group_control_items)
                .is_none_or(|n| n > l.group_control_bytes)
            || window > l.max_bytes - l.control_bytes
            || window > l.group_bytes - l.group_control_bytes
        {
            return Err(RuntimeError::InvalidLimits);
        }
        Ok(())
    }
    pub(super) fn proposal_class(
        &self,
        group: GroupIdentity,
        event: &Event,
        ordinary: Class,
    ) -> Result<Class, RuntimeError> {
        let Some(profile) = &self.control_profile else {
            return Ok(ordinary);
        };
        let Event::Propose { operation, bytes } = event else {
            return Ok(ordinary);
        };
        if group == profile.parent()
            && profile.classify_command(*operation, bytes)
                == Ok(LogicalActivationCommandClass::Control)
        {
            if bytes.capacity() > profile.limits().command_bytes {
                return Err(RuntimeError::EventTooLarge);
            }
            return Ok(Class::Control);
        }
        Ok(ordinary)
    }
}
