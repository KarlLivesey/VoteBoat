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
//! Explicit immutable child-storage reservation profile, selected before use.
use super::*;
impl<C: LogCodec + Clone, S: SnapshotCodec + Clone, W: WireCodec + Clone>
    LogicalApplication<C, S, W>
{
    /// Select budget classification from the actual activation wrapper before
    /// any child application history exists. This grants no activation authority.
    pub fn with_activation_control_profile(
        mut self,
        profile: LogicalActivationControlProfile,
    ) -> Result<Self, ApplicationError> {
        if self.control_profile.is_some()
            || self.state != application::State::new(&self.child_bootstrap)?
            || profile.parent() != self.mapping.parent
            || !profile.mappings().contains(&self.mapping)
            || profile.limits().command_bytes > self.limits.parent_log.max_command_bytes
            || self.limits.control_actions < 2
            || self
                .limits
                .max_command_bytes
                .checked_mul(2)
                .is_none_or(|n| {
                    n > self.limits.control_action_bytes
                        || n > self.limits.max_retained_action_bytes
                })
        {
            return Err(ApplicationError::InvalidCommand);
        }
        self.control_profile = Some(profile);
        self.configuration = self.configuration_bytes()?;
        self.validate_read_envelopes()?;
        self.encode_state(self.limits.max_child_checkpoint_bytes)?;
        Ok(self)
    }
    pub fn activation_control_profile(&self) -> Option<&LogicalActivationControlProfile> {
        self.control_profile.as_ref()
    }
}
