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
//! Inspect original exported ownership without cancelling or releasing reads.
use super::*;
impl<Q, R> ReadRequests<Q, R> {
    /// True only for the selected group's terminal ordinary outputs already
    /// exported to the host, with no remaining invocation, cancellation or
    /// execution ownership. Outputs remain charged until original completion.
    /// This inspection grants no quorum-read or activation authority.
    pub fn retains_only_exported_ordinary_outputs(&self, group: GroupIdentity) -> bool {
        self.binding.authorization.is_logical()
            && self.binding.owner == self.binding.authorization.owner()
            && self
                .control_profile
                .as_ref()
                .is_some_and(|p| p.parent() == group)
            && self.ready.is_empty()
            && self.admissions.is_empty()
            && self.active.is_empty()
            && self.execution.is_drained()
            && self.pending.values().all(|p| {
                p.ticket.binding == self.binding
                    && p.ticket.group == group
                    && p.class == ReadCapacityClass::Ordinary
                    && p.stepped
                    && p.terminal
                    && p.has_output
                    && p.exported
                    && !p.released
                    && p.outcome.is_none()
                    && p.query.is_none()
                    && p.cancellation.is_none()
            })
    }
}
