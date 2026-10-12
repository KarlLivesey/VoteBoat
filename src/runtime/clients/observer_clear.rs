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
//! Inspect original exported ownership without completing or releasing outputs.
use super::*;
impl<R: ApplicationReceipt> ClientRouter<R> {
    /// True only for the selected group's terminal ordinary outputs already
    /// exported to the host, with no remaining admission/publication stream.
    /// Outputs remain charged and must still be completed unchanged by the host.
    /// This grants no consensus, application, or activation authority.
    pub fn retains_only_exported_ordinary_outputs(&self, group: GroupIdentity) -> bool {
        self.binding.authorization.is_logical()
            && self.binding.owner == self.binding.authorization.owner()
            && self
                .control_profile
                .as_ref()
                .is_some_and(|p| p.parent() == group)
            && self.ready.is_empty()
            && self.admissions.is_empty()
            && self.positions.is_empty()
            && self.pending.values().all(|p| {
                p.ticket.binding == self.binding
                    && p.ticket.group == group
                    && !p.control
                    && p.stepped
                    && p.terminal
                    && p.has_output
                    && p.exported
                    && !p.released
                    && p.outcome.is_none()
            })
    }
}
