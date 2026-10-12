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
//! Pure authority envelopes carry separate trusted-base and requested views.
use super::*;
use crate::{membership::ConfigurationChange, secure::PeerIdentity};
fn base(p: &GroupLog, id: ConfigurationId) -> Option<Membership> {
    let floor = p.membership_at(p.base_index()).ok()?;
    if floor.id() == id {
        return Some(floor);
    }
    let entry = p
        .entries
        .iter()
        .take_while(|e| e.index <= p.commit_index)
        .find(|e| match &e.payload {
            EntryPayload::Configuration(r) => match &r.change {
                ConfigurationChange::Learners(next) => next.id() == id,
                ConfigurationChange::Joint { id: next, .. }
                | ConfigurationChange::Final { id: next } => *next == id,
            },
            _ => false,
        })?;
    p.membership_at(entry.index).ok()
}
impl<C: LogCodec + Clone, S: SnapshotCodec + Clone, W: WireCodec + Clone>
    LogicalApplication<C, S, W>
{
    pub(super) fn authority_request(
        &self,
        m: &Message,
        p: &GroupLog,
        candidate: PeerIdentity,
        configuration: ConfigurationId,
    ) -> bool {
        let Ok(current) = p.membership() else {
            return false;
        };
        m.configuration == current.id()
            && configuration > m.configuration
            && m.context.origin == m.sender
            && current.voter_store(m.to).is_some()
            && !current.is_voter(candidate.node)
            && candidate.node != m.from
            && candidate.node != m.to
    }
    pub(super) fn authority_reply(
        &self,
        m: &Message,
        p: &GroupLog,
        facts: (PeerIdentity, ConfigurationId, u64, u64, bool),
    ) -> bool {
        let (candidate, configuration, index, term, granted) = facts;
        let Some(base) = base(p, m.configuration) else {
            return false;
        };
        if configuration <= m.configuration
            || candidate.node == m.from
            || candidate.node == m.to
            || base.replica_store(m.to) != Some(m.context.origin.identity)
            || base.voter_store(self.mapping.voter) != Some(self.mapping.store)
        {
            return false;
        }
        let Ok(committed) = p.membership_at(p.commit_index) else {
            return false;
        };
        let eligible = committed.voter_store(candidate.node) == Some(candidate.store)
            && (configuration == committed.id()
                || committed
                    .joint()
                    .is_some_and(|j| configuration == j.next.id()))
            && p.commit_index > 0;
        if granted {
            eligible && index == p.commit_index && p.term_at(index) == Some(term)
        } else {
            !eligible && index == 0 && term == 0
        }
    }
}
