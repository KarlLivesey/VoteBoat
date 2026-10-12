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
//! Enumerate original physical witnesses; never trust relay counters or reports.
use super::types::*;
use std::collections::BTreeSet;
pub fn check(s: &State, p: Profile) -> Option<Violation> {
    if s.packet.map(|packet| packet.ticket) != s.credit {
        return Some(Violation::Ownership);
    }
    if let Some(violation) = request_check(s, p) {
        return Some(violation);
    }
    if !s.released[s.epoch as usize] {
        return None;
    }
    let c = p.context(s.epoch);
    let local = s.actors[0].synced;
    if local.group_incarnation != c.incarnation
        || local.term != c.term
        || local.configuration != c.configuration
    {
        return Some(Violation::MissingLocalPersistence);
    }
    let mut witnesses = BTreeSet::new();
    for id in &s.claims {
        let Some(original) = s.originals.get(*id as usize).and_then(|r| *r) else {
            return Some(Violation::ImaginaryVoter);
        };
        if !(1..=3).contains(&original.physical.node) {
            return Some(Violation::ImaginaryVoter);
        }
        let expected_incarnation =
            1 + u8::from(p == Profile::Store && s.epoch == 1 && original.physical.node == 2);
        if original.physical.store != 99 + original.physical.node as u16
            || original.physical.incarnation != expected_incarnation
        {
            return Some(Violation::Store);
        }
        if original.context != c {
            return Some(Violation::Scope);
        }
        if !witnesses.insert(original.physical.node) {
            return Some(Violation::DuplicateVoter);
        }
        if !original.granted {
            return Some(Violation::ImaginaryVoter);
        }
        let disk = original.actual_synced;
        if disk.term != c.term
            || disk.configuration != c.configuration
            || disk.group_incarnation != c.incarnation
        {
            return Some(Violation::WrittenBeforeSync);
        }
        match c.kind {
            Kind::Election => {
                if disk.vote != c.candidate {
                    return Some(Violation::WrittenBeforeSync);
                }
                let before = original.before;
                let last = if before.prefix == 0 {
                    0
                } else {
                    before.terms[before.prefix as usize - 1]
                };
                if (last, before.prefix) > (c.last_term, c.prefix) {
                    return Some(Violation::StaleCandidate);
                }
            }
            Kind::Append => {
                if disk.prefix < c.prefix {
                    return Some(Violation::WrittenBeforeSync);
                }
                for i in 0..c.prefix as usize {
                    if disk.entries[i] != (i as u8 + 1) * 11 || disk.terms[i] != c.term {
                        return Some(Violation::MissingPrefix);
                    }
                }
            }
            Kind::Read => {
                // Initial read authority is a qualified current-term committed/
                // applied entry; original fresh request scope is checked above.
                if local.prefix == 0 || local.terms[0] != c.term || local.entries[0] != 11 {
                    return Some(Violation::MissingPrefix);
                }
            }
        }
    }
    if witnesses.len() <= 3 / 2 {
        return Some(Violation::ImaginaryVoter);
    }
    None
}

fn request_check(s: &State, p: Profile) -> Option<Violation> {
    if let Some(ticket) = s.request_ticket {
        if ticket.relay != RELAY
            || ticket.generation != s.generation
            || s.requests[s.epoch as usize] != Some(ticket.original)
            || s.retired[s.epoch as usize]
        {
            return Some(Violation::Ownership);
        }
        let original = ticket.original;
        let c = p.context(s.epoch);
        let disk = original.local_synced;
        if original.physical != s.expected_physical(p, 0) || original.context != c {
            return Some(Violation::Scope);
        }
        if disk.group_incarnation != c.incarnation
            || disk.term != c.term
            || disk.configuration != c.configuration
            || (c.kind == Kind::Election && disk.vote != c.candidate)
        {
            return Some(Violation::MissingLocalPersistence);
        }
        if c.kind == Kind::Append
            && (disk.prefix < c.prefix
                || (0..c.prefix as usize)
                    .any(|i| disk.entries[i] != (i as u8 + 1) * 11 || disk.terms[i] != c.term))
        {
            return Some(Violation::MissingPrefix);
        }
    }
    None
}
