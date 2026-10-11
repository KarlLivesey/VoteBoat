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
//! Independent assertions use no transition or projection helper.
use super::{program::Job, types::*};
pub fn check(state: &State) -> Result<(), &'static str> {
    for read in &state.reads {
        if read.completed.is_some() && read.completed != read.invoked_after {
            return Err("fresh read returned older acknowledged value");
        }
        if read.completed.is_some() && !majority(read.evidence) {
            return Err("read lacks distinct logical quorum");
        }
    }
    for votes in state.grants {
        if votes.into_iter().any(|v| v == 3) {
            return Err("logical voter durably granted both candidates in one term");
        }
    }
    for election in &state.elections {
        if !majority(election.logical) || election.counted != election.logical.count_ones() as u8 {
            return Err("physical representatives counted as extra logical voters");
        }
    }
    for release in &state.releases {
        if release.expected != release.source {
            return Err("old action completion freed a fresh effect");
        }
        let source = release.source;
        let record = state.cells[source.cell as usize]
            .records
            .get(source.action as usize - 1)
            .ok_or("release has no original child command")?;
        if record.ticket != source
            || release.child_commit < source.action
            || release.child_apply < source.action
        {
            return Err("written child action escaped before exact commit and apply");
        }
    }
    for (cell_id, cell) in state.cells.iter().enumerate() {
        check_cell(cell_id, cell)?;
    }
    for packet in &state.packets {
        packet_source(state, *packet)?;
    }
    let credits = usize::from(state.pending.is_some_and(|p| p.wait_owned))
        + usize::from(state.held.is_some());
    if credits > 2 || state.packets.len() > 2 {
        return Err("owned buffers exceed declared bounds");
    }
    if state.terminal
        && (state.pending.is_some() || state.held.is_some() || !state.packets.is_empty())
    {
        return Err("terminal retained original ownership");
    }
    Ok(())
}
fn majority(mask: u8) -> bool {
    mask & 3 == 3 || mask & 5 == 5 || mask & 6 == 6
}
fn check_cell(cell_id: usize, cell: &Cell) -> Result<(), &'static str> {
    if cell.records.len() > 48
        || cell.applied > cell.committed
        || cell.committed as usize > cell.records.len()
    {
        return Err("child ordered boundaries invalid");
    }
    for physical in cell.physical {
        if physical.synced > physical.written || physical.written as usize > cell.records.len() {
            return Err("physical durable prefix exceeds written prefix");
        }
    }
    let mut image = Image::default();
    for (index, record) in cell.records.iter().enumerate() {
        let position = index as u8 + 1;
        let t = record.ticket;
        if t.cell as usize != cell_id
            || t.action != position
            || t.child_incarnation != 1
            || t.session != t.epoch
            || t.physical + 1 != t.epoch
        {
            return Err("child action identity scope changed");
        }
        if record.committed != (position <= cell.committed)
            || record.applied != (position <= cell.applied)
        {
            return Err("child commit/application has a hole");
        }
        if !record.applied {
            continue;
        }
        if record.expected_revision != image.revision {
            return Err("applied child action used stale revision");
        }
        if !matches!(record.body, Body::Claim(_)) && t.epoch != image.epoch {
            return Err("old representative action applied after fence");
        }
        match record.body {
            Body::Claim(epoch) => {
                if epoch != image.epoch + 1 {
                    return Err("representative fence did not advance once");
                }
                image.epoch = epoch;
            }
            Body::Promise {
                term,
                candidate,
                last,
            } => {
                let local = image
                    .entries
                    .iter()
                    .enumerate()
                    .rev()
                    .find_map(|(i, e)| e.map(|e| (e.term, i as u8 + 1)))
                    .unwrap_or((0, 0));
                if term < image.term
                    || last < local
                    || (term == image.term && image.vote.is_some_and(|old| old != candidate))
                {
                    return Err("promise violates term vote or candidate freshness");
                }
                image.term = term;
                image.vote = Some(candidate);
            }
            Body::Append { index, entry } => {
                if entry.term != image.term
                    || index == 0
                    || index > 2
                    || (index == 2 && image.entries[0].is_none())
                {
                    return Err("parent accepted nonmatching or noncontiguous prefix");
                }
                image.entries[index as usize - 1] = Some(entry);
            }
            Body::Commit(index) => {
                if index < image.commit || image.entries[index as usize - 1].is_none() {
                    return Err("parent commit boundary has no payload");
                }
                image.commit = index;
            }
            Body::Capsule(packet) => validate_capsule(packet, t, image)?,
        }
        image.revision += 1;
    }
    if image != cell.image {
        return Err("representative replacement lost committed parent state");
    }
    Ok(())
}
fn validate_capsule(packet: Capsule, ticket: Ticket, image: Image) -> Result<(), &'static str> {
    if packet.ticket != ticket
        || packet.parent_incarnation != 1
        || packet.logical_store != ticket.cell + 10
        || packet.configuration != 1
        || packet.requester_session != 1
        || packet.requester != packet.candidate.map_or(packet.term, |c| c.bit())
        || packet.term != image.term
    {
        return Err("capsule identity term or original ticket mismatch");
    }
    match packet.kind {
        Kind::Vote if packet.candidate != image.vote => {
            return Err("vote response differs from durable promise");
        }
        Kind::Append | Kind::Read
            if packet.index == 0
                || packet.index > 2
                || packet.entry != image.entries[packet.index as usize - 1] =>
        {
            return Err("capsule lacks exact parent payload");
        }
        _ => {}
    }
    Ok(())
}
fn packet_source(state: &State, packet: Capsule) -> Result<(), &'static str> {
    let ticket = packet.ticket;
    if ticket.action == 0 {
        return Err("cached response escaped without child capsule");
    }
    let cell = &state.cells[ticket.cell as usize];
    let record = cell
        .records
        .get(ticket.action as usize - 1)
        .ok_or("capsule child record absent")?;
    if record.ticket != ticket
        || record.body != Body::Capsule(packet)
        || !record.committed
        || !record.applied
    {
        return Err("response capsule lacks original committed applied child result");
    }
    Ok(())
}
pub fn transition(before: &State, step: Step, after: &State) -> Result<(), &'static str> {
    if matches!(step, Step::Commit | Step::Resolve) {
        let pending = before.pending.unwrap();
        let cell = &before.cells[pending.cell];
        let mut actual = 0;
        for (i, physical) in cell.physical.iter().enumerate() {
            let available = before
                .outage
                .is_none_or(|o| o.restored || o.cell != pending.cell || o.live & (1 << i) != 0);
            if available && physical.synced >= pending.position {
                actual |= 1 << i;
            }
        }
        if !majority(actual) {
            return Err("child commitment has no matching synchronized physical majority");
        }
    }
    if step == Step::Apply {
        let p = before.pending.unwrap();
        let c = &before.cells[p.cell];
        let original = &c.records[p.position as usize - 1];
        if c.physical[original.ticket.physical as usize].synced < p.position {
            return Err("selected child applier has no synchronized original prefix");
        }
    }
    if step == Step::Recover {
        let p = before.pending.unwrap();
        let c = &before.cells[p.cell];
        let selected = c.image.epoch.saturating_sub(1) as usize;
        if c.physical[selected].synced < c.applied {
            return Err("selected representative recovery lacks own synchronized prefix");
        }
        let mut matching = 0;
        for (i, s) in c.physical.iter().enumerate() {
            let available = before
                .outage
                .is_none_or(|o| o.restored || o.cell != p.cell || o.live & (1 << i) != 0);
            if available && s.synced >= c.applied {
                matching |= 1 << i;
            }
        }
        if !majority(matching) || after.cache[p.cell] != Some(c.image) {
            return Err("recovery lacks quorum or exact applied child image");
        }
    }
    if step == Step::Repair {
        let old = before.outage.unwrap();
        let repaired = after.outage.unwrap();
        if repaired.cell != old.cell || repaired.live != old.live || !repaired.restored {
            return Err("repair changed original outage identity");
        }
    }
    if step == Step::Parent {
        parent_transition(before, after)?;
    }
    check(after)
}
fn parent_transition(before: &State, after: &State) -> Result<(), &'static str> {
    match before.job() {
        Job::Ack(value) => {
            let leader = before.cells[0].image;
            let index = leader.commit;
            let mut matching = 0;
            for packet in &before.packets {
                packet_source(before, *packet)?;
                if packet.kind == Kind::Append
                    && packet.term == leader.term
                    && packet.context == 2 + index
                    && packet.index == index
                    && packet.entry == leader.entries[index as usize - 1]
                    && packet.entry.is_some_and(|e| e.value == value)
                {
                    matching |= 1 << packet.ticket.cell;
                }
            }
            if !majority(matching) || after.writes.last() != Some(&value) {
                return Err("acknowledged write lacks exact current-term logical quorum");
            }
        }
        Job::Read(old) => {
            let read = after.reads.iter().find(|r| r.old == old).unwrap();
            if read.completed.is_none() {
                return Ok(());
            }
            let (term, context, index) = if old { (1, 5, 1) } else { (2, 6, 2) };
            let mut matching = 0;
            for packet in &before.packets {
                packet_source(before, *packet)?;
                if packet.kind == Kind::Read
                    && packet.term == term
                    && packet.context == context
                    && packet.index == index
                    && packet
                        .entry
                        .is_some_and(|e| Some(e.value) == read.completed)
                {
                    matching |= 1 << packet.ticket.cell;
                }
            }
            if !majority(matching) || read.evidence != matching {
                return Err("read lacks exact original-context authorized responses");
            }
        }
        _ => {}
    }
    Ok(())
}
