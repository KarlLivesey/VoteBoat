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
//! Relay protocol uses reported originals; independent oracle checks own sync.
use super::types::*;
pub fn actions(s: &State, p: Profile, rule: Rule) -> Vec<Action> {
    let mut out = Vec::new();
    request_actions(s, p, &mut out);
    source_actions(s, rule, &mut out);
    transport_actions(s, p, rule, &mut out);
    if !s.released[s.epoch as usize] && s.claims.len() >= 2 && local_ready(s, p) {
        out.push(Action::Release);
    }
    if s.epoch == 0
        && s.released[0]
        && s.retired[0]
        && s.request_ticket.is_none()
        && s.queue.is_empty()
        && s.packet.is_none()
        && s.obsolete.is_none()
        && s.actors.iter().all(|a| a.io == Io::Emitted)
    {
        out.push(Action::Next);
    }
    out
}
fn request_actions(s: &State, p: Profile, out: &mut Vec<Action>) {
    let epoch = s.epoch as usize;
    if s.requests[epoch].is_none() && local_ready(s, p) {
        out.push(Action::IssueRequest);
    }
    if s.requests[epoch].is_some() && !s.retired[epoch] {
        if s.request_ticket.is_none() {
            out.push(Action::ConsumeRequest);
        } else if s.queue.is_empty()
            && s.packet.is_none()
            && s.obsolete.is_none()
            && s.actors
                .iter()
                .all(|a| matches!(a.io, Io::Idle | Io::Emitted))
        {
            out.push(Action::ReturnRequest);
            if s.released[epoch] && s.actors.iter().all(|a| a.io == Io::Emitted) {
                out.push(Action::RetireRequest);
            }
        }
    }
}
fn source_actions(s: &State, rule: Rule, out: &mut Vec<Action>) {
    for (i, a) in s.actors.iter().enumerate() {
        let id = s.epoch * 3 + i as u8;
        if s.originals[id as usize].is_none() {
            match a.io {
                Io::Idle if i == 0 || s.request_ticket.is_some() => {
                    out.push(Action::Receive(i as u8));
                }
                Io::Accepted => out.push(Action::Write(i as u8)),
                Io::Written => {
                    out.push(Action::Sync(i as u8));
                    if rule == Rule::WrittenAck {
                        out.push(Action::Emit(i as u8));
                    }
                }
                Io::Synced => out.push(Action::Emit(i as u8)),
                _ => (),
            }
        }
        if !a.crashed && matches!(a.io, Io::Accepted | Io::Written) {
            out.push(Action::SourceCrash(i as u8));
        }
    }
}
fn transport_actions(s: &State, p: Profile, rule: Rule, out: &mut Vec<Action>) {
    for (i, original) in s.originals.iter().enumerate() {
        let id = i as u8;
        let Some(original) = original else {
            continue;
        };
        if reply_matches(s, *original, p, rule)
            && s.transmitted & (1 << id) == 0
            && !s.queue.contains(&id)
        {
            if s.queue.len() < 2 {
                out.push(Action::Capture(id));
            } else {
                out.push(Action::Backpressure(id));
            }
        }
        if s.queue.contains(&id) {
            out.push(Action::Cancel(id));
        }
        if s.wire_history & (1 << id) != 0 {
            out.push(Action::Replay(id));
        }
    }
    if !s.queue.is_empty() && s.packet.is_none() {
        out.push(Action::Export);
    }
    if let Some(packet) = s.packet {
        if packet.terminal {
            out.push(Action::Complete);
        } else if !s.stalled {
            out.extend([Action::Deliver, Action::Lose]);
        }
    }
    if let Some(packet) = s.obsolete {
        if packet.terminal {
            out.extend([Action::CompleteOld, Action::DiscardOld]);
        } else if !s.stalled {
            out.push(Action::DeliverOld);
        }
    }
    if !s.restarted {
        out.push(Action::RestartRelay);
    }
    out.push(if s.stalled {
        Action::Resume
    } else {
        Action::Stall
    });
}
fn reply_matches(s: &State, original: Original, p: Profile, rule: Rule) -> bool {
    let Some(ticket) = s.request_ticket else {
        return false;
    };
    rule == Rule::ScopeOmitted
        || (original.context == ticket.original.context
            && original.physical == s.expected_physical(p, original.physical.node as usize - 1))
}
fn local_ready(s: &State, p: Profile) -> bool {
    let c = p.context(s.epoch);
    let a = s.actors[0];
    matches!(a.io, Io::Synced | Io::Emitted)
        && a.synced.term == c.term
        && a.synced.group_incarnation == c.incarnation
        && a.synced.configuration == c.configuration
        && match c.kind {
            Kind::Election => a.synced.vote == c.candidate,
            Kind::Append => {
                a.synced.prefix >= c.prefix
                    && (0..c.prefix as usize).all(|i| {
                        a.synced.entries[i] == (i as u8 + 1) * 11 && a.synced.terms[i] == c.term
                    })
            }
            Kind::Read => a.synced.prefix > 0 && a.synced.terms[0] == c.term,
        }
}
pub fn apply(s: &State, action: Action, p: Profile, rule: Rule) -> State {
    let mut n = s.clone();
    match action {
        Action::IssueRequest => {
            n.requests[n.epoch as usize] = Some(ParentRequest {
                physical: n.actors[0].physical,
                context: p.context(n.epoch),
                local_synced: n.actors[0].synced,
            });
        }
        Action::ConsumeRequest => {
            n.request_ticket = Some(RequestTicket {
                relay: RELAY,
                generation: n.generation,
                original: n.requests[n.epoch as usize].unwrap(),
            });
        }
        Action::ReturnRequest => n.request_ticket = None,
        Action::RetireRequest => {
            n.retired[n.epoch as usize] = true;
            n.request_ticket = None;
        }
        Action::Receive(i) => receive(&mut n, i as usize, p, rule),
        Action::Write(i) => {
            let a = &mut n.actors[i as usize];
            a.written = a.prepared;
            a.io = Io::Written;
        }
        Action::Sync(i) => {
            let a = &mut n.actors[i as usize];
            a.synced = a.written;
            a.io = Io::Synced;
        }
        Action::Emit(i) => emit(&mut n, i as usize, p),
        Action::SourceCrash(i) => {
            let a = &mut n.actors[i as usize];
            a.written = a.synced;
            a.prepared = a.synced;
            a.io = Io::Idle;
            a.crashed = true;
        }
        Action::Capture(id) => {
            n.queue.push(id);
        }
        Action::Backpressure(_) => (),
        Action::Cancel(id) => {
            n.queue.retain(|v| *v != id);
        }
        Action::Export => export(&mut n),
        Action::Deliver => {
            let packet = n.packet.unwrap();
            deliver(&mut n, packet, p, rule);
            n.packet.as_mut().unwrap().terminal = true;
        }
        Action::Lose => n.packet.as_mut().unwrap().terminal = true,
        Action::Complete => {
            let packet = n.packet.take().unwrap();
            assert_eq!(n.credit, Some(packet.ticket));
            n.credit = None;
        }
        Action::Replay(id) => count(&mut n, id, p, rule),
        Action::RestartRelay => restart(&mut n),
        Action::DeliverOld => n.obsolete.as_mut().unwrap().terminal = true,
        Action::CompleteOld => {
            if rule == Rule::OldCompletion {
                n.credit = None;
            }
        }
        Action::DiscardOld => n.obsolete = None,
        Action::Stall => n.stalled = true,
        Action::Resume => n.stalled = false,
        Action::Release => n.released[n.epoch as usize] = true,
        Action::Next => next(&mut n, p),
    }
    n
}
fn receive(s: &mut State, i: usize, p: Profile, rule: Rule) {
    let c = if i == 0 {
        p.context(s.epoch)
    } else {
        s.request_ticket.unwrap().original.context
    };
    let a = &mut s.actors[i];
    a.before = a.synced;
    a.prepared = a.synced;
    a.prepared.group_incarnation = c.incarnation;
    a.prepared.configuration = c.configuration;
    a.prepared.term = c.term;
    a.granted = true;
    if c.kind == Kind::Election {
        let last = if a.synced.prefix == 0 {
            0
        } else {
            a.synced.terms[a.synced.prefix as usize - 1]
        };
        a.granted = c.term >= a.synced.term
            && (rule == Rule::FreshnessOmitted
                || (c.last_term, c.prefix) >= (last, a.synced.prefix))
            && (a.synced.term != c.term || a.synced.vote == 0 || a.synced.vote == c.candidate);
        if a.granted {
            a.prepared.vote = c.candidate;
        }
    } else if c.kind == Kind::Append {
        // Original full-prefix Append input, not recovery from an oracle image.
        for j in 0..c.prefix as usize {
            a.prepared.entries[j] = (j as u8 + 1) * 11;
            a.prepared.terms[j] = c.term;
        }
        a.prepared.prefix = c.prefix;
    }
    a.io = Io::Accepted;
}
fn emit(s: &mut State, i: usize, p: Profile) {
    let a = &mut s.actors[i];
    s.originals[s.epoch as usize * 3 + i] = Some(Original {
        physical: a.physical,
        context: p.context(s.epoch),
        granted: a.granted,
        before: a.before,
        reported: a.written,
        actual_synced: a.synced,
    });
    a.io = Io::Emitted;
}
fn export(s: &mut State) {
    s.sequence += 1;
    // Finite lifetime: each of six originals exports once per relay generation.
    assert!(s.sequence <= 6);
    let mut ids = [None; 2];
    for (slot, id) in s.queue.drain(..).enumerate() {
        ids[slot] = Some(id);
        s.transmitted |= 1 << id;
        s.wire_history |= 1 << id;
    }
    let ticket = Ticket {
        generation: s.generation,
        sequence: s.sequence,
    };
    s.packet = Some(Packet {
        relay: RELAY,
        ticket,
        ids,
        terminal: false,
    });
    s.credit = Some(ticket);
}
fn restart(s: &mut State) {
    s.obsolete = s.packet.take();
    s.credit = None;
    s.queue.clear();
    s.request_ticket = None;
    s.generation = 2;
    s.sequence = 0;
    s.transmitted = 0;
    s.restarted = true;
}
fn next(s: &mut State, p: Profile) {
    s.epoch = 1;
    s.claims.clear();
    for i in 0..3 {
        if p == Profile::Store && i == 1 {
            s.actors[i].physical = s.expected_physical(p, i);
            s.actors[i].synced = Image::default();
            s.actors[i].written = Image::default();
        }
        s.actors[i].io = Io::Idle;
    }
}
fn deliver(s: &mut State, packet: Packet, p: Profile, rule: Rule) {
    for id in packet.ids.into_iter().flatten() {
        count(s, id, p, rule);
    }
    if rule == Rule::RelayVote && !s.claims.contains(&9) && s.claims.len() < 3 {
        s.claims.push(packet.relay.node);
        s.claims.sort_unstable();
    }
}
fn count(s: &mut State, id: u8, p: Profile, rule: Rule) {
    if s.released[s.epoch as usize] || s.claims.len() >= 3 {
        return;
    }
    let original = s.originals[id as usize].unwrap();
    let c = p.context(s.epoch);
    if !original.granted {
        return;
    }
    if rule != Rule::ScopeOmitted {
        if original.context != c
            || original.physical != s.expected_physical(p, original.physical.node as usize - 1)
        {
            return;
        }
        if c.kind == Kind::Append
            && (original.reported.prefix < c.prefix
                || original.reported.entries[c.prefix as usize - 1] != c.prefix * 11)
        {
            return;
        }
    }
    if rule != Rule::DuplicateVote
        && s.claims.iter().any(|i| {
            *i < 6 && s.originals[*i as usize].unwrap().physical.node == original.physical.node
        })
    {
        return;
    }
    s.claims.push(id);
    s.claims.sort_unstable();
}
