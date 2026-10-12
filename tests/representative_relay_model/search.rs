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
//! Exhaust the declared finite graph and replay actual first unsafe schedules.
use super::{oracle, protocol, types::*};
use std::collections::{hash_map::Entry, HashMap};
const STATES: usize = 5_000_000;
const EDGES: usize = 80_000_000;
const DEPTH: usize = 128;
struct Node {
    state: State,
    parent: Option<(usize, Action)>,
    depth: usize,
}
pub fn explore(p: Profile, rule: Rule) -> Result<Graph, Failure> {
    let initial = State::initial(p);
    let mut seen = HashMap::from([(seen_key(&initial), 0)]);
    let mut nodes = vec![Node {
        state: initial,
        parent: None,
        depth: 0,
    }];
    let mut report = Graph::default();
    let mut cursor = 0;
    while cursor < nodes.len() {
        let (s, depth) = (nodes[cursor].state.clone(), nodes[cursor].depth);
        report.terminals += usize::from(s.terminal());
        let actions = protocol::actions(&s, p, rule);
        for action in actions {
            report.edges += 1;
            report.requests_issued += usize::from(action == Action::IssueRequest);
            report.requests_consumed += usize::from(action == Action::ConsumeRequest);
            report.requests_returned += usize::from(action == Action::ReturnRequest);
            report.requests_retired += usize::from(action == Action::RetireRequest);
            assert!(report.edges <= EDGES, "relay edgecap: unexplored frontier");
            report.loss += usize::from(action == Action::Lose);
            report.replay += usize::from(matches!(action, Action::Replay(_)));
            report.relay_restart += usize::from(action == Action::RestartRelay);
            report.source_crash += usize::from(matches!(action, Action::SourceCrash(_)));
            report.cancelled += usize::from(matches!(action, Action::Cancel(_)));
            report.backpressure += usize::from(matches!(action, Action::Backpressure(_)));
            report.stall += usize::from(action == Action::Stall);
            let next = protocol::apply(&s, action, p, rule);
            if let Some(violation) = oracle::check(&next, p) {
                let mut trace = original_trace(&nodes, cursor);
                trace.push(action);
                return Err(Failure { violation, trace });
            }
            match seen.entry(seen_key(&next)) {
                Entry::Occupied(entry) => {
                    let original = &nodes[*entry.get()].state;
                    if *original != next {
                        assert_equivalent(original, &next, p, rule);
                        report.projected_collisions += 1;
                    }
                }
                Entry::Vacant(entry) => {
                    let depth = depth + 1;
                    assert!(depth <= DEPTH, "relay depthcap: unexplored frontier");
                    let index = nodes.len();
                    entry.insert(index);
                    nodes.push(Node {
                        state: next,
                        parent: Some((cursor, action)),
                        depth,
                    });
                    if nodes.len() > STATES {
                        let old = nodes.iter().filter(|n| n.state.epoch == 0).count();
                        panic!(
                        "relay statecap: states={} edges={} depth={} frontier={} epochs=[{},{}]",
                        nodes.len(),
                        report.edges,
                        depth,
                        nodes.len() - cursor,
                        old,
                        nodes.len() - old
                    );
                    }
                    report.depth = report.depth.max(depth);
                }
            }
        }
        cursor += 1;
    }
    report.states = nodes.len();
    report.frontier = nodes.len() - cursor;
    Ok(report)
}
fn original_trace(nodes: &[Node], mut index: usize) -> Vec<Action> {
    let mut out = Vec::new();
    while let Some((parent, action)) = nodes[index].parent {
        out.push(action);
        index = parent;
    }
    out.reverse();
    out
}
pub fn replay(p: Profile, rule: Rule, trace: &[Action]) -> Violation {
    let mut state = State::initial(p);
    for (i, &action) in trace.iter().enumerate() {
        assert!(
            protocol::actions(&state, p, rule).contains(&action),
            "original enabled action {action:?}"
        );
        state = protocol::apply(&state, action, p, rule);
        if let Some(violation) = oracle::check(&state, p) {
            assert_eq!(
                i + 1,
                trace.len(),
                "replay reached an earlier unrelated violation"
            );
            return violation;
        }
    }
    panic!("original trace did not reproduce an invariant violation");
}

pub(super) fn seen_key(s: &State) -> State {
    let mut key = s.clone();
    for a in &mut key.actors {
        if matches!(a.io, Io::Idle | Io::Emitted) {
            a.before = Image::default();
            a.prepared = Image::default();
            a.written = Image::default();
            a.granted = false;
        }
    }
    // Only ownership nonce labels are canonicalized; actual nodes remain exact.
    key.sequence = 0;
    let mut names = Vec::new();
    for ticket in [
        key.packet.as_mut().map(|p| &mut p.ticket),
        key.credit.as_mut(),
        key.obsolete.as_mut().map(|p| &mut p.ticket),
    ]
    .into_iter()
    .flatten()
    {
        let original = *ticket;
        let label = match names.iter().position(|t| *t == original) {
            Some(i) => i,
            None => {
                names.push(original);
                names.len() - 1
            }
        };
        ticket.sequence = label as u8 + 1;
    }
    key
}
pub(super) fn assert_equivalent(a: &State, b: &State, p: Profile, rule: Rule) {
    let actions = protocol::actions(a, p, rule);
    assert_eq!(actions, protocol::actions(b, p, rule));
    assert_eq!(oracle::check(a, p), oracle::check(b, p));
    for action in actions {
        let x = protocol::apply(a, action, p, rule);
        let y = protocol::apply(b, action, p, rule);
        assert_eq!(
            seen_key(&x),
            seen_key(&y),
            "non-congruent actor projection at {action:?}"
        );
        assert_eq!(oracle::check(&x, p), oracle::check(&y, p));
    }
}
