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
use super::{oracle, types::*};
use std::collections::{HashMap, VecDeque};
const DEPTH: usize = 384;
const STATES: usize = 500_000;
const EDGES: usize = 5_000_000;
#[derive(Debug, Default)]
pub struct Report {
    pub states: usize,
    pub edges: usize,
    pub depth: usize,
    pub terminals: usize,
    pub losses: usize,
    pub cuts: usize,
    pub recoveries: usize,
    pub partitions: usize,
    pub repairs: usize,
    pub quorum_before_third: usize,
    pub observed_before_third: usize,
    pub refused_terminals: usize,
    pub read_terminals: usize,
    pub depth_cap: bool,
    pub state_cap: bool,
    pub edge_cap: bool,
    pub exhausted: bool,
    pub frontier: usize,
    pub depth_frontier: usize,
}
pub fn explore(graph: Graph) -> Report {
    let initial = State::new(graph);
    let mut seen = HashMap::from([(initial.clone(), 0usize)]);
    let mut queue = VecDeque::from([initial]);
    let mut report = Report::default();
    while let Some(state) = queue.pop_front() {
        let depth = seen[&state];
        report.depth = report.depth.max(depth);
        oracle::check(&state).unwrap_or_else(|e| panic!("{graph:?} {e}: {state:?}"));
        if state.terminal {
            report.terminals += 1;
            report.refused_terminals += usize::from(state.refused != 0);
            report.read_terminals +=
                usize::from(state.reads.iter().any(|r| r.completed == Some(Value::Y)));
        }
        let steps = state.steps(Weak::None);
        if depth == DEPTH && !steps.is_empty() {
            report.depth_cap = true;
            report.depth_frontier += 1;
            continue;
        }
        for step in steps {
            if report.edges == EDGES {
                report.edge_cap = true;
                break;
            }
            report.edges += 1;
            count(&mut report, &state, step);
            let next = state.advance(step, Weak::None);
            oracle::transition(&state, step, &next)
                .unwrap_or_else(|e| panic!("{graph:?} {e} after {step:?}: {state:?} -> {next:?}"));
            if !seen.contains_key(&next) {
                if seen.len() == STATES {
                    report.state_cap = true;
                    break;
                }
                seen.insert(next.clone(), depth + 1);
                queue.push_back(next);
            }
        }
        if report.state_cap || report.edge_cap {
            break;
        }
    }
    report.states = seen.len();
    report.frontier = queue.len();
    report.exhausted =
        queue.is_empty() && !report.depth_cap && !report.state_cap && !report.edge_cap;
    report
}
fn count(report: &mut Report, state: &State, step: Step) {
    report.losses += usize::from(step == Step::Lose);
    report.cuts += usize::from(matches!(step, Step::Cut(_)));
    report.recoveries += usize::from(step == Step::Recover);
    report.partitions += usize::from(matches!(step, Step::Partition(_)));
    report.repairs += usize::from(step == Step::Repair);
    if let Some(p) = state.pending {
        let all = state.cells[p.cell]
            .physical
            .iter()
            .all(|s| s.synced >= p.position);
        report.quorum_before_third += usize::from(step == Step::Commit && !all);
        report.observed_before_third += usize::from(step == Step::Observe && !all);
    }
}
#[derive(Debug)]
pub struct Counterexample {
    pub trace: Vec<Step>,
    pub failure: &'static str,
    pub states: usize,
    pub edges: usize,
}
pub fn counterexample(graph: Graph, weak: Weak) -> Counterexample {
    let initial = State::new(graph);
    let mut states = vec![initial.clone()];
    let mut parents = vec![None];
    let mut seen = HashMap::from([(initial, 0usize)]);
    let mut queue = VecDeque::from([(0usize, 0usize)]);
    let mut edges = 0;
    while let Some((index, depth)) = queue.pop_front() {
        let state = &states[index];
        if goal(state, weak) {
            if let Err(failure) = oracle::check(state) {
                let mut trace = Vec::new();
                let mut cursor = index;
                while let Some((parent, step)) = parents[cursor] {
                    trace.push(step);
                    cursor = parent;
                }
                trace.reverse();
                return Counterexample {
                    trace,
                    failure,
                    states: states.len(),
                    edges,
                };
            }
        }
        assert!(
            depth < DEPTH || state.steps(weak).is_empty(),
            "counterexample depth cap"
        );
        if depth == DEPTH {
            continue;
        }
        let steps = state.steps(weak);
        for step in steps {
            edges += 1;
            assert!(edges <= EDGES, "counterexample edge cap");
            let next = states[index].advance(step, weak);
            if !seen.contains_key(&next) {
                assert!(states.len() < STATES, "counterexample state cap");
                let id = states.len();
                seen.insert(next.clone(), id);
                states.push(next);
                parents.push(Some((index, step)));
                queue.push_back((id, depth + 1));
            }
        }
    }
    panic!("{weak:?} exhausted without required counterexample")
}
fn goal(state: &State, weak: Weak) -> bool {
    match weak {
        Weak::WorkerOnly => state.reads.iter().any(|r| r.old && r.completed.is_some()),
        Weak::EraseVote => state.grants[0][0] == 3,
        Weak::CountPhysical => state
            .elections
            .iter()
            .any(|e| e.counted != e.logical.count_ones() as u8),
        Weak::StaleCompletion => state.releases.iter().any(|r| r.source != r.expected),
        Weak::Written => state
            .releases
            .iter()
            .any(|r| r.child_apply < r.source.action),
        Weak::None => false,
    }
}
pub fn replay(graph: Graph, trace: &[Step], weak: Weak) -> State {
    trace
        .iter()
        .fold(State::new(graph), |state, step| state.advance(*step, weak))
}
pub fn complete(initial: State) -> Vec<Step> {
    let mut states = vec![initial.clone()];
    let mut parents = vec![None];
    let mut seen = HashMap::from([(initial, 0usize)]);
    let mut queue = VecDeque::from([(0usize, 0usize)]);
    while let Some((index, depth)) = queue.pop_front() {
        if states[index].terminal {
            let mut trace = Vec::new();
            let mut cursor = index;
            while let Some((parent, step)) = parents[cursor] {
                trace.push(step);
                cursor = parent;
            }
            trace.reverse();
            return trace;
        }
        assert!(depth < DEPTH, "safe continuation depth cap");
        for step in states[index].steps(Weak::None) {
            let next = states[index].advance(step, Weak::None);
            oracle::transition(&states[index], step, &next).unwrap();
            if !seen.contains_key(&next) {
                assert!(states.len() < STATES, "safe continuation state cap");
                let id = states.len();
                seen.insert(next.clone(), id);
                states.push(next);
                parents.push(Some((index, step)));
                queue.push_back((id, depth + 1));
            }
        }
    }
    panic!("safe counterpart cannot progress")
}
