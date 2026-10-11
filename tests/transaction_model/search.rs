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
use super::{oracle, protocol::*};
use std::collections::{HashSet, VecDeque};
#[derive(Debug)]
pub struct Report {
    pub states: usize,
    pub edges: usize,
    pub losses: usize,
    pub restarts: usize,
    pub finished_commit_states: usize,
    pub finished_abort_states: usize,
    pub two_commit_finished_states: usize,
    pub commit_abort_finished_states: usize,
    pub depth_limited: bool,
    pub state_limited: bool,
    pub exhausted: bool,
}
pub fn explore(initial: State, depth_limit: usize, state_limit: usize) -> Report {
    let mut seen = HashSet::from([initial.clone()]);
    let mut queue = VecDeque::from([(initial, 0)]);
    let mut report = Report {
        states: 1,
        edges: 0,
        losses: 0,
        restarts: 0,
        finished_commit_states: 0,
        finished_abort_states: 0,
        two_commit_finished_states: 0,
        commit_abort_finished_states: 0,
        depth_limited: false,
        state_limited: false,
        exhausted: false,
    };
    while let Some((state, depth)) = queue.pop_front() {
        oracle::check(&state).unwrap_or_else(|error| panic!("{error}: {state:?}"));
        if depth == depth_limit {
            report.depth_limited = true;
            continue;
        }
        for step in state.steps(Weakening::None) {
            report.edges += 1;
            report.losses += usize::from(matches!(step, Step::Lose(_)));
            report.restarts += usize::from(step == Step::Restart);
            let next = state.advance(step, Weakening::None);
            oracle::check(&next)
                .unwrap_or_else(|error| panic!("{error} after {step:?}: {state:?} -> {next:?}"));
            if !seen.contains(&next) {
                if seen.len() == state_limit {
                    report.state_limited = true;
                    break;
                }
                seen.insert(next.clone());
                queue.push_back((next, depth + 1));
            }
        }
        if report.state_limited {
            break;
        }
    }
    report.states = seen.len();
    for state in &seen {
        let phases = state.coordinator.map(|c| c.phase);
        let commit = CoordinatorPhase::Finished(Decision::Commit);
        let abort = CoordinatorPhase::Finished(Decision::Abort);
        report.finished_commit_states += usize::from(phases.contains(&commit));
        report.finished_abort_states += usize::from(phases.contains(&abort));
        report.two_commit_finished_states += usize::from(phases == [commit; 2]);
        report.commit_abort_finished_states +=
            usize::from(phases == [commit, abort] || phases == [abort, commit]);
    }
    report.exhausted = queue.is_empty() && !report.depth_limited && !report.state_limited;
    report
}

pub fn counterexample(state: State, weak: Weakening) -> (Vec<Step>, &'static str) {
    let mut queue = VecDeque::from([(state.clone(), Vec::new())]);
    let mut seen = HashSet::from([state]);
    while let Some((state, history)) = queue.pop_front() {
        if let Err(error) = oracle::check(&state) {
            return (history, error);
        }
        if history.len() == 3 {
            continue;
        }
        for step in state.steps(weak) {
            let next = state.advance(step, weak);
            if seen.insert(next.clone()) {
                let mut trace = history.clone();
                trace.push(step);
                queue.push_back((next, trace));
            }
        }
    }
    panic!("weakening {weak:?} had no counterexample within three additional steps");
}
