// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::{oracle, protocol, scenarios::*, types::*};
use std::collections::{HashSet, VecDeque};

const DEPTH: usize = 1024;
const STATES: usize = 500_000;
const EDGES: usize = 5_000_000;
#[derive(Debug, Default)]
pub struct Report {
    pub states: usize,
    pub edges: usize,
    pub depth: usize,
    pub frontier: usize,
    pub state_limited: bool,
    pub edge_limited: bool,
    pub depth_limited: bool,
    pub exhausted: bool,
    pub terminals: usize,
    pub child_first: usize,
    pub parent_first: usize,
    pub losses: usize,
    pub restarts: usize,
    pub observed: usize,
    pub rollback_eligibility: usize,
    pub readiness_refusals: usize,
    pub overlap_refusals: usize,
}
pub fn explore(profile: Profile) -> Report {
    let start = initial(profile);
    let mut seen = HashSet::from([start.clone()]);
    let mut queue = VecDeque::from([(start, 0)]);
    let mut report = Report::default();
    while let Some((state, depth)) = queue.pop_front() {
        report.depth = report.depth.max(depth);
        oracle::check(&state).unwrap_or_else(|e| panic!("{e}: {state:?}"));
        let script = program(profile, state.choice);
        let steps = protocol::steps(&state, &script, Weakening::None);
        if depth == DEPTH && !steps.is_empty() {
            report.depth_limited = true;
            continue;
        }
        for action in steps {
            if report.edges == EDGES {
                report.edge_limited = true;
                break;
            }
            report.edges += 1;
            report.losses += usize::from(action == Step::Lose);
            report.restarts +=
                usize::from(matches!(action, Step::Restart { .. } | Step::RestartParent));
            report.observed +=
                usize::from(matches!(action, Step::Observe(_) | Step::ObserveParent));
            let next = protocol::advance(&state, action, &script, Weakening::None);
            oracle::check(&next).unwrap_or_else(|e| panic!("{e} after {action:?}: {state:?}"));
            if seen.contains(&next) {
                continue;
            }
            if seen.len() == STATES {
                report.state_limited = true;
                break;
            }
            seen.insert(next.clone());
            queue.push_back((next, depth + 1));
        }
        if report.state_limited || report.edge_limited {
            break;
        }
    }
    report.states = seen.len();
    report.frontier = queue.len();
    for s in &seen {
        if s.terminals != 0 {
            report.terminals += 1;
            report.child_first += usize::from(s.choice == Some(Layer::Child));
            report.parent_first += usize::from(s.choice == Some(Layer::Parent));
        }
        report.rollback_eligibility += usize::from(s.rollback_reeligible);
        report.readiness_refusals += usize::from(s.ready_refused);
        report.overlap_refusals += usize::from(s.overlap_refused);
    }
    report.exhausted = report.frontier == 0
        && !report.state_limited
        && !report.edge_limited
        && !report.depth_limited;
    report
}
pub fn counterexample(profile: Profile, weak: Weakening) -> (Vec<Step>, &'static str) {
    let start = initial(profile);
    let mut queue = VecDeque::from([(start.clone(), Vec::new())]);
    let mut seen = HashSet::from([start]);
    while let Some((state, trace)) = queue.pop_front() {
        if let Err(error) = oracle::check(&state) {
            return (trace, error);
        }
        assert!(
            seen.len() < STATES && trace.len() < DEPTH,
            "counterexample search truncated"
        );
        let script = program(profile, state.choice);
        for action in protocol::steps(&state, &script, weak) {
            let next = protocol::advance(&state, action, &script, weak);
            if seen.insert(next.clone()) {
                let mut history = trace.clone();
                history.push(action);
                queue.push_back((next, history));
            }
        }
    }
    panic!("no actual counterexample for {profile:?}/{weak:?}");
}
pub fn replay(profile: Profile, weak: Weakening, trace: &[Step]) -> Result<(), &'static str> {
    let mut state = initial(profile);
    for &action in trace {
        let script = program(profile, state.choice);
        assert!(protocol::steps(&state, &script, weak).contains(&action));
        state = protocol::advance(&state, action, &script, weak);
        oracle::check(&state)?;
    }
    Ok(())
}
pub fn safe_progress(profile: Profile, trace: &[Step]) -> usize {
    let mut state = initial(profile);
    for &action in trace {
        let script = program(profile, state.choice);
        if !protocol::steps(&state, &script, Weakening::None).contains(&action) {
            // The exact unsafe Commit is unavailable without a joint quorum.
            break;
        }
        state = protocol::advance(&state, action, &script, Weakening::None);
        oracle::check(&state).unwrap();
    }
    for resumed in 0..DEPTH {
        if state.terminals != 0 {
            return resumed;
        }
        let script = program(profile, state.choice);
        let available = protocol::steps(&state, &script, Weakening::None);
        // Take the selected uncommitted cut before further replication changes
        // its witness set; normal Next requires complete actual consumption.
        let action = *available
            .iter()
            .find(|s| **s == Step::Next)
            .or_else(|| available.first())
            .unwrap_or_else(|| panic!("safe refusal stranded accepted work: {state:?}"));
        state = protocol::advance(&state, action, &script, Weakening::None);
        oracle::check(&state).unwrap();
    }
    panic!("safe resumed schedule exceeded declared depth");
}
