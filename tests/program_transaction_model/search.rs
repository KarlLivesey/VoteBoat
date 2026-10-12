// SPDX-License-Identifier: RPL-1.5
use super::{oracle, protocol, types::*};
use std::collections::{HashMap, VecDeque};
const STATE_CAP: usize = 100_000;
const EDGE_CAP: usize = 500_000;
const DEPTH_CAP: usize = 128;
pub struct Report {
    pub states: usize,
    pub edges: usize,
    pub depth: usize,
    pub terminals: usize,
    pub trace: Option<Vec<Action>>,
    pub violation: Option<Violation>,
}
pub fn search(mode: Mode) -> Report {
    let start = State::new(mode);
    let mut states = vec![(start, None, 0)];
    let mut seen = HashMap::from([(start, 0)]);
    let mut queue = VecDeque::from([0]);
    let mut report = Report {
        states: 1,
        edges: 0,
        depth: 0,
        terminals: 0,
        trace: None,
        violation: None,
    };
    while let Some(index) = queue.pop_front() {
        let (state, _, depth) = states[index];
        report.depth = report.depth.max(depth);
        if state
            .tx
            .iter()
            .all(|t| t.finished && t.stage == Step::Done && t.pending.is_none())
        {
            report.terminals += 1;
        }
        let actions = protocol::actions(&state);
        assert!(
            depth < DEPTH_CAP || actions.is_empty(),
            "depth frontier not qualified"
        );
        for action in actions {
            report.edges += 1;
            assert!(report.edges <= EDGE_CAP, "edge cap not qualified");
            let next = protocol::apply(state, action);
            if let Err(violation) = oracle::check(&next) {
                let mut trace = vec![action];
                let mut current = index;
                while let Some((parent, action)) = states[current].1 {
                    trace.push(action);
                    current = parent;
                }
                trace.reverse();
                report.trace = Some(trace);
                report.violation = Some(violation);
                report.states = states.len();
                return report;
            }
            if let std::collections::hash_map::Entry::Vacant(entry) = seen.entry(next) {
                let child = states.len();
                entry.insert(child);
                states.push((next, Some((index, action)), depth + 1));
                queue.push_back(child);
                assert!(states.len() <= STATE_CAP, "state cap not qualified");
            }
        }
    }
    report.states = states.len();
    report
}
pub fn replay(mode: Mode, trace: &[Action]) -> Violation {
    let mut state = State::new(mode);
    for (i, action) in trace.iter().enumerate() {
        assert!(protocol::actions(&state).contains(action));
        state = protocol::apply(state, *action);
        if let Err(violation) = oracle::check(&state) {
            assert_eq!(i + 1, trace.len(), "same original first violation");
            return violation;
        }
    }
    panic!("negative trace did not violate the independent oracle")
}
