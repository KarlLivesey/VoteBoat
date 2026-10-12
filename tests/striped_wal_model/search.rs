// SPDX-License-Identifier: RPL-1.5
use super::{oracle, protocol, types::*};
use std::collections::{hash_map::Entry, HashMap};
const MAX_STATES: usize = 100_000;
const MAX_EDGES: usize = 500_000;
const MAX_DEPTH: usize = 64;
struct Node {
    state: State,
    parent: Option<(usize, Action)>,
    depth: usize,
}
pub fn search(scenario: Scenario, rule: Rule) -> Result<Graph, Failure> {
    let initial = State::initial(scenario);
    let mut seen = HashMap::from([(initial, 0)]);
    let mut nodes = vec![Node {
        state: initial,
        parent: None,
        depth: 0,
    }];
    let mut graph = Graph {
        states: 1,
        edges: 0,
        depth: 0,
        terminals: 0,
        frontier: 0,
    };
    let mut cursor = 0;
    while cursor < nodes.len() {
        let node = &nodes[cursor];
        let (s, depth) = (node.state, node.depth);
        graph.terminals += usize::from(s.terminal(scenario));
        for (action, next) in protocol::successors(s, scenario, rule) {
            graph.edges += 1;
            assert!(
                graph.edges <= MAX_EDGES,
                "edge cap leaves unexplored frontier"
            );
            if let Some(violation) = oracle::violation(next, action, scenario) {
                let mut original_trace = trace(&nodes, cursor);
                original_trace.push(action);
                return Err(Failure {
                    violation,
                    original_trace,
                });
            }
            if let Entry::Vacant(entry) = seen.entry(next) {
                let depth = depth + 1;
                assert!(depth <= MAX_DEPTH, "depth cap leaves unexplored frontier");
                let index = nodes.len();
                entry.insert(index);
                nodes.push(Node {
                    state: next,
                    parent: Some((cursor, action)),
                    depth,
                });
                assert!(
                    nodes.len() <= MAX_STATES,
                    "state cap leaves unexplored frontier"
                );
                graph.depth = graph.depth.max(depth);
            }
        }
        cursor += 1;
    }
    graph.states = nodes.len();
    Ok(graph)
}
fn trace(nodes: &[Node], mut index: usize) -> Vec<Action> {
    let mut out = Vec::new();
    while let Some((parent, action)) = nodes[index].parent {
        out.push(action);
        index = parent;
    }
    out.reverse();
    out
}
impl Failure {
    pub fn replay(&self, scenario: Scenario, rule: Rule) -> Violation {
        let mut state = State::initial(scenario);
        for action in &self.original_trace {
            let next = protocol::successors(state, scenario, rule)
                .into_iter()
                .find(|(a, _)| a == action)
                .expect("exact original enabled action")
                .1;
            if let Some(violation) = oracle::violation(next, *action, scenario) {
                return violation;
            }
            state = next;
        }
        panic!("original trace no longer reaches first violation");
    }
}
