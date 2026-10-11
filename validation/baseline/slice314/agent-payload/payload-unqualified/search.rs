// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::{model::Model, oracle, types::*};
use std::collections::{HashSet, VecDeque};
const DEPTH: usize = 384;
const STATES: usize = 500_000;
const EDGES: usize = 5_000_000;
#[derive(Default, Debug)]
struct Stats {
    states: usize,
    edges: usize,
    attempts: usize,
    depth: usize,
    depth_cap: bool,
    state_cap: bool,
    edge_cap: bool,
    terminals: usize,
    recovered_terminals: usize,
    checkpoint_terminals: usize,
}
fn actions() -> Vec<Action> {
    let mut a = vec![
        Action::Admit,
        Action::Commit,
        Action::Apply,
        Action::Observe,
        Action::Lose,
        Action::Quorum,
        Action::Resolve,
        Action::Recover(0),
        Action::Recover(1),
        Action::Power,
        Action::Process,
        Action::Ack(0),
        Action::Ack(1),
        Action::Submit(Kind::Anchor1),
        Action::Submit(Kind::Reclaim0),
    ];
    for p in 0..3 {
        a.extend([Action::Write(p), Action::Sync(p)]);
    }
    a.extend([
        Action::CheckpointStage(1),
        Action::CheckpointSync(1),
        Action::CheckpointAnchor(1),
    ]);
    a
}
#[test]
fn finite_payload_checkpoint_graph() {
    let initial = Model::initial();
    let mut seen = HashSet::from([initial.clone()]);
    let mut queue = VecDeque::from([(initial, 0)]);
    let actions = actions();
    let mut stats = Stats::default();
    while let Some((state, depth)) = queue.pop_front() {
        stats.depth = stats.depth.max(depth);
        oracle::check(&state).unwrap();
        if state.served && state.acknowledged == [true; 2] {
            stats.terminals += 1;
            if state.power || state.process {
                stats.recovered_terminals += 1;
            }
            if state.stores.iter().any(|s| s.anchor.is_some()) {
                stats.checkpoint_terminals += 1;
            }
        }
        if depth == DEPTH {
            stats.depth_cap = true;
            continue;
        }
        for action in &actions {
            stats.attempts += 1;
            let mut next = state.clone();
            if !next.step(*action, Weakening::None) {
                continue;
            }
            stats.edges += 1;
            if stats.edges == EDGES {
                stats.edge_cap = true;
                break;
            }
            if !seen.contains(&next) {
                if seen.len() == STATES {
                    stats.state_cap = true;
                    break;
                }
                seen.insert(next.clone());
                queue.push_back((next, depth + 1));
            }
        }
        if stats.state_cap || stats.edge_cap {
            break;
        }
    }
    stats.states = seen.len();
    println!("profileB ordinary-child assumed; fixed18caller commands+2typed early submissions, other2cells fixed synced X/Y prelude; selected representative one power-or-process cut; child checkpoint/suffix on selected physical1 (all3 physical checkpoint paths in controls); stats={stats:?}; remaining_frontier={}",queue.len());
    assert!(
        !stats.depth_cap && !stats.state_cap && !stats.edge_cap,
        "selected graph reached declared resource cap; frontier unqualified"
    );
    assert!(queue.is_empty());
    assert!(stats.terminals > 0 && stats.recovered_terminals > 0 && stats.checkpoint_terminals > 0);
}
