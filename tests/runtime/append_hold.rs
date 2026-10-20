// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Held follower Appends under `TimedShard`, in simulated whole milliseconds:
//! the hold ends exactly at `append_hold_ms`, or at the leader's heartbeat when
//! that comes first. Messages are delivered within the millisecond they leave.
use super::*;

type Node = AutoNode<HostLogStore, HostReady, HostTimers, FixedEntropy>;

fn cluster(append_hold_ms: u64) -> Vec<Node> {
    let timers = TimerConfig {
        append_hold_ms,
        ..timer_config()
    };
    (1..=3)
        .map(|id| {
            let mut log = HostLogStore::new(id as u128);
            create_auto_groups(&mut log);
            AutoNode::open(
                id,
                log,
                HostReady::new(3),
                |owner| HostTimers::new(owner, 3),
                FixedEntropy((id - 1) * 8),
                MonoTime(0),
                timers,
            )
        })
        .collect()
}
fn last_index(node: &Node) -> u64 {
    node.runtime.core(group(1)).unwrap().state().last_index()
}
fn pump(nodes: &mut [Node], ms: u64) {
    pump_auto(nodes, MonoTime(ms), None, &mut Vec::new());
}
/// Elect group 1's leader and run to its next heartbeat; return the leader and
/// that millisecond (the following heartbeat is `heartbeat_ms` later).
fn elected(nodes: &mut [Node]) -> (usize, u64) {
    for ms in 0..=100 {
        pump(nodes, ms);
    }
    let leader = nodes
        .iter()
        .position(|n| n.runtime.core(group(1)).unwrap().role() == Role::Leader)
        .unwrap();
    let beat = nodes[leader].runtime.deadline(group(1)).unwrap().deadline.0;
    for ms in 101..=beat {
        pump(nodes, ms);
    }
    let next = nodes[leader].runtime.deadline(group(1)).unwrap().deadline.0;
    assert_eq!(next, beat + timer_config().heartbeat_ms);
    for n in nodes.iter() {
        assert_eq!(last_index(n), last_index(&nodes[leader]));
    }
    (leader, beat)
}
/// Propose one write at `ms`; return the follower whose Append is held.
fn propose_and_hold(nodes: &mut [Node], leader: usize, ms: u64) -> usize {
    nodes[leader].runtime.admit(group(1), proposal(9)).unwrap();
    pump(nodes, ms);
    let tip = last_index(&nodes[leader]);
    let behind = (0..nodes.len())
        .filter(|i| last_index(&nodes[*i]) < tip)
        .collect::<Vec<_>>();
    assert_eq!(behind.len(), 1, "one follower is sent at once, one is held");
    let core = nodes[leader].runtime.core(group(1)).unwrap();
    assert!(core.has_held_appends());
    // The quorum follower's acknowledgement already committed the write.
    assert_eq!(core.state().commit_index, tip);
    behind[0]
}

#[test]
fn hold_ends_exactly_at_the_configured_bound() {
    let mut nodes = cluster(2);
    let (leader, beat) = elected(&mut nodes);
    let held = propose_and_hold(&mut nodes, leader, beat);
    let tip = last_index(&nodes[leader]);
    // A host waiting on its next deadline wakes for the bound.
    assert!(nodes[leader].runtime.next_deadline() <= Some(MonoTime(beat + 2)));
    pump(&mut nodes, beat + 1);
    assert!(last_index(&nodes[held]) < tip, "not a millisecond early");
    pump(&mut nodes, beat + 2);
    assert_eq!(last_index(&nodes[held]), tip);
    let core = nodes[leader].runtime.core(group(1)).unwrap();
    assert!(!core.has_held_appends());
    // Released by its bound: the heartbeat is still two milliseconds away.
    assert_eq!(
        nodes[leader].runtime.deadline(group(1)).unwrap().deadline,
        MonoTime(beat + timer_config().heartbeat_ms)
    );
}

#[test]
fn heartbeat_ends_a_hold_before_a_later_bound() {
    let heartbeat = timer_config().heartbeat_ms;
    let mut nodes = cluster(heartbeat);
    let (leader, beat) = elected(&mut nodes);
    // Held at beat + 1: its bound is beat + 1 + heartbeat, after the next beat.
    let held = propose_and_hold(&mut nodes, leader, beat + 1);
    let tip = last_index(&nodes[leader]);
    for ms in beat + 2..beat + heartbeat {
        pump(&mut nodes, ms);
        assert!(last_index(&nodes[held]) < tip);
    }
    pump(&mut nodes, beat + heartbeat);
    assert_eq!(last_index(&nodes[held]), tip);
    assert!(!nodes[leader]
        .runtime
        .core(group(1))
        .unwrap()
        .has_held_appends());
}

#[test]
fn zero_bound_turns_holds_off() {
    let mut nodes = cluster(0);
    let (leader, beat) = elected(&mut nodes);
    nodes[leader].runtime.admit(group(1), proposal(9)).unwrap();
    pump(&mut nodes, beat);
    for n in &nodes {
        assert_eq!(last_index(n), last_index(&nodes[leader]));
        assert_eq!(
            n.runtime.core(group(1)).unwrap().append_hold(),
            AppendHold::Off
        );
    }
}
