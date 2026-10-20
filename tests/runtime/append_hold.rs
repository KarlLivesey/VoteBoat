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
fn pump_cut(nodes: &mut [Node], ms: u64, cut: usize) {
    pump_auto(nodes, MonoTime(ms), Some(cut), &mut Vec::new());
}
fn commit_index(node: &Node) -> u64 {
    node.runtime.core(group(1)).unwrap().state().commit_index
}

#[test]
fn new_leader_probes_a_lagging_follower_at_once() {
    // Holds last as long as the heartbeat, so a held probe would be visible.
    let heartbeat = timer_config().heartbeat_ms;
    let mut nodes = cluster(heartbeat);
    let (old, beat) = elected(&mut nodes);
    let lagging = (old + 1) % 3;
    let other = (old + 2) % 3;
    // The lagging follower misses three committed writes.
    for (k, operation) in (10..13).enumerate() {
        nodes[old]
            .runtime
            .admit(group(1), proposal(operation))
            .unwrap();
        pump_cut(&mut nodes, beat + k as u64, lagging);
    }
    let committed = commit_index(&nodes[old]);
    assert_eq!(last_index(&nodes[other]), committed);
    assert!(last_index(&nodes[lagging]) < committed);
    // The old leader is cut off; only `other` can win (its log is ahead).
    let mut ms = beat + 3;
    let elected_at = loop {
        ms += 1;
        assert!(ms < beat + 1000, "no new leader");
        pump_cut(&mut nodes, ms, old);
        if nodes[other].runtime.core(group(1)).unwrap().role() == Role::Leader {
            break ms;
        }
    };
    // In the millisecond of the new leader's no-op, the lagging follower's
    // probe is rejected, rewound and answered, and its ack commits the no-op:
    // with the old leader silent, nothing else could.
    let tip = last_index(&nodes[other]);
    assert!(tip > committed);
    assert_eq!(
        last_index(&nodes[lagging]),
        tip,
        "caught up at ms {elected_at}"
    );
    assert_eq!(commit_index(&nodes[other]), tip);
}

#[test]
fn a_silent_quorum_follower_delays_the_commit_by_exactly_the_bound() {
    // Which follower carries the batch at once is deterministic; cut that one
    // off before the batch, so only the held follower can complete the quorum.
    let mut found = 0;
    for cut in 0..3 {
        let mut nodes = cluster(2);
        let (leader, beat) = elected(&mut nodes);
        if cut == leader {
            continue;
        }
        nodes[leader].runtime.admit(group(1), proposal(9)).unwrap();
        pump_cut(&mut nodes, beat, cut);
        let tip = last_index(&nodes[leader]);
        if commit_index(&nodes[leader]) == tip {
            continue; // `cut` was the follower the quorum did not need
        }
        found += 1;
        let held = 3 - leader - cut;
        assert!(nodes[leader]
            .runtime
            .core(group(1))
            .unwrap()
            .has_held_appends());
        pump_cut(&mut nodes, beat + 1, cut);
        assert!(
            commit_index(&nodes[leader]) < tip,
            "not a millisecond early"
        );
        assert!(last_index(&nodes[held]) < tip);
        pump_cut(&mut nodes, beat + 2, cut);
        assert_eq!(last_index(&nodes[held]), tip);
        assert_eq!(commit_index(&nodes[leader]), tip);
    }
    assert_eq!(found, 1);
}

fn tip_of(node: &Node, g: u128) -> u64 {
    node.runtime.core(group(g)).unwrap().state().last_index()
}
fn holding(node: &Node, g: u128) -> bool {
    node.runtime.core(group(g)).unwrap().has_held_appends()
}

#[test]
fn groups_sharing_a_shard_each_release_at_their_own_bound() {
    // Round 3: both groups share one shard, log store and timer service, and
    // hold one millisecond apart. Each arms its own deadline in the shared
    // index, and polling releases each exactly at its own bound.
    let mut nodes = cluster(2);
    let (leader, beat) = elected(&mut nodes);
    let deadline = |nodes: &[Node], g| nodes[leader].runtime.deadline(group(g)).unwrap().deadline;
    assert_eq!(
        nodes[leader].runtime.core(group(2)).unwrap().role(),
        Role::Leader,
        "one shard leads both groups"
    );
    // The fixture's fixed schedule: no heartbeat of either group can end a
    // hold before its bound.
    assert_eq!(deadline(&nodes, 1), MonoTime(beat + 4));
    assert_eq!(deadline(&nodes, 2), MonoTime(beat + 1));
    let held = |nodes: &[Node], g: u128| {
        let tip = tip_of(&nodes[leader], g);
        (0..nodes.len())
            .filter(|i| tip_of(&nodes[*i], g) < tip)
            .collect::<Vec<_>>()
    };
    nodes[leader].runtime.admit(group(1), proposal(9)).unwrap();
    pump(&mut nodes, beat);
    assert!(holding(&nodes[leader], 1));
    assert_eq!(held(&nodes, 1).len(), 1);
    pump(&mut nodes, beat + 1); // group 2's heartbeat
    nodes[leader].runtime.admit(group(2), proposal(9)).unwrap();
    pump(&mut nodes, beat + 1);
    assert!(holding(&nodes[leader], 1) && holding(&nodes[leader], 2));
    assert_eq!(held(&nodes, 1).len(), 1);
    assert_eq!(held(&nodes, 2).len(), 1);
    assert_eq!(
        nodes[leader].runtime.next_deadline(),
        Some(MonoTime(beat + 2))
    );
    pump(&mut nodes, beat + 2);
    assert!(held(&nodes, 1).is_empty(), "group 1 released at its bound");
    assert!(!holding(&nodes[leader], 1));
    assert_eq!(held(&nodes, 2).len(), 1, "group 2 not a millisecond early");
    assert_eq!(
        nodes[leader].runtime.next_deadline(),
        Some(MonoTime(beat + 3))
    );
    pump(&mut nodes, beat + 3);
    assert!(held(&nodes, 2).is_empty(), "group 2 released at its bound");
    assert!(!holding(&nodes[leader], 2));
    assert_eq!(deadline(&nodes, 1), MonoTime(beat + 4));
    assert_eq!(deadline(&nodes, 2), MonoTime(beat + 5));
}

#[test]
fn a_late_host_poll_releases_an_expired_hold_on_that_poll() {
    let mut nodes = cluster(2);
    let (leader, beat) = elected(&mut nodes);
    let held = propose_and_hold(&mut nodes, leader, beat);
    let tip = last_index(&nodes[leader]);
    assert!(holding(&nodes[leader], 1));
    // The host misses beat + 2. Its next poll is one millisecond late and
    // still before the heartbeat: the hold deadline must suffice by itself.
    let progress = nodes[leader]
        .runtime
        .poll_timers(MonoTime(beat + 3))
        .unwrap();
    assert_eq!(progress.released, 1, "first late poll admits the release");
    pump(&mut nodes, beat + 3);
    assert_eq!(last_index(&nodes[held]), tip);
    assert!(!holding(&nodes[leader], 1));
    assert_eq!(
        nodes[leader].runtime.deadline(group(1)).unwrap().deadline,
        MonoTime(beat + 4)
    );
}
