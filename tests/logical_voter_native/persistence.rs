// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Real parent-generated persistence over one actual child cohort.
use super::{
    aliases::Bridge,
    cluster::{Cluster, HISTORY},
    drive::claim_in,
    parent::Parent,
    setup, support,
};
use std::time::{Duration, Instant};
use voteboat::{
    application::StateMachine,
    contracts::HardState,
    identity::*,
    log::*,
    logical_voter::*,
    native::{connect::NativePeerProtocol, startup::NativeOpenMode},
    raft::*,
    runtime::*,
    worker::PersistenceWorker,
};
pub(super) fn input(parent: &mut Parent, rpc: Rpc, sequence: u64) {
    // Trusted local Raft input control. This is not a logical native envelope.
    parent
        .owner
        .admit(
            setup::mapping().parent,
            Event::Receive(Message {
                group: setup::mapping().parent,
                configuration: ConfigurationId::new(1).unwrap(),
                from: support::node(12),
                sender: StoreBinding {
                    identity: support::identity(102),
                    session: StoreSession::new(1).unwrap(),
                },
                to: setup::mapping().voter,
                term: 7,
                context: RequestContext {
                    origin: StoreBinding {
                        identity: support::identity(102),
                        session: StoreSession::new(1).unwrap(),
                    },
                    sequence,
                },
                rpc,
            }),
        )
        .unwrap();
}
fn budget(completions: usize) -> LogicalBridgeBudget {
    LogicalBridgeBudget {
        node_polls: 3,
        completions,
        transitions: 1,
        node: NodePollBudget::default(),
    }
}
pub(super) fn pump_providers(
    bridge: &mut Bridge<'_>,
    parent: &mut Parent,
    clock: Instant,
    completions: usize,
) {
    bridge.pump_persistence(&mut parent.port).unwrap();
    bridge.pump_release(parent.driver.releases()).unwrap();
    if let Some(snapshots) = &mut parent.snapshots {
        bridge
            .pump_snapshots(&mut snapshots.port, &parent.port)
            .unwrap();
    }
    bridge
        .advance(
            MonoTime(clock.elapsed().as_millis() as u64),
            budget(completions),
        )
        .unwrap();
    bridge.pump_persistence(&mut parent.port).unwrap();
    bridge.pump_release(parent.driver.releases()).unwrap();
    if let Some(snapshots) = &mut parent.snapshots {
        bridge
            .pump_snapshots(&mut snapshots.port, &parent.port)
            .unwrap();
    }
}
fn pump(bridge: &mut Bridge<'_>, parent: &mut Parent, clock: Instant, completions: usize) {
    pump_providers(bridge, parent, clock, completions);
    parent.poll();
}
pub(super) fn settle(bridge: &mut Bridge<'_>, parent: &mut Parent, clock: Instant) -> Vec<Message> {
    let end = Instant::now() + Duration::from_secs(15);
    let mut messages = Vec::new();
    loop {
        pump(bridge, parent, clock, 4);
        messages.extend(parent.take_sends());
        if bridge.is_drained() && parent.is_drained() {
            return messages;
        }
        assert!(
            Instant::now() < end,
            "original parent logical work must settle"
        );
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
fn withheld(bridge: &mut Bridge<'_>, parent: &mut Parent, clock: Instant) {
    assert_eq!(parent.poll().persistence_batches, 1);
    assert_eq!(parent.worker.usage().requests, 1);
    for _ in 0..32 {
        pump(bridge, parent, clock, 0);
        assert_eq!(parent.worker.usage().requests, 1);
        assert!(parent
            .owner
            .core(setup::mapping().parent)
            .unwrap()
            .has_pending_dependency());
        assert_eq!(
            parent
                .owner
                .core(setup::mapping().parent)
                .unwrap()
                .state()
                .hard_state,
            HardState::default()
        );
        assert!(parent.take_sends().is_empty());
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
fn command(index: u64, delta: i64) -> LogEntry {
    LogEntry {
        index,
        term: 7,
        payload: EntryPayload::Command {
            operation: OperationId::new(8000 + u128::from(index)).unwrap(),
            bytes: delta.to_le_bytes().to_vec().into(),
        },
    }
}
pub(super) fn append(parent: &mut Parent, index: u64, delta: i64) {
    input(
        parent,
        Rpc::Append {
            previous_index: index - 1,
            previous_term: if index == 1 { 0 } else { 7 },
            entries: vec![command(index, delta)].into(),
            leader_commit: index,
        },
        index + 1,
    );
}
fn phase(cluster: &mut Cluster, first: bool) -> GroupLog {
    let clock = cluster.clock;
    let mut bridge = LogicalChildBridge::new(
        &mut cluster.nodes,
        setup::mapping(),
        support::node(if first { 1 } else { 2 }),
    )
    .unwrap();
    let image = claim_in(&mut bridge, clock, if first { 8101 } else { 8102 }, false);
    let mut parent = Parent::new(&image);
    if first {
        input(
            &mut parent,
            Rpc::Vote {
                last_index: 0,
                last_term: 0,
            },
            1,
        );
        withheld(&mut bridge, &mut parent, clock);
        let sends = settle(&mut bridge, &mut parent, clock);
        assert!(sends
            .iter()
            .any(|m| matches!(m.rpc, Rpc::Voted { granted: true })));
    }
    append(
        &mut parent,
        if first { 1 } else { 2 },
        if first { 5 } else { -2 },
    );
    let sends = settle(&mut bridge, &mut parent, clock);
    assert!(sends
        .iter()
        .any(|m| matches!(m.rpc, Rpc::Appended { success: true, .. })));
    assert_eq!(
        parent.apps[&setup::mapping().parent].applied_index(),
        if first { 1 } else { 2 }
    );
    assert_eq!(
        parent.apps[&setup::mapping().parent]
            .0
            .read_applied(if first { 1 } else { 2 })
            .unwrap(),
        if first { 5 } else { 3 }
    );
    assert!(bridge.is_drained());
    bridge.close();
    parent.close()
}
fn assert_state(state: &GroupLog, entries: usize) {
    assert_eq!(state.bootstrap, setup::parent());
    assert_eq!(
        state.hard_state,
        HardState {
            term: 7,
            voted_for: Some(support::node(12))
        }
    );
    assert_eq!(
        state.ballot_origin,
        Some(BallotOrigin {
            configuration: ConfigurationId::new(1).unwrap(),
            candidate_store: support::identity(102)
        })
    );
    assert_eq!(state.commit_index, entries as u64);
    assert_eq!(
        state.entries,
        match entries {
            1 => vec![command(1, 5)],
            2 => vec![command(1, 5), command(2, -2)],
            _ => unreachable!(),
        }
    );
}
fn history(protocol: NativePeerProtocol) {
    let _serial = HISTORY.lock().unwrap();
    let mut cluster = Cluster::new(protocol);
    let first = phase(&mut cluster, true);
    assert_state(&first, 1);
    cluster.checkpoint();
    let bases = cluster
        .nodes
        .iter()
        .map(|node| {
            node.local()
                .owner
                .core(setup::mapping().child)
                .unwrap()
                .state()
                .base_index()
        })
        .collect::<Vec<_>>();
    let second = phase(&mut cluster, false);
    assert_state(&second, 2);
    cluster.catch_up();
    for (node, base) in cluster.nodes.iter().zip(&bases) {
        let state = node
            .local()
            .owner
            .core(setup::mapping().child)
            .unwrap()
            .state();
        assert_eq!(state.base_index(), *base);
        assert!(state.last_index() > *base);
    }
    cluster.close();
    cluster.open(NativeOpenMode::Recover);
    let recovered = cluster.claim(3, 8103, false);
    assert_eq!(recovered.representative().epoch.get(), 3);
    assert_state(&recovered.image().parent, 2);
    assert_eq!(recovered.image().parent, second);
    cluster.finish();
}
#[test]
fn tcp_original_parent_promise_and_payload_survive_child_checkpoint_suffix_restart() {
    history(NativePeerProtocol::TcpTls);
}
#[cfg(feature = "quic")]
#[test]
fn quic_original_parent_promise_and_payload_survive_child_checkpoint_suffix_restart() {
    history(NativePeerProtocol::Quic);
}
