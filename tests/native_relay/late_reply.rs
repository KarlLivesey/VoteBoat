// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Release a retained actual source3 ReadAck into a different live read collector.
use super::{
    cluster::*,
    history::{complete_read, finish, read, write},
    observed_wire::*,
    support::*,
};
use voteboat::{native::connect::NativePeerProtocol, raft::RequestContext, runtime::*};
fn seen(
    c: &Cluster,
    at: u64,
    from: u64,
    kind: Kind,
    encoded: bool,
    context: RequestContext,
) -> bool {
    c.observations.borrow().iter().any(|r| {
        r.at == at
            && r.from == node(from)
            && r.kind == kind
            && r.encoded == encoded
            && r.context == context
    })
}
fn history(protocol: NativePeerProtocol) {
    history_in(Cluster::new(protocol));
}
fn history_in(mut c: Cluster) {
    c.campaign();
    write(&mut c, 11, 3);
    c.until(|c| c.value(2) == 3);
    c.held_read_acks.insert(node(3));
    if c.tree.max_hops() > 1 {
        c.held_read_acks.insert(node(2));
        let original = c.boats[0].read(group(352), ()).unwrap();
        c.until(|c| c.held_inputs.contains_key(&node(3)) && c.held_inputs.contains_key(&node(2)));
        assert_eq!(c.held_inputs[&node(3)].0.connection.local.node, node(10));
        assert_eq!(c.held_inputs[&node(3)].0.connection.peer.node, node(3));
        c.held_read_acks.remove(&node(2));
        c.held_inputs.remove(&node(2));
        assert_eq!(complete_read(&mut c, original), 3);
    } else {
        assert_eq!(read(&mut c), 3);
    }
    let old = c
        .observations
        .borrow()
        .iter()
        .find(|r| r.at == 1 && r.from == node(1) && r.kind == Kind::Probe && r.encoded)
        .unwrap()
        .context;
    c.phase = "old source ack encoded";
    c.until(|c| seen(c, 3, 3, Kind::Ack, true, old));
    c.phase = "old full input captured";
    c.until(|c| c.held_inputs.contains_key(&node(3)));
    let old_input = c.held_inputs[&node(3)];
    assert!(
        !seen(&c, 9, 3, Kind::Ack, false, old),
        "old original remains in its real channel"
    );
    c.held_read_acks.insert(node(2));
    let current = c.boats[0].read(group(352), ()).unwrap();
    c.until(|c| {
        c.observations
            .borrow()
            .iter()
            .any(|r| r.at == 1 && r.kind == Kind::Probe && r.encoded && r.context != old)
    });
    let new = c
        .observations
        .borrow()
        .iter()
        .find(|r| r.at == 1 && r.kind == Kind::Probe && r.encoded && r.context != old)
        .unwrap()
        .context;
    assert_ne!(old, new);
    c.phase = "new original probe reaches source";
    c.until(|c| seen(c, 3, 1, Kind::Probe, false, new));
    c.until(|c| c.held_inputs.contains_key(&node(2)));
    let discarded = c.discarded;
    assert_eq!(c.held_inputs[&node(3)], old_input);
    c.held_read_acks.remove(&node(3));
    c.held_inputs.remove(&node(3));
    c.phase = "old input consumed";
    c.until(|c| seen(c, 9, 3, Kind::Ack, false, old));
    c.held_read_acks.insert(node(3));
    assert!(
        !seen(&c, 9, 3, Kind::Ack, false, new),
        "fresh original must remain held behind old output"
    );
    c.phase = "old parent intake";
    c.until(|c| seen(c, 1, 3, Kind::Ack, false, old));
    assert!(
        c.discarded > discarded,
        "old original was rejected at parent intake"
    );
    assert!(
        c.boats[0].poll_read().is_none(),
        "old original cannot complete the new read"
    );
    c.held_read_acks.remove(&node(2));
    c.held_inputs.remove(&node(2));
    let end = std::time::Instant::now() + std::time::Duration::from_secs(15);
    loop {
        c.poll();
        if let Some(reply) = c.boats[0].poll_read() {
            assert_eq!(reply.ticket(), current);
            let ReadOutcome::Read { result, .. } = c.boats[0].complete_read(reply).unwrap() else {
                panic!("fresh majority read missing")
            };
            assert_eq!(result, Ok(3));
            break;
        }
        assert!(std::time::Instant::now() < end);
    }
    c.held_read_acks.clear();
    c.held_inputs.clear();
    std::fs::remove_dir_all(finish(c)).unwrap();
}
#[test]
fn tcp_retired_actual_source_read_ack_cannot_satisfy_next_read() {
    history(NativePeerProtocol::TcpTls);
}
#[cfg(feature = "quic")]
#[test]
fn quic_retired_actual_source_read_ack_cannot_satisfy_next_read() {
    history(NativePeerProtocol::Quic);
}
#[test]
fn tcp_two_helper_tree_retired_leaf_ack_cannot_satisfy_next_parent_read() {
    history_in(Cluster::new_tree(NativePeerProtocol::TcpTls));
}
#[cfg(feature = "quic")]
#[test]
fn quic_two_helper_tree_retired_leaf_ack_cannot_satisfy_next_parent_read() {
    history_in(Cluster::new_tree(NativePeerProtocol::Quic));
}
