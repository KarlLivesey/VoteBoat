// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Genuine physical votes and majority progress over two nonvoting helper levels.
use super::{cluster::*, history::*, support::*};
use voteboat::{native::connect::NativePeerProtocol, runtime::*};
fn history(protocol: NativePeerProtocol) {
    let mut c = Cluster::new_tree(protocol);
    assert_eq!(c.tree.max_hops(), 2);
    assert!(
        c.boats[0].peers().unwrap().source_view(node(3)).is_none(),
        "provisioned catalog cannot mint an observed origin"
    );
    c.gates[1].writes.set(true);
    c.campaign();
    write(&mut c, 61, 3);
    c.until(|c| c.value(2) == 3);
    assert_eq!(
        c.value(1),
        0,
        "two-hop voter3, not held voter2, supplied the majority"
    );
    let source = c.boats[0].peers().unwrap().source_view(node(3)).unwrap();
    assert_eq!(source.source().node, node(3));
    assert_eq!(source.receiving_access().peer.node, node(9));
    assert!(source.direct_access().is_none());
    let path: Vec<_> = source.delegation_path().unwrap().hops().collect();
    assert_eq!(path.len(), 2);
    assert_eq!(path[0].peer.node, node(3));
    assert_eq!(path[0].local.node, node(10));
    assert_eq!(path[1].peer, path[0].local);
    assert_eq!(path[1].local.node, node(9));
    let reverse = c.boats[2].peers().unwrap().source_view(node(1)).unwrap();
    assert_eq!(reverse.receiving_access().peer.node, node(10));
    assert_eq!(reverse.delegation_path().unwrap().len(), 2);
    assert!(
        !c.root.join("9").exists() && !c.root.join("10").exists(),
        "helpers have no native WAL owners"
    );
    c.gates[1].writes.set(false);
    c.until(|c| c.value(1) == 3);
    c.gates[3].reads.set(true);
    c.gates[3].writes.set(true);
    write(&mut c, 62, 4);
    assert_eq!(read(&mut c), 7);
    assert_eq!(c.value(2), 3);
    c.gates[3].reads.set(false);
    c.gates[3].writes.set(false);
    c.until(|c| c.value(2) == 7);
    c.held_read_acks.insert(node(2));
    assert_eq!(
        read(&mut c),
        7,
        "fresh original read requires voter3's two-hop acknowledgement"
    );
    c.held_read_acks.clear();
    c.held_inputs.clear();
    std::fs::remove_dir_all(finish(c)).unwrap();
}
#[test]
fn tcp_two_helper_tree_keeps_original_physical_votes_and_surviving_branch_majority() {
    history(NativePeerProtocol::TcpTls);
}
#[cfg(feature = "quic")]
#[test]
fn quic_two_helper_tree_keeps_original_physical_votes_and_surviving_branch_majority() {
    history(NativePeerProtocol::Quic);
}
