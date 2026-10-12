// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Fresh branch sessions reconsume actual originals; historical closes stay scoped.
use super::{channels::*, cluster::*, history::*, observed_wire::*, support::*};
use std::time::{Duration, Instant};
use voteboat::{
    identity::StoreSession,
    native::{connect::NativePeerProtocol, relay::*},
    runtime::*,
    secure::*,
};
struct FreshBranch {
    physical: RelayHop<Box<dyn SecureSession>>,
    parent: RelayHop<Gated>,
    branch: RelayForwarder<Gated, ObservedWire>,
    leaf_gate: Gate,
    link_gate: Gate,
}
fn fresh(c: &Cluster) -> FreshBranch {
    let child = LocalIdentity {
        node: node(10),
        store: voteboat::identity::StoreBinding {
            identity: identity(10),
            session: StoreSession::new(1001).unwrap(),
        },
    };
    let physical = LocalIdentity {
        node: node(3),
        store: c.boats[2].local().owner.identity().store,
    };
    let child_plan = RelayHelperPlan::for_tree(child, c.tree.clone()).unwrap();
    let (a, b, leaf_gate) = pair(c.protocol, physical, child, 2, c.clock);
    let physical = RelayHop::for_voter(
        a,
        NativeRelayPlan::for_tree(physical, c.tree.clone()).unwrap(),
        Default::default(),
    )
    .unwrap_or_else(|e| panic!("fresh physical {:?}", e.reason));
    let leaf = RelayHop::for_helper(b, child_plan.clone(), Default::default())
        .unwrap_or_else(|e| panic!("fresh leaf {:?}", e.reason));
    let (a, b, link_gate) = pair(c.protocol, child, c.helper_identity, 2, c.clock);
    let link = RelayHop::for_helper(
        Gated {
            actual: a,
            gate: Gate::default(),
        },
        child_plan.clone(),
        Default::default(),
    )
    .unwrap_or_else(|e| panic!("fresh child link {:?}", e.reason));
    let parent = RelayHop::for_helper(
        b,
        RelayHelperPlan::for_tree(c.helper_identity, c.tree.clone()).unwrap(),
        Default::default(),
    )
    .unwrap_or_else(|e| panic!("fresh parent link {:?}", e.reason));
    let branch = RelayForwarder::build(RelayForwarderParts {
        plan: child_plan,
        codec: ObservedWire::new(10, c.observations.clone()),
        limits: Default::default(),
        hops: vec![leaf, link],
    })
    .unwrap_or_else(|e| panic!("fresh branch {:?}", e.reason));
    FreshBranch {
        physical,
        parent,
        branch,
        leaf_gate,
        link_gate,
    }
}
fn replace(c: &mut Cluster, fresh: FreshBranch) {
    let (mut physical, mut parent) = (Some(fresh.physical), Some(fresh.parent));
    let end = Instant::now() + Duration::from_secs(15);
    while physical.is_some() || parent.is_some() {
        c.poll();
        let now = c.now();
        if let Some(hop) = physical.take() {
            match c.boats[2].replace_peer_channel(hop, now) {
                Ok(old) => assert!(old.is_drained()),
                Err((NodeError::Peer(PeerDriverError::NotQuiescent), original)) => {
                    physical = Some(*original)
                }
                Err((e, _)) => panic!("branch physical replacement {:?}", e),
            }
        }
        if let Some(hop) = parent.take() {
            match c.helper.attach_source_hop(hop) {
                Ok(old) => assert!(old.unwrap().is_drained()),
                Err(e) if e.reason == RelayNetworkError::Overloaded => parent = Some(*e.hop),
                Err(e) => panic!("branch parent replacement {:?}", e.reason),
            }
        }
        assert!(Instant::now() < end, "original branch IO not reclaimed");
        std::thread::park_timeout(Duration::from_millis(1));
    }
    let old = std::mem::replace(&mut c.branches[0], fresh.branch);
    old.into_parts()
        .unwrap_or_else(|_| panic!("old branch not reclaimed"));
    c.gates[2] = fresh.leaf_gate;
    c.gates[3] = fresh.link_gate;
}
fn history(protocol: NativePeerProtocol) {
    let mut c = Cluster::new_tree(protocol);
    c.campaign();
    write(&mut c, 36411, 3);
    c.until(|c| c.value(2) == 3);
    let old = c.boats[0]
        .peers()
        .unwrap()
        .source_view(node(3))
        .unwrap()
        .delegation_path()
        .unwrap();
    c.held_read_acks.extend([node(2), node(3)]);
    let read = c.boats[0].read(group(352), ()).unwrap();
    c.until(|c| c.held_inputs.len() == 2);
    let before: Vec<_> = c
        .forwarded_probes
        .iter()
        .filter(|(t, _)| t.peer == node(3))
        .copied()
        .collect();
    assert!(!before.is_empty());
    c.retiring = true;
    c.branches[0].close();
    let now = c.now();
    c.boats[2].retire_peer_channel(now).unwrap();
    c.held_read_acks.remove(&node(3));
    c.held_inputs.remove(&node(3));
    c.until(|c| c.branches[0].is_drained() && c.helper.source_capture(node(3)).is_none());
    assert!(c.boats[0].poll_read().is_none());
    let fresh = fresh(&c);
    replace(&mut c, fresh);
    let prior = c.forwarded_probes.len();
    assert_eq!(complete_read(&mut c, read), 3);
    c.until(|c| {
        c.forwarded_probes[prior..]
            .iter()
            .any(|original| before.contains(original))
    });
    let new = c.boats[0]
        .peers()
        .unwrap()
        .source_view(node(3))
        .unwrap()
        .delegation_path()
        .unwrap();
    assert_ne!(old, new);
    assert_eq!(new.len(), 2);
    let hops: Vec<_> = new.hops().collect();
    assert_eq!(hops[0].local.node, node(10));
    assert_eq!(hops[1].local.node, node(9));
    assert_ne!(
        hops[0].local.store.session,
        old.hops().next().unwrap().local.store.session
    );
    c.until(|c| {
        c.controls.iter().any(|(t, f)| {
            t.connection.peer.node == node(1)
                && f.kind() == RelayFrameKind::SourceOpened
                && f.source_capture()
                    .is_some_and(|s| s.source().node == node(3) && s.path() == new)
        })
    });
    replay_old_close(&mut c, old, new);
    assert!(c.boats.iter().all(|b| b.state() == NodeState::Running));
    c.held_read_acks.clear();
    c.held_inputs.clear();
    std::fs::remove_dir_all(finish(c)).unwrap();
}
fn replay_old_close(
    c: &mut Cluster,
    old: voteboat::transport::AuthenticatedDelegationPath,
    new: voteboat::transport::AuthenticatedDelegationPath,
) {
    let select = |kind, path| {
        c.controls
            .iter()
            .find(|(t, f)| {
                t.connection.peer.node == node(1)
                    && f.kind() == kind
                    && f.source_capture()
                        .is_some_and(|s| s.source().node == node(3) && s.path() == path)
            })
            .unwrap()
            .1
            .clone()
    };
    let opened = select(RelayFrameKind::SourceOpened, new);
    let closed = select(RelayFrameKind::SourceClosed, old);
    let barrier = c
        .controls
        .iter()
        .find(|(t, f)| {
            t.connection.peer.node == node(1)
                && f.kind() == RelayFrameKind::SourceOpened
                && f.source_capture()
                    .is_some_and(|s| s.source().node == node(2))
        })
        .unwrap()
        .1
        .clone();
    let physical = LocalIdentity {
        node: node(1),
        store: c.boats[0].local().owner.identity().store,
    };
    let (a, b, _) = pair(c.protocol, physical, c.helper_identity, 2, c.clock);
    let mut parent = Some(
        RelayHop::for_voter(
            a,
            NativeRelayPlan::for_tree(physical, c.tree.clone()).unwrap(),
            Default::default(),
        )
        .unwrap_or_else(|e| panic!("fresh parent physical {:?}", e.reason)),
    );
    let mut helper = RelayHop::for_helper(
        b,
        RelayHelperPlan::for_tree(c.helper_identity, c.tree.clone()).unwrap(),
        Default::default(),
    )
    .unwrap_or_else(|e| panic!("fresh control helper {:?}", e.reason));
    let now = c.now();
    c.boats[0].retire_peer_channel(now).unwrap();
    let end = Instant::now() + Duration::from_secs(15);
    while let Some(hop) = parent.take() {
        c.poll();
        let now = c.now();
        match c.boats[0].replace_peer_channel(hop, now) {
            Ok(old) => assert!(old.is_drained()),
            Err((NodeError::Peer(PeerDriverError::NotQuiescent), original)) => {
                parent = Some(*original)
            }
            Err((e, _)) => panic!("control channel replacement {:?}", e),
        }
        assert!(Instant::now() < end);
    }
    send_control(c, &mut helper, opened);
    c.until(|c| c.boats[0].peers().unwrap().source_view(node(3)).is_some());
    send_control(c, &mut helper, closed);
    assert!(c.boats[0].peers().unwrap().source_view(node(2)).is_none());
    send_control(c, &mut helper, barrier);
    c.until(|c| c.boats[0].peers().unwrap().source_view(node(2)).is_some());
    assert_eq!(
        c.boats[0]
            .peers()
            .unwrap()
            .source_view(node(3))
            .unwrap()
            .delegation_path(),
        Some(new)
    );
    let mut helper = Some(helper);
    while let Some(hop) = helper.take() {
        c.poll();
        match c.helper.attach_source_hop(hop) {
            Ok(old) => assert!(old.unwrap().is_drained()),
            Err(e) if e.reason == RelayNetworkError::Overloaded => helper = Some(*e.hop),
            Err(e) => panic!("restored helper source {:?}", e.reason),
        }
        assert!(Instant::now() < end);
    }
}
fn send_control(c: &mut Cluster, hop: &mut RelayHop<Gated>, frame: RelayFrame) {
    let kind = frame.kind();
    let original = frame.source_capture().unwrap();
    let ticket = hop
        .submit(frame)
        .unwrap_or_else(|e| panic!("actual control replay {:?}", e.reason));
    let end = Instant::now() + Duration::from_secs(15);
    loop {
        c.poll();
        hop.poll(c.now(), Default::default()).unwrap();
        if let Some(completion) = hop.take_send() {
            assert_eq!(completion.ticket, ticket);
            assert_eq!(completion.frame.kind(), kind);
            assert_eq!(completion.frame.source_capture(), Some(original));
            assert_eq!(completion.result, voteboat::outbound::LocalSendResult::Sent);
            break;
        }
        assert!(Instant::now() < end, "original control IO not complete");
        std::thread::park_timeout(Duration::from_millis(1));
    }
    c.poll();
}
#[test]
fn tcp_two_helper_tree_restart_reconsumes_original_and_old_close_preserves_fresh_path() {
    history(NativePeerProtocol::TcpTls);
}
#[cfg(feature = "quic")]
#[test]
fn quic_two_helper_tree_restart_reconsumes_original_and_old_close_preserves_fresh_path() {
    history(NativePeerProtocol::Quic);
}
