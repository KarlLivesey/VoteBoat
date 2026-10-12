// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Downstream-owned network injected through the common owning facade.
use super::*;
use std::{cell::Cell, rc::Rc};
use voteboat::{connect::ConnectDirection, secure::*, transport::*};
#[path = "node_network/provider.rs"]
mod provider;
use provider::*;

type Direct = PeerDriver<Connector, Factory>;
type Selected =
    NetworkNode<Ready, Timers, Entropy, Counter, HostWorker, HostOutbound, Worker, Watched<Direct>>;

#[test]
fn supplied_network_owns_poll_close_and_reclaim_without_catalog_authority() {
    let p = parts(3, true);
    let calls = Rc::new(Calls::default());
    let network = WatchedParts {
        inner: p.peers.unwrap(),
        calls: calls.clone(),
    };
    assert!(network.source_identity(node(2)).is_some());
    let mut n = Selected::from_parts(
        NetworkNodeParts {
            local: p.local,
            peers: Some(network),
        },
        NodeLimits::default(),
        MonoTime(0),
    )
    .unwrap_or_else(|r| panic!("{:?}", r.reason));
    assert_eq!(calls.builds.get(), 1);
    assert!(n.peers().unwrap().source_view(node(2)).is_none());
    for _ in 0..4 {
        n.poll(MonoTime(0), NodePollBudget::default()).unwrap();
    }
    let view = n.peers().unwrap().source_view(node(2)).unwrap();
    assert_eq!(view.source().node, node(2));
    assert_eq!(view.source().store.identity, identity(2));
    assert_eq!(view.direct_access().unwrap().peer, view.source());
    assert!(calls.polls.get() >= 4);
    calls.substituted_source.set(Some(node(2)));
    assert_eq!(
        n.request_learner_readiness(
            group(1),
            node(3),
            Counter::new(100).unwrap().readiness_requirements(),
        ),
        Err(NodeError::WrongPeerStore)
    );
    assert!(n.local().owner.is_drained());
    calls.substituted_source.set(None);
    n.disconnect(node(2), MonoTime(0)).unwrap();
    assert!(n.peers().unwrap().source_view(node(2)).is_none());
    n.begin_shutdown();
    for _ in 0..20 {
        n.poll(MonoTime(0), NodePollBudget::default()).unwrap();
        if n.is_drained() {
            break;
        }
    }
    assert!(n.is_drained());
    assert_eq!(calls.closes.get(), 1);
    let returned = n
        .into_parts()
        .unwrap_or_else(|_| panic!("owned network not reclaimed"));
    assert!(Rc::ptr_eq(&returned.peers.unwrap().calls, &calls));
}

#[test]
fn supplied_network_build_refusal_returns_original_provider_and_local_parts() {
    let p = parts(3, true);
    let outbound = p.local.outbound.binding();
    let owner = p.local.owner.identity();
    let calls = Rc::new(Calls::default());
    calls.reject.set(true);
    let result = Selected::from_parts(
        NetworkNodeParts {
            local: p.local,
            peers: Some(WatchedParts {
                inner: p.peers.unwrap(),
                calls: calls.clone(),
            }),
        },
        NodeLimits::default(),
        MonoTime(0),
    );
    let rejected = match result {
        Ok(_) => panic!("refusal bypassed"),
        Err(r) => r,
    };
    assert_eq!(
        rejected.reason,
        NodeError::Peer(PeerDriverError::WrongBinding)
    );
    assert_eq!(rejected.parts.local.outbound.binding(), outbound);
    assert_eq!(rejected.parts.local.owner.identity(), owner);
    assert!(rejected.parts.local.owner.is_drained());
    assert!(Rc::ptr_eq(&rejected.parts.peers.unwrap().calls, &calls));
    assert_eq!(calls.builds.get(), 1);
    assert_eq!((calls.polls.get(), calls.closes.get()), (0, 0));
}
