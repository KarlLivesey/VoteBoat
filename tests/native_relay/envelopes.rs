// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Full declared envelope refusals return the original genuine ready resources.
use super::{channels::*, support::*};
use std::{collections::BTreeMap, time::Instant};
use voteboat::{
    native::{connect::NativePeerProtocol, relay::*, wire::NativeWireCodec},
    secure::*,
};
#[test]
fn declared_deep_controls_and_codec_envelopes_refuse_whole_ready_sessions_and_parts() {
    let tree = RelayTreePlan::new(
        BTreeMap::from([(node(1), identity(1)), (node(2), identity(2))]),
        (9..=24)
            .map(|n| (node(n), identity(u128::from(n))))
            .collect(),
        (9..24).map(|n| (node(n), node(n + 1))).collect(),
        BTreeMap::from([(node(1), node(9)), (node(2), node(24))]),
        8,
        16,
    )
    .unwrap();
    let local = |n| LocalIdentity {
        node: node(n),
        store: voteboat::identity::StoreBinding {
            identity: identity(u128::from(n)),
            session: voteboat::identity::StoreSession::new(1).unwrap(),
        },
    };
    let physical = local(1);
    let helper = local(9);
    let plan = RelayHelperPlan::for_tree(helper, tree).unwrap();
    let (mut a, b, _) = pair(
        NativePeerProtocol::TcpTls,
        physical,
        helper,
        1,
        Instant::now(),
    );
    let binding = b.binding().unwrap();
    let small = RelayNetworkLimits {
        frame_bytes: 1024,
        ..Default::default()
    };
    let rejected = RelayHop::for_helper(b, plan.clone(), small)
        .expect_err("declared1460-byte source control exceeds1024 frame");
    assert_eq!(rejected.reason, RelayNetworkError::InvalidLimits);
    assert_eq!(require_authenticated(&rejected.session), Ok(binding));
    let small = RelayNetworkLimits {
        decoded_bytes: 1024,
        ..Default::default()
    };
    let rejected = RelayHop::for_helper(rejected.session, plan.clone(), small)
        .expect_err("fixed full-path metadata cannot decode in1024");
    assert_eq!(rejected.reason, RelayNetworkError::InvalidLimits);
    assert_eq!(require_authenticated(&rejected.session), Ok(binding));
    let limits = RelayNetworkLimits {
        frame_bytes: 2048,
        ..Default::default()
    };
    let hop = RelayHop::for_helper(rejected.session, plan.clone(), limits)
        .unwrap_or_else(|e| panic!("control capable hop {:?}", e.reason));
    let parts = RelayForwarderParts {
        plan,
        codec: NativeWireCodec::with_leadership_transfer(Default::default()).unwrap(),
        limits,
        hops: vec![hop],
    };
    let rejected = RelayForwarder::build(parts)
        .expect_err("selected inner codec maximum cannot fit2048 forwarding envelope");
    assert_eq!(rejected.reason, RelayNetworkError::InvalidLimits);
    assert_eq!(rejected.parts.hops.len(), 1);
    assert_eq!(
        rejected.parts.hops[0].authenticated_binding(),
        Some(binding)
    );
    for mut hop in rejected.parts.hops {
        hop.abort();
        assert!(hop.into_session().is_ok());
    }
    a.revoke();
}
