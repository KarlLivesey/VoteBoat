// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::{
    cluster::{ChildNode, Cluster, HISTORY},
    drive, setup, support,
};
use std::cell::Cell;
use voteboat::{
    logical_voter::*,
    native::connect::NativePeerProtocol,
    runtime::{MonoTime, NodeError, NodePollBudget},
};
fn history(protocol: NativePeerProtocol) {
    let _serial = HISTORY.lock().unwrap();
    let mut cluster = Cluster::new(protocol);
    let clock = cluster.clock;
    {
        let mut bridge: LogicalBackingBridge<'_, ChildNode> =
            LogicalBackingBridge::new(&mut cluster.nodes, setup::mapping(), support::node(1))
                .unwrap();
        let before = bridge.usage();
        for slot in 0..3 {
            assert!(bridge.pump_backing(slot, |view| view.is_none()).unwrap());
        }
        let called = Cell::new(false);
        assert_eq!(
            bridge.pump_backing(3, |_| called.set(true)),
            Err(LogicalBridgeError::InvalidAssembly)
        );
        assert!(!called.get());
        assert_eq!(bridge.usage(), before);
        // The old physical alias accepts this exact same bridge. Withholding
        // actual opaque child completions still blocks Claim/Image authority.
        let image = drive::claim_in(&mut bridge, clock, 89001, true);
        assert_eq!(image.mapping(), setup::mapping());
        assert_eq!(image.representative().physical.node, support::node(1));
        assert_eq!(image.representative().physical.store, support::identity(1));
        assert!(image.child_barrier().index() >= image.claim().first_index);
        bridge.close();
        assert!(bridge.pump_backing(0, |view| view.is_none()).unwrap());
        assert!(bridge.is_drained());
    }
    // Exercise an actual provider error, preserving its complete typed reason
    // and the historical bridge Node variant. No fabricated backing output.
    cluster.poll();
    {
        let mut bridge =
            LogicalBackingBridge::new(&mut cluster.nodes, setup::mapping(), support::node(1))
                .unwrap();
        let expected = LogicalBridgeError::Node {
            node: 0,
            reason: Box::new(NodeError::TimeWentBack),
        };
        assert_eq!(
            bridge.advance(
                MonoTime(0),
                LogicalBridgeBudget {
                    node_polls: 1,
                    node: NodePollBudget::default(),
                    ..LogicalBridgeBudget::default()
                },
            ),
            Err(expected.clone())
        );
        assert_eq!(bridge.failure(), Some(&expected));
        assert!(bridge.take_image().is_none());
        assert_eq!(bridge.usage(), LogicalBridgeUsage::default());
        bridge.close();
        assert!(bridge.pump_backing(0, |view| view.is_none()).unwrap());
        assert!(bridge.is_drained());
    }
    cluster.finish();
}
#[test]
fn generic_backing_consumes_original_tcp_outputs_and_preserves_errors() {
    history(NativePeerProtocol::TcpTls);
}
#[cfg(feature = "quic")]
#[test]
fn generic_backing_consumes_original_quic_outputs_and_preserves_errors() {
    history(NativePeerProtocol::Quic);
}
