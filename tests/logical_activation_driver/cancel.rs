// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
// Unless explicitly acquired and licensed from Licensor under another license,
// the contents of this file are subject to the Reciprocal Public License
// ("RPL") Version 1.5, or subsequent versions as allowed by the RPL, and You may
// not copy or use this file in either source code or executable form, except
// in compliance with the terms and conditions of the RPL.
//
// All software distributed under the RPL is provided strictly on an "AS IS"
// basis, WITHOUT WARRANTY OF ANY KIND, EITHER EXPRESS OR IMPLIED, AND LICENSOR
// HEREBY DISCLAIMS ALL SUCH WARRANTIES, INCLUDING WITHOUT LIMITATION, ANY
// WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, QUIET
// ENJOYMENT, OR NON-INFRINGEMENT. See the RPL for specific language governing
// rights and limitations under the RPL.
//! Cancelled observation retains the original authorized dispatch dependency.
use super::*;
#[test]
fn original_cancel_before_configuration_dispatch_retains_owning_driver_tcp() {
    history(NativePeerProtocol::TcpTls);
}
#[cfg(feature = "quic")]
#[test]
fn original_cancel_before_configuration_dispatch_retains_owning_driver_quic() {
    history(NativePeerProtocol::Quic);
}
fn history(protocol: NativePeerProtocol) {
    let _serial = crate::history_guard();
    let mut cohort = member::Member::new(protocol);
    let clock = &cohort.clock.clone();
    let mut bridge =
        LogicalChildBridge::new(&mut cohort.nodes, setup::mapping(), support::node(1)).unwrap();
    let image = drive::claim_in(&mut bridge, clock, 5101, false);
    let mut parent = LogicalNode::from_parts(
        &image,
        fixture::build(&image),
        fixture::limits(),
        MonoTime(0),
    )
    .unwrap_or_else(|e| panic!("actual parent {:?}", e.reason));
    progress::elect(&mut bridge, &mut parent, clock);
    let original = super::history::intent(6100);
    let mut driver = ChildActivationDriver::new(
        &mut parent,
        original.clone(),
        ChildActivationDriverLimits::default(),
    )
    .unwrap_or_else(|e| panic!("actual driver {:?}", e.reason));
    let end = harness::hang_guard();
    loop {
        progress::child_step(&mut driver, &mut bridge, clock, 3);
        // No child work/read is held; the sole attempt is the exact original
        // configuration admitted at the end of this driver transition.
        let usage = bridge.usage();
        if usage.actions == 0 && usage.child_attempts == 1 {
            break;
        }
        assert!(Instant::now() < end, "original configuration admission");
    }
    assert_old(&bridge);
    driver.stop_waiting_with_child(&mut bridge).unwrap();
    progress::child_step(&mut driver, &mut bridge, clock, 0);
    assert_old(&bridge);
    assert_eq!(bridge.usage().retained_outputs, 1);
    assert_eq!(bridge.usage().child_attempts, 1);
    assert!(!driver.is_drained());
    assert!(!bridge.is_drained());
    while !driver.is_drained() {
        progress::child_step(&mut driver, &mut bridge, clock, 3);
        assert!(Instant::now() < end, "actual original dispatch settles");
    }
    assert_eq!(bridge.usage().child_attempts, 0);
    let node = driver
        .try_reclaim()
        .unwrap_or_else(|_| panic!("original ownership drain"));
    assert!(node.local().applications[&setup::mapping().parent]
        .status(original.operations().begin)
        .unwrap()
        .outcome
        .is_none());
    node.close_intake().unwrap();
    progress::drain(&mut bridge, node, clock);
    bridge.close();
    assert!(bridge.is_drained());
    drop(bridge);
    drop(
        parent
            .into_parts()
            .unwrap_or_else(|_| panic!("actual facade drain")),
    );
    cohort.finish();
}
fn assert_old(bridge: &aliases::Bridge<'_>) {
    for node in 0..3 {
        bridge
            .inspect_backing(node, |local| {
                assert_eq!(
                    local
                        .owner
                        .core(setup::mapping().child)
                        .unwrap()
                        .membership()
                        .id(),
                    ConfigurationId::new(1).unwrap()
                );
            })
            .unwrap();
    }
}
