// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

#[test]
fn stable_configured_progress_skips_held_append_and_joint_keeps_legacy_retries() {
    let mut stable = staged();
    stable.clear_replication_requests();
    let effects = stable.step(Event::Heartbeat).unwrap();
    let held = message_to(effects, 3);
    let repeated = stable.broadcast_commit_progress().unwrap();
    assert!(repeated.is_empty());
    assert_eq!(stable.requests[&node(3)].context, held.context);
    let mut joint = joint(false);
    committed_fixture(&mut joint, 3);
    let effects = joint.step(Event::Heartbeat).unwrap();
    let held = message_to(effects, 3);
    let repeated = message_to(joint.broadcast_commit_progress().unwrap(), 3);
    assert_eq!(repeated.context, held.context);
    assert_eq!(repeated.rpc, held.rpc);
}

#[test]
fn uncommitted_membership_retains_progress_fallback() {
    let mut leader = core();
    let effects = accept(
        &mut leader,
        77,
        ConfigurationChange::Learners(configuration(2, &[1, 2, 3], &[4])),
    );
    let effects = durable(&mut leader, effects);
    let held = message_to(effects, 3);
    assert!(!leader.progress_sends_enabled());
    let retry = message_to(leader.broadcast_commit_progress().unwrap(), 3);
    assert_eq!(retry.context, held.context);
    assert_eq!(retry.rpc, held.rpc);
}
