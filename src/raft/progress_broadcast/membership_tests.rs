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

#[test]
fn configured_learner_joint_final_and_retirement_states_never_emit_commit_notices() {
    let mut final_state = joint(false);
    committed_fixture(&mut final_state, 3);
    let effects = accept(
        &mut final_state,
        101,
        ConfigurationChange::Final { id: cid(4) },
    );
    durable(&mut final_state, effects);
    // Final continues the Joint lifecycle operation101; Learners operation100
    // is already complete and cannot be reused. Preserve both accepted Final
    // and fully committed demotion/retirement states.
    let mut retired = joint(false);
    committed_fixture(&mut retired, 3);
    let effects = accept(&mut retired, 101, ConfigurationChange::Final { id: cid(4) });
    durable(&mut retired, effects);
    committed_fixture(&mut retired, 4);
    retired.commit_notices = true;
    assert_eq!(retired.role(), Role::Follower);
    assert!(retired.step(Event::Heartbeat).unwrap().is_empty());
    let mut states = vec![staged(), joint(false), final_state];
    for state in &mut states {
        // Defense in depth: retain the startup capability while entering a
        // configuration-bearing history, as a real initially plain core may.
        state.commit_notices = true;
        state.clear_replication_requests();
        let effects = state.broadcast_commit_progress().unwrap();
        assert!(effects.iter().all(|e| !matches!(
            e,
            Effect::Send(Message {
                rpc: Rpc::CommitNotice { .. },
                ..
            })
        )));
        state.role = Role::Follower;
        assert!(state.enable_commit_notices().is_err());
    }
}
