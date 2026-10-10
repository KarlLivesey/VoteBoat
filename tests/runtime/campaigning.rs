// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

fn step(runtime: &mut TimedShard<HostReady, HostTimers, FixedEntropy>, now: MonoTime) {
    let visit = runtime.poll(now).unwrap().unwrap();
    runtime
        .step_next(visit, now)
        .unwrap()
        .unwrap()
        .result
        .unwrap();
    runtime.finish(visit).unwrap();
}
#[test]
fn pause_invalidates_queued_election_and_resume_arms_a_fresh_timer() {
    let (mut runtime, _) = host_timed(1, None);
    let old = runtime.deadline(group(1)).unwrap();
    runtime
        .admit(group(1), Event::SetCampaigning { enabled: false })
        .unwrap();
    let now = MonoTime(old.deadline.0 + 1);
    assert_eq!(runtime.poll_timers(now).unwrap().admitted, 1);
    step(&mut runtime, now);
    assert!(runtime.deadline(group(1)).is_none());
    step(&mut runtime, now); // The queued expiration is now stale.
    assert_eq!(runtime.core(group(1)).unwrap().state().hard_state.term, 1);
    runtime
        .admit(group(1), Event::SetCampaigning { enabled: true })
        .unwrap();
    step(&mut runtime, now);
    let fresh = runtime.deadline(group(1)).unwrap();
    assert_ne!(fresh, old);
    assert!(fresh.deadline > now);
    assert_eq!(fresh.kind, TimerKind::Election);
}
#[test]
fn paused_follower_keeps_replication_contact_without_rearming_an_election() {
    let (mut runtime, _) = host_timed(1, None);
    runtime
        .admit(group(1), Event::SetCampaigning { enabled: false })
        .unwrap();
    step(&mut runtime, MonoTime(1));
    runtime
        .admit(group(1), Event::Receive(heartbeat(1)))
        .unwrap();
    step(&mut runtime, MonoTime(2));
    assert!(runtime.deadline(group(1)).is_none());
    assert_eq!(runtime.poll_timers(MonoTime(1000)).unwrap().admitted, 0);
}

#[test]
fn paused_leader_retains_automatic_heartbeat_deadlines() {
    let mut log = HostLogStore::new(1);
    append(&mut log, vec![LogMutation::Create(bootstrap(1, 1))]);
    let mut core = Raft::recover(
        node(1),
        log.binding(),
        log.state(group(1)).unwrap(),
        log.limits(),
    )
    .unwrap();
    let mut effects = core.step(Event::Campaign).unwrap();
    while let Some(effect) = effects.pop() {
        if let Effect::Persist(update) = effect {
            effects.extend(persist_effect(&mut core, &mut log, update).unwrap());
        }
    }
    assert_eq!(core.role(), Role::Leader);
    let mut shard = Shard::new(owner(log.binding()), small_limits(), HostReady::new(3)).unwrap();
    shard.register(core).unwrap();
    let timers = HostTimers::new(owner(log.binding()), 3);
    let mut runtime =
        TimedShard::new(shard, timers, FixedEntropy(0), timer_config(), MonoTime(0)).unwrap();
    runtime
        .admit(group(1), Event::SetCampaigning { enabled: false })
        .unwrap();
    step(&mut runtime, MonoTime(1));
    let heartbeat = runtime.deadline(group(1)).unwrap();
    assert_eq!(heartbeat.kind, TimerKind::Heartbeat);
    assert_eq!(runtime.poll_timers(heartbeat.deadline).unwrap().admitted, 1);
    step(&mut runtime, heartbeat.deadline);
    assert_eq!(runtime.core(group(1)).unwrap().role(), Role::Leader);
    assert!(!runtime.core(group(1)).unwrap().campaigning_enabled());
    assert!(runtime.deadline(group(1)).unwrap().deadline > heartbeat.deadline);
}
