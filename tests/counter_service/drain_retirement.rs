// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use voteboat::{identity::*, log::*, native::log_store::*};
const RETIRE: &str = "19780";

fn status(c: &Cluster, id: usize, expected: &str) {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let text = c.wait_configuration_status(id, RETIRE);
        if text.contains(expected) {
            return;
        }
        assert!(Instant::now() < deadline, "expected {expected}: {text}");
        std::thread::park_timeout(Duration::from_millis(10));
    }
}
fn saved(c: &Cluster, id: usize) -> GroupLog {
    let _gate = fixture_gate();
    NativeLogStore::recover(
        FileLogIo::open(c.root.join(id.to_string())).unwrap(),
        StoreIdentity {
            id: StoreId::new(id as u128).unwrap(),
            incarnation: StoreIncarnation::new(1).unwrap(),
        },
        LogLimits::default(),
    )
    .unwrap()
    .state(GroupIdentity {
        id: GroupId::new(1).unwrap(),
        incarnation: GroupIncarnation::new(1).unwrap(),
    })
    .unwrap()
}
fn assert_retired(c: &Cluster, id: usize, source: usize, checkpoint: bool) {
    let state = saved(c, id);
    let committed = state.membership_at(state.commit_index).unwrap();
    assert_eq!(committed.id().get(), 4);
    assert!(committed.joint().is_none());
    assert!(committed.stable().learners().is_empty());
    assert_eq!(committed.stable().voter_stores().len(), 2);
    assert!(!committed
        .stable()
        .voter_stores()
        .contains_key(&NodeId::new(source as u64).unwrap()));
    for operation in [19751, 19780] {
        assert!(committed
            .operations()
            .contains(&OperationId::new(operation).unwrap()));
    }
    if checkpoint {
        let snapshot = state.snapshot.unwrap();
        assert_eq!(state.membership_at(snapshot.index).unwrap().id().get(), 4);
    }
}
fn finish_retirement(c: &mut Cluster, survivors: &[usize], source: usize, quic: bool) {
    c.leader();
    for &id in survivors {
        status(c, id, "action=completed");
    }
    let leader = c.leader();
    assert!(c
        .ok(leader, &["configure", RETIRE])
        .contains("action=completed"));
    assert!(authenticated_write(c, &["add", "19781", "3"]).contains("Value(10)"));
    assert!(authenticated_write(c, &["add", "19700", "7"]).contains("duplicate=true"));
    if quic {
        for &id in survivors {
            c.ok(id, &["checkpoint"]);
            drain::wait_manual_checkpoint(c, id);
        }
    }
    c.stop();
    for &id in survivors {
        assert_retired(c, id, source, quic);
        c.start(id, "recover-member");
    }
    let leader = c.leader();
    assert!(c
        .ok(leader, &["configure", RETIRE])
        .contains("action=completed"));
    assert!(authenticated_write(c, &["add", "19781", "3"]).contains("duplicate=true"));
    assert!(authenticated_write(c, &["add", "19782", "1"]).contains("Value(11)"));
    // Old source files still describe the retained learner. Recovery must keep
    // the active gate; removing a remote assignment never erases local intent.
    c.start(source, "recover-member");
    drain::wait_status(c, source, "phase=Active");
    let blocked = c.request(source, &["add", "19783", "100"]);
    assert!(!blocked.status.success());
    assert!(String::from_utf8_lossy(&blocked.stdout).contains("Draining"));
    assert_eq!(c.routed(&["read"]), "OK value=11\n");
    c.stop();
}
fn history(quic: bool) {
    let (mut c, source, _) = membership_drain::prepare(quic);
    // The retirement expects configuration3, not the original voter set.
    assert!(!c.request(source, &["configure", RETIRE]).status.success());
    status(&c, source, "inconclusive_local_absence");
    let output = run(&mut drain_runner::command(&c, source, "19701", 3));
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    drain::joined(&mut c, source);
    let survivors = (1..=3).filter(|id| *id != source).collect::<Vec<_>>();
    let leader = c.leader();
    c.command_principal = Some(1);
    assert!(!c.request(leader, &["configure", RETIRE]).status.success());
    c.command_principal = Some(3);
    let follower = *survivors.iter().find(|id| **id != leader).unwrap();
    drain::kill(&mut c, follower);
    let unread = UnobservedCommand::send(&c, leader, "configure 19780");
    status(&c, leader, "action=wait_for_commit");
    drain::kill(&mut c, leader);
    unread.disconnect();
    let durable = saved(&c, leader);
    assert_eq!(durable.membership().unwrap().id().get(), 4);
    assert_eq!(
        durable
            .membership_at(durable.commit_index)
            .unwrap()
            .id()
            .get(),
        3
    );
    for &id in &survivors {
        c.start(id, "recover-member");
    }
    finish_retirement(&mut c, &survivors, source, quic);
}
#[test]
fn drained_learner_retirement_recovers_unknown_and_stale_source_tcp() {
    history(false);
}
#[cfg(feature = "quic")]
#[test]
fn drained_learner_retirement_recovers_unknown_and_stale_source_quic_checkpoint() {
    history(true);
}
