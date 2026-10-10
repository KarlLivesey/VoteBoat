// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

#[test]
fn original_planned_drain_survives_another_election_tcp() {
    history(false);
}

#[cfg(feature = "quic")]
#[test]
fn original_planned_drain_survives_another_election_quic() {
    history(true);
}

fn history(quic: bool) {
    let (mut c, source, target) = membership_drain::prepare(quic);
    let path = c.membership_drain.as_ref().unwrap().1.clone();
    let plan = fs::read(&path).unwrap();
    c.ok(source, &["drain-node", "1", "19701"]);
    drain::wait_status(&c, source, "phase=Active");
    let command = format!("move-leader 19701 1 {target} {target} 1");
    let unread = UnobservedCommand::send(&c, source, &command);
    let (leader, completed) = leadership::status(&mut c, "19701", "phase=Completed");
    unread.disconnect();
    assert_eq!(leader, target, "{completed}");
    let third = (1..=3).find(|id| *id != source && *id != target).unwrap();
    let third_text = third.to_string();
    leader_request(
        &mut c,
        &["move-leader", "19762", "1", &third_text, &third_text, "1"],
    );
    let (leader, _) = leadership::status(&mut c, "19762", "phase=Completed");
    assert_eq!(leader, third);
    assert_eq!(
        leadership::status(&mut c, "19701", "phase=Completed").1,
        completed
    );
    assert!(c.ok(source, &["status"]).contains("role=Follower"));
    let journal = c.root.join(source.to_string()).join("drain.record");
    let original = fs::read(&journal).unwrap();
    finish(&mut c, source);
    assert_eq!(fs::read(&path).unwrap(), plan);
    assert_eq!(fs::read(&journal).unwrap(), original);
    for id in [target, third] {
        c.start(id, "recover-member");
    }
    assert_eq!(
        leadership::status(&mut c, "19701", "phase=Completed").1,
        completed
    );
    assert!(authenticated_write(&c, &["add", "19760", "3"]).contains("duplicate=true"));
    assert!(authenticated_write(&c, &["add", "19700", "7"]).contains("duplicate=true"));
    assert_eq!(c.routed(&["read"]), "OK value=10\n");
    c.stop();
}
