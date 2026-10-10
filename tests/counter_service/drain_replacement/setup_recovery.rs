// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

const ORIGINAL: &[&str] = &["move-leader", "19750", "2", "1", "1", "1"];

fn interrupt(c: &mut Cluster) -> String {
    if c.leader() == 1 {
        leader_request(c, &["move-leader", "19749", "2", "2", "2", "1"]);
        leadership::status(c, "19749", "phase=Completed");
    }
    let source = c.leader();
    assert_ne!(source, 1);
    let unread = UnobservedCommand::send(c, source, &ORIGINAL.join(" "));
    let (leader, receipt) = leadership::status(c, "19750", "phase=Completed");
    unread.disconnect();
    assert_eq!(leader, 1, "{receipt}");
    for field in [
        "operation=19750 ".to_owned(),
        format!("source={source} "),
        "target=1 ".to_owned(),
        "configuration=2 ".to_owned(),
    ] {
        assert!(receipt.contains(&field), "{receipt}");
    }
    assert!(receipt.contains("evidence=quorum_read"), "{receipt}");
    assert!(c.ok(source, &["status"]).contains("role=Follower"));
    let replay = leader_request(c, ORIGINAL);
    assert_eq!(replay.trim(), receipt.split(" evidence=").next().unwrap());
    receipt
}

pub(super) fn handoff(c: &mut Cluster, lost_reply: bool) -> Option<String> {
    let receipt = lost_reply.then(|| interrupt(c));
    let leader = c.leader();
    if leader != 1 {
        leader_request(c, ORIGINAL);
        leadership::status(c, "19750", "phase=Completed");
    }
    let until = Instant::now() + Duration::from_secs(15);
    while c.leader() != 1 {
        assert!(Instant::now() < until, "source did not become leader");
        std::thread::park_timeout(Duration::from_millis(10));
    }
    receipt
}

#[test]
fn original_replacement_setup_recovers_unread_handoff_tcp() {
    replacement_history(false, true);
}

#[cfg(feature = "quic")]
#[test]
fn original_replacement_setup_recovers_unread_handoff_quic() {
    replacement_history(true, true);
}

#[test]
fn fixed_handoff_classification_keeps_conflicts_and_unrelated_failures_terminal() {
    for text in [
        "ERR NOT_LEADER\n",
        "UNKNOWN LeadershipChanged; retry the same operation ID and delta\n",
        "UNKNOWN authenticated read failed; retry the same operation ID and delta\n",
        "UNKNOWN reply deadline expired; retry the same operation ID and delta\n",
    ] {
        assert!(retryable_leader_response(ORIGINAL, text));
    }
    for text in [
        "ERR AUTHORIZATION\n",
        "ERR not_proposed=Application(OperationConflict)\n",
        "ERR not_proposed=Busy\n",
        "UNKNOWN unspecified outcome\n",
        "UNKNOWN LeadershipChanged; changed operation\n",
        "UNKNOWN reply deadline expired; retry the same operation ID and delta\nextra\n",
    ] {
        assert!(!retryable_leader_response(ORIGINAL, text));
    }
    assert!(!retryable_leader_response(
        &["move-leader", "19750", "2", "1", "1"],
        "ERR NOT_LEADER\n"
    ));
}
