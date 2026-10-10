// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

#[test]
fn original_source_follower_resume_preserves_historical_intent_tcp() {
    history(false);
}
#[cfg(feature = "quic")]
#[test]
fn original_source_follower_resume_preserves_historical_intent_quic() {
    history(true);
}

fn history(quic: bool) {
    let mut c = cluster(quic);
    let source = c.leader();
    let target = source % 3 + 1;
    let op = "96500";
    let first = begin(&c, source, target, op);
    check_identity(&first, op, source, target);
    let (leader, completed) = status(&mut c, op, "phase=Completed");
    assert_ne!(leader, source, "handoff must change the routing endpoint");
    cancellation::same_intent(&first, &completed);
    let refused = c.request(source, &["resume-leadership", op]);
    assert!(!refused.status.success());
    assert_eq!(
        String::from_utf8(refused.stdout).unwrap(),
        "ERR NOT_LEADER\n"
    );
    assert_eq!(
        String::from_utf8(refused.stderr).unwrap().trim_end(),
        "Error: \"request unsuccessful; preserve the operation ID and payload when retrying a write\""
    );
    let resumed = leader_request(&mut c, &["resume-leadership", op]);
    assert_eq!(resumed, completed);
    let cancelled = leader_request(&mut c, &["cancel-leadership", op]);
    assert_eq!(cancelled, completed, "Completed must stay historical");
    if quic {
        cancellation::checkpoint(&c, leader);
    }
    c.stop();
    for node in 1..=3 {
        c.start(node, "recover");
    }
    assert_eq!(status(&mut c, op, "phase=Completed").1, completed);
    assert_eq!(
        leader_request(&mut c, &["resume-leadership", op]),
        completed
    );
    assert_eq!(
        leader_request(&mut c, &["cancel-leadership", op]),
        completed
    );
    let retry = authenticated_write(&c, &["add", "96000", "7"]);
    assert!(retry.contains("Value(7)") && retry.contains("duplicate=true"));
    assert!(authenticated_write(&c, &["add", "96000", "8"]).contains("OperationConflict"));
    assert_eq!(c.routed(&["read"]), "OK value=7\n");
    assert!(authenticated_write(&c, &["add", "96501", "3"]).contains("Value(10)"));
    c.stop();
    fs::remove_dir_all(&c.root).unwrap();
}

#[test]
fn explicit_resume_follower_and_read_refusals_allow_original_endpoint_selection() {
    for text in [
        "ERR NOT_LEADER\n",
        "ERR NotRead(ReadNotReady)\n",
        "ERR Unavailable(LeadershipChanged)\n",
    ] {
        assert!(retryable_leader_response(
            &["resume-leadership", "96500"],
            text
        ));
    }
}

#[test]
fn resume_modified_or_unrelated_failures_remain_terminal() {
    for text in [
        "ERR NOT_LEADER extra=unknown\n",
        "ERR NOT_LEADER\nextra\n",
        "ERR AUTHORIZATION\n",
        "ERR no matching intent\n",
        "UNKNOWN unspecified outcome\n",
        "UNKNOWN authenticated read failed; retry the same operation ID and delta\n",
        "OK partial\n",
        "",
    ] {
        assert!(!retryable_leader_response(
            &["resume-leadership", "96500"],
            text
        ));
    }
}
