// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

fn record(text: &str) -> &str {
    text.trim().split(" evidence=").next().unwrap()
}

fn observe_busy(c: &mut Cluster, words: &[&str]) {
    for _ in 0..8 {
        let node = groups::leader(c, "7", "3");
        let reply = c.request(node, words);
        let text = String::from_utf8(reply.stdout).unwrap();
        if !reply.status.success() && text == "ERR not_proposed=Busy\n" {
            return;
        }
        assert!(reply.status.success(), "unexpected refusal: {text}");
        assert!(text.contains("phase=Pending"), "{text}");
    }
    panic!("active unavailable-target handoff did not refuse original replay");
}

fn history(quic: bool) {
    let mut c = group_admin::setup(quic);
    c.leadership_maintenance = true;
    for node in 1..=3 {
        c.start(node, "create");
    }
    assert!(authenticated_write(&c, &["group", "7", "3", "add", "42", "5"]).contains("Value(5)"));
    assert!(authenticated_write(&c, &["group", "8", "2", "add", "42", "8"]).contains("Value(8)"));
    let source = groups::leader(&mut c, "7", "3");
    let target = source % 3 + 1;
    drain::kill(&mut c, target);
    begin(&mut c, ("7", "3", "9"), OP, target);
    let (_, pending) = status(&mut c, ("7", "3"), OP, "phase=Pending");
    let target_text = target.to_string();
    let bound = leadership::words(OP, "9", source, target);
    let words = bound.each_ref().map(String::as_str);
    let mut scoped = vec!["group", "7", "3"];
    scoped.extend_from_slice(&words);
    observe_busy(&mut c, &scoped);
    // Busy proves non-admission. Preserve every field while the volatile
    // attempt ends; a new operation ID would not be the same recovery.
    let repeated = command(&mut c, ("7", "3"), &words);
    assert_eq!(record(&repeated), record(&pending));
    let cancelled = command(&mut c, ("7", "3"), &["cancel-leadership", OP]);
    assert!(cancelled.contains("phase=Cancelled"), "{cancelled}");
    let (_, saved) = status(&mut c, ("7", "3"), OP, "phase=Cancelled");
    assert_eq!(record(&cancelled), record(&saved));
    let node = groups::leader(&mut c, "7", "3");
    let conflict = c.request(
        node,
        &[
            "group",
            "7",
            "3",
            "move-leader",
            OP,
            "9",
            &source.to_string(),
            &source.to_string(),
            "1",
            &target_text,
            "999",
            "1",
        ],
    );
    assert!(!conflict.status.success());
    assert!(String::from_utf8(conflict.stdout)
        .unwrap()
        .contains("OperationConflict"));
    assert_eq!(status(&mut c, ("7", "3"), OP, "phase=Cancelled").1, saved);
    status(&mut c, ("8", "2"), OP, "phase=Absent");
    let checkpoint = quic.then(|| historical::checkpoint(&mut c));
    c.stop();
    if let Some((node, through)) = checkpoint {
        historical::verify_checkpoint(&c, node, through);
    }
    for node in 1..=3 {
        c.start(node, "recover");
    }
    assert_eq!(status(&mut c, ("7", "3"), OP, "phase=Cancelled").1, saved);
    assert_eq!(record(&command(&mut c, ("7", "3"), &words)), record(&saved));
    status(&mut c, ("8", "2"), OP, "phase=Absent");
    for (group, incarnation, value) in [("7", "3", "5"), ("8", "2", "8")] {
        assert_eq!(
            authenticated_write(&c, &["group", group, incarnation, "add", "42", value]),
            format!("OK outcome=Value({value}) duplicate=true\n")
        );
        assert_eq!(
            c.routed(&["group", group, incarnation, "read"]),
            format!("OK value={value}\n")
        );
    }
    c.stop();
}

#[test]
fn original_handoff_replay_after_busy_preserves_cancelled_record_tcp() {
    history(false);
}

#[test]
#[cfg(feature = "quic")]
fn original_handoff_replay_after_busy_preserves_cancelled_record_quic() {
    history(true);
}
