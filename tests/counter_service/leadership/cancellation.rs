// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

#[test]
fn admitted_cancel_survives_source_loss_and_original_retry_tcp() {
    history(false);
}

#[cfg(feature = "quic")]
#[test]
fn admitted_cancel_survives_source_loss_and_original_retry_quic() {
    history(true);
}

fn history(quic: bool) {
    let mut c = cluster(quic);
    let source = c.leader();
    let target = source % 3 + 1;
    let survivor = target % 3 + 1;
    let op = "96400";
    stop_one(&mut c, target);
    let first = begin(&c, source, target, op);
    assert!(first.contains("phase=Pending"), "{first}");
    check_identity(&first, op, source, target);
    status(&mut c, op, "phase=Pending");
    if quic {
        checkpoint(&c, source);
    }

    // No majority can commit cancellation. Cut only after its proposal is
    // admitted; neither local suspension nor an unread reply is a durable cancel.
    stop_one(&mut c, survivor);
    let lost = UnobservedCommand::send(&c, source, &format!("cancel-leadership {op}"));
    wait_event(&c, source, "operation=96400 intent_index=");
    stop_one(&mut c, source);
    lost.disconnect();
    c.start(source, "recover");
    c.start(survivor, "recover");
    // The accepted suffix can commit when a majority returns. Recovery may
    // therefore expose Pending or Cancelled, never absence or a new intent.
    let (_, recovered) = status(&mut c, op, "phase=");
    assert!(
        recovered.contains("phase=Pending") || recovered.contains("phase=Cancelled"),
        "{recovered}"
    );
    same_intent(&first, &recovered);

    let cancelled = leader_request(&mut c, &["cancel-leadership", op]);
    assert!(cancelled.contains("phase=Cancelled"), "{cancelled}");
    same_intent(&first, &cancelled);
    let (leader, observed) = status(&mut c, op, "phase=Cancelled");
    same_intent(&first, &observed);
    assert!(observed.contains("evidence=quorum_read"));

    if quic {
        checkpoint(&c, leader);
    }
    c.stop();
    for node in 1..=3 {
        c.start(node, "recover");
    }
    let (_, reopened) = status(&mut c, op, "phase=Cancelled");
    assert_eq!(observed, reopened);
    let duplicate = leader_request(&mut c, &["cancel-leadership", op]);
    assert_eq!(duplicate, reopened);
    assert!(authenticated_write(&c, &["add", "96000", "7"]).contains("duplicate=true"));
    assert!(authenticated_write(&c, &["add", "96401", "3"]).contains("Value(10)"));
    assert_eq!(c.routed(&["read"]), "OK value=10\n");
    c.stop();
    fs::remove_dir_all(&c.root).unwrap();
}

pub(super) fn checkpoint(c: &Cluster, node: usize) {
    let committed = c.ok(node, &["status"]);
    let through = field(&committed, "committed=");
    c.ok(node, &["checkpoint"]);
    let end = Instant::now() + Duration::from_secs(10);
    loop {
        let maintenance = c.ok(node, &["maintenance"]);
        if field(&maintenance, "checkpoint_base=") >= through {
            return;
        }
        assert!(
            Instant::now() < end,
            "checkpoint below {through}: {maintenance}"
        );
        std::thread::park_timeout(Duration::from_millis(10));
    }
}

fn field(text: &str, prefix: &str) -> u64 {
    text.split_whitespace()
        .find_map(|word| word.strip_prefix(prefix))
        .unwrap()
        .parse()
        .unwrap()
}

pub(super) fn same_intent(first: &str, current: &str) {
    assert_eq!(
        first.split(" phase=").next(),
        current.split(" phase=").next(),
        "the cancellation must retain every original intent field"
    );
}
