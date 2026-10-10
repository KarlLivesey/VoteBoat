// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

#[test]
fn prepared_source_completes_previous_handoff_before_new_drain_tcp() {
    history(false);
}
#[cfg(feature = "quic")]
#[test]
fn prepared_source_completes_previous_handoff_before_new_drain_quic() {
    history(true);
}
fn history(quic: bool) {
    let mut c = drain::cluster(quic);
    let original = c.leader();
    let source = original % 3 + 1;
    let target = source % 3 + 1;
    prepare_source_leader(&mut c, source);
    let completed = c.ok(source, &["leadership-status", "19750"]);
    preparation_identity(&completed, original, source);
    assert!(completed.contains("phase=Completed"), "{completed}");
    assert!(completed.contains("evidence=quorum_read"), "{completed}");
    let first = c.ok(
        source,
        &[
            "drain-node",
            "1",
            "19701",
            "1",
            &target.to_string(),
            &target.to_string(),
            "1",
        ],
    );
    assert!(first.contains("phase=Active"), "{first}");
    drain::wait_status(&c, source, "phase=Active");
    c.ok(source, &["cancel-drain", "1", "19701"]);
    drain::wait_status(&c, source, "resuming=false");
    assert!(authenticated_write(&c, &["add", "19700", "7"]).contains("duplicate=true"));
    c.stop();
}
