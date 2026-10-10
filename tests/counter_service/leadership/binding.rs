// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

#[test]
fn explicit_original_handoff_survives_prior_target_election_tcp() {
    history(false);
}

#[cfg(feature = "quic")]
#[test]
fn explicit_original_handoff_survives_prior_target_election_quic_checkpoint() {
    history(true);
}

fn history(quic: bool) {
    let mut c = cluster(quic);
    let source = c.leader();
    let target = source % 3 + 1;
    let bound = words("23911", "1", source, target);
    begin(&c, source, target, "23910");
    let (leader, _) = status(&mut c, "23910", "phase=Completed");
    assert_eq!(leader, target);
    assert!(c
        .ok(target, &["leadership-status", "23911"])
        .contains("phase=Absent"));
    let unread = UnobservedCommand::send(&c, target, &bound.join(" "));
    let (_, completed) = status(&mut c, "23911", "phase=Completed");
    unread.disconnect();
    check_identity(&completed, "23911", source, target);
    let term = completed
        .split_whitespace()
        .find_map(|w| w.strip_prefix("intent_term="))
        .unwrap();
    assert!(
        completed.contains(&format!("term: {term} }}")),
        "{completed}"
    );
    let replay = leader_request(&mut c, &bound.each_ref().map(String::as_str));
    assert_eq!(replay.trim(), completed.split(" evidence=").next().unwrap());
    conflicts(&c, target, &bound);
    if quic {
        for id in 1..=3 {
            c.ok(id, &["checkpoint"]);
            drain::wait_manual_checkpoint(&c, id);
        }
    }
    c.stop();
    for id in 1..=3 {
        c.start(id, "recover");
    }
    assert_eq!(status(&mut c, "23911", "phase=Completed").1, completed);
    assert_eq!(
        leader_request(&mut c, &bound.each_ref().map(String::as_str)).trim(),
        completed.split(" evidence=").next().unwrap()
    );
    assert!(authenticated_write(&c, &["add", "96000", "7"]).contains("duplicate=true"));
    assert!(authenticated_write(&c, &["add", "23912", "3"]).contains("Value(10)"));
    c.stop();
    fs::remove_dir_all(&c.root).unwrap();
}

fn conflicts(c: &Cluster, target: usize, original: &[String; 9]) {
    for field in [3, 4, 5, 6, 7, 8] {
        let mut changed = original.clone();
        changed[field] = "999".into();
        let result = c.request(target, &changed.each_ref().map(String::as_str));
        assert!(!result.status.success());
        assert!(String::from_utf8(result.stdout)
            .unwrap()
            .contains("OperationConflict"));
    }
    for field in [3, 4, 5] {
        let mut malformed = original.clone();
        malformed[field] = "0".into();
        assert!(!c
            .request(target, &malformed.each_ref().map(String::as_str))
            .status
            .success());
    }
}
