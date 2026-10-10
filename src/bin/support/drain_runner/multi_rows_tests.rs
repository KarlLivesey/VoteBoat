// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

#[test]
fn progress_binds_identity_active_phase_count_and_plan_digest() {
    let text = format!("OK sequence=1 operation=2 phase=Active multi=true membership_change=true groups=2 ready=false plan_digest={}", "ab".repeat(32));
    let original = Progress::parse(&text, 1, 2).unwrap();
    let ready = Progress::parse(&text.replace("ready=false", "ready=true"), 1, 2).unwrap();
    original.verify(&ready).unwrap();
    for invalid in [
        text.replace("sequence=1", "sequence=3"),
        text.replace("operation=2", "operation=3"),
        format!("{text} sequence=1"),
        text.replace("phase=Active", "phase=Cancelled"),
        text.replace("multi=true", "multi=false"),
        text.replace("membership_change=true", "membership_change=false"),
        text.replace("groups=2", "groups=0"),
        text.replace("groups=2", "groups=257"),
        text.replace("abab", "zzzz"),
        text.replace("ready=false", "ready=unknown"),
    ] {
        assert!(Progress::parse(&invalid, 1, 2).is_err(), "{invalid}");
    }
    for changed in [
        text.replace("groups=2", "groups=3"),
        text.replace("abab", "cdcd"),
    ] {
        assert!(original
            .verify(&Progress::parse(&changed, 1, 2).unwrap())
            .is_err());
    }
}
fn voter() -> &'static str {
    "OK sequence=1 operation=2 offset=0 groups=2 group=7 incarnation=3 configuration=9 done=false kind=voter target=1 store=1 store_incarnation=1 configuration_operation=7001"
}
#[test]
fn assignment_rows_reject_wrong_identity_bounds_missing_and_duplicate_fields() {
    let original = Row::parse(voter(), 1, 2, 0, 2).unwrap();
    for invalid in [
        voter().replace("operation=2", "operation=0"),
        voter().replace("offset=0", "offset=1"),
        voter().replace("groups=2", "groups=3"),
        voter().replace("group=7", "group=0"),
        voter().replace("incarnation=3", "incarnation=0"),
        voter().replace("configuration=9", "configuration=0"),
        voter().replace("kind=voter", "kind=unknown"),
        voter().replace("target=1", "target=0"),
        voter().replace("store=1", "store=0"),
        voter().replace("store_incarnation=1", "store_incarnation=0"),
        voter().replace("configuration_operation=7001", "configuration_operation=0"),
        voter().replace("done=false", "done=unknown"),
        format!("{} target=2", voter()),
        voter().replace(" store=1", ""),
    ] {
        assert!(Row::parse(&invalid, 1, 2, 0, 2).is_err(), "{invalid}");
    }
    let done = Row::parse(&voter().replace("done=false", "done=true"), 1, 2, 0, 2).unwrap();
    assert_eq!(original.assignment, done.assignment);
    for changed in [
        voter().replace("group=7", "group=8"),
        voter().replace("store=1", "store=2"),
    ] {
        assert_ne!(
            original.assignment,
            Row::parse(&changed, 1, 2, 0, 2).unwrap().assignment
        );
    }
    let retained = "OK sequence=1 operation=2 offset=0 groups=2 group=1 incarnation=1 configuration=3 done=true kind=retained";
    assert!(Row::parse(retained, 1, 2, 0, 2)
        .unwrap()
        .assignment
        .voter
        .is_none());
}
