// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::{scenarios::Profile, search, types::Weakening};

#[test]
fn new_only_child_activation_has_a_replayed_durable_witness_counterexample() {
    counterexample(Profile::Child, Weakening::NewOnly, "recoverable witnesses");
}
#[test]
fn new_only_parent_read_has_a_replayed_logical_witness_counterexample() {
    counterexample(Profile::Parent, Weakening::NewOnly, "new-only");
}
#[test]
fn overlapping_activation_without_parent_intent_has_a_replayed_counterexample() {
    counterexample(
        Profile::Overlap,
        Weakening::IgnoreIntent,
        "authoritative parent intent",
    );
}
fn counterexample(profile: Profile, weak: Weakening, expected: &str) {
    let (trace, error) = search::counterexample(profile, weak);
    println!(
        "{profile:?}/{weak:?}: steps={} error={error}; trace={trace:?}",
        trace.len()
    );
    assert!(error.contains(expected), "{error}");
    assert_eq!(search::replay(profile, weak, &trace), Err(error));
    let resumed = search::safe_progress(profile, &trace);
    println!("{profile:?}/{weak:?}: safe original schedule refuses violation, resumes in {resumed} actions");
    assert!(!trace.is_empty());
}
