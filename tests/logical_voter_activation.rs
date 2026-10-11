// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Independent finite C/D/E logical-voter activation profiles. These assume
//! qualified ordinary child/parent Raft commitment preservation, not all RPCs.
//! Serial admitted mutation/catch-up cuts bound the graph; I/O, commit, apply,
//! observation/loss and physical restart remain distinct within every cut.
//! Recovery rebuilds from selected own checkpoint/suffix with surviving child
//! commit authority. No simultaneous all-child crash or arbitrary overlap proof.
#[path = "logical_voter_activation/actions.rs"]
mod actions;
#[path = "logical_voter_activation/controls.rs"]
mod controls;
#[path = "logical_voter_activation/journal.rs"]
mod journal;
#[path = "logical_voter_activation/oracle.rs"]
mod oracle;
#[path = "logical_voter_activation/protocol.rs"]
mod protocol;
#[path = "logical_voter_activation/scenarios.rs"]
mod scenarios;
#[path = "logical_voter_activation/search.rs"]
mod search;
#[path = "logical_voter_activation/types.rs"]
mod types;

#[test]
fn child_activation_preserves_parent_promises_across_rollback_and_replacement() {
    let report = qualify(scenarios::Profile::Child);
    assert!(report.rollback_eligibility > 0);
}
#[test]
fn distinct_parent_cell_requires_payload_and_recovers_checkpoint_plus_suffix() {
    let report = qualify(scenarios::Profile::Parent);
    assert!(report.readiness_refusals > 0);
}
#[test]
fn replicated_parent_intents_serialize_both_orders_across_loss_and_restart() {
    let report = qualify(scenarios::Profile::Overlap);
    assert!(report.child_first > 0 && report.parent_first > 0);
    assert!(report.overlap_refusals > 0);
}
fn qualify(profile: scenarios::Profile) -> search::Report {
    let report = search::explore(profile);
    println!("{profile:?}: {report:?}; baseline=qualified ordinary Raft; depth_cap=1024 state_cap=500000 edge_cap=5000000");
    assert!(report.terminals > 0 && report.states > 100 && report.edges > report.states);
    assert!(report.losses > 0 && report.restarts > 0 && report.observed > 0);
    assert!(
        report.exhausted,
        "selected activation graph truncated: {report:?}"
    );
    report
}
