// SPDX-License-Identifier: RPL-1.5
//! Finite independent application-level isolation model; not a WAL/Raft proof.
mod program_transaction_model {
    pub mod oracle;
    pub mod protocol;
    pub mod search;
    pub mod types;
}
use program_transaction_model::{search, types::*};
#[test]
fn finite_three_participant_two_key_program_interleavings_preserve_atomicity_and_isolation() {
    let r = search::search(Mode::Strict);
    assert!(r.violation.is_none());
    assert!(r.terminals > 0);
    println!("program model strict states={} edges={} depth={} terminals={} caps(states=100000,edges=500000,depth=128) frontier=0",r.states,r.edges,r.depth,r.terminals);
}
#[test]
fn weakened_program_rules_generate_and_replay_original_oracle_violations() {
    for (mode, expected) in [
        (Mode::SkipVersion, Violation::ReadDependencyCycle),
        (Mode::PartialPrepare, Violation::PartialFootprint),
        (Mode::EarlyRelease, Violation::EarlyVisibility),
        (Mode::TimeoutAbort, Violation::SplitDecision),
    ] {
        let r = search::search(mode);
        assert_eq!(r.violation, Some(expected));
        let trace = r.trace.unwrap();
        assert_eq!(search::replay(mode, &trace), expected);
        println!("program model {mode:?} states={} edges={} depth={} original_trace={trace:?} violation={expected:?}",r.states,r.edges,r.depth);
    }
}
