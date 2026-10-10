// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use voteboat::{application::*, identity::OperationId, log::*};
#[path = "application/checks.rs"]
mod checks;
#[path = "application/host.rs"]
mod host;
#[path = "application/mutants.rs"]
mod mutants;

// These adapters describe selected application semantics, not a universal
// command/result schema. A downstream provider supplies its own observations.
trait Scenario {
    type Receipt: ApplicationReceipt;
    type App: StateMachine<Receipt = Self::Receipt>
        + CheckpointStateMachine
        + ReadableStateMachine
        + BoundedStateMachine;
    type Reply: std::fmt::Debug + Eq;
    type Value: std::fmt::Debug + Eq;
    fn fresh(capacity: usize) -> Self::App;
    fn requirements() -> Option<voteboat::raft::ReadinessRequirements>;
    fn command(index: u64, operation: u128, value: i64) -> LogEntry;
    fn reply(receipt: &<Self::App as StateMachine>::Receipt) -> Self::Reply;
    fn replies(replay: bool) -> Vec<Self::Reply>;
    fn query() -> <Self::App as ReadableStateMachine>::Query;
    fn value(result: <Self::App as ReadableStateMachine>::ReadResult) -> Self::Value;
    fn expected_value(replay: bool) -> Self::Value;
}
struct CounterCase;
impl Scenario for CounterCase {
    type Receipt = CounterReceipt;
    type App = Counter;
    type Reply = (CounterOutcome, bool);
    type Value = i64;
    fn fresh(capacity: usize) -> Counter {
        Counter::new(capacity).unwrap()
    }
    fn requirements() -> Option<voteboat::raft::ReadinessRequirements> {
        Some(Self::fresh(3).readiness_requirements())
    }
    fn command(index: u64, operation: u128, value: i64) -> LogEntry {
        LogEntry {
            index,
            term: 1,
            payload: EntryPayload::Command {
                operation: OperationId::new(operation).unwrap(),
                bytes: value.to_le_bytes().to_vec(),
            },
        }
    }
    fn reply(receipt: &CounterReceipt) -> Self::Reply {
        (receipt.outcome, receipt.duplicate)
    }
    fn replies(replay: bool) -> Vec<Self::Reply> {
        if replay {
            vec![
                (CounterOutcome::Value(7), true),
                (CounterOutcome::Value(18), true),
                (CounterOutcome::OperationConflict, true),
                (CounterOutcome::Value(23), false),
            ]
        } else {
            vec![
                (CounterOutcome::Value(7), false),
                (CounterOutcome::Value(18), false),
            ]
        }
    }
    fn query() {}
    fn value(result: i64) -> i64 {
        result
    }
    fn expected_value(replay: bool) -> i64 {
        if replay {
            23
        } else {
            18
        }
    }
}
#[test]
fn counter_satisfies_shared_application_read_and_checkpoint_obligations() {
    checks::exercise::<CounterCase>();
}
#[test]
fn independent_host_set_application_satisfies_same_public_obligations() {
    checks::exercise::<host::HostCase>();
}
#[test]
fn shared_application_checker_rejects_bad_receipts_partial_failure_and_future_reads() {
    mutants::reject::<1>("receipt identity");
    mutants::reject::<2>("failed batch changed applied boundary");
    mutants::reject::<3>("unapplied read boundary");
    mutants::reject::<4>("actual receipt capacity exceeds declared bound");
}
