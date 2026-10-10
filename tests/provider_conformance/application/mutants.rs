// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
#[derive(Clone)]
struct Broken<const MODE: u8>(Counter);
impl<const MODE: u8> StateMachine for Broken<MODE> {
    type Receipt = CounterReceipt;
    fn deployment_requirements(&self) -> Option<voteboat::raft::ReadinessRequirements> {
        self.0.deployment_requirements()
    }
    fn applied_index(&self) -> u64 {
        self.0.applied_index()
    }
    fn apply_batch(
        &mut self,
        entries: &[LogEntry],
    ) -> Result<Vec<CounterReceipt>, ApplicationError> {
        let result = self.0.apply_batch(entries);
        if MODE == 2 && result.is_err() && !entries.is_empty() {
            let _ = self.0.apply_batch(&entries[..1]);
        }
        result.map(|mut receipts| {
            if MODE == 1 {
                for receipt in &mut receipts {
                    receipt.index += 1;
                }
            }
            receipts
        })
    }
}
impl<const MODE: u8> BoundedStateMachine for Broken<MODE> {
    fn receipt_bytes_bound(&self, entries: &[LogEntry]) -> Result<usize, ApplicationError> {
        if MODE == 4 {
            Ok(0)
        } else {
            self.0.receipt_bytes_bound(entries)
        }
    }
}
impl<const MODE: u8> ReadableStateMachine for Broken<MODE> {
    type Query = ();
    type ReadResult = i64;
    fn read_at(&self, index: u64, (): ()) -> Result<i64, ApplicationError> {
        self.0.read_at(
            if MODE == 3 {
                index.min(self.applied_index())
            } else {
                index
            },
            (),
        )
    }
}
impl<const MODE: u8> CheckpointStateMachine for Broken<MODE> {
    fn schema_version(&self) -> u64 {
        self.0.schema_version()
    }
    fn checkpoint(&self, max_bytes: usize) -> Result<Vec<u8>, ApplicationError> {
        self.0.checkpoint(max_bytes)
    }
    fn restore_checkpoint(
        &mut self,
        schema: u64,
        index: u64,
        bytes: &[u8],
    ) -> Result<(), ApplicationError> {
        self.0.restore_checkpoint(schema, index, bytes)
    }
}
struct BrokenCase<const MODE: u8>;
impl<const MODE: u8> Scenario for BrokenCase<MODE> {
    type Receipt = CounterReceipt;
    type App = Broken<MODE>;
    type Reply = <CounterCase as Scenario>::Reply;
    type Value = i64;
    fn fresh(capacity: usize) -> Self::App {
        Broken(CounterCase::fresh(capacity))
    }
    fn requirements() -> Option<voteboat::raft::ReadinessRequirements> {
        CounterCase::requirements()
    }
    fn command(index: u64, operation: u128, value: i64) -> LogEntry {
        CounterCase::command(index, operation, value)
    }
    fn reply(receipt: &CounterReceipt) -> Self::Reply {
        CounterCase::reply(receipt)
    }
    fn replies(replay: bool) -> Vec<Self::Reply> {
        CounterCase::replies(replay)
    }
    fn query() {}
    fn value(result: i64) -> i64 {
        result
    }
    fn expected_value(replay: bool) -> i64 {
        CounterCase::expected_value(replay)
    }
}
pub(super) fn reject<const MODE: u8>(expected: &str) {
    let failure = std::panic::catch_unwind(checks::exercise::<BrokenCase<MODE>>).unwrap_err();
    let message = failure
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| failure.downcast_ref::<&str>().copied())
        .unwrap_or("non-string panic");
    assert!(
        message.contains(expected),
        "unexpected checker failure: {message}"
    );
}
