// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Atomic refusal against an actual consumed native child-quorum image.
use voteboat::{
    application::*, log::LogEntry, logical_voter::VerifiedLogicalImage, snapshot::CheckpointError,
};
#[derive(Clone)]
struct RefusingApp {
    counter: Counter,
    schema: u64,
    fail_restore: bool,
}
impl StateMachine for RefusingApp {
    type Receipt = CounterReceipt;
    fn applied_index(&self) -> u64 {
        self.counter.applied_index()
    }
    fn apply_batch(
        &mut self,
        entries: &[LogEntry],
    ) -> Result<Vec<CounterReceipt>, ApplicationError> {
        self.counter.apply_batch(entries)
    }
}
impl CheckpointStateMachine for RefusingApp {
    fn schema_version(&self) -> u64 {
        self.schema
    }
    fn checkpoint(&self, max: usize) -> Result<Vec<u8>, ApplicationError> {
        self.counter.checkpoint(max)
    }
    fn restore_checkpoint(
        &mut self,
        schema: u64,
        index: u64,
        bytes: &[u8],
    ) -> Result<(), ApplicationError> {
        self.counter.restore_checkpoint(schema, index, bytes)?;
        if self.fail_restore {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        Ok(())
    }
}
pub fn refusal_is_atomic(image: &VerifiedLogicalImage, restored: &super::application::App) {
    let mut applied = restored.clone();
    let before = applied.checkpoint(4096).unwrap();
    assert!(matches!(
        image.recover_parent(&mut applied),
        Err(CheckpointError::InvalidBoundary)
    ));
    assert_eq!(applied.checkpoint(4096).unwrap(), before);
    for (schema, fail_restore, expected) in [
        (COUNTER_SCHEMA + 1, false, CheckpointError::InvalidBoundary),
        (
            COUNTER_SCHEMA,
            true,
            CheckpointError::Application(ApplicationError::InvalidCheckpoint),
        ),
    ] {
        let mut fresh = RefusingApp {
            counter: Counter::new(16).unwrap(),
            schema,
            fail_restore,
        };
        let before = fresh.checkpoint(4096).unwrap();
        let result = image.recover_parent(&mut fresh);
        match result {
            Err(error) => assert_eq!(error, expected),
            Ok(_) => panic!("invalid recovery accepted"),
        }
        assert_eq!(fresh.applied_index(), 0);
        assert_eq!(fresh.counter.read_applied(0).unwrap(), 0);
        assert_eq!(fresh.checkpoint(4096).unwrap(), before);
    }
}
