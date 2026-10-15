// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Public-provider opt-in direction and the unchanged Counter reference.
use super::*;
use std::cell::Cell;

#[path = "prepared_admission/wrapper.rs"]
mod wrapper;

struct ReadyOnly {
    inner: Counter,
    factories: Cell<usize>,
}
struct General {
    inner: Counter,
    factories: Cell<usize>,
    enabled: bool,
}
macro_rules! application {
    ($name:ident) => {
        impl StateMachine for $name {
            type Receipt = CounterReceipt;
            fn applied_index(&self) -> u64 {
                self.inner.applied_index()
            }
            fn apply_batch(
                &mut self,
                entries: &[LogEntry],
            ) -> Result<Vec<CounterReceipt>, ApplicationError> {
                self.inner.apply_batch(entries)
            }
        }
        impl BoundedStateMachine for $name {
            fn receipt_bytes_bound(&self, entries: &[LogEntry]) -> Result<usize, ApplicationError> {
                self.inner.receipt_bytes_bound(entries)
            }
        }
    };
}
application!(ReadyOnly);
application!(General);
impl ProposalAdmission for ReadyOnly {
    fn supports_ready_batch(&self) -> bool {
        true
    }
    fn prepare_ready_admission<'pending>(
        &self,
        pending: impl Iterator<Item = (OperationId, &'pending [u8])>,
    ) -> Option<ReadyAdmission<'_>> {
        self.factories.set(self.factories.get() + 1);
        self.inner.prepare_ready_admission(pending)
    }
    fn validate_proposal<'pending>(
        &self,
        operation: OperationId,
        bytes: &[u8],
        pending: impl Iterator<Item = (OperationId, &'pending [u8])>,
    ) -> Result<usize, ApplicationError> {
        self.inner.validate_proposal(operation, bytes, pending)
    }
}
impl ProposalAdmission for General {
    fn prepare_proposal_admission<'pending>(
        &self,
        pending: impl Iterator<Item = (OperationId, &'pending [u8])>,
    ) -> Option<PreparedAdmission<'_>> {
        self.factories.set(self.factories.get() + 1);
        if !self.enabled {
            return None;
        }
        let mut prepared = self.inner.prepare_proposal_admission(pending)?;
        Some(Box::new(move |operation, bytes| {
            prepared(operation, bytes).map(|bound| bound + 17)
        }))
    }
    fn validate_proposal<'pending>(
        &self,
        operation: OperationId,
        bytes: &[u8],
        pending: impl Iterator<Item = (OperationId, &'pending [u8])>,
    ) -> Result<usize, ApplicationError> {
        self.inner
            .validate_proposal(operation, bytes, pending)
            .map(|bound| bound + 17)
    }
}
fn operation(id: u128) -> OperationId {
    OperationId::new(id).unwrap()
}

#[test]
fn ready_only_provider_does_not_opt_into_general_preparation_or_consume_pending() {
    let host = ReadyOnly {
        inner: Counter::new(2).unwrap(),
        factories: Cell::new(0),
    };
    let consumed = Cell::new(0);
    let pending = [(operation(1), &[0][..])].into_iter().inspect(|_| {
        consumed.set(consumed.get() + 1);
    });
    assert!(host.prepare_proposal_admission(pending).is_none());
    assert_eq!(consumed.get(), 0);
    assert_eq!(host.factories.get(), 0);
    assert!(host.supports_ready_batch());
    let mut ready: PreparedAdmission<'_> = host
        .prepare_ready_admission([(operation(1), &[0][..])].into_iter())
        .unwrap();
    let bound = std::mem::size_of::<CounterReceipt>();
    assert_eq!(ready(operation(2), &[0; 8]), Ok(bound));
    assert_eq!(ready(operation(1), &[7; 8]), Ok(bound));
    assert_eq!(
        ready(operation(3), &[0; 8]),
        Err(ApplicationError::DedupCapacity)
    );
    assert_eq!(host.factories.get(), 1);
}

#[test]
fn general_provider_forwards_ready_preparation_without_granting_ready_batching() {
    let host = General {
        inner: Counter::new(2).unwrap(),
        factories: Cell::new(0),
        enabled: true,
    };
    assert!(!host.supports_ready_batch());
    let pending = [(operation(1), vec![1])];
    let mut prepared: ReadyAdmission<'_> = host
        .prepare_ready_admission(pending.iter().map(|(id, bytes)| (*id, bytes.as_slice())))
        .unwrap();
    let mut accepted = Vec::new();
    for id in [1, 2, 1, 3] {
        let bytes = [id as u8; 8];
        let oracle = host.validate_proposal(
            operation(id),
            &bytes,
            pending
                .iter()
                .chain(accepted.iter())
                .map(|(id, bytes)| (*id, bytes.as_slice())),
        );
        assert_eq!(prepared(operation(id), &bytes), oracle);
        if oracle.is_err() {
            break;
        }
        assert_eq!(oracle, Ok(std::mem::size_of::<CounterReceipt>() + 17));
        accepted.push((operation(id), bytes.to_vec()));
    }
    assert_eq!(accepted.len(), 3);
    assert_eq!(host.factories.get(), 1);
    drop(prepared);
    assert_eq!(host.inner.remaining_operations(), 2);
}

#[test]
fn general_none_reaches_ready_fallback_without_consuming_pending() {
    let host = General {
        inner: Counter::new(1).unwrap(),
        factories: Cell::new(0),
        enabled: false,
    };
    let pending = std::iter::once_with(|| panic!("None must not consume pending"));
    assert!(host.prepare_ready_admission(pending).is_none());
    assert_eq!(host.factories.get(), 1);
    assert_eq!(
        host.validate_proposal(operation(1), &[0; 8], std::iter::empty()),
        Ok(std::mem::size_of::<CounterReceipt>() + 17)
    );
}

#[test]
fn counter_general_context_owns_pending_ids_and_keeps_syntax_before_capacity() {
    let mut counter = Counter::new(3).unwrap();
    counter.apply_batch(&[command(1, 1, 7)]).unwrap();
    let before = counter.checkpoint(4096).unwrap();
    let mut prepared = {
        let temporary = [(operation(1), vec![7; 8]), (operation(2), vec![0; 3])];
        counter
            .prepare_proposal_admission(temporary.iter().map(|(id, bytes)| (*id, bytes.as_slice())))
            .unwrap()
    };
    let bound = std::mem::size_of::<CounterReceipt>();
    assert_eq!(prepared(operation(1), &[9; 8]), Ok(bound));
    assert_eq!(prepared(operation(3), &[1; 8]), Ok(bound));
    assert_eq!(
        prepared(operation(4), &[0; 7]),
        Err(ApplicationError::InvalidCommand)
    );
    drop(prepared);
    assert_eq!(counter.checkpoint(4096).unwrap(), before);
    let mut prepared = counter
        .prepare_proposal_admission([(operation(2), &[][..])].into_iter())
        .unwrap();
    assert_eq!(prepared(operation(3), &[1; 8]), Ok(bound));
    assert_eq!(
        prepared(operation(4), &[0; 8]),
        Err(ApplicationError::DedupCapacity)
    );
    drop(prepared);
    assert_eq!(counter.checkpoint(4096).unwrap(), before);
}
