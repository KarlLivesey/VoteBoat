// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use voteboat::contracts::StorageError;
struct Faulted {
    inner: HostLogStore,
    error: StorageError,
}
impl LogStore for Faulted {
    fn binding(&self) -> StoreBinding {
        self.inner.binding()
    }
    fn limits(&self) -> LogLimits {
        self.inner.limits()
    }
    fn state(&self, _: GroupIdentity) -> Result<GroupLog, StorageError> {
        Err(self.error.clone())
    }
    fn append_batch(
        &mut self,
        mutations: Vec<LogMutation>,
    ) -> Result<Vec<LogTicket>, StorageError> {
        self.inner.append_batch(mutations)
    }
    fn barrier(&mut self, tickets: &[LogTicket]) -> Result<DurableLog, StorageError> {
        self.inner.barrier(tickets)
    }
    fn fetch_range(
        &self,
        group: GroupIdentity,
        generation: LogGeneration,
        from: u64,
        count: usize,
        bytes: usize,
    ) -> Result<Vec<LogEntry>, StorageError> {
        self.inner
            .fetch_range(group, generation, from, count, bytes)
    }
}
#[test]
fn default_comparison_never_turns_provider_errors_into_a_mismatch() {
    let mut inner = HostLogStore::new(1);
    append(&mut inner, vec![LogMutation::Create(bootstrap(1, 3))]);
    let expected = inner.state(group(1)).unwrap();
    for error in [
        StorageError::Fenced,
        StorageError::WrongIdentity,
        StorageError::StaleTicket,
        StorageError::Rejected("missing"),
        StorageError::Corrupt("state"),
        StorageError::Uncertain("sync".into()),
    ] {
        let faulted = Faulted {
            inner: HostLogStore::new(1),
            error: error.clone(),
        };
        let object: &dyn LogStore = &faulted;
        for group in [group(1), group(99)] {
            assert_eq!(object.matches_state(group, &expected), Err(error.clone()));
        }
    }
}

#[test]
fn checkpoint_default_is_object_safe_and_preserves_raw_provider_error_outcomes() {
    let mut inner = HostLogStore::new(1);
    append(&mut inner, vec![LogMutation::Create(bootstrap(1, 3))]);
    let expected = inner.state(group(1)).unwrap();
    for group in [group(1), group(99)] {
        let mut state = expected.clone();
        state.bootstrap.group = group;
        let core =
            voteboat::raft::Raft::recover(node(1), inner.binding(), state, inner.limits()).unwrap();
        for error in [
            StorageError::Fenced,
            StorageError::WrongIdentity,
            StorageError::StaleTicket,
            StorageError::Rejected("missing"),
            StorageError::Corrupt("state"),
            StorageError::Uncertain("sync".into()),
        ] {
            let mut faulted = Faulted {
                inner: HostLogStore::new(1),
                error: error.clone(),
            };
            let object: &mut dyn LogStore = &mut faulted;
            assert_eq!(
                object.matches_checkpoint_state(core.checkpoint_log()),
                Err(error)
            );
        }
    }
}
