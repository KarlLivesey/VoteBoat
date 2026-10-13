// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use std::cell::Cell;
use voteboat::raft::Effect;

struct Probe {
    binding: StoreBinding,
    result: Result<bool, StorageError>,
    comparisons: Cell<u32>,
    states: Cell<u32>,
}
impl LogStore for Probe {
    fn binding(&self) -> StoreBinding {
        self.binding
    }
    fn limits(&self) -> LogLimits {
        LogLimits::default()
    }
    fn state(&self, _: GroupIdentity) -> Result<GroupLog, StorageError> {
        self.states.set(self.states.get() + 1);
        Err(StorageError::Fenced)
    }
    fn matches_state(&self, _: GroupIdentity, _: &GroupLog) -> Result<bool, StorageError> {
        self.comparisons.set(self.comparisons.get() + 1);
        self.result.clone()
    }
    fn append_batch(&mut self, _: Vec<LogMutation>) -> Result<Vec<LogTicket>, StorageError> {
        panic!("rejected verification must not append")
    }
    fn barrier(&mut self, _: &[LogTicket]) -> Result<DurableLog, StorageError> {
        panic!("rejected verification must not publish")
    }
    fn fetch_range(
        &self,
        _: GroupIdentity,
        _: LogGeneration,
        _: u64,
        _: usize,
        _: usize,
    ) -> Result<Vec<LogEntry>, StorageError> {
        panic!("verification must not fetch")
    }
}
fn attempt(raft: &mut Raft, probe: &mut Probe) -> Result<Vec<Effect>, CheckpointError> {
    let mut snapshots = HostSnapshots::new();
    // A later failure makes successful comparison observable without pinning data.
    snapshots.identity.store = identity(99);
    let application = Counter::new(20).unwrap();
    let reference = SnapshotRef {
        store: identity(1),
        group: group(1),
        generation: SnapshotGeneration::new(1).unwrap(),
        configuration: ConfigurationId::new(1).unwrap(),
        index: 1,
        term: 1,
        application_schema: 1,
        file_bytes: 100,
        checksum: 7,
    };
    let result = compact_replica(raft, probe, &mut snapshots, &application, reference);
    assert!(snapshots.pins.is_empty());
    assert!(snapshots.current.is_none());
    assert_eq!(application.applied_index(), 0);
    result
}
#[test]
fn binding_precedes_comparison_and_comparison_errors_precede_snapshot_work() {
    for result in [
        Ok(true),
        Ok(false),
        Err(StorageError::Fenced),
        Err(StorageError::Rejected("missing")),
        Err(StorageError::Corrupt("comparison")),
    ] {
        let mut raft = committed_core();
        let before = raft.state().clone();
        let mut probe = Probe {
            binding: raft.storage_binding(),
            result: result.clone(),
            comparisons: Cell::new(0),
            states: Cell::new(0),
        };
        probe.binding.session = StoreSession::new(99).unwrap();
        assert_eq!(
            attempt(&mut raft, &mut probe),
            Err(CheckpointError::InvalidBinding)
        );
        assert_eq!(probe.comparisons.get(), 0);
        probe.binding = raft.storage_binding();
        let expected = match result {
            Err(error) => CheckpointError::Storage(error),
            Ok(_) => CheckpointError::InvalidBinding,
        };
        assert_eq!(attempt(&mut raft, &mut probe), Err(expected));
        assert_eq!(probe.comparisons.get(), 1);
        assert_eq!(probe.states.get(), 0);
        assert_eq!(*raft.state(), before);
    }
}
