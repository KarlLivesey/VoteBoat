// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
#[path = "aggregate/flow.rs"]
mod flow;
fn aggregate_limits() -> BatchLimits {
    BatchLimits {
        max_proposal_entries: 16384,
        ..wide()
    }
}
fn aggregate_log_limits() -> LogLimits {
    LogLimits {
        max_entries_per_group: 65536,
        max_batch_bytes: 4 * 1024 * 1024,
        ..LogLimits::default()
    }
}
fn empty(id: u128) -> Proposal {
    Proposal {
        operation: OperationId::new(id).unwrap(),
        bytes: Vec::new(),
    }
}
fn aggregate_replicas() -> BTreeMap<NodeId, Raft> {
    let mut cores = replicas();
    for core in cores.values_mut() {
        core.limits = aggregate_log_limits();
    }
    cores
        .get_mut(&node(1))
        .unwrap()
        .configure_batch_limits(aggregate_limits())
        .unwrap();
    cores
}
#[test]
fn aggregate_ceiling_preserves_default_locked_and_fenced_error_precedence() {
    assert_eq!(BatchLimits::default().max_proposal_entries, 64);
    let mut core = leader();
    core.configure_batch_limits(aggregate_limits()).unwrap();
    assert_eq!(core.batch_limits(), aggregate_limits());
    core.role = Role::Follower;
    assert_eq!(core.propose_batch(Vec::new()), Err(RaftError::NotLeader));
    let invalid = BatchLimits {
        max_proposal_entries: 16385,
        ..wide()
    };
    assert_eq!(core.configure_batch_limits(invalid), Err(RaftError::Busy));
    core.storage_failed();
    assert_eq!(core.configure_batch_limits(invalid), Err(RaftError::Fenced));
}
#[test]
fn aggregate_count_does_not_override_allocated_staging_retained_or_command_budgets() {
    for scenario in 0..6 {
        let mut core = leader();
        core.limits = aggregate_log_limits();
        core.configure_batch_limits(aggregate_limits()).unwrap();
        let mut input: Vec<_> = (1..=16384).map(empty).collect();
        let error: RaftError = match scenario {
            0 => {
                input.push(empty(16385));
                StorageError::Rejected("proposal count budget").into()
            }
            1 => {
                input = Vec::with_capacity(16385);
                input.push(empty(1));
                StorageError::Rejected("proposal allocation count budget").into()
            }
            2 => {
                core.limits.max_batch_bytes = LogLimits::default().max_batch_bytes;
                StorageError::Rejected("proposal batch owned staging budget").into()
            }
            3 => {
                // The retained-entry data budget is typed backpressure (ST2
                // round 3, I2), still checked in the same order.
                core.limits.max_entries_per_group = 16384;
                RaftError::StoragePressure
            }
            4 => {
                input.last_mut().unwrap().bytes = vec![0; core.limits.max_command_bytes + 1];
                StorageError::Rejected("command byte budget").into()
            }
            _ => {
                input.last_mut().unwrap().bytes = Vec::with_capacity(core.limits.max_batch_bytes);
                StorageError::Rejected("proposal batch owned staging budget").into()
            }
        };
        let before = core.state().clone();
        assert_eq!(core.propose_batch(input), Err(error));
        assert_eq!(core.state(), &before);
        assert!(!core.has_pending_dependency());
    }
}
#[test]
fn aggregate16384_originals_require_exact_durability_then_replicate_in1024_chunks() {
    let mut cores = aggregate_replicas();
    let source = cores.get_mut(&node(1)).unwrap();
    let effects = source
        .propose_batch((1..=16384).map(empty).collect())
        .unwrap();
    assert_eq!(effects.len(), 1);
    assert!(
        matches!(&effects[0], Effect::Persist(update) if update.suffix.as_ref().unwrap().entries.len() == 16384)
    );
    let exact = ticket(source);
    assert_eq!(
        source.complete(&DurableLog {
            tickets: vec![exact]
        }),
        Err(RaftError::WrongCompletion)
    );
    assert!(source.replicate_admitted(exact).unwrap().is_empty());
    assert_eq!(source.state().last_index(), 1);
    assert_eq!(source.state().commit_index, 1);
    let mut wrong = exact;
    wrong.binding.session = StoreSession::new(2).unwrap();
    assert_eq!(
        source.complete(&DurableLog {
            tickets: vec![wrong]
        }),
        Err(RaftError::WrongCompletion)
    );
    let effects = source
        .complete(&DurableLog {
            tickets: vec![exact],
        })
        .unwrap();
    assert_eq!(source.state().last_index(), 16385);
    assert_eq!(source.state().commit_index, 1);
    reject_oversized_append(&mut cores, &effects);
    let (applied, widths) = drive(&mut cores, effects);
    assert!(widths.contains(&1024));
    let originals: Vec<_> = (1..=16384).collect();
    for id in [1, 2, 3] {
        let core = &cores[&node(id)];
        assert_eq!(core.state().commit_index, 16385);
        assert_eq!(applied[&node(id)], originals);
        Raft::recover(node(id), core.binding, core.state().clone(), core.limits).unwrap();
    }
    let state = cores[&node(1)].state().clone();
    let mut restarted = Raft::recover(
        node(1),
        cores[&node(1)].binding,
        state,
        aggregate_log_limits(),
    )
    .unwrap();
    assert_eq!(restarted.batch_limits(), BatchLimits::default());
    restarted
        .configure_batch_limits(aggregate_limits())
        .unwrap();
    assert_eq!(restarted.replay_committed().len(), 16385);
    assert_eq!(
        restarted.complete(&DurableLog {
            tickets: vec![exact]
        }),
        Err(RaftError::WrongCompletion)
    );
}
