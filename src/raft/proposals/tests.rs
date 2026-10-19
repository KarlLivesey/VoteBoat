// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Real Core transition checks; fixture receipts model host durability only.
use super::*;
use crate::quorum::{Limits, Policy, Tree};

#[path = "tests/batch_limits.rs"]
mod batch_limit_tests;
mod compact;
mod entries;
mod incoming;

fn node(value: u64) -> NodeId {
    NodeId::new(value).unwrap()
}
fn proposal(value: u128) -> Proposal {
    Proposal {
        operation: OperationId::new(value).unwrap(),
        bytes: vec![value as u8],
    }
}
fn leader() -> Raft {
    leader_at(1)
}
fn leader_at(term: u64) -> Raft {
    let stores: BTreeMap<_, _> = (1..=3)
        .map(|n| {
            (
                node(n),
                StoreIdentity {
                    id: StoreId::new(u128::from(n)).unwrap(),
                    incarnation: StoreIncarnation::new(1).unwrap(),
                },
            )
        })
        .collect();
    let bootstrap = Bootstrap {
        group: GroupIdentity {
            id: GroupId::new(1).unwrap(),
            incarnation: GroupIncarnation::new(1).unwrap(),
        },
        configuration: ConfigurationId::new(1).unwrap(),
        policy: Policy::new(
            Tree::Majority(stores.keys().copied().map(Tree::Voter).collect()),
            Limits::default(),
        )
        .unwrap(),
        voter_stores: stores,
    };
    let binding = StoreBinding {
        identity: bootstrap.voter_stores[&node(1)],
        session: StoreSession::new(1).unwrap(),
    };
    let mut states = BTreeMap::new();
    apply_batch(
        &mut states,
        &[LogMutation::Create(bootstrap.clone())],
        LogLimits::default(),
    )
    .unwrap();
    let update = LogUpdate {
        group: bootstrap.group,
        expected_revision: states[&bootstrap.group].revision,
        hard_state: HardState {
            term,
            voted_for: None,
        },
        commit_index: 1,
        suffix: Some(Suffix {
            from: 1,
            entries: vec![LogEntry {
                index: 1,
                term: 1,
                payload: EntryPayload::Noop,
            }]
            .into(),
        }),
        snapshot: None,
        snapshot_membership: None,
    };
    apply_batch(
        &mut states,
        &[LogMutation::Update(update)],
        LogLimits::default(),
    )
    .unwrap();
    let mut core = Raft::recover(
        node(1),
        binding,
        states.remove(&bootstrap.group).unwrap(),
        LogLimits::default(),
    )
    .unwrap();
    core.role = Role::Leader;
    core.initialize_replication().unwrap();
    core
}
fn ticket(core: &Raft) -> LogTicket {
    let next = core.pending.as_ref().unwrap().next.metadata();
    LogTicket {
        binding: core.binding,
        batch: core.last_batch + 1,
        group: next.group,
        revision: next.revision,
        generation: next.generation,
        last_index: next.last_index,
        term: next.hard_state.term,
    }
}
fn rejected(core: &mut Raft, input: Vec<Proposal>) {
    let before = core.state().clone();
    let configuration = core.membership().clone();
    assert!(core.propose_batch(input).is_err());
    assert_eq!(core.state(), &before);
    assert_eq!(core.membership(), &configuration);
    assert!(core.pending.is_none());
}

fn payload_pointers(core: &Raft) -> Vec<*const u8> {
    core.state()
        .entries
        .iter()
        .filter_map(|entry| {
            if let EntryPayload::Command { bytes, .. } = &entry.payload {
                Some(bytes.as_ptr())
            } else {
                None
            }
        })
        .collect()
}

fn complete_exact(core: &mut Raft) {
    let exact = ticket(core);
    core.admitted(exact).unwrap();
    core.complete(&DurableLog {
        tickets: vec![exact],
    })
    .unwrap();
}

#[test]
fn plain_append_and_commit_preserve_historical_payloads_and_exact_accounting() {
    let mut core = leader();
    core.propose_batch((1..=64).map(proposal).collect())
        .unwrap();
    complete_exact(&mut core);
    let retained = payload_pointers(&core);
    let membership = core.membership() as *const Membership;
    let peers = core.connection_replicas(3).unwrap();
    let before = core.state().clone();
    core.propose_batch((65..=68).map(proposal).collect())
        .unwrap();
    assert!(core.pending.as_ref().unwrap().next.full().is_none());
    assert_eq!(core.state(), &before);
    assert_eq!(core.membership() as *const Membership, membership);
    assert_eq!(core.connection_replicas(3).unwrap(), peers);
    assert!(core.connection_replicas(2).is_err());
    complete_exact(&mut core);
    assert_eq!(&payload_pointers(&core)[..64], retained);
    core.persist(
        core.durable.hard_state,
        core.durable.last_index(),
        None,
        After::Commit,
        None,
    )
    .unwrap();
    assert!(core.pending.as_ref().unwrap().next.full().is_none());
    let all = payload_pointers(&core);
    complete_exact(&mut core);
    assert_eq!(payload_pointers(&core), all);
    assert_eq!(core.state().commit_index, 69);
    assert_eq!(core.membership() as *const Membership, membership);
    let old_charge = core.state().entries.len() * std::mem::size_of::<LogEntry>()
        + core
            .state()
            .entries
            .iter()
            .map(LogEntry::retained_payload_bytes)
            .sum::<usize>();
    assert_eq!(core.retained_log_bytes(), Some(old_charge));
    assert!(core.log_summary.matches(core.state()));
    assert_eq!(core.rollback_replicas(), 3);
}

#[test]
fn stale_summary_cannot_admit_an_ordinary_transition() {
    let mut core = leader();
    let before = core.state().clone();
    let mut other_revision = before.clone();
    other_revision.revision = LogRevision::new(other_revision.revision.get() + 1).unwrap();
    core.log_summary = LogSummary::from_validated(&other_revision);
    assert!(core.propose_batch(vec![proposal(1)]).is_err());
    assert_eq!(core.state(), &before);
    assert!(core.pending.is_none());
}

#[test]
fn four_original_commands_share_one_uncommitted_transition() {
    let mut core = leader();
    let membership = core.membership().clone();
    let proposals: Vec<_> = (1..=4).map(proposal).collect();
    let effects = core.propose_batch(proposals.clone()).unwrap();
    assert_eq!(effects.len(), 1);
    let Effect::Persist(update) = &effects[0] else {
        panic!("one persistence transition required");
    };
    assert_eq!(update.group, core.state().bootstrap.group);
    assert_eq!(update.hard_state, core.state().hard_state);
    assert_eq!(update.commit_index, 1);
    let entries = &update.suffix.as_ref().unwrap().entries;
    assert_eq!(entries.len(), 4);
    for (index, (entry, original)) in entries.iter().zip(proposals).enumerate() {
        assert_eq!(entry.index, index as u64 + 2);
        assert_eq!(
            entry.payload,
            EntryPayload::Command {
                operation: original.operation,
                bytes: original.bytes.into()
            }
        );
    }
    assert_eq!(core.state().last_index(), 1);
    assert_eq!(core.state().commit_index, 1);
    assert_eq!(core.membership(), &membership);
    assert_eq!(core.propose_batch(vec![proposal(9)]), Err(RaftError::Busy));
}

#[test]
fn written_and_stale_completions_cannot_publish_batch_or_quorum_evidence() {
    let mut core = leader();
    core.propose_batch((1..=4).map(proposal).collect()).unwrap();
    let exact = ticket(&core);
    assert_eq!(
        core.complete(&DurableLog {
            tickets: vec![exact]
        }),
        Err(RaftError::WrongCompletion)
    );
    let mut foreign = exact;
    foreign.binding.session = StoreSession::new(2).unwrap();
    assert_eq!(core.admitted(foreign), Err(RaftError::WrongCompletion));
    foreign = exact;
    foreign.generation = LogGeneration::new(exact.generation.get() + 1).unwrap();
    assert_eq!(core.admitted(foreign), Err(RaftError::WrongCompletion));
    core.admitted(exact).unwrap();
    assert_eq!(core.state().last_index(), 1);
    assert_eq!(core.progress[&node(1)], 1);
    assert_eq!(
        core.complete(&DurableLog {
            tickets: vec![foreign]
        }),
        Err(RaftError::WrongCompletion)
    );
    let effects = core
        .complete(&DurableLog {
            tickets: vec![exact],
        })
        .unwrap();
    assert_eq!(core.state().last_index(), 5);
    assert_eq!(core.state().commit_index, 1);
    assert_eq!(core.progress[&node(1)], 5);
    assert!(effects
        .iter()
        .all(|effect| matches!(effect, Effect::Send(_))));
    assert_eq!(effects.len(), 2);
    assert_eq!(core.progress[&node(2)], 0);
    for effect in effects {
        let Effect::Send(message) = effect else {
            unreachable!()
        };
        assert_eq!(message.group, core.state().bootstrap.group);
        assert_eq!(message.configuration, core.membership().id());
        assert_eq!(message.context.origin, core.binding);
        assert_eq!(message.term, core.state().hard_state.term);
        let Rpc::Append {
            previous_index,
            entries,
            leader_commit,
            ..
        } = message.rpc
        else {
            panic!("normal append required")
        };
        assert_eq!(previous_index, 1);
        assert_eq!(entries.len(), 4);
        assert_eq!(leader_commit, 1);
    }
}

#[test]
fn invalid_member_shape_and_aggregate_budgets_leave_no_partial_append() {
    let mut core = leader();
    rejected(&mut core, Vec::new());
    rejected(&mut core, (1..=65).map(proposal).collect());
    let mut overallocated = Vec::with_capacity(65);
    overallocated.push(proposal(1));
    rejected(&mut core, overallocated);
    let mut large = proposal(4);
    large.bytes = vec![0; core.limits.max_command_bytes + 1];
    rejected(
        &mut core,
        vec![proposal(1), proposal(2), proposal(3), large],
    );
    let max_command = core.limits.max_command_bytes;
    rejected(
        &mut core,
        (1..=20)
            .map(|id| Proposal {
                operation: OperationId::new(id).unwrap(),
                bytes: vec![0; max_command],
            })
            .collect(),
    );
    let mut capacity = proposal(5);
    capacity.bytes = Vec::with_capacity(core.limits.max_batch_bytes + 1);
    rejected(&mut core, vec![capacity]);
    core.limits.max_entries_per_group = 4;
    rejected(&mut core, (1..=4).map(proposal).collect());
}

#[test]
fn singleton_and_duplicate_ids_preserve_original_commands() {
    let mut single = leader();
    let mut batch = leader();
    let original = proposal(7);
    let expected = single
        .step(Event::Propose {
            operation: original.operation,
            bytes: original.bytes.clone(),
        })
        .unwrap();
    assert_eq!(
        batch.propose_batch(vec![original.clone()]).unwrap(),
        expected
    );
    let mut duplicate = leader();
    let effects = duplicate
        .propose_batch(vec![original.clone(), original])
        .unwrap();
    let Effect::Persist(update) = &effects[0] else {
        panic!("expected persistence");
    };
    let entries = &update.suffix.as_ref().unwrap().entries;
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].payload, entries[1].payload);
    assert_ne!(entries[0].index, entries[1].index);
}

#[test]
fn legacy_singletons_keep_custom_small_limits_and_spare_payload_capacity() {
    for spare_capacity in [false, true] {
        let mut single = leader();
        single.limits.max_batch_bytes = 256;
        single.limits.max_command_bytes = 256;
        single.limits.validate().unwrap();
        let bytes = if spare_capacity {
            let mut bytes = Vec::with_capacity(4096);
            bytes.push(7);
            bytes
        } else {
            vec![7; 256]
        };
        let original_capacity = bytes.capacity();
        let original_len = bytes.len();
        let result = single
            .step(Event::Propose {
                operation: OperationId::new(7).unwrap(),
                bytes,
            })
            .unwrap();
        let Effect::Persist(update) = &result[0] else {
            panic!("expected original persistence path");
        };
        let EntryPayload::Command { bytes, .. } =
            &update.suffix.as_ref().unwrap().entries[0].payload
        else {
            panic!("expected original command");
        };
        assert_eq!(bytes.len(), original_len);
        assert_eq!(
            bytes.retained_bytes(),
            (original_len + 2 * std::mem::size_of::<std::sync::atomic::AtomicUsize>())
                .next_multiple_of(std::mem::align_of::<std::sync::atomic::AtomicUsize>().max(2))
        );
        assert!(original_capacity >= bytes.len());
    }
    let mut batched = leader();
    batched.limits.max_batch_bytes = 256;
    batched.limits.max_command_bytes = 256;
    let bytes = vec![7; 256];
    rejected(
        &mut batched,
        vec![Proposal {
            operation: OperationId::new(7).unwrap(),
            bytes,
        }],
    );
}

#[test]
fn direct_batch_uses_the_same_fence_dependency_and_transfer_guards() {
    let mut core = leader();
    core.role = Role::Follower;
    assert_eq!(
        core.propose_batch(vec![proposal(1)]),
        Err(RaftError::NotLeader)
    );
    core.role = Role::Leader;
    core.checkpoint_requested = Some(RequestContext {
        origin: core.binding,
        sequence: 1,
    });
    assert_eq!(core.propose_batch(vec![proposal(1)]), Err(RaftError::Busy));
    core.checkpoint_requested = None;
    let target = PeerIdentity {
        node: node(2),
        store: core.membership().voter_store(node(2)).unwrap(),
    };
    core.step(Event::TransferLeadership(LeadershipTransferRequest {
        operation: OperationId::new(20).unwrap(),
        target,
        configuration: core.membership().id(),
    }))
    .unwrap();
    assert_eq!(core.propose_batch(vec![proposal(1)]), Err(RaftError::Busy));
    core.storage_failed();
    assert_eq!(
        core.propose_batch(vec![proposal(1)]),
        Err(RaftError::Fenced)
    );
}

#[test]
fn index_exhaustion_is_rejected_before_pending_state() {
    let mut core = leader();
    core.durable
        .entries
        .edit(|entries| entries.last_mut().unwrap().index = u64::MAX - 1);
    assert_eq!(
        core.propose_batch(vec![proposal(1), proposal(2)]),
        Err(RaftError::Exhausted)
    );
    assert!(core.pending.is_none());
}

#[test]
fn each_existing_checkpoint_install_and_readiness_dependency_stays_closed() {
    for dependency in 0..5 {
        let mut core = leader();
        let context = RequestContext {
            origin: core.binding,
            sequence: 1,
        };
        let message = core.message(node(2), context, Rpc::ReadProbe);
        let reference = SnapshotRef {
            store: core.binding.identity,
            group: core.state().bootstrap.group,
            generation: SnapshotGeneration::new(1).unwrap(),
            configuration: core.membership().id(),
            index: 1,
            term: 1,
            application_schema: 1,
            file_bytes: 64,
            checksum: 0,
        };
        match dependency {
            0 => core.application_install = Some((reference, message)),
            1 => core.checkpoint_reconcile = Some(reference),
            2 => core.checkpoint_requested = Some(context),
            3 => core.staged_snapshot = Some(message),
            _ => core.readiness_check = Some(message),
        }
        assert_eq!(core.propose_batch(vec![proposal(1)]), Err(RaftError::Busy));
        assert!(core.pending.is_none());
    }
}

#[test]
fn shared_conversion_peak_refuses_after_raw_checks_without_pending_or_log_changes() {
    let mut core = leader();
    let mut proposed = proposal(100);
    proposed.bytes = vec![7; 130];
    let input = vec![proposed];
    let raw = size_of::<Vec<Proposal>>()
        + input.capacity() * size_of::<Proposal>()
        + input.len() * size_of::<LogEntry>()
        + input.iter().map(|p| p.bytes.capacity()).sum::<usize>();
    core.limits.max_batch_bytes = raw.max(256);
    core.limits.max_command_bytes = 130;
    core.limits.validate().unwrap();
    assert!(core
        .validate_proposal_batch(&input, input.capacity())
        .is_ok());
    let before = core.state().clone();
    assert_eq!(
        core.propose_batch(input),
        Err(StorageError::Rejected("proposal shared conversion peak budget").into())
    );
    assert!(core.pending.is_none());
    assert_eq!(core.state(), &before);
}
