// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Actual core transitions with modeled provider tokens, not disk durability.
use super::*;
use crate::membership::{Configuration, ConfigurationChange, ConfigurationRecord};
use crate::quorum::{Limits, Policy, Tree};
use membership_tests::{core, durable};

fn command(index: u64, operation: u128, bytes: Vec<u8>) -> LogEntry {
    LogEntry {
        index,
        term: 1,
        payload: EntryPayload::Command {
            operation: OperationId::new(operation).unwrap(),
            bytes: bytes.into(),
        },
    }
}

fn reply(core: &Raft, rpc: Rpc) -> Message {
    core.message(
        NodeId::new(2).unwrap(),
        RequestContext {
            origin: core.binding,
            sequence: 7,
        },
        rpc,
    )
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

fn admit(core: &mut Raft) -> DurableLog {
    let exact = ticket(core);
    core.admitted(exact).unwrap();
    DurableLog {
        tickets: vec![exact],
    }
}

fn pending_entries(core: &mut Raft, entries: Vec<LogEntry>, after: After) -> LogUpdate {
    let commit = entries.last().unwrap().index;
    let response = reply(
        core,
        Rpc::Appended {
            success: true,
            matching_index: commit,
        },
    );
    let effects = core
        .persist(
            core.state().hard_state,
            commit,
            Some(Suffix {
                from: entries[0].index,
                entries: entries.into(),
            }),
            after,
            Some(response),
        )
        .unwrap();
    let [effect]: [Effect; 1] = effects.try_into().unwrap();
    let Effect::Persist(update) = effect else {
        panic!("one pending transition")
    };
    update
}

#[test]
fn borrowed_and_owned_completion_preserve_full_entries_configuration_and_reply_order() {
    let mut owned = core();
    let mut borrowed = core();
    owned.role = Role::Follower;
    borrowed.role = Role::Follower;
    let stable = owned.membership().stable();
    let config = Configuration::new(
        ConfigurationId::new(2).unwrap(),
        stable.policy().clone(),
        stable.voter_stores().clone(),
        BTreeMap::new(),
    )
    .unwrap();
    let entries = vec![
        command(2, u128::MAX, Vec::with_capacity(19)),
        command(3, (1 << 127) | 7, vec![0, 255, 9]),
        LogEntry {
            index: 4,
            term: 1,
            payload: EntryPayload::Noop,
        },
        LogEntry {
            index: 5,
            term: 1,
            payload: EntryPayload::Configuration(Box::new(ConfigurationRecord {
                operation: OperationId::new(8).unwrap(),
                expected: owned.membership().id(),
                change: ConfigurationChange::Learners(config),
            })),
        },
    ];
    pending_entries(&mut owned, entries.clone(), After::Reply);
    pending_entries(&mut borrowed, entries.clone(), After::Reply);
    let exact = admit(&mut owned);
    let reference = owned.complete(&exact).unwrap();
    let exact = admit(&mut borrowed);
    let result = borrowed.complete_borrowed(&exact).unwrap();
    assert_eq!(result.committed(), Some(entries.as_slice()));
    assert!(matches!(result.following(), [Effect::Send(_)]));
    assert_eq!(result.into_owned(), reference);
    assert_eq!(borrowed.state(), owned.state());
    assert_eq!(borrowed.membership(), owned.membership());
    assert!(matches!(
        &reference[..],
        [Effect::Committed(_), Effect::Send(_)]
    ));
}

#[test]
fn borrowed_commit_only_interval_is_canonical_and_owned_materialization_is_independent() {
    let mut core = core();
    core.role = Role::Follower;
    let entries = vec![
        command(2, 9, vec![1, 2, 3]),
        command(3, u128::MAX, Vec::new()),
    ];
    let effects = core
        .persist(
            core.state().hard_state,
            1,
            Some(Suffix {
                from: 2,
                entries: entries.into(),
            }),
            After::Reply,
            None,
        )
        .unwrap();
    assert!(durable(&mut core, effects).is_empty());
    let canonical = core
        .state()
        .entries
        .view(1..)
        .as_contiguous()
        .unwrap()
        .as_ptr();
    let EntryPayload::Command { bytes, .. } = &core.state().entries[1].payload else {
        unreachable!()
    };
    let canonical_bytes = bytes.as_ptr();
    core.persist(core.state().hard_state, 3, None, After::Commit, None)
        .unwrap();
    let exact = admit(&mut core);
    let result = core.complete_borrowed(&exact).unwrap();
    let committed = result.committed().unwrap();
    assert_eq!(committed.as_ptr(), canonical);
    let EntryPayload::Command { bytes, .. } = &committed[0].payload else {
        unreachable!()
    };
    assert_eq!(bytes.as_ptr(), canonical_bytes);
    let [effect]: [Effect; 1] = result.into_owned().try_into().unwrap();
    let Effect::Committed(mut materialized) = effect else {
        panic!("committed interval only")
    };
    assert_ne!(materialized.as_ptr(), canonical);
    let EntryPayload::Command { bytes, .. } = &mut materialized[0].payload else {
        unreachable!()
    };
    assert_eq!(bytes.as_ptr(), canonical_bytes);
    let mut replacement = bytes.to_vec();
    replacement[0] = 255;
    *bytes = replacement.into();
    assert_ne!(materialized, core.state().entries.view(1..).to_vec());
    core.persist(core.state().hard_state, 3, None, After::Reply, None)
        .unwrap();
    let exact = admit(&mut core);
    let result = core.complete_borrowed(&exact).unwrap();
    assert!(result.committed().is_none());
    assert!(result.into_owned().is_empty());
}

#[test]
fn borrowed_wrong_delayed_and_stale_tickets_preserve_pending_and_fence_order() {
    let mut core = core();
    core.role = Role::Follower;
    let before = core.state().clone();
    pending_entries(
        &mut core,
        vec![command(2, u128::MAX, vec![7])],
        After::Reply,
    );
    let exact = ticket(&core);
    assert!(matches!(
        core.complete_borrowed(&DurableLog {
            tickets: vec![exact]
        }),
        Err(RaftError::WrongCompletion)
    ));
    let mut wrong = exact;
    wrong.binding.session = StoreSession::new(2).unwrap();
    assert_eq!(core.admitted(wrong), Err(RaftError::WrongCompletion));
    core.admitted(exact).unwrap();
    assert!(matches!(
        core.complete_borrowed(&DurableLog {
            tickets: vec![wrong]
        }),
        Err(RaftError::WrongCompletion)
    ));
    assert_eq!(core.state(), &before);
    assert!(core.has_pending_dependency());
    core.storage_failed();
    assert!(matches!(
        core.complete_borrowed(&DurableLog {
            tickets: vec![exact]
        }),
        Err(RaftError::Fenced)
    ));
    let mut reopened = Raft::recover(core.node, wrong.binding, before, core.limits).unwrap();
    assert!(matches!(
        reopened.complete_borrowed(&DurableLog {
            tickets: vec![exact]
        }),
        Err(RaftError::WrongCompletion)
    ));
}

#[test]
fn late_after_durable_failure_exposes_no_borrow_or_owned_output() {
    let mut owned = core();
    let mut borrowed = core();
    for candidate in [&mut owned, &mut borrowed] {
        pending_entries(candidate, vec![command(2, 7, vec![9])], After::Commit);
        candidate.request_sequence = u64::MAX;
    }
    let exact = admit(&mut owned);
    assert_eq!(owned.complete(&exact), Err(RaftError::Exhausted));
    let exact = admit(&mut borrowed);
    assert!(matches!(
        borrowed.complete_borrowed(&exact),
        Err(RaftError::Exhausted)
    ));
    assert_eq!(borrowed.state(), owned.state());
    assert_eq!(borrowed.state().commit_index, 2);
    assert!(!borrowed.has_pending_dependency());
    assert!(!borrowed.is_fenced());
    assert_eq!(borrowed.request_sequence, u64::MAX);
}

#[test]
fn late_vote_reset_failure_exposes_no_output_and_preserves_original_mutations() {
    let mut core = core();
    core.role = Role::Follower;
    let response = reply(&core, Rpc::Voted { granted: true });
    core.persist(
        core.state().hard_state,
        1,
        None,
        After::Reply,
        Some(response),
    )
    .unwrap();
    core.election_reset = u64::MAX;
    let exact = admit(&mut core);
    assert!(matches!(
        core.complete_borrowed(&exact),
        Err(RaftError::Exhausted)
    ));
    assert!(core.granted_vote_reply.is_some());
    assert_eq!(core.election_reset_sequence(), u64::MAX);
    assert!(!core.has_pending_dependency());
    assert!(!core.is_fenced());
}

#[test]
fn snapshot_install_completion_suppresses_committed_output_until_application_install() {
    let mut owned = core();
    let mut borrowed = core();
    let reference = SnapshotRef {
        store: owned.binding.identity,
        group: owned.state().bootstrap.group,
        configuration: owned.membership().id(),
        generation: SnapshotGeneration::new(1).unwrap(),
        index: 4,
        term: 1,
        application_schema: 1,
        file_bytes: 128,
        checksum: 7,
    };
    for candidate in [&mut owned, &mut borrowed] {
        candidate.role = Role::Follower;
        let response = reply(candidate, Rpc::SnapshotAck { index: 4 });
        candidate
            .persist_update(
                LogUpdate {
                    snapshot_membership: None,
                    group: candidate.state().bootstrap.group,
                    expected_revision: candidate.state().revision,
                    hard_state: candidate.state().hard_state,
                    commit_index: 4,
                    suffix: None,
                    snapshot: Some(reference),
                },
                After::SnapshotInstall(reference),
                Some(response),
            )
            .unwrap();
    }
    let exact = admit(&mut owned);
    let expected = owned.complete(&exact).unwrap();
    let exact = admit(&mut borrowed);
    let result = borrowed.complete_borrowed(&exact).unwrap();
    assert!(result.committed().is_none());
    assert_eq!(result.into_owned(), expected);
    assert_eq!(expected, vec![Effect::SnapshotInstalled(reference)]);
    assert_eq!(borrowed.state(), owned.state());
    assert!(borrowed.has_pending_dependency());
    assert_eq!(
        borrowed.snapshot_applied(reference, 3),
        Err(RaftError::WrongCompletion)
    );
    assert!(matches!(
        &borrowed.snapshot_applied(reference, 4).unwrap()[..],
        [Effect::Send(_)]
    ));
}

fn configuration(id: u64, voters: &[u64], learners: &[u64]) -> Configuration {
    let replica = |n| {
        (
            NodeId::new(n).unwrap(),
            StoreIdentity {
                id: StoreId::new(n as u128).unwrap(),
                incarnation: StoreIncarnation::new(1).unwrap(),
            },
        )
    };
    Configuration::new(
        ConfigurationId::new(id).unwrap(),
        Policy::new(
            Tree::Majority(voters.iter().map(|n| Tree::Voter(replica(*n).0)).collect()),
            Limits::default(),
        )
        .unwrap(),
        voters.iter().map(|n| replica(*n)).collect(),
        learners.iter().map(|n| replica(*n)).collect(),
    )
    .unwrap()
}

fn retiring() -> Raft {
    let mut core = core();
    for (index, change) in [
        (
            2,
            ConfigurationChange::Joint {
                id: ConfigurationId::new(2).unwrap(),
                next: configuration(3, &[2, 3], &[1]),
            },
        ),
        (
            3,
            ConfigurationChange::Final {
                id: ConfigurationId::new(3).unwrap(),
            },
        ),
    ] {
        let entries = vec![LogEntry {
            index,
            term: 1,
            payload: EntryPayload::Configuration(Box::new(ConfigurationRecord {
                operation: OperationId::new(u128::MAX).unwrap(),
                expected: core.membership().id(),
                change,
            })),
        }];
        let effects = core
            .persist(
                core.state().hard_state,
                2.min(index),
                Some(Suffix {
                    from: index,
                    entries: entries.into(),
                }),
                After::Reply,
                None,
            )
            .unwrap();
        durable(&mut core, effects);
    }
    core.persist(core.state().hard_state, 3, None, After::Commit, None)
        .unwrap();
    core
}

#[test]
fn borrowed_retirement_keeps_committed_first_and_all_announcements_afterward() {
    let mut owned = retiring();
    let mut borrowed = retiring();
    let exact = admit(&mut owned);
    let expected = owned.complete(&exact).unwrap();
    let exact = admit(&mut borrowed);
    let result = borrowed.complete_borrowed(&exact).unwrap();
    assert_eq!(result.committed().unwrap()[0].index, 3);
    assert!(matches!(
        result.following(),
        [Effect::Send(_), Effect::Send(_)]
    ));
    assert_eq!(result.into_owned(), expected);
    assert_eq!(borrowed.role(), Role::Follower);
    assert_eq!(borrowed.state(), owned.state());
    assert!(borrowed.requests.is_empty());
    assert!(borrowed.progress.is_empty());
}

struct ModeledStore {
    binding: StoreBinding,
    states: BTreeMap<GroupIdentity, GroupLog>,
    batch: u64,
    fail_barrier: bool,
    submissions: usize,
}
impl ModeledStore {
    fn new(core: &Raft) -> Self {
        Self {
            binding: core.binding,
            states: [(core.state().bootstrap.group, core.state().clone())]
                .into_iter()
                .collect(),
            batch: core.last_batch,
            fail_barrier: false,
            submissions: 0,
        }
    }
}
impl LogStore for ModeledStore {
    fn binding(&self) -> StoreBinding {
        self.binding
    }
    fn limits(&self) -> LogLimits {
        LogLimits::default()
    }
    fn state(&self, group: GroupIdentity) -> Result<GroupLog, StorageError> {
        self.states
            .get(&group)
            .cloned()
            .ok_or(StorageError::Rejected("missing modeled group"))
    }
    fn append_batch(
        &mut self,
        mutations: Vec<LogMutation>,
    ) -> Result<Vec<LogTicket>, StorageError> {
        self.submissions += 1;
        let limits = self.limits();
        apply_batch(&mut self.states, &mutations, limits)?;
        self.batch += 1;
        Ok(mutations
            .iter()
            .map(|mutation| {
                let LogMutation::Update(update) = mutation else {
                    panic!("existing modeled group")
                };
                let state = &self.states[&update.group];
                LogTicket {
                    binding: self.binding,
                    batch: self.batch,
                    group: update.group,
                    revision: state.revision,
                    generation: state.generation,
                    last_index: state.last_index(),
                    term: state.hard_state.term,
                }
            })
            .collect())
    }
    fn barrier(&mut self, dependencies: &[LogTicket]) -> Result<DurableLog, StorageError> {
        if self.fail_barrier {
            return Err(StorageError::Uncertain("modeled barrier failure".into()));
        }
        Ok(DurableLog {
            tickets: dependencies.to_vec(),
        })
    }
    fn fetch_range(
        &self,
        _group: GroupIdentity,
        _generation: LogGeneration,
        _from: u64,
        _max_entries: usize,
        _max_bytes: usize,
    ) -> Result<Vec<LogEntry>, StorageError> {
        Err(StorageError::Rejected("unused modeled range"))
    }
}

#[test]
fn borrowed_persistence_helper_keeps_preflight_exactness_barrier_and_late_error_fencing() {
    for failure in 0..4 {
        let mut core = core();
        let mut store = ModeledStore::new(&core);
        let before = core.state().clone();
        let mut update = pending_entries(
            &mut core,
            vec![command(2, u128::MAX, vec![7])],
            After::Commit,
        );
        match failure {
            0 => store.binding.session = StoreSession::new(2).unwrap(),
            1 => {
                let suffix = update.suffix.as_mut().unwrap();
                let mut entries = suffix.entries.to_vec();
                let EntryPayload::Command { bytes, .. } = &mut entries[0].payload else {
                    unreachable!()
                };
                let mut replacement = bytes.to_vec();
                replacement[0] = 8;
                *bytes = replacement.into();
                suffix.entries = entries.into();
            }
            2 => store.fail_barrier = true,
            3 => core.request_sequence = u64::MAX,
            _ => unreachable!(),
        }
        let result = persist_effect_borrowed(&mut core, &mut store, update);
        match failure {
            0 => assert!(matches!(result, Err(RaftError::WrongIdentity))),
            1 => assert!(matches!(result, Err(RaftError::WrongCompletion))),
            2 => assert!(matches!(
                result,
                Err(RaftError::Storage(StorageError::Uncertain(_)))
            )),
            3 => assert!(matches!(result, Err(RaftError::Exhausted))),
            _ => unreachable!(),
        }
        assert!(core.is_fenced());
        assert!(!core.has_pending_dependency());
        assert_eq!(store.submissions, usize::from(failure >= 2));
        if failure < 3 {
            assert_eq!(core.state(), &before);
        } else {
            assert_eq!(core.state().commit_index, 2);
        }
    }
    let mut owned = core();
    let mut borrowed = core();
    owned.role = Role::Follower;
    borrowed.role = Role::Follower;
    let mut owned_store = ModeledStore::new(&owned);
    let mut borrowed_store = ModeledStore::new(&borrowed);
    let update = pending_entries(
        &mut owned,
        vec![command(2, u128::MAX, vec![7])],
        After::Reply,
    );
    let expected = persist_effect(&mut owned, &mut owned_store, update).unwrap();
    let update = pending_entries(
        &mut borrowed,
        vec![command(2, u128::MAX, vec![7])],
        After::Reply,
    );
    let (committed, following) =
        persist_effect_borrowed(&mut borrowed, &mut borrowed_store, update)
            .unwrap()
            .into_parts();
    assert_eq!(
        committed.as_ref().unwrap()[0],
        command(2, u128::MAX, vec![7])
    );
    let mut materialized = vec![Effect::Committed(committed.unwrap().to_vec())];
    materialized.extend(following);
    assert_eq!(materialized, expected);
    assert_eq!(borrowed.state(), owned.state());
    assert_eq!(
        borrowed_store
            .state(borrowed.state().bootstrap.group)
            .unwrap(),
        *borrowed.state()
    );
}

#[test]
fn exact_entry_backing_survives_pending_admission_delayed_durable_and_canonical_install() {
    use crate::application::{Counter, CounterOutcome, StateMachine};
    let mut core = core();
    core.role = Role::Follower;
    let before = core.state().clone();
    let mut app = Counter::new(8).unwrap();
    assert!(app
        .apply_batch(&core.replay_committed())
        .unwrap()
        .is_empty());
    let expected = vec![
        command(2, u128::MAX, 2i64.to_le_bytes().to_vec()),
        command(3, (1 << 127) | 7, 5i64.to_le_bytes().to_vec()),
    ];
    let update = pending_entries(&mut core, expected.clone(), After::Reply);
    let batch = update.suffix.as_ref().unwrap().entries.clone();
    let pointer = batch.as_ptr();
    assert_eq!(
        core.pending
            .as_ref()
            .unwrap()
            .next
            .update()
            .suffix
            .as_ref()
            .unwrap()
            .entries
            .as_ptr(),
        pointer
    );
    let exact = ticket(&core);
    assert!(matches!(
        core.complete_borrowed(&DurableLog {
            tickets: vec![exact]
        }),
        Err(RaftError::WrongCompletion)
    ));
    assert_eq!(core.state(), &before);
    let mut store = ModeledStore::new(&core);
    let tickets = store
        .append_batch(vec![LogMutation::Update(update)])
        .unwrap();
    assert_eq!(tickets, vec![exact]);
    assert_eq!(
        store.states[&exact.group].entries.get(1).unwrap() as *const LogEntry,
        pointer
    );
    core.admitted(exact).unwrap();
    assert_eq!(core.state(), &before);
    assert_eq!(app.applied_index(), 1);
    assert_eq!(
        core.pending
            .as_ref()
            .unwrap()
            .next
            .update()
            .suffix
            .as_ref()
            .unwrap()
            .entries
            .as_ptr(),
        pointer
    );
    let mut wrong = exact;
    wrong.batch += 1;
    assert!(matches!(
        core.complete_borrowed(&DurableLog {
            tickets: vec![wrong]
        }),
        Err(RaftError::WrongCompletion)
    ));
    assert_eq!(core.state(), &before);
    let completion = store.barrier(&tickets).unwrap();
    let result = core.complete_borrowed(&completion).unwrap();
    assert_eq!(result.committed().unwrap(), expected);
    assert_eq!(result.committed().unwrap().as_ptr(), pointer);
    assert!(matches!(result.following(), [Effect::Send(_)]));
    // One application invocation receives the entire two-original commit batch.
    let receipts = app.apply_batch(result.committed().unwrap()).unwrap();
    assert_eq!(
        receipts
            .iter()
            .map(|r| (r.index, r.operation.get(), r.outcome))
            .collect::<Vec<_>>(),
        vec![
            (2, u128::MAX, CounterOutcome::Value(2)),
            (3, (1 << 127) | 7, CounterOutcome::Value(7))
        ]
    );
    drop(result);
    assert_eq!(
        core.state().entries.get(1).unwrap() as *const LogEntry,
        pointer
    );
    assert_eq!(core.state(), &store.states[&exact.group]);
    assert!(!core.has_pending_dependency());
    assert_eq!(batch, expected);
}

#[test]
fn fragmented_canonical_commit_coalesces_once_without_splitting_application_batch() {
    use crate::application::{ApplicationError, Counter, StateMachine};
    for invalid_later in [false, true] {
        let mut core = core();
        core.role = Role::Follower;
        let mut app = Counter::new(8).unwrap();
        app.apply_batch(&core.replay_committed()).unwrap();
        let mut reference = app.clone();
        let first = command(2, u128::MAX, 2i64.to_le_bytes().to_vec());
        let effects = core
            .persist(
                core.state().hard_state,
                1,
                Some(Suffix {
                    from: 2,
                    entries: vec![first.clone()].into(),
                }),
                After::Reply,
                None,
            )
            .unwrap();
        assert!(durable(&mut core, effects).is_empty());
        let first_pointer = core.state().entries.get(1).unwrap() as *const LogEntry;
        let second = command(
            3,
            (1 << 127) | 7,
            if invalid_later {
                vec![5]
            } else {
                5i64.to_le_bytes().to_vec()
            },
        );
        let reference_entries = vec![first.clone(), second.clone()];
        let update = pending_entries(&mut core, vec![second.clone()], After::Reply);
        let second_batch = update.suffix.as_ref().unwrap().entries.clone();
        let exact = admit(&mut core);
        let (committed, following) = core.complete_borrowed(&exact).unwrap().into_parts();
        let committed = committed.unwrap();
        assert!(matches!(&committed, std::borrow::Cow::Owned(_)));
        assert_eq!(committed.as_ref(), &[first, second]);
        assert_ne!(committed.as_ptr(), first_pointer);
        assert_ne!(committed.as_ptr(), second_batch.as_ptr());
        assert!(matches!(following.as_slice(), [Effect::Send(_)]));
        let outcome = app.apply_batch(&committed);
        assert_eq!(outcome, reference.apply_batch(&reference_entries));
        if invalid_later {
            assert_eq!(outcome, Err(ApplicationError::InvalidCommand));
            assert_eq!(app.applied_index(), 1);
            assert_eq!(app.read_applied(1), Ok(0));
            assert_eq!(app.remaining_operations(), 8);
        } else {
            assert_eq!(outcome.unwrap().len(), 2);
            assert_eq!(app.read_applied(3), Ok(7));
        }
        assert_eq!(app.applied_index(), reference.applied_index());
        assert_eq!(app.remaining_operations(), reference.remaining_operations());
        drop(committed);
        assert_eq!(
            core.state().entries.get(1).unwrap() as *const LogEntry,
            first_pointer
        );
        assert_eq!(
            core.state().entries.get(2).unwrap() as *const LogEntry,
            second_batch.as_ptr()
        );
    }
}
