// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Volatile cores: the core's log is the only copy and completion is a token.
use super::*;
use membership_tests::{core, durable};

fn volatile() -> (Raft, VolatileLog) {
    let mut core = core();
    core.volatile = true;
    core.incarnation = Raft::next_volatile_incarnation().unwrap();
    let log = VolatileLog::new(core.binding, core.limits).unwrap();
    (core, log)
}

fn token(effects: &[Effect]) -> VolatilePersist {
    let [Effect::PersistVolatile(token)] = effects else {
        panic!("one payload-free persistence token, got {effects:?}")
    };
    *token
}

fn propose(core: &mut Raft, operation: u128, bytes: Vec<u8>) -> Vec<Effect> {
    core.step(Event::Propose {
        operation: OperationId::new(operation).unwrap(),
        bytes,
    })
    .unwrap()
}

fn command_bytes(entry: &LogEntry) -> *const u8 {
    let EntryPayload::Command { bytes, .. } = &entry.payload else {
        panic!("command entry")
    };
    bytes.as_ptr()
}

#[test]
fn volatile_persist_carries_no_entries_and_installs_the_exact_proposal() {
    let (mut core, log) = volatile();
    let mut twin = membership_tests::core();
    let before = core.state().clone();
    let effects = propose(&mut core, 9, vec![1, 2, 3]);
    let persist = token(&effects);
    assert_eq!(persist.revision().get(), before.revision.get() + 1);
    assert_eq!(persist.generation(), before.generation);
    // Nothing is installed before completion; the plan owns the only copy.
    assert_eq!(core.state(), &before);
    let pending = core.pending.as_ref().unwrap();
    let planned = &pending.next.suffix().unwrap().entries;
    assert_eq!(planned.backing_count(), 1);
    let planned_bytes = command_bytes(&planned[0]);
    let (committed, following) = persist_volatile(&mut core, &log, persist)
        .unwrap()
        .into_parts();
    assert!(committed.is_none());
    assert!(following.iter().all(|e| matches!(e, Effect::Send(_))));
    assert!(!following.is_empty());
    let installed = core.state().entries.last().unwrap();
    assert_eq!(installed.index, 2);
    assert_eq!(command_bytes(installed), planned_bytes);
    // The durable seam reaches the same canonical state and effects.
    let effects = propose(&mut twin, 9, vec![1, 2, 3]);
    let expected = durable(&mut twin, effects);
    assert_eq!(core.state(), twin.state());
    assert_eq!(following, expected);
}

#[test]
fn volatile_token_is_three_words_and_never_sizes_effect() {
    assert_eq!(std::mem::size_of::<VolatilePersist>(), 24);
    assert!(std::mem::size_of::<LogUpdate>() > std::mem::size_of::<VolatilePersist>());
}

#[test]
fn volatile_step_is_busy_until_persist_completes() {
    let (mut core, log) = volatile();
    let persist = token(&propose(&mut core, 9, vec![1]));
    assert!(core.has_pending_dependency());
    assert_eq!(core.step(Event::Heartbeat), Err(RaftError::Busy));
    assert_eq!(
        core.propose_batch(vec![Proposal {
            operation: OperationId::new(10).unwrap(),
            bytes: vec![2],
        }]),
        Err(RaftError::Busy)
    );
    assert!(!core.is_fenced());
    persist_volatile(&mut core, &log, persist).unwrap();
    assert!(!core.has_pending_dependency());
    assert!(core.step(Event::Heartbeat).is_ok());
}

#[test]
fn volatile_core_rejects_durable_seam_calls() {
    let (mut core, log) = volatile();
    let persist = token(&propose(&mut core, 9, vec![1]));
    let update = core.pending.as_ref().unwrap().next.update();
    let next = core.pending.as_ref().unwrap().next.metadata();
    let ticket = LogTicket {
        binding: core.binding,
        batch: core.last_batch + 1,
        group: next.group,
        revision: next.revision,
        generation: next.generation,
        last_index: next.last_index,
        term: next.hard_state.term,
    };
    let completion = DurableLog {
        tickets: vec![ticket],
    };
    assert_eq!(
        core.validate_persist_effect(&update),
        Err(RaftError::WrongCompletion)
    );
    assert_eq!(
        core.admit_effect(&update, ticket),
        Err(RaftError::WrongCompletion)
    );
    assert_eq!(core.admitted(ticket), Err(RaftError::WrongCompletion));
    assert_eq!(core.complete(&completion), Err(RaftError::WrongCompletion));
    // None of these refusals admitted, installed or fenced anything.
    assert!(!core.is_fenced());
    assert!(core.pending.as_ref().unwrap().ticket.is_none());
    assert_eq!(core.last_batch, 0);
    persist_volatile(&mut core, &log, persist).unwrap();
    assert_eq!(core.state().last_index(), 2);
    // And a durable core refuses a volatile token, fencing like any failure.
    let mut durable_core = membership_tests::core();
    let effects = propose(&mut durable_core, 9, vec![1]);
    assert!(matches!(&effects[..], [Effect::Persist(_)]));
    assert!(matches!(
        persist_volatile(&mut durable_core, &log, persist),
        Err(RaftError::WrongCompletion)
    ));
    assert!(durable_core.is_fenced());
    assert!(!durable_core.has_pending_dependency());
}

#[test]
fn volatile_commit_only_round_borrows_canonical_interval() {
    let (mut core, log) = volatile();
    core.role = Role::Follower;
    let entries = vec![
        LogEntry {
            index: 2,
            term: 1,
            payload: EntryPayload::Command {
                operation: OperationId::new(9).unwrap(),
                bytes: vec![1, 2, 3].into(),
            },
        },
        LogEntry {
            index: 3,
            term: 1,
            payload: EntryPayload::Command {
                operation: OperationId::new(u128::MAX).unwrap(),
                bytes: Vec::new().into(),
            },
        },
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
    let result = persist_volatile(&mut core, &log, token(&effects)).unwrap();
    assert!(result.committed().is_none());
    assert!(result.following().is_empty());
    let canonical: *const LogEntry = &core.state().entries[1];
    let canonical_bytes = command_bytes(&core.state().entries[1]);
    let effects = core
        .persist(core.state().hard_state, 3, None, After::Commit, None)
        .unwrap();
    let result = persist_volatile(&mut core, &log, token(&effects)).unwrap();
    let committed = result.committed().unwrap();
    assert!(std::ptr::eq(&committed[0], canonical));
    assert_eq!(committed.len(), 2);
    assert_eq!(command_bytes(&committed[0]), canonical_bytes);
    assert!(result.following().is_empty());
    assert_eq!(core.state().commit_index, 3);
}

#[test]
fn volatile_storage_failure_drops_pending_and_steps_down() {
    let (mut core, log) = volatile();
    assert_eq!(core.role(), Role::Leader);
    let before = core.state().clone();
    let persist = token(&propose(&mut core, 9, vec![1]));
    let mut other = log.binding();
    other.session = StoreSession::new(2).unwrap();
    let wrong = VolatileLog::new(other, log.limits()).unwrap();
    assert!(matches!(
        persist_volatile(&mut core, &wrong, persist),
        Err(RaftError::WrongIdentity)
    ));
    assert!(core.is_fenced());
    assert_eq!(core.role(), Role::Follower);
    assert!(!core.has_pending_dependency());
    assert_eq!(core.state(), &before);
    assert!(matches!(
        persist_volatile(&mut core, &log, persist),
        Err(RaftError::Fenced)
    ));
    // A stale or mismatched token fences a live core in the same way.
    let (mut core, log) = volatile();
    let persist = token(&propose(&mut core, 9, vec![1]));
    persist_volatile(&mut core, &log, persist).unwrap();
    let next = token(&propose(&mut core, 10, vec![2]));
    assert_ne!(next, persist);
    let completed = core.state().clone();
    assert!(matches!(
        persist_volatile(&mut core, &log, persist),
        Err(RaftError::WrongCompletion)
    ));
    assert!(core.is_fenced());
    assert!(!core.has_pending_dependency());
    assert_eq!(core.state(), &completed);
}

#[test]
fn proposal_batches_collect_a_tight_suffix_before_owned_planning() {
    for width in [1, 2, 3, 7, 64] {
        let (mut core, log) = volatile();
        let proposals = (0..width)
            .map(|i| Proposal {
                operation: OperationId::new(100 + i as u128).unwrap(),
                bytes: vec![i as u8; i],
            })
            .collect::<Vec<_>>();
        // An exact-length source collects tight today, so owned planning does
        // not reallocate to normalize capacity. std does not promise this, so
        // it is checked here rather than asserted in `propose_batch`.
        let persist = token(&core.propose_batch(proposals).unwrap());
        let suffix = core.pending.as_ref().unwrap().next.suffix();
        let entries = &suffix.unwrap().entries;
        assert_eq!(entries.len(), width);
        assert_eq!(entries.backing_count(), 1);
        persist_volatile(&mut core, &log, persist).unwrap();
        assert_eq!(core.state().last_index(), 1 + width as u64);
    }
}

#[test]
fn volatile_plan_moves_the_input_suffix_allocation() {
    // `CommandBytes` is shared, so payload pointers survive an entry clone;
    // the suffix buffer itself does not. A copying plan fails this check.
    fn plan(core: &mut Raft) -> (Vec<Effect>, *const LogEntry, *const LogEntry) {
        let entries = (2..=3)
            .map(|index| LogEntry {
                index,
                term: 1,
                payload: EntryPayload::Command {
                    operation: OperationId::new(u128::from(index)).unwrap(),
                    bytes: vec![index as u8].into(),
                },
            })
            .collect::<Vec<_>>();
        assert_eq!(entries.capacity(), entries.len());
        let input = entries.as_ptr();
        core.role = Role::Follower;
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
        let pending = core.pending.as_ref().unwrap();
        let planned = &pending.next.suffix().unwrap().entries;
        assert_eq!(planned.len(), 2);
        (effects, input, planned.first_backing_ptr().unwrap())
    }
    let (mut core, log) = volatile();
    let (effects, input, planned) = plan(&mut core);
    assert_eq!(planned, input);
    persist_volatile(&mut core, &log, token(&effects)).unwrap();
    assert_eq!(core.state().last_index(), 3);
    // Control: the durable seam keeps a copy of this small batch for its store.
    let mut durable_core = membership_tests::core();
    let (effects, input, planned) = plan(&mut durable_core);
    assert!(matches!(&effects[..], [Effect::Persist(_)]));
    assert_ne!(planned, input);
}

/// A leader and two followers on volatile logs, all from the same history.
fn volatile_cluster() -> Vec<(Raft, VolatileLog)> {
    (1..=3u64)
        .map(|id| {
            let mut core = core();
            if id != 1 {
                core.node = NodeId::new(id).unwrap();
                core.binding.identity.id = StoreId::new(u128::from(id)).unwrap();
                core.role = Role::Follower;
                core.requests.clear();
                core.progress.clear();
                core.next_index.clear();
            }
            core.volatile = true;
            core.incarnation = Raft::next_volatile_incarnation().unwrap();
            let log = VolatileLog::new(core.binding, core.limits).unwrap();
            (core, log)
        })
        .collect()
}
/// Run every core to quiescence, each finishing its own effects before its
/// next message, as the actors do. Returns the nonempty appends sent.
fn settle(cluster: &mut [(Raft, VolatileLog)], effects: Vec<Effect>) -> Vec<EntryBatch> {
    let mut inbox: Vec<std::collections::VecDeque<Message>> = vec![Default::default(); 3];
    let mut sent = Vec::new();
    let mut work = vec![(0, effects)];
    loop {
        while let Some((slot, effects)) = work.pop() {
            let mut queue: std::collections::VecDeque<Effect> = effects.into();
            while let Some(effect) = queue.pop_front() {
                match effect {
                    Effect::PersistVolatile(token) => {
                        let (core, log) = &mut cluster[slot];
                        let (_, following) =
                            persist_volatile(core, log, token).unwrap().into_parts();
                        queue.extend(following);
                    }
                    Effect::Send(message) => {
                        if let Rpc::Append { entries, .. } = &message.rpc {
                            if !entries.is_empty() {
                                sent.push(entries.clone());
                            }
                        }
                        inbox[(message.to.get() - 1) as usize].push_back(message);
                    }
                    _ => {}
                }
            }
        }
        let Some(slot) = (0..3).find(|slot| !inbox[*slot].is_empty()) else {
            return sent;
        };
        let message = inbox[slot].pop_front().unwrap();
        work.push((slot, cluster[slot].0.step(Event::Receive(message)).unwrap()));
    }
}

#[test]
fn wide_batches_share_one_backing_from_proposal_to_every_follower() {
    // A batch of at least the sharing width is frozen once: the leader's
    // canonical log, both appends and each follower's canonical log hold
    // ranges of the same backing. A narrower batch is copied per peer.
    for width in [SHARE_MIN - 1, SHARE_MIN, 64] {
        let mut cluster = volatile_cluster();
        let proposals = (0..width)
            .map(|n| Proposal {
                operation: OperationId::new(500 + n as u128).unwrap(),
                bytes: Vec::new(),
            })
            .collect();
        let effects = cluster[0].0.propose_batch(proposals).unwrap();
        let sent = settle(&mut cluster, effects);
        let wide = width >= SHARE_MIN;
        let leader = &cluster[0].0.state().entries;
        let first = leader.len() - width;
        assert_eq!(cluster[0].0.state().commit_index, 1 + width as u64);
        assert_eq!(sent.len(), 2);
        for append in &sent {
            assert_eq!(append.len(), width);
            assert_eq!(std::ptr::eq(&append[0], &leader[first]), wide);
        }
        for (follower, _) in &cluster[1..] {
            let entries = &follower.state().entries;
            assert_eq!(entries.to_vec(), leader.to_vec());
            assert_eq!(std::ptr::eq(&entries[first], &leader[first]), wide);
        }
    }
}
