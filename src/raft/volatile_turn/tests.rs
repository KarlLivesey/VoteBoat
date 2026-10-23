// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use crate::raft::membership_tests::{core, node};

fn ready() -> (Raft, VolatileLog) {
    let mut core = core();
    core.volatile = true;
    core.incarnation = Raft::next_volatile_incarnation().unwrap();
    let log = VolatileLog::new(core.binding, core.limits).unwrap();
    (core, log)
}
fn proposal() -> Event {
    Event::Propose {
        operation: OperationId::new(90).unwrap(),
        bytes: vec![1],
    }
}

#[test]
fn queue_and_mode_refusals_are_pre_admission_and_busy_keeps_the_original_plan() {
    let (mut core, log) = ready();
    let state = core.state().clone();
    let mut out = VecDeque::from([Effect::Committed(Vec::new())]);
    assert_eq!(
        core.step_volatile_into(&log, proposal(), &mut out)
            .unwrap_err(),
        RaftError::NonemptyEffects
    );
    assert_eq!(core.state(), &state);
    assert!(!core.batch_limits_locked);
    assert_eq!(out, VecDeque::from([Effect::Committed(Vec::new())]));
    out.clear();
    let mut durable = membership_tests::core();
    assert_eq!(
        durable
            .step_volatile_into(&log, proposal(), &mut out)
            .unwrap_err(),
        RaftError::NotVolatile
    );
    assert!(!durable.batch_limits_locked);
    assert!(!durable.is_fenced());
    let effects = core.step(proposal()).unwrap();
    let revision = core.pending.as_ref().unwrap().next.metadata().revision;
    assert_eq!(
        core.step_volatile_into(&log, proposal(), &mut out)
            .unwrap_err(),
        RaftError::Busy
    );
    assert_eq!(
        core.pending.as_ref().unwrap().next.metadata().revision,
        revision
    );
    let Effect::PersistVolatile(key) = effects[0] else {
        panic!("token")
    };
    persist_volatile(&mut core, &log, key).unwrap();
}

#[test]
fn wrong_binding_fences_pending_but_does_not_change_last_installed_state() {
    let (mut core, log) = ready();
    core.step(proposal()).unwrap();
    let before = core.state().clone();
    let mut binding = log.binding();
    binding.session = StoreSession::new(binding.session.get() + 1).unwrap();
    let wrong = VolatileLog::new(binding, log.limits()).unwrap();
    let mut out = VecDeque::new();
    assert_eq!(
        core.step_volatile_into(&wrong, proposal(), &mut out)
            .unwrap_err(),
        RaftError::WrongIdentity
    );
    assert!(core.is_fenced());
    assert!(!core.has_pending_dependency());
    assert_eq!(core.state(), &before);
    assert!(out.is_empty());
    assert!(matches!(core.completion_delivery, Delivery::Host));
}

#[test]
fn exact_local_key_and_changed_predecessor_fail_closed() {
    for corrupt in 0..4 {
        let (mut core, log) = ready();
        let mut out = VecDeque::new();
        let mut expected = core.state().clone();
        let result = core.volatile_turn(&log, true, &mut out, |core, out| {
            core.step_into(proposal(), out)?;
            // Pending remains visible until the whole event/finish hook returns.
            assert!(core.has_pending_dependency());
            assert_eq!(core.state(), &expected);
            assert!(out.is_empty());
            let Delivery::Local { due: Some(key), .. } = &mut core.completion_delivery else {
                panic!("local key")
            };
            match corrupt {
                0 => key.incarnation += 1,
                1 => key.revision = LogRevision::new(key.revision.get() + 1).unwrap(),
                2 => key.generation = LogGeneration::new(key.generation.get() + 1).unwrap(),
                _ => {
                    core.durable
                        .edit(|state| state.hard_state.voted_for = Some(node(3)));
                    expected = core.state().clone();
                }
            }
            Ok(())
        });
        assert!(result.is_err());
        assert!(core.is_fenced());
        assert_eq!(core.state(), &expected);
        assert!(out.is_empty());
        assert!(!core.has_pending_dependency());
        assert!(matches!(core.completion_delivery, Delivery::Host));
    }
}

#[test]
fn continuation_failure_keeps_the_successfully_installed_append() {
    let (mut core, log) = ready();
    core.request_sequence = u64::MAX;
    let previous = core.state().clone();
    let mut out = VecDeque::new();
    assert_eq!(
        core.step_volatile_into(&log, proposal(), &mut out)
            .unwrap_err(),
        RaftError::Exhausted
    );
    assert_eq!(core.state().last_index(), previous.last_index() + 1);
    assert_eq!(core.state().commit_index, previous.commit_index);
    assert!(core.is_fenced());
    assert!(out.is_empty());
    assert!(matches!(core.completion_delivery, Delivery::Host));
}

#[test]
fn scope_restores_host_dispatch_on_unwind() {
    let (mut core, log) = ready();
    let mut out = VecDeque::new();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = core.volatile_turn(&log, true, &mut out, |core, out| {
            core.step_into(proposal(), out)?;
            panic!("test unwind after staging")
        });
    }));
    assert!(result.is_err());
    assert!(matches!(core.completion_delivery, Delivery::Host));
    assert!(core.has_pending_dependency());
    // Unwind is not rollback. Recovery is explicit, with a fresh domain.
    let recovered = Raft::recover_volatile(core.node, &log, core.state().clone()).unwrap();
    assert!(!recovered.has_pending_dependency());
    assert_ne!(recovered.incarnation, core.incarnation);
}

#[test]
fn reads_force_functional_host_dispatch_and_old_tokens_never_complete_new_work() {
    let (mut core, log) = ready();
    core.step(Event::Read {
        request: ReadRequestId::new(1).unwrap(),
    })
    .unwrap();
    let mut out = VecDeque::new();
    let turn = core.step_volatile_into(&log, proposal(), &mut out).unwrap();
    assert_eq!(turn.completed_local_units(), 0);
    assert!(turn.committed().is_none());
    let Some(Effect::PersistVolatile(old)) = out.pop_front() else {
        panic!("legacy fallback token")
    };
    persist_volatile_into(&mut core, &log, old, &mut out).unwrap();
    core.clear_reads();
    out.clear();
    core.step_volatile_into(&log, proposal(), &mut out).unwrap();
    assert!(matches!(
        persist_volatile_into(&mut core, &log, old, &mut out),
        Err(RaftError::WrongCompletion)
    ));
    assert!(core.is_fenced());
    let mut recovered = Raft::recover_volatile(core.node, &log, core.state().clone()).unwrap();
    assert!(matches!(
        persist_volatile_into(&mut recovered, &log, old, &mut out),
        Err(RaftError::WrongCompletion)
    ));
    assert!(recovered.is_fenced());
}

#[test]
fn ineligible_plan_preserves_the_same_pending_plan_and_host_token() {
    let (mut core, log) = ready();
    let mut out = VecDeque::new();
    let before = core.state().clone();
    let turn = core
        .volatile_turn(&log, true, &mut out, |core, out| {
            core.persist_into(
                HardState {
                    term: 2,
                    voted_for: None,
                },
                core.state().commit_index,
                None,
                After::Reply,
                None,
                out,
            )
        })
        .unwrap();
    assert_eq!(turn.completed_local_units(), 0);
    assert!(turn.committed().is_none());
    assert_eq!(core.state(), &before);
    // append_eligible permits ballot changes on plain logs. The independent
    // same-hard-state check must defer even that append representation.
    assert!(core.pending.as_ref().unwrap().next.is_append_or_commit());
    assert_ne!(
        core.pending.as_ref().unwrap().next.metadata().hard_state,
        before.hard_state
    );
    assert_eq!(
        core.pending
            .as_ref()
            .unwrap()
            .next
            .metadata()
            .hard_state
            .term,
        2
    );
    let Some(Effect::PersistVolatile(key)) = out.pop_front() else {
        panic!("exact deferred plan")
    };
    persist_volatile_into(&mut core, &log, key, &mut out).unwrap();
    assert_eq!(core.state().hard_state.term, 2);
}

#[test]
fn cold_interval_keeps_the_exact_queued_position_and_releases_the_core_borrow() {
    let (mut core, log) = ready();
    let mut out = VecDeque::new();
    core.step_volatile_into(&log, proposal(), &mut out).unwrap();
    out.clear();
    core.role = Role::Follower;
    // Model an effect appended by the original finish hook after staging.
    let turn = core
        .volatile_turn(&log, true, &mut out, |core, out| {
            core.persist_commit_into(2, After::Commit, None, out)?;
            let context = core.context()?;
            out.push_back(Effect::Send(core.message(node(2), context, Rpc::ReadProbe)));
            Ok(())
        })
        .unwrap();
    assert_eq!(turn.completed_local_units(), 1);
    assert_eq!(turn.commit_position(), 1);
    let interval = turn.committed().unwrap();
    assert_eq!(
        interval.iter().map(|entry| entry.index).collect::<Vec<_>>(),
        vec![2]
    );
    out.insert(turn.commit_position(), Effect::Committed(interval.to_vec()));
    // The cold consumer can now mutate the core; no borrowed interval remains.
    core.step(Event::Heartbeat).unwrap();
    assert!(matches!(out.pop_front(), Some(Effect::Send(_))));
    // Failure of that send leaves the application effect undelivered.
    assert!(matches!(out.front(), Some(Effect::Committed(entries)) if entries[0].index==2));
}

#[test]
fn public_higher_term_and_transfer_controls_use_the_existing_functional_dispatch() {
    let (mut core, log) = ready();
    let mut out = VecDeque::new();
    let transfer = LeadershipTransferRequest {
        operation: OperationId::new(700).unwrap(),
        target: PeerIdentity {
            node: node(2),
            store: membership_tests::store(2),
        },
        configuration: core.membership().id(),
    };
    let turn = core
        .step_volatile_into(&log, Event::TransferLeadership(transfer), &mut out)
        .unwrap();
    assert_eq!(turn.completed_local_units(), 0);
    assert!(out.iter().all(|effect| matches!(effect, Effect::Send(_))));
    assert_eq!(core.leadership_transfer().unwrap().request, transfer);
    out.clear();
    assert_eq!(
        core.step_volatile_into(&log, proposal(), &mut out)
            .unwrap_err(),
        RaftError::Busy
    );
    let context = core.leadership_transfer().unwrap().context;
    core.step_volatile_into(&log, Event::CancelLeadershipTransfer { context }, &mut out)
        .unwrap();
    assert!(core.leadership_transfer().is_none());
    let sender = StoreBinding {
        identity: membership_tests::store(2),
        session: StoreSession::new(1).unwrap(),
    };
    let message = Message {
        group: core.state().bootstrap.group,
        configuration: core.membership().id(),
        from: node(2),
        sender,
        to: node(1),
        term: 2,
        context: RequestContext {
            origin: sender,
            sequence: 900,
        },
        rpc: Rpc::Append {
            previous_index: 1,
            previous_term: 1,
            entries: EntryBatch::default(),
            leader_commit: 1,
        },
    };
    let turn = core
        .step_volatile_into(&log, Event::Receive(message), &mut out)
        .unwrap();
    assert_eq!(turn.completed_local_units(), 0);
    assert!(turn.committed().is_none());
    assert_eq!(core.state().hard_state.term, 1);
    let Some(Effect::PersistVolatile(key)) = out.pop_front() else {
        panic!("higher-term persistence stays external")
    };
    persist_volatile_into(&mut core, &log, key, &mut out).unwrap();
    assert_eq!(core.state().hard_state.term, 2);
    assert!(matches!(out.front(), Some(Effect::Send(_))));
}

#[test]
fn selected_proposal_batch_nonempty_refusal_does_not_admit_or_drain() {
    let (mut core, log) = ready();
    let mut batch = core.proposal_batch(1).unwrap();
    batch.push(OperationId::new(1).unwrap(), vec![1]).unwrap();
    let before = core.state().clone();
    let original = Effect::Committed(Vec::new());
    let mut out = VecDeque::from([original.clone()]);
    assert_eq!(
        core.propose_entries_volatile_into(&log, batch, &mut out)
            .unwrap_err(),
        RaftError::NonemptyEffects
    );
    assert_eq!(out, VecDeque::from([original]));
    assert_eq!(core.state(), &before);
    assert!(!core.has_pending_dependency());
    assert!(!core.batch_limits_locked);
}
