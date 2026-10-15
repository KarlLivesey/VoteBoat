// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Real Core controls; fixture tickets model host evidence, not physical fsync.
use super::*;
use crate::quorum::{Limits, Policy, Tree};

fn node(n: u64) -> NodeId {
    NodeId::new(n).unwrap()
}
fn leader() -> Raft {
    let mut core = crate::raft::membership_tests::core();
    for peer in [node(2), node(3)] {
        core.progress.insert(peer, 1);
    }
    core.configure_append_window(Some(limits())).unwrap();
    core
}
fn limits() -> AppendWindowLimits {
    AppendWindowLimits {
        max_units: 4,
        max_originals: 8,
        max_retained_bytes: 1024 * 1024,
    }
}
fn proposal(id: u128) -> Proposal {
    Proposal {
        operation: OperationId::new(id).unwrap(),
        bytes: vec![id as u8, 3, 7],
    }
}
fn ticket(core: &Raft, append: &StagedAppend, batch: u64) -> LogTicket {
    let unit = core.append_unit(append.id).unwrap();
    let next = unit.next.metadata();
    LogTicket {
        binding: core.binding,
        batch,
        group: next.group,
        revision: next.revision,
        generation: next.generation,
        last_index: next.last_index,
        term: next.hard_state.term,
    }
}
fn completion(ticket: LogTicket) -> DurableLog {
    DurableLog {
        tickets: vec![ticket],
    }
}
fn retired(value: AppendCompletion) -> (Vec<AppendUnitId>, Vec<Effect>) {
    let AppendCompletion::Retired { units, effects } = value else {
        panic!("window still waiting")
    };
    (units, effects)
}

#[test]
fn default_remains_serial_and_refusal_returns_original_owned_buffers() {
    let mut core = crate::raft::membership_tests::core();
    assert_eq!(core.append_window_limits(), None);
    let input = vec![proposal(u128::MAX)];
    let ptr = input.as_ptr();
    let bytes = input[0].bytes.as_ptr();
    let rejected = core.stage_append_window(input).unwrap_err();
    assert_eq!(rejected.reason, RaftError::Busy);
    assert_eq!(rejected.proposals.as_ptr(), ptr);
    assert_eq!(rejected.proposals[0].bytes.as_ptr(), bytes);
    assert!(core.configure_append_window(Some(limits())).is_err());
    assert!(matches!(
        core.propose_batch(rejected.proposals).unwrap().as_slice(),
        [Effect::Persist(_)]
    ));
}

#[test]
fn three_revisions_wait_for_all_exact_tickets_then_match_independent_serial_log() {
    let mut core = leader();
    let before = core.state().clone();
    let mut reference = BTreeMap::from([(before.bootstrap.group, before.clone())]);
    let mut appends = Vec::new();
    let mut tickets = Vec::new();
    for (n, id) in [u128::MAX, 1 << 127, (1 << 96) | 9].into_iter().enumerate() {
        let append = core.stage_append_window(vec![proposal(id)]).unwrap();
        apply_batch(
            &mut reference,
            &[LogMutation::Update(append.update.clone())],
            core.limits,
        )
        .unwrap();
        let receipt = ticket(&core, &append, n as u64 + 1);
        core.validate_append_effect(append.id, &append.update)
            .unwrap();
        core.admitted_append(append.id, receipt).unwrap();
        tickets.push(receipt);
        appends.push(append);
        assert_eq!(core.state(), &before);
        assert_eq!(core.progress[&node(1)], 1);
    }
    assert!(matches!(
        core.complete_append_unit(appends[2].id, &completion(tickets[2]))
            .unwrap(),
        AppendCompletion::Waiting
    ));
    assert!(!core.can_stage_append_window());
    assert_eq!(core.state(), &before);
    assert!(matches!(
        core.complete_append_unit(appends[0].id, &completion(tickets[0]))
            .unwrap(),
        AppendCompletion::Waiting
    ));
    assert_eq!(core.state(), &before);
    let (ids, effects) = retired(
        core.complete_append_unit(appends[1].id, &completion(tickets[1]))
            .unwrap(),
    );
    assert_eq!(ids, appends.iter().map(|a| a.id).collect::<Vec<_>>());
    assert_eq!(core.state(), &reference[&before.bootstrap.group]);
    assert_eq!(core.progress[&node(1)], 4);
    assert!(!core.has_pending_dependency());
    assert!(!effects
        .iter()
        .any(|e| matches!(e, Effect::Committed(_) | Effect::Persist(_))));
}

#[test]
fn wrong_full_effect_wrong_ticket_and_wrong_order_cannot_mutate_or_seal() {
    let mut core = leader();
    let first = core.stage_append_window(vec![proposal(7)]).unwrap();
    let second = core.stage_append_window(vec![proposal(9)]).unwrap();
    let mut altered = first.update.clone();
    altered.suffix.as_mut().unwrap().entries[0].payload = EntryPayload::Noop;
    assert_eq!(
        core.validate_append_effect(first.id, &altered),
        Err(RaftError::WrongCompletion)
    );
    let one = ticket(&core, &first, 1);
    let two = ticket(&core, &second, 2);
    assert_eq!(
        core.admitted_append(second.id, two),
        Err(RaftError::WrongCompletion)
    );
    let mut wrong = one;
    wrong.last_index += 1;
    assert_eq!(
        core.admitted_append(first.id, wrong),
        Err(RaftError::WrongCompletion)
    );
    core.admitted_append(first.id, one).unwrap();
    assert_eq!(
        core.complete_append_unit(first.id, &completion(two))
            .unwrap_err(),
        RaftError::WrongCompletion
    );
    assert!(core.can_stage_append_window());
    core.admitted_append(second.id, two).unwrap();
    assert!(matches!(
        core.complete_append_unit(first.id, &completion(one))
            .unwrap(),
        AppendCompletion::Waiting
    ));
    assert_eq!(
        core.complete_append_unit(first.id, &completion(one))
            .unwrap_err(),
        RaftError::WrongCompletion
    );
    retired(
        core.complete_append_unit(second.id, &completion(two))
            .unwrap(),
    );
}

#[test]
fn sealed_or_budget_refusal_preserves_prior_window_and_legacy_valid_input() {
    let mut core = leader();
    let one = core.stage_append_window(vec![proposal(1)]).unwrap();
    let before = core.append_window.as_ref().unwrap().accepted;
    core.seal_append_window();
    let input = vec![proposal(17)];
    let ptr = input.as_ptr();
    let rejected = core.stage_append_window(input).unwrap_err();
    assert_eq!(rejected.proposals.as_ptr(), ptr);
    assert_eq!(core.append_window.as_ref().unwrap().accepted, before);
    assert_eq!(core.step(Event::Heartbeat).unwrap_err(), RaftError::Busy);
    let t = ticket(&core, &one, 1);
    core.admitted_append(one.id, t).unwrap();
    assert_eq!(core.complete(&completion(t)).unwrap_err(), RaftError::Busy);
    retired(core.complete_append_unit(one.id, &completion(t)).unwrap());
    let mut narrow = crate::raft::membership_tests::core();
    narrow
        .configure_append_window(Some(AppendWindowLimits {
            max_retained_bytes: 1,
            ..limits()
        }))
        .unwrap();
    let rejected = narrow.stage_append_window(vec![proposal(21)]).unwrap_err();
    assert!(matches!(
        narrow.propose_batch(rejected.proposals).unwrap().as_slice(),
        [Effect::Persist(_)]
    ));
    assert_eq!(input_cost(&[proposal(1)], usize::MAX), None);
}

#[test]
fn early_first_unit_preserves_original_context_and_never_exposes_descendant() {
    let mut core = leader();
    // Existing empty requests become aliases, preserving their exact contexts.
    for peer in [node(2), node(3)] {
        core.append_for(peer).unwrap();
    }
    let a = core.stage_append_window(vec![proposal(1)]).unwrap();
    let b = core.stage_append_window(vec![proposal(2)]).unwrap();
    let ta = ticket(&core, &a, 1);
    let tb = ticket(&core, &b, 2);
    assert!(core.append_replication_reservation(a.id).is_some());
    let early = core.replicate_admitted_append(a.id, ta).unwrap();
    assert_eq!(early.len(), 2);
    let requests = core.requests.clone();
    assert!(core.replicate_admitted_append(b.id, tb).unwrap().is_empty());
    for (peer, request) in &requests {
        assert_eq!(core.requests[peer].context, request.context);
        assert_eq!(core.admitted_ranges[peer].ticket, ta);
        #[cfg(feature = "native")]
        {
            assert!(core.admitted_entry(*peer, request, 2).is_some());
            assert!(core.admitted_entry(*peer, request, 3).is_none());
        }
    }
    assert!(matches!(
        core.complete_append_unit(a.id, &completion(ta)).unwrap(),
        AppendCompletion::Waiting
    ));
    let (_, effects) = retired(core.complete_append_unit(b.id, &completion(tb)).unwrap());
    assert!(effects.is_empty());
    for (peer, request) in requests {
        assert_eq!(core.requests[&peer].context, request.context);
    }
    assert_eq!(core.empty_append_aliases.len(), 2);
}

#[test]
fn final_head_has_one_commit_plan_and_no_early_application() {
    let fixture = crate::raft::membership_tests::core();
    let mut state = fixture.state().clone();
    state
        .bootstrap
        .voter_stores
        .retain(|peer, _| *peer == node(1));
    state.bootstrap.policy = Policy::new(Tree::Voter(node(1)), Limits::default()).unwrap();
    let mut core = Raft::recover(node(1), fixture.binding, state, LogLimits::default()).unwrap();
    core.role = Role::Leader;
    core.initialize_replication().unwrap();
    core.configure_append_window(Some(limits())).unwrap();
    let a = core.stage_append_window(vec![proposal(11)]).unwrap();
    let b = core.stage_append_window(vec![proposal(12)]).unwrap();
    let ta = ticket(&core, &a, 1);
    let tb = ticket(&core, &b, 2);
    core.admitted_append(a.id, ta).unwrap();
    core.admitted_append(b.id, tb).unwrap();
    assert!(matches!(
        core.complete_append_unit(a.id, &completion(ta)).unwrap(),
        AppendCompletion::Waiting
    ));
    let (_, effects) = retired(core.complete_append_unit(b.id, &completion(tb)).unwrap());
    let [Effect::Persist(commit)] = effects.as_slice() else {
        panic!("one commit persistence required")
    };
    assert_eq!(
        commit.expected_revision,
        LogRevision::new(b.update.expected_revision.get() + 1).unwrap()
    );
    assert_eq!(commit.commit_index, 3);
    assert_eq!(core.state().commit_index, 1);
    assert!(commit.suffix.is_none());
}

#[test]
fn late_callback_error_fences_without_escaped_output() {
    let mut core = leader();
    let a = core.stage_append_window(vec![proposal(1)]).unwrap();
    let t = ticket(&core, &a, 1);
    core.admitted_append(a.id, t).unwrap();
    core.request_sequence = u64::MAX;
    assert_eq!(
        core.complete_append_unit(a.id, &completion(t)).unwrap_err(),
        RaftError::Exhausted
    );
    assert!(core.is_fenced());
    assert!(core.append_window.is_none());
    assert!(core.requests.is_empty());
    assert_eq!(core.state().commit_index, 1);
}

#[test]
fn actual_full_capacities_and_independent_entry_owners_fit_the_peak_charge() {
    let mut core = leader();
    let mut exported = Vec::new();
    for id in [7, u128::MAX, 1 << 100] {
        let mut command = proposal(id);
        command.bytes.reserve_exact(37);
        let mut input = Vec::with_capacity(5);
        input.push(command);
        let raw = size_of::<Vec<Proposal>>()
            + input.capacity() * size_of::<Proposal>()
            + input.iter().map(|p| p.bytes.capacity()).sum::<usize>();
        let peak = core
            .append_window_reservation(&input, input.capacity())
            .unwrap();
        let staged = core.stage_append_window(input).unwrap();
        let unit = core.append_unit(staged.id).unwrap();
        assert_ne!(
            staged.update.suffix.as_ref().unwrap().entries.as_ptr(),
            unit.next.update().suffix.as_ref().unwrap().entries.as_ptr()
        );
        exported.push(staged);
        let window = core.append_window.as_ref().unwrap();
        let entries = |s: &Suffix| {
            s.entries.capacity() * size_of::<LogEntry>()
                + s.entries
                    .iter()
                    .map(LogEntry::retained_payload_bytes)
                    .sum::<usize>()
        };
        let actual = size_of::<Window>()
            + window.units.capacity() * size_of::<Unit>()
            + window.tail.capacity() * size_of::<LogEntry>()
            + window
                .tail
                .iter()
                .map(LogEntry::retained_payload_bytes)
                .sum::<usize>()
            + window
                .units
                .iter()
                .map(|u| entries(u.next.update().suffix.as_ref().unwrap()))
                .sum::<usize>()
            + exported
                .iter()
                .map(|u| size_of::<StagedAppend>() + entries(u.update.suffix.as_ref().unwrap()))
                .sum::<usize>();
        assert!(actual <= window.retained);
        assert!(window.retained + raw <= peak);
        assert!(peak <= limits().max_retained_bytes);
    }
}

#[test]
fn stale_window_ids_and_each_ticket_scope_field_are_rejected() {
    let mut core = leader();
    let a = core.stage_append_window(vec![proposal(1)]).unwrap();
    let correct = ticket(&core, &a, 1);
    let mut foreign_binding = correct.binding;
    foreign_binding.session = StoreSession::new(2).unwrap();
    let mut foreign_group = correct.group;
    foreign_group.incarnation = GroupIncarnation::new(2).unwrap();
    let wrong = [
        LogTicket {
            binding: foreign_binding,
            ..correct
        },
        LogTicket {
            batch: 0,
            ..correct
        },
        LogTicket {
            group: foreign_group,
            ..correct
        },
        LogTicket {
            revision: LogRevision::new(correct.revision.get() + 1).unwrap(),
            ..correct
        },
        LogTicket {
            generation: LogGeneration::new(correct.generation.get() + 1).unwrap(),
            ..correct
        },
        LogTicket {
            last_index: correct.last_index + 1,
            ..correct
        },
        LogTicket {
            term: correct.term + 1,
            ..correct
        },
    ];
    for invalid in wrong {
        assert_eq!(
            core.admitted_append(a.id, invalid),
            Err(RaftError::WrongCompletion)
        );
        assert_eq!(core.last_batch, 0);
        assert!(core.append_unit(a.id).unwrap().ticket.is_none());
    }
    core.admitted_append(a.id, correct).unwrap();
    retired(
        core.complete_append_unit(a.id, &completion(correct))
            .unwrap(),
    );
    let b = core.stage_append_window(vec![proposal(2)]).unwrap();
    assert_ne!(a.id, b.id);
    assert_eq!(
        core.validate_append_effect(a.id, &a.update),
        Err(RaftError::WrongCompletion)
    );
    assert_eq!(
        core.complete_append_unit(a.id, &completion(correct))
            .unwrap_err(),
        RaftError::WrongCompletion
    );
    assert!(core.can_stage_append_window());
    core.storage_failed();
    assert!(core.append_window.is_none());
    assert!(core.is_fenced());
    assert_eq!(
        core.validate_append_effect(b.id, &b.update),
        Err(RaftError::Fenced)
    );
}

#[test]
fn startup_limits_and_aggregate_count_never_raise_the_legacy_envelope() {
    let valid = limits();
    for invalid in [
        AppendWindowLimits {
            max_units: 1,
            ..valid
        },
        AppendWindowLimits {
            max_units: 65,
            ..valid
        },
        AppendWindowLimits {
            max_originals: 3,
            ..valid
        },
        AppendWindowLimits {
            max_originals: 65,
            ..valid
        },
        AppendWindowLimits {
            max_retained_bytes: 0,
            ..valid
        },
        AppendWindowLimits {
            max_retained_bytes: LogLimits::default().max_batch_bytes + 1,
            ..valid
        },
    ] {
        assert!(invalid
            .validate(LogLimits::default(), BatchLimits::default())
            .is_err());
    }
    let mut core = leader();
    for id in 1..=4 {
        core.stage_append_window(vec![proposal(id)]).unwrap();
    }
    assert!(!core.can_stage_append_window());
    let original = proposal(100);
    let rejected = core
        .stage_append_window(vec![original.clone()])
        .unwrap_err();
    assert_eq!(rejected.proposals, vec![original]);
    assert_eq!(core.append_window.as_ref().unwrap().tail.len(), 4);
    assert_eq!(core.state().last_index(), 1);
}
