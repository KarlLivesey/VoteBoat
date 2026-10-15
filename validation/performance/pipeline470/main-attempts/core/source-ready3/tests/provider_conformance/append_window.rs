// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Downstream-only Core/host storage composition; no private Core state access.
use super::*;
use std::collections::VecDeque;
use voteboat::{
    application::{
        ApplicationError, BoundedStateMachine, Counter, CounterOutcome, CounterReceipt,
        ProposalAdmission, StateMachine,
    },
    identity::*,
    log::*,
    raft::*,
};

fn limits() -> AppendWindowLimits {
    AppendWindowLimits {
        max_units: 4,
        max_originals: 8,
        max_retained_bytes: 1024 * 1024,
    }
}
fn fresh() -> (Raft, HostLogStore) {
    let mut store = HostLogStore::new(1);
    append(&mut store, vec![LogMutation::Create(bootstrap(1, 1))]);
    let core = Raft::recover(
        node(1),
        store.binding(),
        store.state(group(1)).unwrap(),
        store.limits(),
    )
    .unwrap();
    (core, store)
}
fn leader() -> (Raft, HostLogStore) {
    let (mut core, mut store) = fresh();
    core.configure_append_window(Some(limits())).unwrap();
    let mut effects = VecDeque::from(core.step(Event::Campaign).unwrap());
    for _ in 0..16 {
        let Some(effect) = effects.pop_front() else {
            break;
        };
        match effect {
            Effect::Persist(update) => {
                effects.extend(persist_effect(&mut core, &mut store, update).unwrap())
            }
            Effect::Committed(entries) => assert!(entries
                .iter()
                .all(|e| matches!(e.payload, EntryPayload::Noop))),
            _ => panic!("unexpected single-voter campaign effect"),
        }
    }
    assert!(effects.is_empty());
    assert_eq!(core.role(), Role::Leader);
    assert_eq!(core.state().commit_index, 1);
    (core, store)
}
fn proposal(id: u128, value: i64) -> Proposal {
    Proposal {
        operation: OperationId::new(id).unwrap(),
        bytes: value.to_le_bytes().to_vec(),
    }
}
fn retire(value: AppendCompletion) -> (Vec<AppendUnitId>, Vec<Effect>) {
    let AppendCompletion::Retired { units, effects } = value else {
        panic!("window not retired")
    };
    (units, effects)
}

#[test]
fn public_window_defaults_finite_limits_and_first_attempt_lock_keep_serial_admission() {
    let (mut core, _) = fresh();
    assert_eq!(core.append_window_limits(), None);
    assert!(!core.can_stage_append_window());
    for invalid in [
        AppendWindowLimits {
            max_units: 1,
            ..limits()
        },
        AppendWindowLimits {
            max_units: 65,
            ..limits()
        },
        AppendWindowLimits {
            max_originals: 3,
            ..limits()
        },
        AppendWindowLimits {
            max_originals: 65,
            ..limits()
        },
        AppendWindowLimits {
            max_retained_bytes: 0,
            ..limits()
        },
        AppendWindowLimits {
            max_retained_bytes: LogLimits::default().max_batch_bytes + 1,
            ..limits()
        },
    ] {
        assert!(invalid
            .validate(LogLimits::default(), BatchLimits::default())
            .is_err());
        assert!(core.configure_append_window(Some(invalid)).is_err());
        assert_eq!(core.append_window_limits(), None);
    }
    core.configure_append_window(Some(limits())).unwrap();
    assert_eq!(core.append_window_limits(), Some(limits()));
    core.configure_append_window(None).unwrap();
    let input = vec![proposal(u128::MAX, 5)];
    let pointer = input.as_ptr();
    let rejected = core.stage_append_window(input).unwrap_err();
    assert_eq!(rejected.reason, RaftError::Busy);
    assert_eq!(rejected.proposals.as_ptr(), pointer);
    assert_eq!(
        core.configure_append_window(Some(limits())),
        Err(RaftError::Busy)
    );
    assert!(core.step(Event::Campaign).is_ok());
}

#[test]
fn public_window_host_barrier_and_serial_oracle_preserve_one_commit_and_every_original() {
    let (mut core, mut store) = leader();
    let before = core.state().clone();
    let mut oracle = std::collections::BTreeMap::from([(group(1), before.clone())]);
    let commands = [
        proposal(u128::MAX, 5),
        proposal(1 << 127, 3),
        proposal((1 << 96) | 9, -1),
    ];
    let mut ids = Vec::new();
    let mut tickets = Vec::new();
    for (n, command) in commands.iter().cloned().enumerate() {
        let input = vec![command];
        assert!(core
            .append_window_reservation(&input, input.capacity())
            .is_some());
        let staged = core.stage_append_window(input).unwrap();
        core.validate_append_effect(staged.id, &staged.update)
            .unwrap();
        apply_batch(
            &mut oracle,
            &[LogMutation::Update(staged.update.clone())],
            store.limits(),
        )
        .unwrap();
        let ticket = store
            .append_batch(vec![LogMutation::Update(staged.update)])
            .unwrap()[0];
        if n == 0 {
            assert_eq!(core.append_replication_reservation(staged.id), None); // No remote peer.
            assert!(core
                .replicate_admitted_append(staged.id, ticket)
                .unwrap()
                .is_empty());
        } else {
            core.admitted_append(staged.id, ticket).unwrap();
        }
        ids.push(staged.id);
        tickets.push(ticket);
        assert_eq!(core.state(), &before);
    }
    let durable = store.barrier(&tickets).unwrap();
    // The union is genuinely durable, but each unit still needs its own exact event.
    assert!(matches!(
        core.complete_append_unit(ids[2], &durable).unwrap(),
        AppendCompletion::Waiting
    ));
    assert_eq!(core.state(), &before);
    assert!(!core.can_stage_append_window());
    assert!(matches!(
        core.complete_append_unit(ids[0], &durable).unwrap(),
        AppendCompletion::Waiting
    ));
    assert_eq!(core.state(), &before);
    let (retired, effects) = retire(core.complete_append_unit(ids[1], &durable).unwrap());
    assert_eq!(retired, ids);
    assert_eq!(core.state(), &oracle[&group(1)]);
    let [Effect::Persist(commit)] = effects.as_slice() else {
        panic!("one final commit plan required")
    };
    assert_eq!(commit.expected_revision, core.state().revision);
    assert_eq!(commit.commit_index, 4);
    assert!(commit.suffix.is_none());
    assert_eq!(core.state().commit_index, 1);
    apply_final_commit(&mut core, &mut store, commit.clone(), &commands);
    assert_eq!(
        core.complete_append_unit(ids[0], &durable).unwrap_err(),
        RaftError::WrongCompletion
    );
}

fn apply_final_commit(
    core: &mut Raft,
    store: &mut HostLogStore,
    commit: LogUpdate,
    commands: &[Proposal],
) {
    let mut app = Counter::new(8).unwrap();
    app.apply_batch(core.replay_committed()).unwrap();
    assert_eq!(app.read_applied(1).unwrap(), 0);
    let committed = persist_effect(core, store, commit).unwrap();
    let [Effect::Committed(entries)] = committed.as_slice() else {
        panic!("one complete application interval required")
    };
    let replies = app.apply_batch(entries).unwrap();
    assert_eq!(
        replies.iter().map(|r| r.operation).collect::<Vec<_>>(),
        commands.iter().map(|p| p.operation).collect::<Vec<_>>()
    );
    assert_eq!(
        replies.iter().map(|r| r.outcome).collect::<Vec<_>>(),
        vec![
            CounterOutcome::Value(5),
            CounterOutcome::Value(8),
            CounterOutcome::Value(7)
        ]
    );
    assert_eq!(app.applied_index(), 4);
    assert_eq!(app.read_applied(4).unwrap(), 7);
    assert_eq!(core.state(), &store.state(group(1)).unwrap());
}

#[test]
fn public_window_full_effect_and_all_ticket_fields_reject_without_sealing() {
    let (mut core, mut store) = leader();
    let staged = core
        .stage_append_window(vec![proposal(1 << 127, 7)])
        .unwrap();
    let mut altered = staged.update.clone();
    altered.suffix.as_mut().unwrap().entries[0].payload = EntryPayload::Noop;
    assert_eq!(
        core.validate_append_effect(staged.id, &altered),
        Err(RaftError::WrongCompletion)
    );
    core.validate_append_effect(staged.id, &staged.update)
        .unwrap();
    let t = store
        .append_batch(vec![LogMutation::Update(staged.update)])
        .unwrap()[0];
    let mut other_binding = t.binding;
    other_binding.session = StoreSession::new(2).unwrap();
    let mut other_group = t.group;
    other_group.incarnation = GroupIncarnation::new(2).unwrap();
    for wrong in [
        LogTicket {
            binding: other_binding,
            ..t
        },
        LogTicket {
            group: other_group,
            ..t
        },
        LogTicket { batch: 0, ..t },
        LogTicket {
            revision: LogRevision::new(t.revision.get() + 1).unwrap(),
            ..t
        },
        LogTicket {
            generation: LogGeneration::new(t.generation.get() + 1).unwrap(),
            ..t
        },
        LogTicket {
            last_index: t.last_index + 1,
            ..t
        },
        LogTicket {
            term: t.term + 1,
            ..t
        },
    ] {
        assert_eq!(
            core.admitted_append(staged.id, wrong),
            Err(RaftError::WrongCompletion)
        );
        assert!(core.can_stage_append_window());
    }
    assert_eq!(
        core.complete_append_unit(staged.id, &DurableLog { tickets: vec![t] })
            .unwrap_err(),
        RaftError::WrongCompletion
    );
    assert!(core.can_stage_append_window());
    core.admitted_append(staged.id, t).unwrap();
    assert_eq!(
        core.admitted_append(staged.id, t),
        Err(RaftError::WrongCompletion)
    );
    core.seal_append_window();
    assert!(!core.can_stage_append_window());
    let mut input = Vec::with_capacity(5);
    input.push(proposal(99, 1));
    let pointer = input.as_ptr();
    let rejected = core.stage_append_window(input).unwrap_err();
    assert_eq!(rejected.proposals.as_ptr(), pointer);
    let durable = store.barrier(&[t]).unwrap();
    retire(core.complete_append_unit(staged.id, &durable).unwrap());
    assert_eq!(
        core.complete_append_unit(staged.id, &durable).unwrap_err(),
        RaftError::WrongCompletion
    );
}

struct ContextGuard(Counter);
impl StateMachine for ContextGuard {
    type Receipt = CounterReceipt;
    fn applied_index(&self) -> u64 {
        self.0.applied_index()
    }
    fn apply_batch(
        &mut self,
        entries: &[LogEntry],
    ) -> Result<Vec<CounterReceipt>, ApplicationError> {
        self.0.apply_batch(entries)
    }
}
impl BoundedStateMachine for ContextGuard {
    fn receipt_bytes_bound(&self, entries: &[LogEntry]) -> Result<usize, ApplicationError> {
        self.0.receipt_bytes_bound(entries)
    }
}
impl ProposalAdmission for ContextGuard {
    fn supports_ready_batch(&self) -> bool {
        true
    }
    fn validate_proposal_context(
        &self,
        core: &Raft,
        _: OperationId,
        _: &[u8],
    ) -> Result<(), ApplicationError> {
        if core.has_pending_dependency() {
            Err(ApplicationError::NotApplied)
        } else {
            Ok(())
        }
    }
    fn validate_proposal<'a>(
        &self,
        operation: OperationId,
        bytes: &[u8],
        pending: impl Iterator<Item = (OperationId, &'a [u8])>,
    ) -> Result<usize, ApplicationError> {
        self.0.validate_proposal(operation, bytes, pending)
    }
}
#[test]
fn public_window_app_optin_preserves_context_and_all_pending_original_capacity() {
    let (mut core, _) = leader();
    let mut counter = Counter::new(2).unwrap();
    counter.apply_batch(core.replay_committed()).unwrap();
    let guarded = ContextGuard(counter.clone());
    assert!(guarded.supports_ready_batch());
    assert!(!guarded.supports_append_window());
    assert!(counter.supports_append_window());
    let a = proposal(u128::MAX, 5);
    let b = proposal(1 << 127, 3);
    let c = proposal((1 << 96) | 7, 1);
    guarded
        .validate_proposal_context(&core, a.operation, &a.bytes)
        .unwrap();
    counter
        .validate_proposal(a.operation, &a.bytes, std::iter::empty())
        .unwrap();
    core.stage_append_window(vec![a.clone()]).unwrap();
    assert_eq!(
        guarded.validate_proposal_context(&core, b.operation, &b.bytes),
        Err(ApplicationError::NotApplied)
    );
    counter
        .validate_proposal_context(&core, b.operation, &b.bytes)
        .unwrap();
    counter
        .validate_proposal(
            b.operation,
            &b.bytes,
            [(a.operation, a.bytes.as_slice())].into_iter(),
        )
        .unwrap();
    core.stage_append_window(vec![b.clone()]).unwrap();
    assert_eq!(
        counter.validate_proposal(
            c.operation,
            &c.bytes,
            [&a, &b]
                .into_iter()
                .map(|p| (p.operation, p.bytes.as_slice()))
        ),
        Err(ApplicationError::DedupCapacity)
    );
    // A conflicting retry retains its exact bytes but no second unique-ID slot.
    counter
        .validate_proposal(
            a.operation,
            &(-7i64).to_le_bytes(),
            [&a, &b]
                .into_iter()
                .map(|p| (p.operation, p.bytes.as_slice())),
        )
        .unwrap();
    assert_eq!(
        counter.validate_proposal(c.operation, &[0], std::iter::empty()),
        Err(ApplicationError::InvalidCommand)
    );
    assert_eq!(counter.applied_index(), 1);
    assert_eq!(counter.read_applied(1).unwrap(), 0);
    assert_eq!(core.state().last_index(), 1);
}
