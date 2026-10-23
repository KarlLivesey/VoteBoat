// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use crate::identity::*;
use std::sync::atomic::AtomicUsize;
use std::task::Wake;

fn binding(group: u128) -> WriteBinding {
    WriteBinding {
        group: GroupIdentity {
            id: GroupId::new(group).unwrap(),
            incarnation: GroupIncarnation::new(1).unwrap(),
        },
        node: NodeId::new(1).unwrap(),
        store: StoreBinding {
            identity: StoreIdentity {
                id: StoreId::new(1).unwrap(),
                incarnation: StoreIncarnation::new(1).unwrap(),
            },
            session: StoreSession::new(1).unwrap(),
        },
    }
}
fn limits<R>(originals: usize) -> WritePortLimits {
    let mut limits = WritePortLimits {
        originals,
        command_bytes: 8,
        command_capacity_bytes: originals * 8,
        receipt_bytes: size_of::<R>() + 128,
        waker_bytes: 1024,
        retained_bytes: 0,
    };
    limits.retained_bytes = limits.required_bytes::<R>().unwrap();
    limits
}
fn setup<R>(n: usize) -> (WriteDomain<R>, WriteFactory<R>, WritePort<R>) {
    let limits = limits::<R>(n);
    let (domain, factory) = WriteDomain::new(
        binding(1),
        WriteDomainLimits {
            ports: 2,
            retained_bytes: limits.retained_bytes * 3 + 65536,
        },
    )
    .unwrap();
    let port = factory.open_port(limits).unwrap();
    (domain, factory, port)
}
fn operation(value: u128) -> OperationId {
    OperationId::new(value).unwrap()
}
fn entry(value: u128, index: u64, bytes: &[u8]) -> LogEntry {
    LogEntry {
        index,
        term: 3,
        payload: EntryPayload::Command {
            operation: operation(value),
            bytes: bytes.into(),
        },
    }
}
fn input(value: u128) -> WriteInput<'static> {
    WriteInput {
        operation: operation(value),
        bytes: &[1; 8],
    }
}
fn extract<R>(submission: &mut WriteSubmission<R>) -> OriginalReply<R> {
    submission.take_next().unwrap().unwrap().2
}

#[test]
fn reverse_sparse_originals_keep_exact_content_position_and_individual_outcomes() {
    let (domain, _, mut port) = setup::<u128>(3);
    let ids = [u128::MAX - 17, 19, (1u128 << 103) + 7];
    let (mut submission, mut invocation) = port.begin(&ids.map(input)).unwrap();
    let mut replies: Vec<_> = ids
        .iter()
        .enumerate()
        .map(|(i, id)| {
            let mut reply = extract(&mut submission);
            reply
                .bind_proposal(&domain, &entry(*id, i as u64 + 1, &[1; 8]))
                .unwrap();
            (reply, entry(*id, i as u64 + 1, &[1; 8]))
        })
        .collect();
    drop(submission);
    while let Some((reply, entry)) = replies.pop() {
        let id = match entry.payload {
            EntryPayload::Command { operation, .. } => operation.get(),
            _ => unreachable!(),
        };
        reply.publish_applied(&domain, &entry, id, 0).unwrap();
    }
    for (ordinal, expected) in ids.iter().enumerate() {
        let envelope = invocation.try_take(ordinal).unwrap().unwrap();
        assert!(invocation.matches(&envelope, ordinal));
        assert_eq!(envelope.operation().get(), *expected);
        assert_eq!(
            envelope.outcome(),
            &OriginalOutcome::Applied {
                position: ProposalPosition {
                    index: ordinal as u64 + 1,
                    term: 3
                },
                receipt: *expected
            }
        );
    }
    invocation.finish().unwrap();
    port.reap().unwrap();
}
#[test]
fn wrong_content_full_id_position_and_scope_return_the_exact_receipt() {
    for kind in 0..4 {
        let (domain, _, mut port) = setup::<Vec<u8>>(1);
        let (other, _) = WriteDomain::new(
            binding(2),
            WriteDomainLimits {
                ports: 1,
                retained_bytes: 65536,
            },
        )
        .unwrap();
        let (mut submission, mut invocation) = port.begin(&[input(u128::MAX)]).unwrap();
        let mut reply = extract(&mut submission);
        let proposal = entry(u128::MAX, 7, &[1; 8]);
        reply.bind_proposal(&domain, &proposal).unwrap();
        let mut actual = proposal.clone();
        let owner = if kind == 3 { &other } else { &domain };
        match kind {
            0 => actual.payload = entry(1, 7, &[1; 8]).payload,
            1 => actual.payload = entry(u128::MAX, 7, &[2; 8]).payload,
            2 => actual.index += 1,
            _ => {}
        }
        let receipt = vec![11; 17];
        let address = receipt.as_ptr();
        let rejected = reply
            .publish_applied(owner, &actual, receipt, 17)
            .unwrap_err();
        assert_eq!(rejected.receipt.as_ptr(), address);
        assert_eq!(rejected.receipt, vec![11; 17]);
        assert!(matches!(
            invocation.try_take(0).unwrap().unwrap().into_outcome(),
            OriginalOutcome::Unknown(WriteUnknown::OwnerFailed)
        ));
    }
}
#[test]
fn cancellation_keeps_live_producer_and_exported_envelope_reservations() {
    let (domain, factory, mut port) = setup::<u64>(1);
    let baseline = factory.usage().unwrap();
    let (mut submission, invocation) = port.begin(&[input(9)]).unwrap();
    let mut reply = extract(&mut submission);
    reply.bind_proposal(&domain, &entry(9, 1, &[1; 8])).unwrap();
    drop(invocation);
    drop(submission);
    assert!(matches!(
        port.begin(&[input(9)]),
        Err(WriteError::BusyDrain)
    ));
    assert_eq!(factory.usage().unwrap(), baseline);
    assert_eq!(
        reply
            .publish_applied(&domain, &entry(9, 1, &[1; 8]), 22, 0)
            .unwrap_err()
            .reason,
        WriteError::ConsumerClosed
    );
    let (submission, mut invocation) = port.begin(&[input(9)]).unwrap();
    drop(submission);
    let output = invocation.try_take(0).unwrap().unwrap();
    assert_eq!(output.outcome(), &OriginalOutcome::NotSubmitted);
    invocation.finish().unwrap();
    assert!(matches!(
        port.begin(&[input(9)]),
        Err(WriteError::BusyDrain)
    ));
    drop(output);
    let (submission, invocation) = port.begin(&[input(9)]).unwrap();
    drop(submission);
    drop(invocation);
    drop(port);
    assert_eq!(factory.usage().unwrap().ports, 0);
}
#[test]
fn domain_close_preserves_applied_and_cannot_be_revived_by_factory() {
    let (mut domain, factory, mut port) = setup::<u64>(2);
    let (mut submission, mut invocation) = port.begin(&[input(1), input(2)]).unwrap();
    let mut first = extract(&mut submission);
    first.bind_proposal(&domain, &entry(1, 4, &[1; 8])).unwrap();
    first
        .publish_applied(&domain, &entry(1, 4, &[1; 8]), 123, 0)
        .unwrap();
    let second = extract(&mut submission);
    domain.close(WriteUnknown::Aborted);
    assert!(matches!(
        invocation.try_take(0).unwrap().unwrap().into_outcome(),
        OriginalOutcome::Applied { receipt: 123, .. }
    ));
    assert_eq!(
        invocation.try_take(1).unwrap().unwrap().into_outcome(),
        OriginalOutcome::Unknown(WriteUnknown::Aborted)
    );
    assert!(matches!(
        factory.open_port(limits::<u64>(1)),
        Err(WriteError::Closed)
    ));
    drop(second);
    drop(submission);
    invocation.finish().unwrap();
    assert_eq!(port.reap(), Err(WriteError::Closed));
}
struct Notifier {
    port: Weak<Shared<u64>>,
    count: AtomicUsize,
}
impl Wake for Notifier {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        assert!(
            self.port.upgrade().unwrap().state.try_lock().is_ok(),
            "wake ran under port lock"
        );
        self.count.fetch_add(1, Ordering::SeqCst);
    }
}
#[test]
fn completion_registration_replacement_and_wait_drop_have_no_lost_wake() {
    let (domain, _, mut port) = setup::<u64>(2);
    let (mut submission, mut invocation) = port.begin(&[input(1), input(2)]).unwrap();
    let mut first = extract(&mut submission);
    first.bind_proposal(&domain, &entry(1, 1, &[1; 8])).unwrap();
    let notifier = Arc::new(Notifier {
        port: Arc::downgrade(&port.shared),
        count: AtomicUsize::new(0),
    });
    let waker = Waker::from(notifier.clone());
    let mut cx = Context::from_waker(&waker);
    let mut wait = invocation.waiter(0).unwrap();
    assert!(Pin::new(&mut wait).poll(&mut cx).is_pending());
    first
        .publish_applied(&domain, &entry(1, 1, &[1; 8]), 1, 0)
        .unwrap();
    assert_eq!(notifier.count.load(Ordering::SeqCst), 1);
    assert!(Pin::new(&mut wait).poll(&mut cx).is_ready());
    drop(wait);
    let mut second = extract(&mut submission);
    second
        .bind_proposal(&domain, &entry(2, 2, &[1; 8]))
        .unwrap();
    drop(invocation.waiter(1).unwrap());
    assert_eq!(
        second
            .publish_applied(&domain, &entry(2, 2, &[1; 8]), 2, 0)
            .unwrap_err()
            .reason,
        WriteError::ConsumerClosed
    );
}
#[test]
fn generation_renews_only_when_quiescent_and_stale_capabilities_cannot_complete() {
    let (domain, _, mut port) = setup::<u64>(1);
    port.shared.state.lock().unwrap().generation_max = 2;
    let (mut submission, invocation) = port.begin(&[input(1)]).unwrap();
    let reply = extract(&mut submission);
    let stale = reply.key.copy();
    drop(reply);
    drop(submission);
    drop(invocation);
    for id in 2..=5 {
        let (mut submission, mut invocation) = port.begin(&[input(id)]).unwrap();
        let mut reply = extract(&mut submission);
        reply
            .bind_proposal(&domain, &entry(id, id as u64, &[1; 8]))
            .unwrap();
        assert_eq!(
            stale.check(&port.shared.state.lock().unwrap()),
            Err(WriteError::StaleInvocation)
        );
        reply
            .publish_applied(&domain, &entry(id, id as u64, &[1; 8]), id as u64, 0)
            .unwrap();
        drop(submission);
        drop(invocation.try_take(0).unwrap().unwrap());
        invocation.finish().unwrap();
    }
}

struct WakeCycle {
    shared: Weak<Shared<u64>>,
    port: Arc<Mutex<WritePort<u64>>>,
    invocation: Arc<Mutex<Option<WriteInvocation<u64>>>>,
    observed: Arc<Mutex<Option<WriteError>>>,
    calls: Arc<AtomicUsize>,
}
impl Wake for WakeCycle {
    fn wake(self: Arc<Self>) {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let shared = self.shared.upgrade().unwrap();
        match shared.state.try_lock() {
            Err(std::sync::TryLockError::WouldBlock) => {
                *self.observed.lock().unwrap() = Some(WriteError::ProviderViolation);
                return;
            }
            guard => drop(guard),
        }
        let mut invocation = self.invocation.lock().unwrap().take().unwrap();
        drop(invocation.try_take(0).unwrap().unwrap());
        invocation.finish().unwrap();
        let result = self.port.lock().unwrap().begin(&[input(2)]);
        *self.observed.lock().unwrap() = result.as_ref().err().copied();
        drop(result);
    }
}
#[test]
fn producer_token_is_genuinely_retired_before_readiness_wakes_the_next_invocation() {
    let (domain, _, mut port) = setup::<u64>(1);
    let (mut submission, invocation) = port.begin(&[input(1)]).unwrap();
    let mut reply = extract(&mut submission);
    reply.bind_proposal(&domain, &entry(1, 1, &[1; 8])).unwrap();
    drop(submission);
    let shared = port.shared.clone();
    let port = Arc::new(Mutex::new(port));
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::new(Mutex::new(None));
    let wake = Waker::from(Arc::new(WakeCycle {
        shared: Arc::downgrade(&shared),
        port: port.clone(),
        invocation: Arc::new(Mutex::new(Some(invocation))),
        observed: observed.clone(),
        calls: calls.clone(),
    }));
    // Install the current wait explicitly so the callback can own the whole
    // invocation and synchronously exercise the public consume/finish/begin.
    shared.state.lock().unwrap().waiter = Some((0, wake));
    reply
        .publish_applied(&domain, &entry(1, 1, &[1; 8]), 1, 0)
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        *observed.lock().unwrap(),
        None,
        "producer ownership must be gone before notifying a ready consumer"
    );
}

#[test]
fn stale_completion_through_the_public_publish_branch_cannot_touch_current_ownership() {
    let (domain, _, mut port) = setup::<u64>(1);
    let (mut submission, invocation) = port.begin(&[input(1)]).unwrap();
    let reply = extract(&mut submission);
    let stale = reply.key.copy();
    drop(reply);
    drop(submission);
    drop(invocation);
    let (mut submission, mut invocation) = port.begin(&[input(2)]).unwrap();
    let actual = entry(2, 2, &[1; 8]);
    let mut reply = extract(&mut submission);
    reply.bind_proposal(&domain, &actual).unwrap();
    let owners = port.shared.state.lock().unwrap().owners;
    // Private fault injection recreates an obsolete capability, not a public
    // key constructor. The call itself exercises the changed public publisher.
    let obsolete = OriginalReply {
        key: Lease::new(stale),
        ordinal: 0,
    };
    let result = obsolete.publish_applied(&domain, &actual, 999, 0);
    let (after, _displaced) = {
        let mut state = port.shared.state.lock().unwrap();
        let after = state.owners;
        let displaced = state.cells[0].output.take();
        // Restore the injected test state before asserting so a killed mutant
        // cannot cause an unrelated destructor-underflow cascade.
        state.owners = owners;
        state.cells[0].producer = Producer::Bound(ProposalPosition { index: 2, term: 3 });
        (after, displaced)
    };
    assert_eq!(
        result.unwrap_err().reason,
        WriteError::StaleInvocation,
        "stale public publish must fail at generation check"
    );
    assert_eq!(
        after, owners,
        "stale completion cannot release current ownership"
    );
    reply.publish_applied(&domain, &actual, 2, 0).unwrap();
    drop(submission);
    assert_eq!(
        invocation.try_take(0).unwrap().unwrap().into_outcome(),
        OriginalOutcome::Applied {
            position: ProposalPosition { index: 2, term: 3 },
            receipt: 2
        }
    );
}
#[test]
fn fixed_cells_payload_capacities_and_live_budget_recover_across_success_and_cancellation() {
    let (domain, factory, mut port) = setup::<u64>(7);
    let identity = Arc::as_ptr(&port.shared);
    let cells = port.shared.state.lock().unwrap().cells.as_ptr();
    let payloads: Vec<_> = port
        .shared
        .state
        .lock()
        .unwrap()
        .cells
        .iter()
        .map(|c| (c.bytes.as_ptr(), c.bytes.capacity()))
        .collect();
    let budget = factory.usage().unwrap();
    for round in 0..96u128 {
        let inputs: Vec<_> = (1..=7)
            .map(|i| input((round << 96) | (i << 64) | 17))
            .collect();
        let (mut submission, mut invocation) = port.begin(&inputs).unwrap();
        if round % 3 != 0 {
            for (ordinal, input) in inputs.iter().enumerate() {
                let mut reply = extract(&mut submission);
                let actual = entry(
                    input.operation.get(),
                    round as u64 * 7 + ordinal as u64 + 1,
                    &[1; 8],
                );
                reply.bind_proposal(&domain, &actual).unwrap();
                reply
                    .publish_applied(&domain, &actual, ordinal as u64, 0)
                    .unwrap();
            }
        }
        drop(submission);
        for ordinal in 0..7 {
            let envelope = invocation.try_take(ordinal).unwrap().unwrap();
            assert!(invocation.matches(&envelope, ordinal));
            if round % 3 == 0 {
                assert_eq!(envelope.outcome(), &OriginalOutcome::NotSubmitted);
            }
        }
        invocation.finish().unwrap();
        port.reap().unwrap();
        assert_eq!(Arc::as_ptr(&port.shared), identity);
        assert_eq!(port.shared.state.lock().unwrap().cells.as_ptr(), cells);
        let actual: Vec<_> = port
            .shared
            .state
            .lock()
            .unwrap()
            .cells
            .iter()
            .map(|c| (c.bytes.as_ptr(), c.bytes.capacity()))
            .collect();
        assert_eq!(actual, payloads);
        assert_eq!(factory.usage().unwrap(), budget);
    }
    drop(port);
    assert_eq!(factory.usage().unwrap().ports, 0);
    let next = factory.open_port(limits::<u64>(7)).unwrap();
    drop(next);
    assert_eq!(factory.usage().unwrap().ports, 0);
}
struct WakeProbe {
    port: Weak<Shared<u64>>,
    wakes: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
}
fn unlocked<R>(port: &Weak<Shared<R>>) {
    let port = port.upgrade().unwrap();
    assert!(
        !matches!(
            port.state.try_lock(),
            Err(std::sync::TryLockError::WouldBlock)
        ),
        "user callback ran under port mutex"
    );
}
impl Wake for WakeProbe {
    fn wake(self: Arc<Self>) {
        unlocked(&self.port);
        self.wakes.fetch_add(1, Ordering::SeqCst);
    }
}
impl Drop for WakeProbe {
    fn drop(&mut self) {
        unlocked(&self.port);
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}
#[test]
fn replaced_and_unused_wakers_drop_and_wake_outside_locks() {
    let (domain, _, mut port) = setup::<u64>(1);
    let (mut submission, mut invocation) = port.begin(&[input(1)]).unwrap();
    let mut reply = extract(&mut submission);
    reply.bind_proposal(&domain, &entry(1, 1, &[1; 8])).unwrap();
    let wakes = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let first = Waker::from(Arc::new(WakeProbe {
        port: Arc::downgrade(&port.shared),
        wakes: wakes.clone(),
        drops: drops.clone(),
    }));
    let mut wait = invocation.waiter(0).unwrap();
    assert!(Pin::new(&mut wait)
        .poll(&mut Context::from_waker(&first))
        .is_pending());
    drop(first);
    let second = Waker::from(Arc::new(WakeProbe {
        port: Arc::downgrade(&port.shared),
        wakes: wakes.clone(),
        drops: drops.clone(),
    }));
    assert!(Pin::new(&mut wait)
        .poll(&mut Context::from_waker(&second))
        .is_pending());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    drop(second);
    reply
        .publish_applied(&domain, &entry(1, 1, &[1; 8]), 7, 0)
        .unwrap();
    assert_eq!(wakes.load(Ordering::SeqCst), 1);
    assert_eq!(drops.load(Ordering::SeqCst), 2);
    let no_wake = Waker::noop();
    assert!(Pin::new(&mut wait)
        .poll(&mut Context::from_waker(no_wake))
        .is_ready());
}
#[test]
fn completion_between_candidate_clone_and_atomic_registration_is_seen_ready_without_a_wake() {
    let (domain, _, mut port) = setup::<u64>(1);
    let domain = Arc::new(domain);
    let (mut submission, mut invocation) = port.begin(&[input(1)]).unwrap();
    let mut reply = extract(&mut submission);
    let actual = entry(1, 1, &[1; 8]);
    reply.bind_proposal(&domain, &actual).unwrap();
    let host = domain.clone();
    *port.shared.before_observe.lock().unwrap() = Some(Box::new(move || {
        reply.publish_applied(&host, &actual, 9, 0).unwrap();
    }));
    let notifier = Arc::new(Notifier {
        port: Arc::downgrade(&port.shared),
        count: AtomicUsize::new(0),
    });
    let waker = Waker::from(notifier.clone());
    let mut wait = invocation.waiter(0).unwrap();
    assert!(
        Pin::new(&mut wait)
            .poll(&mut Context::from_waker(&waker))
            .is_ready(),
        "atomic ready check must follow the candidate-clone interleaving"
    );
    assert_eq!(notifier.count.load(Ordering::SeqCst), 0);
}
struct DropReceipt {
    port: Weak<Shared<DropReceipt>>,
    registry: Weak<Registry<DropReceipt>>,
    drops: Arc<AtomicUsize>,
    bytes: Vec<u8>,
}
impl Drop for DropReceipt {
    fn drop(&mut self) {
        unlocked(&self.port);
        assert!(
            self.registry
                .upgrade()
                .unwrap()
                .state
                .lock()
                .unwrap()
                .usage
                .ports
                >= 1,
            "exported R must drop before its live registration is released"
        );
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}
#[test]
fn arbitrary_nested_receipt_drop_and_returned_error_ownership_stay_outside_locks() {
    let (domain, factory, mut port) = setup::<DropReceipt>(1);
    let (mut submission, mut invocation) = port.begin(&[input(1)]).unwrap();
    let mut reply = extract(&mut submission);
    reply.bind_proposal(&domain, &entry(1, 1, &[1; 8])).unwrap();
    let drops = Arc::new(AtomicUsize::new(0));
    let receipt = DropReceipt {
        port: Arc::downgrade(&port.shared),
        registry: Arc::downgrade(&port.shared.registry),
        drops: drops.clone(),
        bytes: vec![1; 8],
    };
    assert!(reply
        .publish_applied(&domain, &entry(1, 1, &[1; 8]), receipt, 8)
        .is_ok());
    drop(submission);
    let envelope = invocation.try_take(0).unwrap().unwrap();
    assert!(
        matches!(envelope.outcome(),OriginalOutcome::Applied { receipt,.. } if receipt.bytes.capacity()==8)
    );
    invocation.finish().unwrap();
    drop(port);
    assert_eq!(factory.usage().unwrap().ports, 1);
    drop(envelope);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(factory.usage().unwrap().ports, 0);
}
#[test]
fn poisoned_cleanup_preserves_applied_wakes_unknown_and_never_cascades_mutex_panics() {
    let (mut domain, factory, mut port) = setup::<u64>(2);
    let (mut submission, mut invocation) = port.begin(&[input(1), input(2)]).unwrap();
    let mut first = extract(&mut submission);
    first.bind_proposal(&domain, &entry(1, 1, &[1; 8])).unwrap();
    first
        .publish_applied(&domain, &entry(1, 1, &[1; 8]), 1, 0)
        .unwrap();
    let second = extract(&mut submission);
    let wakes = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let waker = Waker::from(Arc::new(WakeProbe {
        port: Arc::downgrade(&port.shared),
        wakes: wakes.clone(),
        drops,
    }));
    let mut wait = invocation.waiter(1).unwrap();
    assert!(Pin::new(&mut wait)
        .poll(&mut Context::from_waker(&waker))
        .is_pending());
    let poisoned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _state = port.shared.state.lock().unwrap();
        panic!("test-only port poison");
    }));
    assert!(poisoned.is_err());
    assert!(matches!(
        Pin::new(&mut wait).poll(&mut Context::from_waker(&waker)),
        Poll::Ready(Err(WriteError::Poisoned))
    ));
    drop(wait);
    assert_eq!(wakes.load(Ordering::SeqCst), 1);
    assert!(matches!(
        invocation.try_take(0).unwrap().unwrap().into_outcome(),
        OriginalOutcome::Applied { receipt: 1, .. }
    ));
    assert_eq!(
        invocation.try_take(1).unwrap().unwrap().into_outcome(),
        OriginalOutcome::Unknown(WriteUnknown::OwnerFailed)
    );
    drop(second);
    drop(submission);
    invocation.finish().unwrap();
    domain.close(WriteUnknown::OwnerFailed);
    assert_eq!(port.reap(), Err(WriteError::Closed));
    assert!(matches!(
        factory.open_port(limits::<u64>(1)),
        Err(WriteError::Closed)
    ));
}
#[test]
fn bounded_registration_full_slots_oversize_shape_and_opaque_domains_fail_before_admission() {
    let (domain, factory, mut port) = setup::<u64>(1);
    let another = factory.open_port(limits::<u64>(1)).unwrap();
    assert!(matches!(
        factory.open_port(limits::<u64>(1)),
        Err(WriteError::Overloaded)
    ));
    assert!(matches!(port.begin(&[]), Err(WriteError::InvalidShape)));
    assert!(matches!(
        port.begin(&[WriteInput {
            operation: operation(1),
            bytes: &[1; 9]
        }]),
        Err(WriteError::InvalidShape)
    ));
    let (submission, invocation) = port.begin(&[input(1)]).unwrap();
    let (same_binding, _) = WriteDomain::<u64>::new(
        domain.binding(),
        WriteDomainLimits {
            ports: 1,
            retained_bytes: 65536,
        },
    )
    .unwrap();
    assert_eq!(
        same_binding.validate_submission(&submission),
        Err(WriteError::WrongBinding)
    );
    drop(submission);
    drop(invocation);
    drop(another);
    let recovered = factory.open_port(limits::<u64>(1)).unwrap();
    drop(recovered);
    assert_eq!(factory.usage().unwrap().ports, 1);
    let mut too_small = limits::<u64>(1);
    too_small.retained_bytes -= 1;
    assert!(matches!(
        factory.open_port(too_small),
        Err(WriteError::InvalidLimits)
    ));
}

#[test]
fn unknown_ordinal_and_repeated_polling_do_not_cancel_live_originals() {
    let (domain, _, mut port) = setup::<u64>(1);
    let (mut submission, mut invocation) = port.begin(&[input(9)]).unwrap();
    assert!(matches!(
        invocation.waiter(1),
        Err(WriteError::UnknownOriginal)
    ));
    assert_eq!(
        invocation.try_take(usize::MAX).unwrap_err(),
        WriteError::UnknownOriginal
    );
    for _ in 0..5 {
        assert!(invocation.try_take(0).unwrap().is_none());
    }
    let mut reply = extract(&mut submission);
    reply.bind_proposal(&domain, &entry(9, 1, &[1; 8])).unwrap();
    reply
        .publish_applied(&domain, &entry(9, 1, &[1; 8]), 99, 0)
        .unwrap();
    assert_eq!(
        invocation.try_take(0).unwrap().unwrap().into_outcome(),
        OriginalOutcome::Applied {
            position: ProposalPosition { index: 1, term: 3 },
            receipt: 99
        }
    );
    assert_eq!(
        invocation.try_take(0).unwrap_err(),
        WriteError::AlreadyTerminal
    );
}
