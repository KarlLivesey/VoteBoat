// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Changed public producer and NORMAL Actor consumer controls.
use super::*;
use crate::voteboat_engine::writes::{Client, Observer};
fn client(
    sender: mpsc::Sender<Request>,
    factory: &WriteFactory<OperationId>,
    width: usize,
) -> Client {
    Client::new(
        sender,
        factory,
        crate::voteboat_engine::writes::port_limits(width).unwrap(),
    )
    .unwrap()
}
fn prepare(client: &mut Client, ids: &[u128]) -> (Request, WriteInvocation<OperationId>) {
    let (request, observer) = client.prepare(ids).unwrap();
    assert!(
        matches!(request, Request::Port(_)),
        "normal configured write must use real public port"
    );
    let Observer::Port(invocation) = observer else {
        panic!("normal write must observe public individual outcomes");
    };
    (request, invocation)
}
fn take(
    invocation: &mut WriteInvocation<OperationId>,
    ordinal: usize,
    expected: u128,
) -> OperationId {
    let envelope = invocation
        .try_take(ordinal)
        .unwrap()
        .expect("each original must be ready");
    assert!(invocation.matches(&envelope, ordinal));
    assert_eq!(envelope.operation().get(), expected);
    let OriginalOutcome::Applied { position, receipt } = envelope.into_outcome() else {
        panic!("real applied original required");
    };
    assert!(position.index > 1);
    assert!(position.term > 0);
    assert_eq!(receipt.get(), expected);
    receipt
}
fn commit_update(actor: &mut Actor, request: Request) -> VolatilePersist {
    let first = actor.parts.core.state().last_index() + 1;
    let n = request.len();
    let mut batch = staging(actor);
    actor.include(request, &mut batch).unwrap();
    actor.unbound = Some((first, n));
    let mut effects = actor.parts.core.propose_entries(batch).unwrap();
    let Effect::PersistVolatile(append) = effects.remove(0) else {
        panic!("actual proposal append");
    };
    let mut next = persist_volatile(&mut actor.parts.core, &actor.parts.store.log, append)
        .unwrap()
        .into_owned();
    actor.bind_installed().unwrap();
    let Effect::PersistVolatile(commit) = next.remove(0) else {
        panic!("actual current-term quorum commitment");
    };
    commit
}
#[test]
fn public_ports_use_borrowed_and_materialized_fifo_with_the_same_partial_delivery_prefix() {
    for width in [1usize, 231, 515] {
        for queued in [false, true] {
            let (mut actor, sender, factory) = ready_actor_with_factory::<false>(
                crate::Envelopes::UpstreamDefault,
                width.div_ceil(4),
            );
            let mut client = client(sender, &factory, width);
            let ids: Vec<_> = (0..width)
                .map(|i| u128::MAX - ((i as u128) << 65))
                .collect();
            let (request, mut invocation) = prepare(&mut client, &ids);
            let commit = commit_update(&mut actor, request);
            assert!(invocation.try_take(0).unwrap().is_none());
            let cancelled = width / 2;
            invocation.cancel_wait(cancelled).unwrap();
            let mut effects = vec![Effect::PersistVolatile(commit)];
            if queued {
                effects.push(Effect::Committed(Vec::new()));
            }
            assert_eq!(
                actor.effects(effects).unwrap_err().to_string(),
                "original client dropped result wait"
            );
            assert_eq!(actor.parts.application.applied, width as u64 + 1);
            assert_eq!(actor.parts.core.state().commit_index, width as u64 + 1);
            for (ordinal, id) in ids.iter().copied().enumerate().take(cancelled) {
                take(&mut invocation, ordinal, id);
            }
            assert_eq!(
                invocation.try_take(cancelled).unwrap_err(),
                WriteError::ConsumerClosed
            );
            assert_eq!(actor.pending.len(), width - cancelled - 1);
            for ordinal in cancelled + 1..width {
                assert!(invocation.try_take(ordinal).unwrap().is_none());
            }
            drop(actor);
            for ordinal in cancelled + 1..width {
                assert_eq!(
                    invocation
                        .try_take(ordinal)
                        .unwrap()
                        .unwrap()
                        .into_outcome(),
                    OriginalOutcome::Unknown(WriteUnknown::OwnerFailed)
                );
            }
        }
    }
}
#[test]
fn public_application_failure_publishes_nothing_and_keeps_prior_success_and_fifo_predecessors() {
    for queued in [false, true] {
        let (mut actor, sender, factory) =
            ready_actor_with_factory::<false>(crate::Envelopes::Common64, 2);
        let mut first = client(sender.clone(), &factory, 1);
        let (request, mut prior) = prepare(&mut first, &[17]);
        actor.requests(request).unwrap();
        let mut second = client(sender, &factory, 2);
        let ids = [1u128 << 120, u128::MAX];
        let (request, mut invocation) = prepare(&mut second, &ids);
        let commit = commit_update(&mut actor, request);
        let mut effects = vec![Effect::PersistVolatile(commit)];
        if queued {
            effects.push(Effect::Committed(vec![LogEntry {
                index: 99,
                term: 1,
                payload: EntryPayload::Noop,
            }]));
        } else {
            actor.parts.application.applied = 99;
        }
        assert!(actor
            .effects(effects)
            .unwrap_err()
            .to_string()
            .contains("IndexGap"));
        assert!(
            invocation.try_take(0).unwrap().is_none(),
            "no receipt may escape before whole-interval apply"
        );
        assert!(invocation.try_take(1).unwrap().is_none());
        drop(actor);
        take(&mut prior, 0, 17);
        for ordinal in 0..2 {
            assert_eq!(
                invocation
                    .try_take(ordinal)
                    .unwrap()
                    .unwrap()
                    .into_outcome(),
                OriginalOutcome::Unknown(WriteUnknown::OwnerFailed)
            );
        }
    }
}
#[test]
fn public_previous_send_error_precedes_owned_application_and_replies() {
    let (mut actor, sender, factory) =
        ready_actor_with_factory::<false>(crate::Envelopes::Common64, 1);
    let mut client = client(sender, &factory, 1);
    let (request, mut invocation) = prepare(&mut client, &[u128::MAX]);
    let commit = commit_update(&mut actor, request);
    let message = Message {
        group: actor.parts.application.group,
        configuration: ConfigurationId::new(1).unwrap(),
        from: NodeId::new(1).unwrap(),
        sender: actor.parts.store.log.binding(),
        to: NodeId::new(2).unwrap(),
        term: 1,
        context: RequestContext {
            origin: actor.parts.store.log.binding(),
            sequence: 1,
        },
        rpc: Rpc::ReadProbe,
    };
    assert_eq!(
        actor
            .effects(vec![Effect::PersistVolatile(commit), Effect::Send(message)])
            .unwrap_err()
            .to_string(),
        "unknown recipient"
    );
    assert_eq!(actor.parts.application.applied, 1);
    assert!(invocation.try_take(0).unwrap().is_none());
    drop(actor);
    assert!(matches!(
        invocation.try_take(0).unwrap().unwrap().into_outcome(),
        OriginalOutcome::Unknown(_)
    ));
}
#[test]
fn public_duplicate_admission_keeps_baseline_replacement_prefix_and_tail_closure() {
    let (mut actor, sender, factory) =
        ready_actor_with_factory::<false>(crate::Envelopes::Common64, 2);
    let mut old = client(sender.clone(), &factory, 1);
    let (request, mut prior) = prepare(&mut old, &[u128::MAX]);
    actor.include(request, &mut staging(&actor)).unwrap();
    let mut new = client(sender, &factory, 3);
    let ids = [1u128 << 111, u128::MAX, 17];
    let (request, mut invocation) = prepare(&mut new, &ids);
    let mut batch = staging(&actor);
    assert_eq!(
        actor.include(request, &mut batch).unwrap_err().to_string(),
        "duplicate original admission"
    );
    assert_eq!(
        prior.try_take(0).unwrap().unwrap().into_outcome(),
        OriginalOutcome::Unknown(WriteUnknown::OwnerFailed)
    );
    assert!(invocation.try_take(0).unwrap().is_none());
    assert!(invocation.try_take(1).unwrap().is_none());
    assert_eq!(
        invocation.try_take(2).unwrap().unwrap().into_outcome(),
        OriginalOutcome::NotSubmitted
    );
    assert_eq!(actor.pending.len(), 2);
    assert_eq!(batch.len(), 1);
    drop(actor);
    for ordinal in 0..2 {
        assert!(matches!(
            invocation
                .try_take(ordinal)
                .unwrap()
                .unwrap()
                .into_outcome(),
            OriginalOutcome::Unknown(_)
        ));
    }
}
#[test]
fn public_scope_cancellation_before_step_and_busy_drain_until_real_pending_owner_drops() {
    let (mut actor, sender, factory) =
        ready_actor_with_factory::<false>(crate::Envelopes::Common64, 1);
    let mut client = client(sender, &factory, 4);
    let ids = [17, u128::MAX];
    let (request, invocation) = prepare(&mut client, &ids);
    actor.include(request, &mut staging(&actor)).unwrap();
    drop(invocation);
    let Err(error) = client.prepare(&ids) else {
        panic!("pending producer must keep BusyDrain");
    };
    assert!(error.to_string().contains("BusyDrain"));
    drop(actor);
    let Err(error) = client.prepare(&ids) else {
        panic!("dead host cannot revive");
    };
    assert!(error.to_string().contains("Closed"));
    assert!(matches!(
        factory.open_port(crate::voteboat_engine::writes::port_limits(1).unwrap()),
        Err(WriteError::Closed)
    ));
}

fn drive_round(
    actors: &mut [Actor],
    sender: &mpsc::Sender<Request>,
    pool: &mut super::super::r3_ports::Pool<'_>,
    next: &mut u128,
    width: usize,
    per: usize,
    publish: bool,
) -> usize {
    let mut left = width;
    let mut requests = 0;
    while left > 0 {
        let n = left.min(per);
        left -= n;
        pool.enqueue(next, n).unwrap();
        requests += 1;
    }
    let first = actors[0].parts.receiver.try_recv().unwrap();
    actors[0].requests(first).unwrap();
    while let Ok(more) = actors[0].parts.receiver.try_recv() {
        actors[0].requests(more).unwrap();
    }
    if let Some(more) = actors[0].deferred.take() {
        actors[0].requests(more).unwrap();
    }
    super::super::r3_steps::r3_service(&mut actors[0], publish).unwrap();
    super::super::r3_steps::r3_pump(actors, publish).unwrap();
    let _ = sender;
    requests
}
fn drain(pool: &mut super::super::r3_ports::Pool<'_>, expected_first: u128) -> usize {
    let mut count = 0;
    while let Some(actual) = pool.try_take_front().unwrap() {
        assert_eq!(actual, expected_first + count as u128);
        count += 1;
    }
    count
}
#[test]
fn actual_three_replicas_all_widths_cross_two_default_boundaries_and_resume_after_snapshot_install()
{
    for width in [1usize, 231, 515] {
        let per = if width == 1 { 1 } else { 4 };
        let clients_count = width.div_ceil(per);
        let collection::Replicas {
            mut actors,
            clients,
            factories,
            ..
        } = collection::replicas_for(clients_count);
        assert_eq!(actors.len(), factories.len());
        let binding = actors[0].parts.domain.binding();
        let (domain, factory, maximum) =
            super::super::r3_ports::registration(binding, clients_count * 4 + 1, width, per)
                .unwrap();
        actors[0].parts.domain = domain;
        let mut pool =
            super::super::r3_ports::Pool::new(&factory, &clients[0], width, per, maximum).unwrap();
        actors[0].step(Event::Campaign).unwrap();
        super::super::r3_steps::r3_pump(&mut actors, true).unwrap();
        let mut next = 1;
        let rounds = 12000usize.div_ceil(width);
        let mut replies = 0;
        for _ in 0..rounds {
            let first = next;
            assert_eq!(
                drive_round(
                    &mut actors,
                    &clients[0],
                    &mut pool,
                    &mut next,
                    width,
                    per,
                    true
                ),
                width.div_ceil(per)
            );
            replies += drain(&mut pool, first);
            assert_eq!(
                pool.unreplied(),
                0,
                "healthy original pump resolves every round"
            );
        }
        assert_eq!(replies, rounds * width);
        assert_eq!(pool.live_ports(), width.div_ceil(per));
        assert_default_checkpoints(&actors, next, replies, width);
        // Hold the actual existing follower and its queued peer progress. The
        // same cores, default suffix policy and public ports remain in place.
        let paused = actors[2].parts.paused.clone();
        paused[2].store(true, Ordering::Release);
        let lag_rounds = 6000usize.div_ceil(width);
        for _ in 0..lag_rounds {
            let first = next;
            partition_round(&mut actors, &mut pool, &mut next, width, per);
            assert_eq!(drain(&mut pool, first), width);
        }
        assert!(actors[2].parts.application.applied < next as u64);
        paused[2].store(false, Ordering::Release);
        super::super::r3_steps::r3_pump(&mut actors, true).unwrap();
        assert!(
            actors[2].parts.stats[2]
                .snapshot_installs
                .load(Ordering::Relaxed)
                > 0,
            "actual lagging snapshot installation required"
        );
        let first = next;
        drive_round(
            &mut actors,
            &clients[0],
            &mut pool,
            &mut next,
            width,
            per,
            true,
        );
        assert_eq!(drain(&mut pool, first), width);
        for actor in &actors {
            assert_eq!(actor.parts.core.state().commit_index, next as u64);
            assert_eq!(actor.parts.application.applied, next as u64);
        }
        println!("width={width} per={per} actual_requests={} healthy_originals={replies} snapshot_installs={} resumed_originals={width} live_reusable_ports={} pool_bookkeeping_capacity_bytes={}",width.div_ceil(per),actors[2].parts.stats[2].snapshot_installs.load(Ordering::Relaxed),pool.live_ports(),pool.bookkeeping_bytes());
    }
}
#[test]
fn actual_adapter_trace_guards_preserve_telemetry_only_and_skipped_original_controls() {
    for width in [1usize, 231, 515] {
        let per = if width == 1 { 1 } else { 4 };
        let client_count = width.div_ceil(per);
        for control in 0..3 {
            let collection::Replicas {
                mut actors,
                clients,
                ..
            } = collection::replicas_for(client_count);
            let (domain, factory, maximum) = super::super::r3_ports::registration(
                actors[0].parts.domain.binding(),
                client_count * 4 + 1,
                width,
                per,
            )
            .unwrap();
            actors[0].parts.domain = domain;
            let mut pool =
                super::super::r3_ports::Pool::new(&factory, &clients[0], width, per, maximum)
                    .unwrap();
            let publish = control != 1;
            actors[0].step(Event::Campaign).unwrap();
            super::super::r3_steps::r3_pump(&mut actors, publish).unwrap();
            let mut next = 1;
            let mut replied = 0;
            let mut chunks = Vec::new();
            for _ in 0..3 {
                let before = next;
                let requests = drive_round(
                    &mut actors,
                    &clients[0],
                    &mut pool,
                    &mut next,
                    width,
                    per,
                    publish,
                );
                chunks.push((before, next - 1, requests, width % per));
                while let Some(id) = pool.try_take_front().unwrap() {
                    if !(control == 2 && id == 1) {
                        replied += 1;
                    }
                }
            }
            let expected = 3 * width + 1;
            let commit: Vec<_> = actors
                .iter()
                .map(|a| a.parts.core.state().commit_index)
                .collect();
            let app: Vec<_> = actors.iter().map(|a| a.parts.application.applied).collect();
            let reported: Vec<_> = actors
                .iter()
                .map(|a| a.parts.frontiers[a.parts.slot].load(Ordering::Acquire))
                .collect();
            let receipts: Vec<_> = actors
                .iter()
                .map(|a| {
                    a.parts.stats[a.parts.slot]
                        .application_receipts
                        .load(Ordering::Relaxed)
                })
                .collect();
            let checks = [
                ("reply_count", replied == 3 * width),
                ("unreplied", pool.unreplied() == 0),
                ("commit", commit == vec![expected as u64; 3]),
                ("application", app == vec![expected as u64; 3]),
                ("reported", reported == vec![expected as u64; 3]),
                (
                    "all3_owned_receipts",
                    receipts == vec![(3 * width) as u64; 3],
                ),
                (
                    "nonfenced",
                    actors.iter().all(|a| !a.parts.core.is_fenced()),
                ),
            ];
            let failed: Vec<_> = checks
                .iter()
                .filter(|(_, ok)| !ok)
                .map(|(name, _)| *name)
                .collect();
            assert_eq!(
                failed,
                match control {
                    0 => vec![],
                    1 => vec!["reported"],
                    _ => vec!["reply_count"],
                }
            );
            let intervals = actual_intervals(&actors);
            println!(
                "{}",
                serde_json::json!({"scope":"untimed_actual_candidate_adapter_control","width":width,"per":per,"control":control,"chunks_first_last_request_count_tail":chunks,"replied":replied,"remaining_originals":pool.unreplied(),"commit":commit,"applied":app,"reported":reported,"actual_owned_receipts":receipts,"actual_application_intervals":intervals,"failed_conjuncts":failed,"pool_ports":pool.live_ports(),"bookkeeping_capacity_bytes":pool.bookkeeping_bytes()})
            );
        }
    }
}

fn committed_owned(actor: &mut Actor, commit: VolatilePersist) -> Vec<LogEntry> {
    let effects = persist_volatile(&mut actor.parts.core, &actor.parts.store.log, commit)
        .unwrap()
        .into_owned();
    let mut entries = Vec::new();
    for effect in effects {
        let Effect::Committed(mut actual) = effect else {
            panic!("actual single-voter commit interval required");
        };
        entries.append(&mut actual);
    }
    let receipts = actor.parts.application.apply_batch(&entries).unwrap();
    assert_eq!(receipts.len(), entries.len());
    assert_eq!(
        actor.parts.application.applied,
        actor.parts.core.state().commit_index
    );
    entries
}
#[test]
fn malformed_current_results_publish_nothing_and_keep_all_pending_in_both_contexts() {
    for (owned, kind) in [false, true]
        .into_iter()
        .flat_map(|owned| (0..6).map(move |kind| (owned, kind)))
    {
        let (mut actor, sender, factory) =
            ready_actor_with_factory::<false>(crate::Envelopes::UpstreamDefault, 2);
        let mut first = client(sender.clone(), &factory, 1);
        let (request, mut prior) = prepare(&mut first, &[11]);
        actor.requests(request).unwrap();
        let mut client = client(sender, &factory, 3);
        let ids = [17, (1u128 << 120) | 17, u128::MAX];
        let (request, mut invocation) = prepare(&mut client, &ids);
        let commit = commit_update(&mut actor, request);
        let materialized;
        let mut effects = VecDeque::new();
        let entries = if owned {
            materialized = committed_owned(&mut actor, commit);
            super::super::applied::Applied::Owned(&materialized)
        } else {
            let entries = persist_volatile_into(
                &mut actor.parts.core,
                &actor.parts.store.log,
                commit,
                &mut effects,
            )
            .unwrap()
            .unwrap();
            let receipts = actor.parts.application.apply_entries(entries).unwrap();
            assert_eq!(receipts.len(), ids.len());
            super::super::applied::Applied::Borrowed(entries)
        };
        let values = match kind {
            0 => vec![ids[0]],
            1 => vec![ids[0], ids[1], ids[2], 37],
            2 => vec![ids[0], ids[1], ids[1]],
            3 => vec![ids[0], ids[1], ids[2] ^ (1u128 << 100)],
            4 => vec![ids[1], ids[0], ids[2]],
            _ => vec![],
        };
        let values = values
            .into_iter()
            .map(|id| OperationId::new(id).unwrap())
            .collect();
        let error = Actor::<false>::deliver_results(
            &mut actor.pending,
            &actor.parts.domain,
            entries,
            values,
        )
        .unwrap_err();
        // Check the publication boundary first: the old behavior must be killed
        // here, rather than by a changed error string or a pending-count check.
        for ordinal in 0..ids.len() {
            assert!(
                invocation.try_take(ordinal).unwrap().is_none(),
                "malformed current receipt set must publish no new Applied: owned={owned} kind={kind} ordinal={ordinal}"
            );
        }
        assert!(error.to_string().contains(match kind {
            0 | 5 => "missing original",
            1 => "extra original",
            _ => "differs",
        }));
        assert_eq!(actor.pending.len(), ids.len());
        for id in ids {
            assert!(actor
                .pending
                .get_mut(&OperationId::new(id).unwrap())
                .is_some());
        }
        assert!(effects.is_empty());
        assert_eq!(actor.parts.application.applied, 5);
        assert_eq!(actor.parts.core.state().commit_index, 5);
        drop(actor);
        take(&mut prior, 0, 11);
        for ordinal in 0..ids.len() {
            assert_eq!(
                invocation
                    .try_take(ordinal)
                    .unwrap()
                    .unwrap()
                    .into_outcome(),
                OriginalOutcome::Unknown(WriteUnknown::OwnerFailed)
            );
        }
    }
}
#[test]
fn actual_adapter_polling_carries_global_fifo_prefix_and_counts_originals() {
    let (mut actor, sender, _) =
        ready_actor_with_factory::<false>(crate::Envelopes::UpstreamDefault, 2);
    let (domain, factory, maximum) =
        super::super::r3_ports::registration(actor.parts.domain.binding(), 9, 8, 4).unwrap();
    actor.parts.domain = domain;
    let mut pool = super::super::r3_ports::Pool::new(&factory, &sender, 8, 4, maximum).unwrap();
    let mut next = 1;
    pool.enqueue(&mut next, 4).unwrap();
    pool.enqueue(&mut next, 4).unwrap();
    assert_eq!(pool.unreplied(), 8);
    let mut entries = Vec::new();
    for _ in 0..2 {
        let request = actor.parts.receiver.try_recv().unwrap();
        let commit = commit_update(&mut actor, request);
        entries.extend(committed_owned(&mut actor, commit));
    }
    for id in 5..=8 {
        let op = OperationId::new(id).unwrap();
        let entry = entries
            .iter()
            .find(|e| matches!(e.payload,EntryPayload::Command { operation,.. } if operation==op))
            .unwrap();
        actor
            .pending
            .remove(&op)
            .unwrap()
            .publish(&actor.parts.domain, entry, op)
            .unwrap();
    }
    for _ in 0..3 {
        assert_eq!(pool.try_take_front().unwrap(), None);
        assert_eq!(pool.unreplied(), 8);
    }
    for id in 1..=4 {
        let op = OperationId::new(id).unwrap();
        let entry = entries
            .iter()
            .find(|e| matches!(e.payload,EntryPayload::Command { operation,.. } if operation==op))
            .unwrap();
        actor
            .pending
            .remove(&op)
            .unwrap()
            .publish(&actor.parts.domain, entry, op)
            .unwrap();
        assert_eq!(pool.try_take_front().unwrap(), Some(id));
        assert_eq!(pool.unreplied(), 8 - id as usize);
        if id < 4 {
            assert_eq!(pool.try_take_front().unwrap(), None);
        }
    }
    for id in 5..=8 {
        assert_eq!(pool.try_take_front().unwrap(), Some(id));
    }
    assert_eq!(pool.unreplied(), 0);
    let capacity = pool.bookkeeping_bytes();
    pool.enqueue(&mut next, 4).unwrap();
    assert_eq!(pool.live_ports(), 2);
    assert_eq!(pool.bookkeeping_bytes(), capacity);
}

fn partition_round(
    actors: &mut [Actor],
    pool: &mut super::super::r3_ports::Pool<'_>,
    next: &mut u128,
    width: usize,
    per: usize,
) {
    let mut left = width;
    while left > 0 {
        let n = left.min(per);
        left -= n;
        pool.enqueue(next, n).unwrap();
    }
    let first_request = actors[0].parts.receiver.try_recv().unwrap();
    actors[0].requests(first_request).unwrap();
    super::super::r3_steps::r3_service(&mut actors[0], true).unwrap();
    // Original peer order, with only the deliberately partitioned
    // follower omitted; no replacement core or fabricated completion.
    loop {
        let mut any = false;
        for actor in actors.iter_mut().take(2) {
            while let Ok(message) = actor.parts.peer_receiver.try_recv() {
                actor.step(Event::Receive(message)).unwrap();
                super::super::r3_steps::r3_service(actor, true).unwrap();
                any = true;
            }
        }
        if !any {
            break;
        }
    }
}

fn actual_intervals(actors: &[Actor]) -> Vec<Vec<(u64, u64, usize)>> {
    let intervals: Vec<_> = actors
        .iter()
        .map(|a| {
            a.parts.stats[a.parts.slot]
                .application_intervals
                .lock()
                .unwrap()
                .clone()
        })
        .collect();
    intervals
}

fn assert_default_checkpoints(actors: &[Actor], next: u128, replies: usize, width: usize) {
    for actor in actors {
        let slot = actor.parts.slot;
        assert_eq!(actor.parts.core.state().commit_index, next as u64);
        assert_eq!(actor.parts.application.applied, next as u64);
        assert_eq!(
            actor.parts.stats[slot]
                .application_receipts
                .load(Ordering::Relaxed),
            replies as u64
        );
        assert_eq!(
            actor.parts.stats[slot]
                .checkpoint_count
                .load(Ordering::Relaxed),
            2
        );
        assert!(actor.parts.core.state().base_index() >= 9000);
        assert!(actor.parts.core.state().entries.len() <= 6000 + width);
        assert_eq!(actor.parts.snapshots.pins.len(), 1);
        assert!(!actor.parts.core.is_fenced());
    }
}
