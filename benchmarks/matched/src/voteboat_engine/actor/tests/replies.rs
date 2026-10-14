// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

type Wait = oneshot::Receiver<OperationId>;

fn operation(value: u128) -> OperationId {
    OperationId::new(value).unwrap()
}

fn request(ids: &[OperationId]) -> (Request, Vec<Wait>) {
    let mut originals = Vec::new();
    let mut waits = Vec::new();
    for &operation in ids {
        let (reply, wait) = oneshot::channel();
        originals.push(Original {
            operation,
            bytes: Vec::new(),
            reply,
        });
        waits.push(wait);
    }
    (Request { originals }, waits)
}

fn received(waits: Vec<Wait>, ids: &[OperationId]) {
    assert_eq!(waits.len(), ids.len());
    for (mut wait, &expected) in waits.into_iter().zip(ids) {
        assert_eq!(wait.try_recv().unwrap(), expected);
    }
}

fn canceled(wait: &mut Wait) {
    assert_eq!(wait.try_recv(), Err(oneshot::error::TryRecvError::Closed));
}

#[test]
fn nonmonotonic_full_width_ids_commit_in_proposal_order_with_every_receipt() {
    let (mut actor, _sender) = ready_actor::<false>(crate::Envelopes::UpstreamDefault);
    let mut ids: Vec<_> = (0..128)
        .map(|n| operation(1_u128 << ((n * 73) % 128)))
        .collect();
    ids.extend([
        operation(u128::MAX),
        operation(0xfedc_ba98_7654_3210_0123_4567_89ab_cdef),
    ]);
    let (request, waits) = request(&ids);
    actor.requests(request).unwrap();
    let committed: Vec<_> = actor
        .parts
        .core
        .state()
        .entries
        .iter()
        .filter_map(|entry| match &entry.payload {
            EntryPayload::Command { operation, .. } => Some(*operation),
            _ => None,
        })
        .collect();
    assert_eq!(committed, ids);
    assert_eq!(actor.parts.application.applied, ids.len() as u64 + 1);
    assert_eq!(actor.parts.core.state().commit_index, ids.len() as u64 + 1);
    received(waits, &ids);
    assert!(actor.pending.is_empty());
}

#[test]
fn duplicate_existing_replaces_old_wait_and_keeps_only_admitted_prefix() {
    let (mut actor, _sender) = ready_actor_clients::<false>(crate::Envelopes::UpstreamDefault, 1);
    let old_id = operation(u128::MAX);
    let (old, mut old_waits) = request(&[old_id]);
    let mut proposals = Vec::new();
    actor.include(old, &mut proposals).unwrap();
    let prefix = operation(1 << 100);
    let tail = operation(17);
    let (duplicate, mut waits) = request(&[prefix, old_id, tail]);
    assert_eq!(
        actor
            .include(duplicate, &mut proposals)
            .unwrap_err()
            .to_string(),
        "duplicate original admission"
    );
    canceled(&mut old_waits[0]);
    canceled(&mut waits[2]);
    assert_eq!(
        proposals.iter().map(|p| p.operation).collect::<Vec<_>>(),
        [old_id, prefix]
    );
    assert_eq!(actor.pending.len(), 2);
    actor.deliver(vec![prefix, old_id]).unwrap();
    assert_eq!(waits[0].try_recv().unwrap(), prefix);
    assert_eq!(waits[1].try_recv().unwrap(), old_id);
    assert!(actor.pending.is_empty());
}

#[test]
fn duplicate_within_batch_replaces_first_wait_and_cancels_unvisited_tail() {
    let (mut actor, _sender) = ready_actor_clients::<false>(crate::Envelopes::UpstreamDefault, 1);
    let id = operation(1 << 127);
    let (duplicate, mut waits) = request(&[id, id, operation(9)]);
    let mut proposals = Vec::new();
    assert_eq!(
        actor
            .include(duplicate, &mut proposals)
            .unwrap_err()
            .to_string(),
        "duplicate original admission"
    );
    canceled(&mut waits[0]);
    canceled(&mut waits[2]);
    assert_eq!(proposals.len(), 1);
    assert_eq!(proposals[0].operation, id);
    assert_eq!(actor.pending.len(), 1);
    actor.deliver(vec![id]).unwrap();
    assert_eq!(waits[1].try_recv().unwrap(), id);
}

#[test]
fn shape_and_budget_gates_preserve_prior_pending_and_proposal_prefix() {
    checked_budget(1);
    checked_budget(4096);
}

fn checked_budget(clients: usize) {
    let (mut actor, _sender) =
        ready_actor_clients::<false>(crate::Envelopes::UpstreamDefault, clients);
    let bound = clients * 4 + 1;
    let ids: Vec<_> = (1..=bound).map(|n| operation((n as u128) << 64)).collect();
    let mut proposals = Vec::new();
    let mut admitted_waits = Vec::new();
    for chunk in ids.chunks(actor.parts.core.batch_limits().max_proposal_entries) {
        let (next, waits) = request(chunk);
        actor.include(next, &mut proposals).unwrap();
        admitted_waits.extend(waits);
    }
    assert_eq!(actor.pending.len(), bound);
    let (over, mut over_waits) = request(&[ids[0]]);
    assert_eq!(
        actor.include(over, &mut proposals).unwrap_err().to_string(),
        "original result budget exceeded"
    );
    canceled(&mut over_waits[0]);
    let (mut malformed, mut malformed_waits) = request(&[operation(3)]);
    malformed.originals.clear();
    assert_eq!(
        actor
            .include(malformed, &mut proposals)
            .unwrap_err()
            .to_string(),
        "original proposal/reply shape differs"
    );
    canceled(&mut malformed_waits[0]);
    assert_eq!(actor.pending.len(), bound);
    assert_eq!(
        proposals.iter().map(|p| p.operation).collect::<Vec<_>>(),
        ids
    );
    actor.deliver(ids.iter().rev().copied().collect()).unwrap();
    received(admitted_waits, &ids);
    assert!(actor.pending.is_empty());
    println!(
        "layout_scope=actual_standalone_dev_test clients={clients} logical_bound={bound} cohort_available_insert_capacity={} key_bytes={} reply_bytes={} legacy_key_reply_tuple_bytes={}",
        actor.pending.capacity(),
        size_of::<OperationId>(),
        size_of::<Reply>(),
        size_of::<(OperationId, Reply)>()
    );
}

#[test]
fn dropped_client_and_missing_result_stop_delivery_in_input_order() {
    let (mut actor, _sender) = ready_actor_clients::<false>(crate::Envelopes::UpstreamDefault, 1);
    let ids = [operation(1 << 110), operation(3), operation(u128::MAX)];
    let (next, mut waits) = request(&ids);
    actor.include(next, &mut Vec::new()).unwrap();
    drop(waits.remove(1));
    assert_eq!(
        actor.deliver(ids.to_vec()).unwrap_err().to_string(),
        "original client dropped result wait"
    );
    assert_eq!(waits[0].try_recv().unwrap(), ids[0]);
    assert_eq!(
        waits[1].try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    );
    assert_eq!(actor.pending.len(), 1);
    assert_eq!(
        actor.deliver(vec![ids[0], ids[2]]).unwrap_err().to_string(),
        "missing/duplicate original applied result"
    );
    assert_eq!(
        waits[1].try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    );
    actor.deliver(vec![ids[2]]).unwrap();
    assert_eq!(waits[1].try_recv().unwrap(), ids[2]);
}

#[test]
fn actor_drop_cancels_all_admitted_and_deferred_original_waits() {
    let (mut actor, _sender) = ready_actor_clients::<false>(crate::Envelopes::UpstreamDefault, 1);
    let (next, mut waits) = request(&[operation(1 << 127), operation(17)]);
    actor.include(next, &mut Vec::new()).unwrap();
    let (deferred, deferred_waits) = request(&[operation(1 << 65)]);
    actor.deferred = Some(deferred);
    waits.extend(deferred_waits);
    drop(actor);
    for wait in &mut waits {
        canceled(wait);
    }
}

#[test]
fn bounded_churn_preserves_receipts_and_reuses_retained_capacity() {
    let (mut actor, _sender) = ready_actor_clients::<false>(crate::Envelopes::UpstreamDefault, 1);
    let initial_capacity = actor.pending.capacity();
    assert!(initial_capacity >= 5);
    let mut high_water = initial_capacity;
    let mut minimum_capacity = initial_capacity;
    for round in 0..256_u128 {
        let ids: Vec<_> = (1..=5)
            .map(|n| operation((round << 96) | (n << 64) | 1))
            .collect();
        let (next, waits) = request(&ids);
        actor.include(next, &mut Vec::new()).unwrap();
        assert_eq!(actor.pending.len(), 5);
        high_water = high_water.max(actor.pending.capacity());
        actor.deliver(ids.iter().rev().copied().collect()).unwrap();
        received(waits, &ids);
        assert!(actor.pending.is_empty());
        // Available insertion capacity may decrease with tombstones despite retained allocation.
        minimum_capacity = minimum_capacity.min(actor.pending.capacity());
    }
    println!("logical_bound=5 rounds=256 initial_cohort_capacity={initial_capacity} minimum_available_cohort_capacity={minimum_capacity} cohort_capacity_high_water={high_water} final_available_cohort_capacity={}", actor.pending.capacity());
}

#[test]
fn near_bound_partial_rotation_completes_every_original_and_reports_capacity() {
    checked_partial_rotation(1);
    checked_partial_rotation(4096);
}

fn checked_partial_rotation(clients: usize) {
    let (mut actor, _sender) =
        ready_actor_clients::<false>(crate::Envelopes::UpstreamDefault, clients);
    let bound = clients * 4 + 1;
    let rounds = (bound * 4).max(1024);
    let initial_capacity = actor.pending.capacity();
    let initial_ids: Vec<_> = (1..=bound).map(|n| operation((n as u128) << 96)).collect();
    let mut waits = Vec::new();
    for chunk in initial_ids.chunks(actor.parts.core.batch_limits().max_proposal_entries) {
        let (first, next_waits) = request(chunk);
        actor.include(first, &mut Vec::new()).unwrap();
        waits.extend(next_waits);
    }
    let mut active: VecDeque<_> = initial_ids.into_iter().zip(waits).collect();
    let mut minimum_capacity = actor.pending.capacity();
    let mut high_water = minimum_capacity;
    for round in 1..=rounds {
        let (id, mut wait) = active.pop_front().unwrap();
        actor.deliver(vec![id]).unwrap();
        assert_eq!(wait.try_recv().unwrap(), id);
        assert_eq!(actor.pending.len(), bound - 1);
        minimum_capacity = minimum_capacity.min(actor.pending.capacity());
        let next_id = operation(((round as u128) << 64) | (1 << 127) | 1);
        let (next, mut waits) = request(&[next_id]);
        actor.include(next, &mut Vec::new()).unwrap();
        active.push_back((next_id, waits.pop().unwrap()));
        assert_eq!(actor.pending.len(), bound);
        high_water = high_water.max(actor.pending.capacity());
    }
    actor
        .deliver(active.iter().rev().map(|(id, _)| *id).collect())
        .unwrap();
    for (id, mut wait) in active {
        assert_eq!(wait.try_recv().unwrap(), id);
    }
    assert!(actor.pending.is_empty());
    minimum_capacity = minimum_capacity.min(actor.pending.capacity());
    println!("partial_rotation clients={clients} logical_bound={bound} rounds={rounds} initial_cohort_capacity={initial_capacity} minimum_available_cohort_capacity={minimum_capacity} cohort_capacity_high_water={high_water} final_available_cohort_capacity={}", actor.pending.capacity());
}
