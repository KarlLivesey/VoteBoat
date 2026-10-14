// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

fn operation(value: u128) -> OperationId {
    OperationId::new(value).unwrap()
}

#[test]
fn full_identity_trace_matches_independent_hash_map() {
    let mut ids = vec![1, 4, 5, u128::MAX, u128::MAX - 1, u128::MAX - 2];
    ids.extend((0..128).map(|bit| 1_u128 << ((bit * 73) % 128)));
    for cohort in 1..=32_u128 {
        let base = (cohort << 96) | (cohort << 32);
        ids.extend((1..=4).rev().map(|slot| base + slot));
    }
    let mut replies = PendingReplies::new(ids.len());
    let mut oracle = HashMap::new();
    for turn in 0..8192_u64 {
        let id = operation(ids[(turn as usize * 101) % ids.len()]);
        if turn % 7 < 3 {
            assert_eq!(replies.remove(&id), oracle.remove(&id));
        } else {
            assert_eq!(replies.insert(id, turn), oracle.insert(id, turn));
        }
        assert_eq!(replies.len(), oracle.len());
        assert_eq!(replies.is_empty(), oracle.is_empty());
    }
    for value in ids.iter().rev() {
        let id = operation(*value);
        assert_eq!(replies.remove(&id), oracle.remove(&id));
        assert_eq!(replies.len(), oracle.len());
    }
    assert!(oracle.is_empty());
    assert!(replies.is_empty());
}

#[test]
fn missing_slots_preserve_state_and_reused_indices_never_alias_old_identity() {
    let mut replies = PendingReplies::new(5);
    assert_eq!(replies.insert(operation(1), 10), None);
    assert_eq!(replies.insert(operation(4), 40), None);
    let cache = replies.cached;
    assert_eq!(replies.remove(&operation(2)), None);
    assert_eq!(replies.remove(&operation(5)), None);
    assert_eq!(replies.cached, cache);
    assert_eq!(replies.len(), 2);
    assert_eq!(replies.remove(&operation(4)), Some(40));
    assert_eq!(replies.remove(&operation(1)), Some(10));
    assert_eq!(replies.cached, None);
    assert_eq!(replies.index.len(), 0);
    assert_eq!(replies.free.len(), 1);
    assert_eq!(replies.insert(operation(u128::MAX), 90), None);
    assert_eq!(replies.blocks.len(), 1);
    assert!(replies.free.is_empty());
    let cache = replies.cached;
    assert_eq!(replies.remove(&operation(1)), None);
    assert_eq!(replies.remove(&operation(4)), None);
    assert_eq!(replies.cached, cache);
    assert_eq!(replies.remove(&operation(u128::MAX)), Some(90));
    assert!(replies.is_empty());
}

struct Value {
    token: usize,
    drops: Arc<Vec<AtomicUsize>>,
}

impl Drop for Value {
    fn drop(&mut self) {
        self.drops[self.token].fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn replacement_removal_and_table_drop_transfer_each_value_exactly_once() {
    let drops: Arc<Vec<_>> = Arc::new((0..4).map(|_| AtomicUsize::new(0)).collect());
    let value = |token| Value {
        token,
        drops: drops.clone(),
    };
    let mut replies = PendingReplies::new(5);
    assert!(replies.insert(operation(1), value(0)).is_none());
    let replaced = replies.insert(operation(1), value(1)).unwrap();
    assert_eq!(replaced.token, 0);
    assert_eq!(replies.len(), 1);
    assert!(drops.iter().all(|value| value.load(Ordering::Relaxed) == 0));
    drop(replaced);
    assert_eq!(drops[0].load(Ordering::Relaxed), 1);
    assert!(replies.insert(operation(4), value(2)).is_none());
    assert!(replies.insert(operation(5), value(3)).is_none());
    let removed = replies.remove(&operation(4)).unwrap();
    assert_eq!(removed.token, 2);
    assert_eq!(drops[2].load(Ordering::Relaxed), 0);
    assert!(replies.remove(&operation(4)).is_none());
    drop(replies);
    assert_eq!(drops[1].load(Ordering::Relaxed), 1);
    assert_eq!(drops[3].load(Ordering::Relaxed), 1);
    assert_eq!(drops[2].load(Ordering::Relaxed), 0);
    drop(removed);
    assert!(drops.iter().all(|value| value.load(Ordering::Relaxed) == 1));
}

#[test]
fn maximum_sparse_occupancy_and_churn_keep_backing_slots_and_free_list_bounded() {
    sparse_churn(5);
    sparse_churn(16385);
}

fn sparse_churn(bound: usize) {
    let mut replies = PendingReplies::new(bound);
    let arena_capacity = replies.blocks.capacity();
    let free_capacity = replies.free.capacity();
    assert!(arena_capacity <= bound);
    assert!(free_capacity <= bound);
    let mut cohort_high_water = replies.index.capacity();
    for round in 1..=8_u128 {
        let ids: Vec<_> = (1..=bound)
            .map(|value| operation((round << 112) | ((value as u128) << 64) | 1))
            .collect();
        for (value, &id) in ids.iter().enumerate() {
            assert_eq!(replies.insert(id, value), None);
            assert_eq!(replies.len(), value + 1);
        }
        assert_eq!(replies.index.len(), bound);
        assert_eq!(replies.blocks.len(), bound);
        assert!(replies.free.is_empty());
        assert_eq!(replies.blocks.capacity(), arena_capacity);
        cohort_high_water = cohort_high_water.max(replies.index.capacity());
        for (value, id) in ids.iter().enumerate().rev() {
            assert_eq!(replies.remove(id), Some(value));
        }
        assert!(replies.is_empty());
        assert!(replies.index.is_empty());
        assert_eq!(replies.free.len(), bound);
        assert_eq!(replies.free.capacity(), free_capacity);
        assert!(replies
            .blocks
            .iter()
            .all(|block| { block.occupied == 0 && block.slots.iter().all(Option::is_none) }));
    }
    println!(
        "pending_cohort_scope=actual_standalone_dev bound={bound} sparse_rounds=8 arena_capacity={arena_capacity} free_capacity={free_capacity} cohort_available_capacity_high_water={cohort_high_water} block_bytes={} reply_slots_reserved={} index_tuple_bytes={}",
        std::mem::size_of::<Block<super::super::Reply>>(),
        arena_capacity * 4,
        std::mem::size_of::<(u128, usize)>(),
    );
}
