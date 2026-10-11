// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

fn distinct_owners(original: RuntimeOwner) -> [RuntimeOwner; 5] {
    let mut variants = [original; 5];
    variants[0].store.identity.id = StoreId::new(2).unwrap();
    variants[1].store.identity.incarnation = StoreIncarnation::new(2).unwrap();
    variants[2].store.session = StoreSession::new(2).unwrap();
    variants[3].lane = ExecutionLaneId::new(2).unwrap();
    variants[4].generation = RuntimeGeneration::new(2).unwrap();
    variants
}

pub(super) fn exact_owner_isolation<T: TimerService>(factory: impl Fn(RuntimeOwner) -> T) {
    let original = owner(HostLogStore::new(1).binding());
    for foreign in distinct_owners(original) {
        assert_ne!(foreign, original);
        let mut first = factory(original);
        let mut second = factory(foreign);
        let first_token = first
            .register(group(1), TimerKind::Election, MonoTime(10))
            .unwrap();
        let second_token = second
            .register(group(1), TimerKind::Election, MonoTime(10))
            .unwrap();
        assert_eq!(first_token.sequence, second_token.sequence);
        assert_eq!(first_token.owner, original);
        assert_eq!(second_token.owner, foreign);
        assert_eq!(first.cancel(second_token), Err(RuntimeError::StaleTicket));
        assert_eq!(second.cancel(first_token), Err(RuntimeError::StaleTicket));
        assert_eq!((first.len(), second.len()), (1, 1));
        assert_eq!(first.owner(), original);
        assert_eq!(second.owner(), foreign);
        assert_eq!(
            first.poll(MonoTime(11), 1).unwrap(),
            vec![Expiration {
                token: first_token,
                late_ms: 1
            }]
        );
        assert_eq!(second.len(), 1, "another owner retains its deadline");
        assert_eq!(
            second.poll(MonoTime(12), 1).unwrap(),
            vec![Expiration {
                token: second_token,
                late_ms: 2
            }]
        );
        assert!(first.is_empty());
        assert!(second.is_empty());
        assert_eq!(first.cancel(first_token), Err(RuntimeError::StaleTicket));
        assert_eq!(second.cancel(second_token), Err(RuntimeError::StaleTicket));
    }
}

#[test]
fn host_timer_distinct_store_session_lane_and_generation_isolation() {
    exact_owner_isolation(|binding| HostTimers::new(binding, 3));
}

#[cfg(feature = "native")]
#[test]
fn native_timer_distinct_store_session_lane_and_generation_isolation() {
    exact_owner_isolation(|binding| {
        voteboat::native::runtime::DeadlineQueue::new(binding, 3).unwrap()
    });
}
