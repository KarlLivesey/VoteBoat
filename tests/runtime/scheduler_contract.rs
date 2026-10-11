// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

fn scoped_cancel<Q: ReadyScheduler>(mut queue: Q) {
    let old = group(1);
    let mut current = old;
    current.incarnation = GroupIncarnation::new(2).unwrap();
    queue.enqueue(old).unwrap();
    queue.enqueue(current).unwrap();
    queue.enqueue(group(2)).unwrap();
    queue.cancel(old);
    queue.cancel(old);
    queue.cancel(group(3));
    assert_eq!(queue.len(), 2);
    assert_eq!(queue.pop(), Some(current));
    assert_eq!(queue.pop(), Some(group(2)));
    assert_eq!(queue.pop(), None);
    assert!(queue.is_empty());
}

fn atomic_refusal<Q: ReadyScheduler>(mut queue: Q) {
    assert_eq!(queue.capacity(), 3);
    assert!(queue.is_empty());
    assert_eq!(queue.pop(), None);
    for id in 1..=3 {
        queue.enqueue(group(id)).unwrap();
    }
    queue.enqueue(group(2)).unwrap();
    assert_eq!(queue.len(), 3);
    assert_eq!(queue.enqueue(group(4)), Err(RuntimeError::Overloaded));
    for id in 1..=3 {
        assert_eq!(queue.pop(), Some(group(id)));
    }
    assert_eq!(queue.len(), 0);
    assert!(queue.is_empty());
    assert_eq!(queue.pop(), None);
}

fn fair_requeue<Q: ReadyScheduler>(mut queue: Q) {
    for id in 1..=3 {
        queue.enqueue(group(id)).unwrap();
    }
    for _ in 0..32 {
        for id in 1..=3 {
            let ready = queue.pop().unwrap();
            assert_eq!(ready, group(id));
            queue.enqueue(ready).unwrap();
            queue.enqueue(group(2)).unwrap();
            assert_eq!(queue.len(), 3);
        }
    }
}

fn modeled_traces<Q: ReadyScheduler>(make: impl Fn() -> Q) {
    for seed in 1u64..=8 {
        let mut queue = make();
        let mut model = VecDeque::new();
        let mut random = seed;
        for step in 0..256 {
            random = random.wrapping_mul(6364136223846793005).wrapping_add(1);
            let mut identity = group(u128::from((random >> 16) % 5 + 1));
            identity.incarnation = GroupIncarnation::new((random >> 24) % 2 + 1).unwrap();
            match (random >> 32) % 4 {
                0 | 1 => {
                    let expected = if model.contains(&identity) {
                        Ok(())
                    } else if model.len() == 3 {
                        Err(RuntimeError::Overloaded)
                    } else {
                        model.push_back(identity);
                        Ok(())
                    };
                    assert_eq!(queue.enqueue(identity), expected, "seed {seed} step {step}");
                }
                2 => {
                    queue.cancel(identity);
                    model.retain(|g| *g != identity);
                }
                _ => assert_eq!(queue.pop(), model.pop_front(), "seed {seed} step {step}"),
            }
            assert_eq!(queue.capacity(), 3);
            assert_eq!(queue.len(), model.len(), "seed {seed} step {step}");
            assert_eq!(queue.is_empty(), model.is_empty());
        }
        while let Some(expected) = model.pop_front() {
            assert_eq!(queue.pop(), Some(expected));
        }
        assert_eq!(queue.pop(), None);
        assert!(queue.is_empty());
    }
}

fn instance_lifetime<Q: ReadyScheduler>(make: impl Fn() -> Q) {
    let mut first = make();
    let mut survivor = make();
    first.enqueue(group(1)).unwrap();
    survivor.enqueue(group(1)).unwrap();
    first.cancel(group(1));
    assert!(first.is_empty());
    drop(first);
    assert_eq!(survivor.pop(), Some(group(1)));
    assert!(survivor.is_empty());
    let rebuilt = make();
    assert!(rebuilt.is_empty());
}

fn checker<Q: ReadyScheduler>(make: impl Fn() -> Q) {
    scheduler_conformance(make());
    scoped_cancel(make());
    atomic_refusal(make());
    fair_requeue(make());
    modeled_traces(&make);
    instance_lifetime(make);
}

#[test]
fn host_scheduler_obligations() {
    checker(|| HostReady::new(3));
}

#[cfg(feature = "native")]
#[test]
fn native_scheduler_obligations() {
    checker(|| voteboat::native::runtime::FairScheduler::new(3).unwrap());
}

#[derive(Clone, Copy)]
enum Defect {
    CancelById,
    DropOnRefusal,
    EnqueueAtFront,
}
struct BadQueue {
    inner: HostReady,
    defect: Defect,
}
impl ReadyScheduler for BadQueue {
    fn capacity(&self) -> usize {
        self.inner.capacity()
    }
    fn enqueue(&mut self, identity: GroupIdentity) -> Result<(), RuntimeError> {
        if !self.inner.groups.contains(&identity) {
            if self.inner.len() == self.capacity() && matches!(self.defect, Defect::DropOnRefusal) {
                self.inner.groups.clear();
                return Err(RuntimeError::Overloaded);
            }
            if self.inner.len() < self.capacity() && matches!(self.defect, Defect::EnqueueAtFront) {
                self.inner.groups.insert(0, identity);
                return Ok(());
            }
        }
        self.inner.enqueue(identity)
    }
    fn pop(&mut self) -> Option<GroupIdentity> {
        self.inner.pop()
    }
    fn cancel(&mut self, identity: GroupIdentity) {
        if matches!(self.defect, Defect::CancelById) {
            self.inner.groups.retain(|g| g.id != identity.id);
        } else {
            self.inner.cancel(identity);
        }
    }
    fn len(&self) -> usize {
        self.inner.len()
    }
}

fn defective(defect: Defect) -> BadQueue {
    BadQueue {
        inner: HostReady::new(3),
        defect,
    }
}

#[test]
fn shared_assertions_detect_incarnation_blind_cancel() {
    assert!(std::panic::catch_unwind(|| scoped_cancel(defective(Defect::CancelById))).is_err());
}

#[test]
fn shared_assertions_detect_destructive_refusal() {
    assert!(std::panic::catch_unwind(|| atomic_refusal(defective(Defect::DropOnRefusal))).is_err());
}

#[test]
fn shared_assertions_detect_unfair_front_insertion() {
    assert!(std::panic::catch_unwind(|| fair_requeue(defective(Defect::EnqueueAtFront))).is_err());
}
