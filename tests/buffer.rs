// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
// Unless explicitly acquired and licensed from Licensor under another license,
// the contents of this file are subject to the Reciprocal Public License
// ("RPL") Version 1.5, or subsequent versions as allowed by the RPL, and You may
// not copy or use this file in either source code or executable form, except
// in compliance with the terms and conditions of the RPL.
//
// All software distributed under the RPL is provided strictly on an "AS IS"
// basis, WITHOUT WARRANTY OF ANY KIND, EITHER EXPRESS OR IMPLIED, AND LICENSOR
// HEREBY DISCLAIMS ALL SUCH WARRANTIES, INCLUDING WITHOUT LIMITATION, ANY
// WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, QUIET
// ENJOYMENT, OR NON-INFRINGEMENT. See the RPL for specific language governing
// rights and limitations under the RPL.
mod support;
use voteboat::buffer::*;
fn lifetimes(mut first: impl BufferPool, mut other: impl BufferPool) {
    let mut bytes = first.acquire(64, 4).unwrap();
    bytes.as_mut().copy_from_slice(b"boat");
    assert_eq!(
        first.usage(),
        BufferUsage {
            reserved_bytes: 64,
            leases: 1
        }
    );
    assert!(matches!(other.acquire(65, 1), Err(BufferError::Overloaded)));
    assert_eq!(first.usage().leases, 1);
    first.close();
    assert!(matches!(first.acquire(1, 1), Err(BufferError::Closed)));
    drop(first);
    bytes.resize(64).unwrap();
    assert_eq!(&bytes.as_ref()[..4], b"boat");
    assert!(bytes.as_ref()[4..].iter().all(|v| *v == 0));
    assert_eq!(bytes.resize(65), Err(BufferError::TooLarge));
    assert_eq!(bytes.as_ref().len(), 64);
    let second = other.acquire(64, 0).unwrap();
    assert!(matches!(other.acquire(1, 0), Err(BufferError::Overloaded)));
    drop(bytes);
    assert_eq!(
        other.usage(),
        BufferUsage {
            reserved_bytes: 64,
            leases: 1
        }
    );
    drop(second);
    assert_eq!(other.usage(), BufferUsage::default());
    other.close();
}
#[test]
fn independent_downstream_pool_contract_without_native() {
    let pool = support::buffer::HostPool::new(128, 2);
    lifetimes(pool.clone(), pool);
}
#[cfg(feature = "native")]
#[test]
fn native_owned_leases_survive_view_close_and_release_exact_shared_credits() {
    use voteboat::native::buffer::NativeBufferPool;
    let pool = NativeBufferPool::new(BufferLimits {
        reserved_bytes: 128,
        leases: 2,
    })
    .unwrap();
    lifetimes(pool.clone(), pool);
    assert!(NativeBufferPool::new(BufferLimits {
        reserved_bytes: 0,
        leases: 1
    })
    .is_err());
    let pool = NativeBufferPool::new(BufferLimits {
        reserved_bytes: 64,
        leases: 1,
    })
    .unwrap();
    assert!(matches!(pool.acquire(65, 0), Err(BufferError::TooLarge)));
    assert!(matches!(pool.acquire(1, 2), Err(BufferError::TooLarge)));
    assert_eq!(pool.usage(), BufferUsage::default());
    let bytes = pool.acquire(64, 4).unwrap();
    assert_eq!(bytes.capacity(), 4); // reserve maximum, allocate header only
    drop(bytes);
    assert_eq!(pool.usage(), BufferUsage::default());
}
#[cfg(feature = "native")]
#[test]
fn native_pool_bounds_concurrent_owners_and_returns_all_credits() {
    use voteboat::native::buffer::NativeBufferPool;
    let pool = NativeBufferPool::new(BufferLimits {
        reserved_bytes: 1024,
        leases: 16,
    })
    .unwrap();
    let threads: Vec<_> = (0..8)
        .map(|_| {
            let pool = pool.clone();
            std::thread::spawn(move || {
                for _ in 0..1000 {
                    match pool.acquire(64, 8) {
                        Ok(mut buffer) => {
                            buffer.resize(64).unwrap();
                            std::thread::yield_now();
                        }
                        Err(BufferError::Overloaded) => {}
                        Err(error) => panic!("unexpected {error:?}"),
                    }
                }
            })
        })
        .collect();
    for thread in threads {
        thread.join().unwrap();
    }
    assert_eq!(pool.usage(), BufferUsage::default());
    let leases: Vec<_> = (0..16).map(|_| pool.acquire(64, 0).unwrap()).collect();
    assert!(matches!(pool.acquire(1, 0), Err(BufferError::Overloaded)));
    drop(leases);
    assert_eq!(pool.usage(), BufferUsage::default());
}

fn protected<P: BufferPool + Clone>(mut pool: P) {
    assert_eq!(
        pool.control_reserve(),
        Some(BufferLimits {
            reserved_bytes: 32,
            leases: 1
        })
    );
    let other = pool.clone();
    let bulk = pool.acquire(96, 0).unwrap();
    assert!(matches!(other.acquire(1, 0), Err(BufferError::Overloaded)));
    let mut control = other.acquire_class(BufferClass::Control, 32, 4).unwrap();
    control.as_mut().copy_from_slice(b"vote");
    assert_eq!(
        pool.usage(),
        BufferUsage {
            reserved_bytes: 128,
            leases: 2
        }
    );
    pool.close();
    assert!(matches!(
        pool.acquire_class(BufferClass::Control, 1, 0),
        Err(BufferError::Closed)
    ));
    control.resize(32).unwrap();
    assert_eq!(&control.as_ref()[..4], b"vote");
    assert!(control.as_ref()[4..].iter().all(|b| *b == 0));
    drop(pool);
    drop(control);
    drop(bulk);
    assert_eq!(other.usage(), BufferUsage::default());
    // A control allocation may use the entire total, but a failed bulk
    // acquisition must roll back any partially reserved lease/byte counters.
    let all = other.acquire_class(BufferClass::Control, 128, 0).unwrap();
    assert!(matches!(other.acquire(1, 0), Err(BufferError::Overloaded)));
    assert_eq!(
        other.usage(),
        BufferUsage {
            reserved_bytes: 128,
            leases: 1
        }
    );
    drop(all);
    let first = other.acquire(1, 0).unwrap();
    let second = other.acquire(1, 0).unwrap();
    assert!(matches!(other.acquire(1, 0), Err(BufferError::Overloaded)));
    let last = other.acquire_class(BufferClass::Control, 1, 0).unwrap();
    assert_eq!(other.usage().leases, 3);
    drop((first, second, last));
    assert_eq!(other.usage(), BufferUsage::default());
}
#[test]
fn downstream_protected_bytes_and_slots_survive_close_and_failed_admission() {
    protected(support::buffer::HostPool::with_control_reserve(
        128,
        3,
        BufferLimits {
            reserved_bytes: 32,
            leases: 1,
        },
    ));
}
#[cfg(feature = "native")]
#[test]
fn native_protected_bytes_and_slots_roll_back_exactly() {
    use voteboat::native::buffer::NativeBufferPool;
    let limits = BufferLimits {
        reserved_bytes: 128,
        leases: 3,
    };
    for reserve in [
        BufferLimits {
            reserved_bytes: 0,
            leases: 1,
        },
        BufferLimits {
            reserved_bytes: 1,
            leases: 0,
        },
        limits,
        BufferLimits {
            reserved_bytes: 129,
            leases: 1,
        },
    ] {
        assert!(matches!(
            NativeBufferPool::new_with_control_reserve(limits, reserve),
            Err(BufferError::InvalidLimits)
        ));
    }
    protected(
        NativeBufferPool::new_with_control_reserve(
            limits,
            BufferLimits {
                reserved_bytes: 32,
                leases: 1,
            },
        )
        .unwrap(),
    );
    let pool = NativeBufferPool::new_with_control_reserve(
        BufferLimits {
            reserved_bytes: usize::MAX,
            leases: 3,
        },
        BufferLimits {
            reserved_bytes: 1,
            leases: 1,
        },
    )
    .unwrap();
    // Vec capacity overflow deterministically refuses without allocating.
    assert!(matches!(
        pool.acquire_class(BufferClass::Control, usize::MAX, usize::MAX),
        Err(BufferError::AllocationFailed)
    ));
    assert_eq!(pool.usage(), BufferUsage::default());
    assert_eq!(pool.bulk_usage(), BufferUsage::default());
}
#[cfg(feature = "native")]
#[test]
fn concurrent_bulk_owners_cannot_take_control_headroom() {
    use std::sync::{Arc, Barrier};
    use voteboat::native::buffer::NativeBufferPool;
    let pool = NativeBufferPool::new_with_control_reserve(
        BufferLimits {
            reserved_bytes: 9 * 64,
            leases: 9,
        },
        BufferLimits {
            reserved_bytes: 64,
            leases: 1,
        },
    )
    .unwrap();
    let ready = Arc::new(Barrier::new(9));
    let done = Arc::new(Barrier::new(9));
    let mut workers = Vec::new();
    for _ in 0..8 {
        let bytes = pool.acquire(64, 8).unwrap();
        let view = pool.clone();
        let ready = ready.clone();
        let done = done.clone();
        workers.push(std::thread::spawn(move || {
            ready.wait();
            assert!(matches!(view.acquire(1, 0), Err(BufferError::Overloaded)));
            done.wait();
            drop(bytes);
        }));
    }
    ready.wait();
    let control = pool.acquire_class(BufferClass::Control, 64, 0).unwrap();
    assert_eq!(
        pool.usage(),
        BufferUsage {
            reserved_bytes: 9 * 64,
            leases: 9
        }
    );
    assert_eq!(
        pool.bulk_usage(),
        BufferUsage {
            reserved_bytes: 8 * 64,
            leases: 8
        }
    );
    done.wait();
    for worker in workers {
        worker.join().unwrap();
    }
    drop(control);
    assert_eq!(pool.usage(), BufferUsage::default());
    assert_eq!(pool.bulk_usage(), BufferUsage::default());
}

#[test]
fn legacy_default_cannot_claim_a_reserve_without_class_implementation() {
    struct Declared(support::buffer::HostPool);
    impl BufferPool for Declared {
        type Buffer = support::buffer::HostBuffer;
        fn limits(&self) -> BufferLimits {
            self.0.limits()
        }
        fn usage(&self) -> BufferUsage {
            self.0.usage()
        }
        fn acquire(&self, r: usize, i: usize) -> Result<Self::Buffer, BufferError> {
            self.0.acquire(r, i)
        }
        fn control_reserve(&self) -> Option<BufferLimits> {
            Some(BufferLimits {
                reserved_bytes: 32,
                leases: 1,
            })
        }
        fn close(&mut self) {
            self.0.close();
        }
    }
    let pool = Declared(support::buffer::HostPool::new(128, 3));
    for class in [BufferClass::Bulk, BufferClass::Control] {
        assert!(matches!(
            pool.acquire_class(class, 1, 0),
            Err(BufferError::ProviderViolation)
        ));
        assert_eq!(pool.usage(), BufferUsage::default());
    }
}

fn owner(peer: u64) -> BufferOwner {
    use voteboat::identity::*;
    BufferOwner {
        local_node: NodeId::new(1).unwrap(),
        local_store: StoreId::new(1).unwrap(),
        local_incarnation: StoreIncarnation::new(1).unwrap(),
        peer_node: NodeId::new(peer).unwrap(),
        peer_store: StoreId::new(peer.into()).unwrap(),
        peer_incarnation: StoreIncarnation::new(1).unwrap(),
    }
}
fn owner_config() -> (BufferLimits, BufferLimits, BufferOwnerLimits) {
    (
        BufferLimits {
            reserved_bytes: 128,
            leases: 5,
        },
        BufferLimits {
            reserved_bytes: 32,
            leases: 1,
        },
        BufferOwnerLimits {
            owners: 2,
            bulk: BufferLimits {
                reserved_bytes: 48,
                leases: 2,
            },
        },
    )
}
fn owner_lifetimes(root: impl BufferPool + Clone) {
    assert!(matches!(root.acquire(1, 0), Err(BufferError::InvalidOwner)));
    assert!(matches!(
        root.acquire_class(BufferClass::Control, 1, 0),
        Err(BufferError::InvalidOwner)
    ));
    let mut a = root.clone();
    let mut b = root.clone();
    a.bind_owner(owner(2)).unwrap();
    b.bind_owner(owner(3)).unwrap();
    assert_eq!(a.bind_owner(owner(3)), Err(BufferError::InvalidOwner));
    let held_a = a.acquire(48, 1).unwrap();
    assert!(matches!(a.acquire(1, 0), Err(BufferError::Overloaded)));
    let mut reconnect = root.clone();
    reconnect.bind_owner(owner(2)).unwrap();
    assert!(matches!(
        reconnect.acquire(1, 0),
        Err(BufferError::Overloaded)
    ));
    let held_b = b.acquire(48, 1).unwrap();
    let control = a.acquire_class(BufferClass::Control, 32, 1).unwrap();
    assert_eq!(root.usage().reserved_bytes, 128);
    // All total capacity is held: failed owner reservation must roll back.
    drop(held_b);
    let huge_control = b.acquire_class(BufferClass::Control, 48, 0).unwrap();
    assert!(matches!(b.acquire(1, 0), Err(BufferError::Overloaded)));
    drop(huge_control);
    let b1 = b.acquire(24, 0).unwrap();
    let b2 = b.acquire(24, 0).unwrap();
    assert!(matches!(b.acquire(1, 0), Err(BufferError::Overloaded)));
    drop((b1, b2));
    a.close();
    assert!(matches!(a.acquire(1, 0), Err(BufferError::Closed)));
    drop((a, reconnect));
    let mut c = root.clone();
    assert_eq!(c.bind_owner(owner(4)), Err(BufferError::Overloaded));
    drop(held_a);
    // A control frame retains registration even without owner bulk charges.
    assert_eq!(c.bind_owner(owner(4)), Err(BufferError::Overloaded));
    drop(control);
    let mut changed = owner(2);
    changed.peer_incarnation = voteboat::identity::StoreIncarnation::new(2).unwrap();
    c.bind_owner(changed).unwrap();
    let final_frame = c.acquire(48, 0).unwrap();
    drop((c, b));
    assert_eq!(root.usage().reserved_bytes, 48);
    drop(final_frame);
    assert_eq!(root.usage(), BufferUsage::default());
    let mut last = root.clone();
    last.bind_owner(owner(4)).unwrap();
    let one = last.acquire(1, 0).unwrap();
    let two = last.acquire(1, 0).unwrap();
    assert!(matches!(last.acquire(1, 0), Err(BufferError::Overloaded)));
    drop((one, two));
    assert_eq!(root.usage(), BufferUsage::default());
}
#[test]
fn downstream_owner_isolation_reconnect_and_registration_lifetimes() {
    let (total, reserve, owners) = owner_config();
    owner_lifetimes(support::buffer::HostPool::with_owner_limits(
        total, reserve, owners,
    ));
}
#[cfg(feature = "native")]
#[test]
fn native_owner_isolation_reconnect_and_registration_lifetimes() {
    let (total, reserve, owners) = owner_config();
    owner_lifetimes(
        voteboat::native::buffer::NativeBufferPool::new_with_owner_limits(total, reserve, owners)
            .unwrap(),
    );
}
#[test]
fn owner_configuration_rejects_overbooking_and_overflow() {
    let (total, reserve, mut owners) = owner_config();
    for n in [0, 3, 1025, usize::MAX] {
        owners.owners = n;
        assert_eq!(
            owners.validate(total, reserve),
            Err(BufferError::InvalidLimits)
        );
    }
    owners.owners = 2;
    owners.bulk.reserved_bytes = usize::MAX;
    assert_eq!(
        owners.validate(total, reserve),
        Err(BufferError::InvalidLimits)
    );
    owners.bulk.reserved_bytes = 48;
    owners.bulk.leases = 3;
    assert_eq!(
        owners.validate(total, reserve),
        Err(BufferError::InvalidLimits)
    );
}
#[cfg(feature = "native")]
#[test]
fn native_owner_allocation_failure_returns_all_three_budgets() {
    use voteboat::native::buffer::NativeBufferPool;
    let total = BufferLimits {
        reserved_bytes: usize::MAX,
        leases: 3,
    };
    let reserve = BufferLimits {
        reserved_bytes: 1,
        leases: 1,
    };
    let owners = BufferOwnerLimits {
        owners: 1,
        bulk: BufferLimits {
            reserved_bytes: usize::MAX - 1,
            leases: 2,
        },
    };
    let mut pool = NativeBufferPool::new_with_owner_limits(total, reserve, owners).unwrap();
    pool.bind_owner(owner(2)).unwrap();
    assert!(matches!(
        pool.acquire(usize::MAX - 1, usize::MAX - 1),
        Err(BufferError::AllocationFailed)
    ));
    assert_eq!(pool.usage(), BufferUsage::default());
    assert_eq!(pool.bulk_usage(), BufferUsage::default());
    assert_eq!(pool.owner_usage(), Some(BufferUsage::default()));
    let held = pool.acquire(1, 1).unwrap();
    drop(held);
    assert_eq!(pool.owner_usage(), Some(BufferUsage::default()));
}

#[cfg(feature = "native")]
#[test]
fn concurrent_reconnected_owner_cannot_consume_other_owner_bulk_quota() {
    use std::sync::{Arc, Barrier};
    use voteboat::native::buffer::NativeBufferPool;
    let root = NativeBufferPool::new_with_owner_limits(
        BufferLimits {
            reserved_bytes: 17,
            leases: 17,
        },
        BufferLimits {
            reserved_bytes: 1,
            leases: 1,
        },
        BufferOwnerLimits {
            owners: 2,
            bulk: BufferLimits {
                reserved_bytes: 8,
                leases: 8,
            },
        },
    )
    .unwrap();
    let mut a = root.clone();
    let mut b = root.clone();
    a.bind_owner(owner(2)).unwrap();
    b.bind_owner(owner(3)).unwrap();
    let barrier = Arc::new(Barrier::new(9));
    let mut workers = Vec::new();
    for _ in 0..8 {
        let view = a.clone();
        let barrier = barrier.clone();
        workers.push(std::thread::spawn(move || {
            let held = view.acquire(1, 1).unwrap();
            barrier.wait();
            barrier.wait();
            drop(held);
        }));
    }
    barrier.wait();
    let mut reconnect = root.clone();
    reconnect.bind_owner(owner(2)).unwrap();
    assert!(matches!(
        reconnect.acquire(1, 0),
        Err(BufferError::Overloaded)
    ));
    let other = b.acquire(8, 1).unwrap();
    let control = b.acquire_class(BufferClass::Control, 1, 1).unwrap();
    assert_eq!(root.usage().reserved_bytes, 17);
    drop((other, control));
    barrier.wait();
    for worker in workers {
        worker.join().unwrap();
    }
    assert_eq!(root.usage(), BufferUsage::default());
    assert_eq!(a.owner_usage(), Some(BufferUsage::default()));
}
#[test]
fn unsupported_owner_declaration_refuses_binding() {
    struct Declared(support::buffer::HostPool);
    impl BufferPool for Declared {
        type Buffer = support::buffer::HostBuffer;
        fn limits(&self) -> BufferLimits {
            self.0.limits()
        }
        fn usage(&self) -> BufferUsage {
            self.0.usage()
        }
        fn acquire(&self, r: usize, n: usize) -> Result<Self::Buffer, BufferError> {
            self.0.acquire(r, n)
        }
        fn owner_limits(&self) -> Option<BufferOwnerLimits> {
            Some(owner_config().2)
        }
        fn close(&mut self) {
            self.0.close();
        }
    }
    let mut pool = Declared(support::buffer::HostPool::new(128, 5));
    assert_eq!(
        pool.bind_owner(owner(2)),
        Err(BufferError::ProviderViolation)
    );
    assert_eq!(pool.usage(), BufferUsage::default());
}
