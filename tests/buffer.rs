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
