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
