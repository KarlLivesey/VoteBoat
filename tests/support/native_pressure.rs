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

use voteboat::{
    buffer::*,
    native::{buffer::NativeBufferPool, transport::NativeSharedTransportFactory},
    transport::TransportLimits,
};
type PooledFactory = NativeSharedTransportFactory<NativeWireCodec, NativeBufferPool>;
fn pooled_facades(
    root: &std::path::Path,
    recover: bool,
) -> (Vec<Facade<PooledFactory>>, Vec<NativeBufferPool>) {
    let mut pools = Vec::new();
    let nodes = facade_make_with(root, recover, |_| {
        let limits = TransportLimits::default();
        let frame = limits.send_frame_bytes.max(limits.receive_frame_bytes);
        let pool = NativeBufferPool::new_with_owner_limits(
            BufferLimits {
                reserved_bytes: 5 * frame,
                leases: 5,
            },
            BufferLimits {
                reserved_bytes: frame,
                leases: 1,
            },
            BufferOwnerLimits {
                owners: 2,
                bulk: BufferLimits {
                    reserved_bytes: 2 * frame,
                    leases: 2,
                },
            },
        )
        .unwrap();
        pools.push(pool.clone());
        NativeTransportFactory::new(NativeWireCodec::new(Default::default()).unwrap(), limits)
            .unwrap()
            .with_buffers(pool)
            .unwrap()
    });
    (nodes, pools)
}
fn selected_owner(nodes: &[Facade<PooledFactory>], local: usize, peer: usize) -> BufferOwner {
    let a = nodes[local].local().owner.identity().store.identity;
    let b = nodes[peer].local().owner.identity().store.identity;
    BufferOwner {
        local_node: node((local + 1) as u64),
        local_store: a.id,
        local_incarnation: a.incarnation,
        peer_node: node((peer + 1) as u64),
        peer_store: b.id,
        peer_incarnation: b.incarnation,
    }
}
fn pooled_values(nodes: &[Facade<PooledFactory>], replicas: usize, expected: i64) -> bool {
    nodes[..replicas].iter().all(|n| {
        (1..=8).all(|g| {
            let app = &n.local().applications[&group(g)];
            app.read_applied(app.applied_index()) == Ok(expected)
        })
    })
}
fn pooled_write(
    nodes: &mut [Facade<PooledFactory>],
    now: MonoTime,
    replicas: usize,
    operation: u128,
    delta: i64,
    expected: i64,
    duplicate: bool,
) {
    for g in 1..=8 {
        nodes[0]
            .propose(ClientRequest {
                group: group(g),
                operation: OperationId::new(operation).unwrap(),
                bytes: delta.to_le_bytes().to_vec(),
            })
            .unwrap();
    }
    let mut replies = 0;
    facade_drive_at(nodes, now, |nodes| {
        while let Some(reply) = nodes[0].poll_client() {
            match nodes[0].complete_client(reply).unwrap() {
                ClientOutcome::Applied { receipt, .. } => {
                    assert_eq!(receipt.operation, OperationId::new(operation).unwrap());
                    assert_eq!(receipt.duplicate, duplicate);
                    replies += 1;
                }
                other => panic!("pooled write outcome: {other:?}"),
            }
        }
        replies == 8 && pooled_values(nodes, replicas, expected)
    });
}
fn pooled_read(nodes: &mut [Facade<PooledFactory>], now: MonoTime, expected: i64) {
    for g in 1..=8 {
        nodes[0].read(group(g), ()).unwrap();
    }
    let mut replies = 0;
    facade_drive_at(nodes, now, |nodes| {
        while let Some(reply) = nodes[0].poll_read() {
            assert!(
                matches!(nodes[0].complete_read(reply).unwrap(), ReadOutcome::Read { result: Ok(value), .. } if value == expected)
            );
            replies += 1;
        }
        replies == 8
    });
}
#[test]
fn native_shared_peer_quota_pressure_reconnect_restart_and_retry() {
    let root =
        std::env::temp_dir().join(format!("voteboat-node-peer-quota-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    let (mut nodes, pools) = pooled_facades(&root, false);
    for g in 1..=100 {
        nodes[0].control(group(g), NodeControl::Campaign).unwrap();
    }
    facade_drive(&mut nodes, |nodes| {
        nodes.iter().all(|n| {
            n.local()
                .applications
                .values()
                .all(|a| a.read_applied(a.applied_index()) == Ok(7))
        })
    });
    let mut pressured = pools[0].clone();
    pressured.bind_owner(selected_owner(&nodes, 0, 2)).unwrap();
    facade_drive(&mut nodes, |_| pressured.owner_usage().unwrap().leases == 1);
    let frame = TransportLimits::default().send_frame_bytes;
    let old = nodes[0].peers().unwrap().roster().binding(node(3)).unwrap();
    nodes[0].disconnect(node(3), MonoTime(0)).unwrap();
    nodes[2].disconnect(node(1), MonoTime(0)).unwrap();
    facade_drive(&mut nodes, |_| pressured.owner_usage().unwrap().leases == 0);
    let held = pressured.acquire(2 * frame, 0).unwrap();
    assert_eq!(pressured.owner_usage().unwrap().reserved_bytes, 2 * frame);
    assert!(matches!(
        pressured.acquire(1, 0),
        Err(BufferError::Overloaded)
    ));
    // Reconnect at the configured first retry boundary, below the minimum
    // election deadline. The new real TLS session shares the held quota.
    facade_drive_at(&mut nodes, MonoTime(100), |nodes| {
        nodes[0]
            .peers()
            .unwrap()
            .roster()
            .binding(node(3))
            .is_some_and(|b| b.generation != old.generation)
    });
    // Ordinary Append entries to peer3 cannot acquire a Bulk frame. Peer2 still
    // receives actual log entries and supplies durable majority acknowledgements.
    pooled_write(&mut nodes, MonoTime(100), 2, 2, 3, 10, false);
    pooled_read(&mut nodes, MonoTime(100), 10);
    assert!(pooled_values(&nodes[2..], 1, 7));
    assert!(nodes[0].peers().unwrap().usage().staged_batches > 0);
    for pool in &pools {
        assert!(pool.usage().reserved_bytes <= pool.limits().reserved_bytes);
        assert!(pool.usage().leases <= pool.limits().leases);
    }
    assert_eq!(pressured.owner_usage().unwrap().reserved_bytes, 2 * frame);
    pooled_write(&mut nodes, MonoTime(100), 2, 3, 4, 14, false);
    pooled_read(&mut nodes, MonoTime(100), 14);
    assert!(pooled_values(&nodes[2..], 1, 7));
    drop(held);
    facade_drive_at(&mut nodes, MonoTime(100), |nodes| {
        pooled_values(nodes, 3, 14)
    });
    pooled_read(&mut nodes, MonoTime(100), 14);
    let stores = nodes
        .iter()
        .map(|n| n.local().owner.identity().store)
        .collect::<Vec<_>>();
    facade_close_at(nodes, MonoTime(100));
    for pool in &pools {
        assert_eq!(pool.usage(), BufferUsage::default());
    }
    assert_eq!(pressured.owner_usage(), Some(BufferUsage::default()));
    // A surviving host view remains usable after every owning Node has joined.
    let lease = pressured.acquire(frame, 1).unwrap();
    drop(lease);
    drop((pressured, pools));
    let (mut nodes, pools) = pooled_facades(&root, true);
    for (n, old) in nodes.iter().zip(stores) {
        assert_ne!(n.local().owner.identity().store, old);
    }
    for pool in &pools {
        assert_eq!(pool.usage(), BufferUsage::default());
    }
    for g in 1..=100 {
        nodes[0].control(group(g), NodeControl::Campaign).unwrap();
    }
    facade_drive(&mut nodes, |nodes| {
        (1..=100).all(|g| nodes[0].local().owner.core(group(g)).unwrap().role() == Role::Leader)
    });
    pooled_write(&mut nodes, MonoTime(0), 3, 2, 3, 14, true);
    pooled_write(&mut nodes, MonoTime(0), 3, 3, 4, 14, true);
    pooled_write(&mut nodes, MonoTime(0), 3, 4, 1, 15, false);
    pooled_read(&mut nodes, MonoTime(0), 15);
    facade_close(nodes);
    for pool in &pools {
        assert_eq!(pool.usage(), BufferUsage::default());
    }
    std::fs::remove_dir_all(root).unwrap();
}
