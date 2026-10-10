// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::support::buffer::HostPool;
use voteboat::{buffer::*, identity::*};
#[path = "buffer_model.rs"]
mod model;
#[path = "buffer_mutants.rs"]
mod mutants;
use model::History;

fn limits() -> BufferLimits {
    BufferLimits {
        reserved_bytes: 168,
        leases: 7,
    }
}
fn reserve() -> BufferLimits {
    BufferLimits {
        reserved_bytes: 24,
        leases: 1,
    }
}
fn owners() -> BufferOwnerLimits {
    BufferOwnerLimits {
        owners: 2,
        bulk: BufferLimits {
            reserved_bytes: 72,
            leases: 3,
        },
    }
}
fn owner(peer: u64, incarnation: u64) -> BufferOwner {
    BufferOwner {
        local_node: NodeId::new(1).unwrap(),
        local_store: StoreId::new(1).unwrap(),
        local_incarnation: StoreIncarnation::new(1).unwrap(),
        peer_node: NodeId::new(peer).unwrap(),
        peer_store: StoreId::new(peer.into()).unwrap(),
        peer_incarnation: StoreIncarnation::new(incarnation).unwrap(),
    }
}
fn host() -> HostPool {
    HostPool::with_owner_limits(limits(), reserve(), owners())
}
fn quota_boundaries<P: BufferPool + Clone>(state: &mut History<P>) {
    state.acquire(0, BufferClass::Bulk, 1, 0, true); // Unbound view refuses.
    state.bind(0, owner(1, 1)); // Invalid accounting identity grants no slot.
    state.bind(0, owner(2, 1));
    state.bind(1, owner(2, 1));
    for _ in 0..3 {
        state.acquire(0, BufferClass::Bulk, 1, 1, true);
    }
    state.acquire(1, BufferClass::Bulk, 1, 0, true); // Owner lease limit, not bytes.
    state.acquire(0, BufferClass::Control, 1, 1, false);
    state.bind(2, owner(3, 1));
    for _ in 0..3 {
        state.acquire(2, BufferClass::Bulk, 1, 0, false);
    }
    state.acquire(2, BufferClass::Control, 1, 0, false); // Total lease limit.
    while state.len() > 0 {
        state.release(0);
    }
    for view in 0..4 {
        state.reset(view);
    }
}
fn prelude<P: BufferPool + Clone>(state: &mut History<P>) {
    state.bind(0, owner(2, 1));
    state.bind(1, owner(3, 1));
    state.acquire(0, BufferClass::Bulk, 72, 4, true);
    state.write(0, 17);
    state.resize(0, 72);
    state.resize(0, 2);
    state.resize(0, 8);
    state.resize(0, 73);
    state.acquire(0, BufferClass::Bulk, 1, 0, false);
    state.bind(2, owner(2, 1));
    state.acquire(2, BufferClass::Bulk, 1, 0, true);
    state.acquire(1, BufferClass::Bulk, 72, 4, true);
    state.acquire(2, BufferClass::Control, 24, 4, false);
    state.acquire(1, BufferClass::Control, 1, 0, false);
    state.close(0);
    state.acquire(0, BufferClass::Bulk, 1, 0, true);
    state.clone_view(0, 3);
    state.acquire(3, BufferClass::Control, 1, 0, false);
    state.reset(0);
    state.reset(2);
    state.reset(3);
    state.resize(0, 72); // Held bytes remain mutable after their views are dropped.
    state.bind(3, owner(4, 1)); // Held bulk and control keep owner2 registered.
    state.release(0); // swap_remove moves the control buffer to slot0.
    state.bind(3, owner(4, 1)); // Control alone still pins the owner slot.
    state.release(0);
    state.bind(3, owner(4, 1)); // Final owner2 buffer now released.
    state.bind(3, owner(2, 2)); // A bound view cannot silently change incarnation.
}
fn history<P: BufferPool + Clone>(root: P, mut seed: u64) {
    let mut state = History::new(root);
    quota_boundaries(&mut state);
    prelude(&mut state);
    for step in 0..256 {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let action = (seed >> 32) as usize;
        let view = action % 4;
        match (action / 4) % 9 {
            0 => state.bind(view, owner(1 + (action / 36) as u64 % 4, 1 + step % 2)),
            1 | 2 => {
                let r = [0, 1, 24, 48, 72, 73, 144, 168, 169][(action / 36) % 9];
                let initial = [0, 4.min(r), r, r + 1][(action / 324) % 4];
                let default = action.is_multiple_of(2);
                let class = if default || action.is_multiple_of(3) {
                    BufferClass::Bulk
                } else {
                    BufferClass::Control
                };
                state.acquire(view, class, r, initial, default);
            }
            3 if state.len() > 0 => state.release(action % state.len()),
            4 if state.len() > 0 => {
                let slot = action % state.len();
                let r = state.reservation(slot);
                state.resize(slot, [0, 1, r, r + 1][(action / 36) % 4]);
            }
            5 if state.len() > 0 => state.write(action % state.len(), step as u8),
            6 => state.close(view),
            7 => state.reset(view),
            8 => state.clone_view((view + 1) % 4, view),
            _ => (),
        }
        state.verify();
    }
    state.finish();
}
#[test]
fn host_buffer_generated_owner_class_and_byte_lifetimes() {
    for seed in 1..=32 {
        history(host(), seed);
    }
}
#[cfg(feature = "native")]
#[test]
fn native_buffer_generated_owner_class_and_byte_lifetimes() {
    for seed in 1..=32 {
        history(
            voteboat::native::buffer::NativeBufferPool::new_with_owner_limits(
                limits(),
                reserve(),
                owners(),
            )
            .unwrap(),
            seed,
        );
    }
}
