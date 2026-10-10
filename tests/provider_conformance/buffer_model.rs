// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use std::collections::BTreeSet;

struct View<P> {
    pool: P,
    closed: bool,
    owner: Option<BufferOwner>,
}
struct Held<B> {
    buffer: B,
    owner: BufferOwner,
    class: BufferClass,
    reservation: usize,
    bytes: Vec<u8>,
}
pub(super) struct History<P: BufferPool> {
    root: P,
    views: Vec<View<P>>,
    held: Vec<Held<P::Buffer>>,
}
impl<P: BufferPool + Clone> History<P> {
    pub(super) fn new(root: P) -> Self {
        let views = (0..4)
            .map(|_| View {
                pool: root.clone(),
                closed: false,
                owner: None,
            })
            .collect();
        Self {
            root,
            views,
            held: Vec::new(),
        }
    }
    pub(super) fn len(&self) -> usize {
        self.held.len()
    }
    pub(super) fn reservation(&self, at: usize) -> usize {
        self.held[at].reservation
    }
    fn usage(&self, include: impl Fn(&Held<P::Buffer>) -> bool) -> BufferUsage {
        let mut usage = BufferUsage::default();
        for held in self.held.iter().filter(|h| include(h)) {
            usage.reserved_bytes += held.reservation;
            usage.leases += 1;
        }
        usage
    }
    fn registered(&self) -> BTreeSet<BufferOwner> {
        self.views
            .iter()
            .filter_map(|v| v.owner)
            .chain(self.held.iter().map(|h| h.owner))
            .collect()
    }
    pub(super) fn verify(&self) {
        assert_eq!(self.root.limits(), limits());
        assert_eq!(self.root.control_reserve(), Some(reserve()));
        assert_eq!(self.root.owner_limits(), Some(owners()));
        assert_eq!(
            self.root.usage(),
            self.usage(|_| true),
            "live reservations after action"
        );
        assert!(self.registered().len() <= owners().owners);
        for view in &self.views {
            assert_eq!(view.pool.limits(), self.root.limits());
            assert_eq!(view.pool.control_reserve(), self.root.control_reserve());
            assert_eq!(view.pool.owner_limits(), self.root.owner_limits());
            assert_eq!(view.pool.usage(), self.root.usage());
        }
        for held in &self.held {
            assert_eq!(held.buffer.reservation(), held.reservation);
            assert!(held.buffer.capacity() >= held.bytes.len());
            assert!(held.buffer.capacity() <= held.reservation);
            assert_eq!(
                held.buffer.as_ref(),
                held.bytes,
                "preserved prefix and zeroed growth"
            );
        }
    }
    pub(super) fn bind(&mut self, at: usize, owner: BufferOwner) {
        let view = &self.views[at];
        let expected = if view.closed {
            Err(BufferError::Closed)
        } else if owner.local_node == owner.peer_node {
            Err(BufferError::InvalidOwner)
        } else if let Some(existing) = view.owner {
            if existing == owner {
                Ok(())
            } else {
                Err(BufferError::InvalidOwner)
            }
        } else {
            let registered = self.registered();
            if !registered.contains(&owner) && registered.len() == owners().owners {
                Err(BufferError::Overloaded)
            } else {
                Ok(())
            }
        };
        assert_eq!(
            self.views[at].pool.bind_owner(owner),
            expected,
            "exact owner registration"
        );
        if expected.is_ok() {
            self.views[at].owner = Some(owner);
        }
        self.verify();
    }
    fn refusal(
        &self,
        at: usize,
        class: BufferClass,
        r: usize,
        initial: usize,
    ) -> Option<BufferError> {
        let view = &self.views[at];
        if view.closed {
            return Some(BufferError::Closed);
        }
        if r == 0 || r > limits().reserved_bytes || initial > r {
            return Some(BufferError::TooLarge);
        }
        let Some(owner) = view.owner else {
            return Some(BufferError::InvalidOwner);
        };
        let fits = |usage: BufferUsage, limit: BufferLimits| {
            usage.reserved_bytes + r <= limit.reserved_bytes && usage.leases < limit.leases
        };
        if !fits(self.usage(|_| true), limits()) {
            return Some(BufferError::Overloaded);
        }
        if class == BufferClass::Bulk {
            let bulk = BufferLimits {
                reserved_bytes: limits().reserved_bytes - reserve().reserved_bytes,
                leases: limits().leases - reserve().leases,
            };
            if !fits(self.usage(|h| h.class == BufferClass::Bulk), bulk)
                || !fits(
                    self.usage(|h| h.class == BufferClass::Bulk && h.owner == owner),
                    owners().bulk,
                )
            {
                return Some(BufferError::Overloaded);
            }
        }
        None
    }
    pub(super) fn acquire(
        &mut self,
        at: usize,
        class: BufferClass,
        r: usize,
        initial: usize,
        default: bool,
    ) {
        let expected = self.refusal(at, class, r, initial);
        let view = &self.views[at];
        let result = if default {
            assert_eq!(class, BufferClass::Bulk);
            view.pool.acquire(r, initial)
        } else {
            view.pool.acquire_class(class, r, initial)
        };
        match result {
            Ok(buffer) => {
                assert_eq!(expected, None, "provider exceeded modeled allowance");
                self.held.push(Held {
                    buffer,
                    owner: view.owner.unwrap(),
                    class,
                    reservation: r,
                    bytes: vec![0; initial],
                });
            }
            Err(error) => assert_eq!(
                Some(error),
                expected,
                "refusal versus declared uncontended capacity"
            ),
        }
        self.verify();
    }
    pub(super) fn resize(&mut self, at: usize, len: usize) {
        let held = &mut self.held[at];
        let expected = if len > held.reservation {
            Err(BufferError::TooLarge)
        } else {
            Ok(())
        };
        assert_eq!(held.buffer.resize(len), expected);
        if expected.is_ok() {
            held.bytes.resize(len, 0);
        }
        assert_eq!(
            held.buffer.as_mut(),
            held.bytes,
            "preserved prefix and zeroed growth"
        );
        self.verify();
    }
    pub(super) fn write(&mut self, at: usize, salt: u8) {
        let held = &mut self.held[at];
        for (i, byte) in held.bytes.iter_mut().enumerate() {
            *byte = salt.wrapping_add(i as u8);
        }
        held.buffer.as_mut().copy_from_slice(&held.bytes);
        self.verify();
    }
    pub(super) fn release(&mut self, at: usize) {
        self.held.swap_remove(at);
        self.verify();
    }
    pub(super) fn close(&mut self, at: usize) {
        let view = &mut self.views[at];
        view.pool.close();
        view.closed = true;
        self.verify();
    }
    pub(super) fn reset(&mut self, at: usize) {
        self.views[at] = View {
            pool: self.root.clone(),
            closed: false,
            owner: None,
        };
        self.verify();
    }
    pub(super) fn clone_view(&mut self, from: usize, to: usize) {
        let original = &self.views[from];
        self.views[to] = View {
            pool: original.pool.clone(),
            closed: original.closed,
            owner: original.owner,
        };
        self.verify();
    }
    pub(super) fn finish(mut self) {
        for view in &mut self.views {
            view.pool.close();
        }
        self.views.clear();
        self.verify();
        self.held.clear();
        self.verify();
        let mut next = self.root.clone();
        next.bind_owner(owner(4, 3)).unwrap();
        drop(next.acquire(72, 4).unwrap());
        assert_eq!(self.root.usage(), BufferUsage::default());
    }
}
