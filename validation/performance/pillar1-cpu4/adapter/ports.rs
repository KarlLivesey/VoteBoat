// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! R3 producer/observer adaptation only. No pumping, timing or private replies.
use super::*;
use crate::voteboat_engine::writes::{Client, Observer};
struct Slot {
    client: Client,
    busy: bool,
}
struct Cursor {
    slot: usize,
    invocation: Option<WriteInvocation<OperationId>>,
    first: u128,
    count: usize,
    next: usize,
    stalled: bool,
}
pub(super) struct Pool<'a> {
    factory: &'a WriteFactory<OperationId>,
    sender: mpsc::Sender<Request>,
    limits: WritePortLimits,
    slots: Vec<Slot>,
    waits: VecDeque<Cursor>,
    scratch: Vec<u128>,
    maximum: usize,
}
/// The synchronous driver's bounded live request ownership differs from one
/// canonical client. Create its fresh observation scope BEFORE any originals,
/// then move the unique domain into the actual Actor. No stopped domain revives.
pub(super) fn registration(
    binding: WriteBinding,
    original_budget: usize,
    width: usize,
    per: usize,
) -> Result<(WriteDomain<OperationId>, WriteFactory<OperationId>, usize), Failure> {
    if width == 0 || per == 0 {
        return Err("r3 invalid shape".into());
    }
    let maximum = original_budget
        .checked_add(width.div_ceil(per))
        .and_then(|n| n.checked_add(1))
        .ok_or("r3 request budget overflow")?;
    let limits = crate::voteboat_engine::writes::port_limits(per.min(width))?;
    let bytes = limits
        .retained_bytes
        .checked_mul(maximum)
        .and_then(|n| n.checked_add(maximum * 32 + 4096))
        .ok_or("r3 port budget overflow")?;
    let (domain, factory) = WriteDomain::new(
        binding,
        WriteDomainLimits {
            ports: maximum,
            retained_bytes: bytes,
        },
    )
    .map_err(debug_error)?;
    Ok((domain, factory, maximum))
}
impl<'a> Pool<'a> {
    pub fn new(
        factory: &'a WriteFactory<OperationId>,
        sender: &mpsc::Sender<Request>,
        width: usize,
        per: usize,
        maximum: usize,
    ) -> Result<Self, Failure> {
        let limits = crate::voteboat_engine::writes::port_limits(per.min(width))?;
        let mut slots = Vec::new();
        slots.try_reserve_exact(maximum)?;
        let mut waits = VecDeque::new();
        waits.try_reserve_exact(maximum)?;
        let mut scratch = Vec::new();
        scratch.try_reserve_exact(limits.originals)?;
        let mut pool = Self {
            factory,
            sender: sender.clone(),
            limits,
            slots,
            waits,
            scratch,
            maximum,
        };
        for _ in 0..width.div_ceil(per) {
            pool.open()?;
        }
        Ok(pool)
    }
    fn open(&mut self) -> Result<usize, Failure> {
        if self.slots.len() == self.maximum {
            return Err("r3 port pool exhausted".into());
        }
        let client = Client::new(self.sender.clone(), self.factory, self.limits)?;
        let slot = self.slots.len();
        self.slots.push(Slot {
            client,
            busy: false,
        });
        Ok(slot)
    }
    pub fn enqueue(&mut self, first: u128, count: usize) -> Result<(), Failure> {
        if count == 0 || count > self.limits.originals || self.waits.len() == self.maximum {
            return Err("r3 bounded request shape".into());
        }
        let slot = match self.slots.iter().position(|slot| !slot.busy) {
            Some(slot) => slot,
            None => self.open()?,
        };
        self.scratch.clear();
        for ordinal in 0..count {
            self.scratch.push(
                first
                    .checked_add(ordinal as u128)
                    .ok_or("r3 original overflow")?,
            );
        }
        let (request, observer) = self.slots[slot].client.prepare(&self.scratch)?;
        if !matches!(request, Request::Port(_)) {
            return Err("r3 candidate must use public producer".into());
        }
        let Observer::Port(invocation) = observer else {
            return Err("r3 candidate must use public observer".into());
        };
        if invocation.len() != count {
            return Err("r3 invocation cardinality".into());
        }
        self.slots[slot].busy = true;
        // Same baseline ordering: observations enter the GLOBAL FIFO before
        // try_send. A rejected send remains an exact unresolved original set.
        self.waits.push_back(Cursor {
            slot,
            invocation: Some(invocation),
            first,
            count,
            next: 0,
            stalled: false,
        });
        self.sender
            .try_send(request)
            .map_err(|_| "r3 client queue")?;
        Ok(())
    }
    /// Consume at most ONE available original from the GLOBAL FIFO. None keeps
    /// its invocation/ordinal intact; it never creates a cancel-on-drop waiter.
    pub fn try_take_front(&mut self) -> Result<Option<u128>, Failure> {
        let Some(cursor) = self.waits.front_mut() else {
            return Ok(None);
        };
        if cursor.stalled {
            return Ok(None);
        }
        let invocation = cursor.invocation.as_mut().ok_or("r3 missing invocation")?;
        let expected = cursor
            .first
            .checked_add(cursor.next as u128)
            .ok_or("r3 expected ID overflow")?;
        let envelope = match invocation.try_take(cursor.next) {
            Ok(Some(envelope)) => envelope,
            Ok(None) => return Ok(None),
            Err(WriteError::ConsumerClosed | WriteError::AlreadyTerminal) => {
                cursor.stalled = true;
                return Ok(None);
            }
            Err(reason) => return Err(debug_error(reason)),
        };
        if !invocation.matches(&envelope, cursor.next)
            || envelope.operation().get() != expected
            || envelope.binding() != self.factory.binding()
        {
            return Err("r3 wrong envelope".into());
        }
        let OriginalOutcome::Applied { position, receipt } = envelope.into_outcome() else {
            cursor.stalled = true;
            return Ok(None);
        };
        if receipt.get() != expected || position.index == 0 || position.term == 0 {
            return Err("r3 wrong reply".into());
        }
        cursor.next += 1;
        if cursor.next == cursor.count {
            let mut cursor = self.waits.pop_front().ok_or("r3 missing FIFO cursor")?;
            cursor
                .invocation
                .take()
                .ok_or("r3 missing completed invocation")?
                .finish()
                .map_err(debug_error)?;
            self.slots[cursor.slot].busy = false;
        }
        Ok(Some(expected))
    }
    pub fn unreplied(&self) -> usize {
        self.waits
            .iter()
            .map(|cursor| cursor.count - cursor.next)
            .sum()
    }
    pub fn live_ports(&self) -> usize {
        self.slots.len()
    }
    pub fn bookkeeping_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.slots.capacity() * std::mem::size_of::<Slot>()
            + self.waits.capacity() * std::mem::size_of::<Cursor>()
            + self.scratch.capacity() * std::mem::size_of::<u128>()
    }
}
