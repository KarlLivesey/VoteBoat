// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Typed front ownership over the existing full-sized effect ring.
use super::*;

#[cfg(test)]
mod tests;

/// One reusable FIFO, with full [`Effect`]-sized slots and no additional owner.
/// Inspecting, dropping or forgetting a front guard leaves its slot intact.
/// Take a front before calling a provider or completing another core transition.
///
/// ```
/// use voteboat::raft::{Effect, EffectQueue};
/// let mut queue = EffectQueue::from(vec![Effect::Committed(Vec::new())]);
/// assert_eq!(std::mem::size_of::<EffectQueue>(),
///     std::mem::size_of::<std::collections::VecDeque<Effect>>());
/// std::mem::forget(queue.front());
/// assert_eq!(queue.len(), 1);
/// ```
///
/// ```compile_fail
/// use voteboat::raft::EffectQueue;
/// let mut queue = EffectQueue::new();
/// let front = queue.front();
/// queue.clear(); // a live front exclusively borrows the queue
/// drop(front);
/// ```
#[derive(Debug, Default, Eq, PartialEq)]
#[cfg_attr(not(test), repr(transparent))]
pub struct EffectQueue {
    slots: VecDeque<Effect>,
    // Rust drops fields in declaration order: the observed slot allocation is
    // destroyed before this test-only RAII witness. Normal builds have no field.
    #[cfg(test)]
    storage_witness: crate::snapshot::input_lifetime_tests::StorageWitness,
}

/// Snapshot work extracted from its actual front slot.
#[derive(Debug)]
pub struct SnapshotRequest {
    pub to: NodeId,
    pub context: RequestContext,
    pub reference: SnapshotRef,
}

macro_rules! owned_fronts {
    ($($variant:ident($guard:ident => $payload:ty)),* $(,)?) => {
        /// An exclusive selection of the current front, never a detached handle.
        pub enum EffectFront<'q> {
            PersistVolatile(VolatileFront<'q>),
            SnapshotRequired(SnapshotFront<'q>),
            CheckpointRequired(CheckpointFront<'q>),
            $($variant($guard<'q>),)*
        }
        $(
            /// Exclusive front ownership; dropping this guard consumes nothing.
            pub struct $guard<'q> { queue: &'q mut EffectQueue }
            impl $guard<'_> {
                /// Transfer the actual owned payload before external work.
                pub fn take(self) -> $payload {
                    match self.queue.slots.pop_front() {
                        Some(Effect::$variant(value)) => value,
                        _ => unreachable!("exclusive front selection"),
                    }
                }
            }
        )*
        impl EffectQueue {
            /// Select the actual first variant without removing or cloning it.
            pub fn front(&mut self) -> Option<EffectFront<'_>> {
                Some(match self.slots.front()? {
                    Effect::PersistVolatile(_) => EffectFront::PersistVolatile(VolatileFront { queue: self }),
                    Effect::SnapshotRequired { .. } => EffectFront::SnapshotRequired(SnapshotFront { queue: self }),
                    Effect::CheckpointRequired { .. } => EffectFront::CheckpointRequired(CheckpointFront { queue: self }),
                    $(Effect::$variant(_) => EffectFront::$variant($guard { queue: self }),)*
                })
            }
            pub fn append(&mut self, mut other: Self) {
                // Match the old exact-size Vec iterator's reservation before
                // taking any front; per-element growth can yield a larger ring.
                self.slots.reserve(other.len());
                while let Some(front) = other.front() {
                    match front {
                        EffectFront::PersistVolatile(front) => self.push_volatile(front.take()),
                        EffectFront::SnapshotRequired(front) => {
                            let SnapshotRequest { to, context, reference } = front.take();
                            self.slots.push_back(Effect::SnapshotRequired { to, context, reference });
                        }
                        EffectFront::CheckpointRequired(front) => self.slots.push_back(Effect::CheckpointRequired { context: front.take() }),
                        $(EffectFront::$variant(front) => self.slots.push_back(Effect::$variant(front.take())),)*
                    }
                }
            }
            /// Existing durable lease bridge; volatile work cannot escape here.
            pub(crate) fn take_durable(&mut self) -> Option<Effect> {
                Some(match self.front()? {
                    EffectFront::PersistVolatile(_) => return None,
                    EffectFront::SnapshotRequired(front) => {
                        let SnapshotRequest { to, context, reference } = front.take();
                        Effect::SnapshotRequired { to, context, reference }
                    }
                    EffectFront::CheckpointRequired(front) => Effect::CheckpointRequired { context: front.take() },
                    $(EffectFront::$variant(front) => Effect::$variant(front.take()),)*
                })
            }
        }
    };
}
owned_fronts! {
    VerifyLearnerReadiness(ReadinessFront => Message),
    Persist(PersistFront => LogUpdate),
    Send(SendFront => Message),
    Committed(CommittedFront => Vec<LogEntry>),
    ReadReady(ReadFront => ReadBarrier),
    StageSnapshot(StageSnapshotFront => Message),
    SnapshotInstalled(InstalledSnapshotFront => SnapshotRef),
    CheckpointCompacted(CompactedCheckpointFront => SnapshotRef),
}

/// Exclusive volatile front; no Drop action and no reservation authority.
pub struct VolatileFront<'q> {
    queue: &'q mut EffectQueue,
}
impl VolatileFront<'_> {
    /// Copy only the existing token, then destroy the front in its slot.
    pub fn take(self) -> VolatilePersist {
        let Some(Effect::PersistVolatile(token)) = self.queue.slots.front() else {
            unreachable!("exclusive volatile front selection")
        };
        let token = *token;
        drop(self.queue.slots.drain(..1));
        token
    }
}
/// Exclusive snapshot front; dropping it leaves the work queued.
pub struct SnapshotFront<'q> {
    queue: &'q mut EffectQueue,
}
impl SnapshotFront<'_> {
    pub fn take(self) -> SnapshotRequest {
        match self.queue.slots.pop_front() {
            Some(Effect::SnapshotRequired {
                to,
                context,
                reference,
            }) => SnapshotRequest {
                to,
                context,
                reference,
            },
            _ => unreachable!("exclusive snapshot front selection"),
        }
    }
}
/// Exclusive checkpoint front; dropping it leaves the work queued.
pub struct CheckpointFront<'q> {
    queue: &'q mut EffectQueue,
}
impl CheckpointFront<'_> {
    pub fn take(self) -> RequestContext {
        match self.queue.slots.pop_front() {
            Some(Effect::CheckpointRequired { context }) => context,
            _ => unreachable!("exclusive checkpoint front selection"),
        }
    }
}

impl EffectQueue {
    pub fn new() -> Self {
        #[cfg(test)]
        crate::snapshot::input_lifetime_tests::at_boundary("following queue");
        Self::default()
    }
    #[cfg(test)]
    pub(crate) fn watch_storage_destruction(&mut self) {
        self.storage_witness = crate::snapshot::input_lifetime_tests::StorageWitness::armed();
    }
    pub fn len(&self) -> usize {
        self.slots.len()
    }
    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }
    pub fn capacity(&self) -> usize {
        self.slots.capacity()
    }
    #[cfg(test)]
    pub(crate) fn as_slices(&self) -> (&[Effect], &[Effect]) {
        self.slots.as_slices()
    }
    pub fn clear(&mut self) {
        self.slots.clear();
    }
    pub fn truncate(&mut self, len: usize) {
        self.slots.truncate(len);
    }
    /// Restore the PRE-CALL committed boundary after earlier queued work.
    pub fn insert_committed(&mut self, boundary: usize, entries: Vec<LogEntry>) {
        self.slots.insert(boundary, Effect::Committed(entries));
    }
    /// Explicit legacy owned export. Full-envelope conversion remains paid.
    pub fn into_owned(self) -> Vec<Effect> {
        self.slots.into()
    }
    pub(crate) fn push_volatile(&mut self, token: VolatilePersist) {
        self.slots.push_back(Effect::PersistVolatile(token));
    }
    pub(crate) fn push_back(&mut self, effect: Effect) {
        self.slots.push_back(effect);
    }
    pub(crate) fn push_front(&mut self, effect: Effect) {
        self.slots.push_front(effect);
    }
    pub(crate) fn peek(&self) -> Option<&Effect> {
        self.slots.front()
    }
    pub(crate) fn iter(&self) -> impl DoubleEndedIterator<Item = &Effect> {
        self.slots.iter()
    }
    pub(crate) fn range(&self, range: std::ops::RangeFrom<usize>) -> impl Iterator<Item = &Effect> {
        self.slots.range(range)
    }
    pub(crate) fn make_contiguous(&mut self) -> &mut [Effect] {
        self.slots.make_contiguous()
    }
    pub(crate) fn extend(&mut self, other: impl Into<Self>) {
        self.append(other.into());
    }
}
impl From<Vec<Effect>> for EffectQueue {
    fn from(effects: Vec<Effect>) -> Self {
        #[cfg(test)]
        crate::snapshot::input_lifetime_tests::at_boundary("following queue");
        Self {
            slots: effects.into(),
            #[cfg(test)]
            storage_witness: Default::default(),
        }
    }
}
impl From<VecDeque<Effect>> for EffectQueue {
    fn from(slots: VecDeque<Effect>) -> Self {
        Self {
            slots,
            #[cfg(test)]
            storage_witness: Default::default(),
        }
    }
}
impl From<EffectQueue> for Vec<Effect> {
    fn from(queue: EffectQueue) -> Self {
        queue.into_owned()
    }
}

impl EffectQueue {
    pub(crate) fn try_reserve_exact(
        &mut self,
        additional: usize,
    ) -> Result<(), std::collections::TryReserveError> {
        self.slots.try_reserve_exact(additional)
    }
    pub(crate) fn iter_mut(&mut self) -> impl Iterator<Item = &mut Effect> {
        self.slots.iter_mut()
    }
}
impl FromIterator<Effect> for EffectQueue {
    fn from_iter<T: IntoIterator<Item = Effect>>(iter: T) -> Self {
        // Preserve the old Vec collector's known-N allocation policy.
        iter.into_iter().collect::<Vec<_>>().into()
    }
}
