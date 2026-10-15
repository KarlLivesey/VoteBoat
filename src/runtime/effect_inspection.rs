// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Existing Visit/lease identity only, with no ownership transfer or proof.
use super::*;
/// Describes the moved original effect; it contains no ReadBarrier or lease.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EffectLeaseInspectionKind {
    Persist,
    Send(NodeId),
    Committed(u64),
    Read(ReadRequestId),
    External,
}
impl From<LeaseKind> for EffectLeaseInspectionKind {
    fn from(kind: LeaseKind) -> Self {
        match kind {
            LeaseKind::Persist => Self::Persist,
            LeaseKind::Send(peer) => Self::Send(peer),
            LeaseKind::Committed(index) => Self::Committed(index),
            LeaseKind::Read(barrier) => Self::Read(barrier.request()),
            LeaseKind::External => Self::External,
        }
    }
}
/// A sampled original Visit. Absence does not prove completion or durability.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EffectVisitInspection {
    visit: VisitTicket,
    leased: Option<EffectTicket>,
    kind: Option<EffectLeaseInspectionKind>,
    remaining: usize,
    waiting: bool,
}
impl EffectVisitInspection {
    pub fn visit(self) -> VisitTicket {
        self.visit
    }
    pub fn leased(self) -> Option<EffectTicket> {
        self.leased
    }
    pub fn kind(self) -> Option<EffectLeaseInspectionKind> {
        self.kind
    }
    pub fn remaining(self) -> usize {
        self.remaining
    }
    pub fn waiting(self) -> bool {
        self.waiting
    }
}
impl<Q: ReadyScheduler, T: TimerService, E: ElectionEntropy> EffectOwner<Q, T, E> {
    /// Immutable original ownership inspection, including during failed cleanup.
    /// The moved effect's body remains owned by its original downstream holder.
    pub fn inspect_visit(
        &self,
        group: GroupIdentity,
    ) -> Result<Option<EffectVisitInspection>, EffectOwnerError> {
        if self.core(group).is_none() {
            return Err(EffectOwnerError::Runtime(RuntimeError::UnknownGroup));
        }
        Ok(self
            .active
            .values()
            .find(|a| a.visit.group == group)
            .map(|active| EffectVisitInspection {
                visit: active.visit,
                leased: active.leased,
                kind: active.kind.map(EffectLeaseInspectionKind::from),
                remaining: active.effects.len(),
                waiting: active.waiting,
            }))
    }
}
