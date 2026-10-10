// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Host-driven bounded dispatch; only observed committed cores establish progress.
use super::*;
use crate::{identity::GroupIdentity, raft::Raft};
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct DrainDispatchTicket {
    domain: Arc<()>,
    group: GroupIdentity,
    sequence: u64,
}
impl DrainDispatchTicket {
    pub fn group(&self) -> GroupIdentity {
        self.group
    }
}
impl PartialEq for DrainDispatchTicket {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.domain, &other.domain)
            && self.group == other.group
            && self.sequence == other.sequence
    }
}
impl Eq for DrainDispatchTicket {}

#[derive(Debug)]
pub struct DrainDispatch {
    pub ticket: DrainDispatchTicket,
    /// Always Transfer or Configure; the host retains its normal Node ticket.
    pub action: MembershipDrainAction,
}
#[derive(Debug, Default)]
pub struct DrainDispatchBatch {
    pub scanned: usize,
    /// Completed observations in this scan only, never a cached stop certificate.
    pub observed_complete: usize,
    pub unavailable: usize,
    pub requests: Vec<DrainDispatch>,
    pub errors: Vec<(GroupIdentity, DrainPlanError)>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DrainCoordinatorError {
    InvalidLimit,
    Plan(DrainPlanError),
    SequenceExhausted,
    StaleTicket,
}
/// No I/O or background work. The caller restores the source's journal before
/// polling Nodes and supplies fresh core observations from its chosen hosts.
/// Ticket completion releases dispatch ownership only; it grants no authority.
pub struct MembershipDrainCoordinator {
    plan: MembershipDrainPlan,
    domain: Arc<()>,
    outstanding: Vec<Option<DrainDispatchTicket>>,
    in_flight: usize,
    limit: usize,
    cursor: usize,
    sequence: u64,
}
impl MembershipDrainCoordinator {
    pub fn new(plan: MembershipDrainPlan, limit: usize) -> Result<Self, DrainCoordinatorError> {
        if limit == 0 || limit > plan.assignments().len() {
            return Err(DrainCoordinatorError::InvalidLimit);
        }
        Ok(Self {
            outstanding: vec![None; plan.assignments().len()],
            plan,
            domain: Arc::new(()),
            in_flight: 0,
            limit,
            cursor: 0,
            sequence: 0,
        })
    }
    pub fn plan(&self) -> &MembershipDrainPlan {
        &self.plan
    }
    pub fn in_flight(&self) -> usize {
        self.in_flight
    }
    /// Release only after host delivery was rejected, or its accepted wait has
    /// terminated. The next scan still rechecks core state after unknown results.
    /// Dropping this coordinator does not cancel Node work; the host owns cleanup.
    pub fn finish(&mut self, ticket: &DrainDispatchTicket) -> Result<(), DrainCoordinatorError> {
        if !Arc::ptr_eq(&self.domain, &ticket.domain) {
            return Err(DrainCoordinatorError::StaleTicket);
        }
        let index = self
            .plan
            .assignments()
            .binary_search_by_key(&ticket.group, |entry| entry.group)
            .map_err(|_| DrainCoordinatorError::StaleTicket)?;
        if self.outstanding[index].as_ref() != Some(ticket) {
            return Err(DrainCoordinatorError::StaleTicket);
        }
        self.outstanding[index] = None;
        self.in_flight -= 1;
        Ok(())
    }
    /// Scan each selected group at most once, advancing the cursor even when
    /// its host is unavailable or work is held. Errors are per-group so earlier
    /// dispatch ownership is never lost behind an error return.
    /// A full in-flight window pauses scanning at the next group, so an early
    /// group cannot repeatedly take a released slot ahead of later groups.
    pub fn poll<'a>(
        &mut self,
        journal: &impl DrainJournal,
        budget: usize,
        mut observe: impl FnMut(GroupIdentity) -> Option<&'a Raft>,
    ) -> Result<DrainDispatchBatch, DrainCoordinatorError> {
        if budget == 0 || budget > crate::runtime::MAX_LOCAL_DRAIN_GROUPS {
            return Err(DrainCoordinatorError::InvalidLimit);
        }
        self.plan
            .verify(journal)
            .map_err(DrainCoordinatorError::Plan)?;
        let count = budget.min(self.outstanding.len());
        self.sequence
            .checked_add(count as u64)
            .ok_or(DrainCoordinatorError::SequenceExhausted)?;
        let mut batch = DrainDispatchBatch::default();
        for _ in 0..count {
            if self.in_flight == self.limit {
                break;
            }
            let index = self.cursor;
            self.cursor = (self.cursor + 1) % self.outstanding.len();
            batch.scanned += 1;
            if self.outstanding[index].is_some() {
                continue;
            }
            let group = self.plan.assignments()[index].group;
            let Some(core) = observe(group) else {
                batch.unavailable += 1;
                continue;
            };
            if core.state().bootstrap.group != group {
                batch.errors.push((group, DrainPlanError::UnknownGroup));
                continue;
            }
            match self.plan.next(journal, core) {
                Ok(MembershipDrainAction::Completed) => batch.observed_complete += 1,
                Ok(MembershipDrainAction::Wait) => (),
                Ok(action) if self.in_flight < self.limit => {
                    self.sequence += 1;
                    let ticket = DrainDispatchTicket {
                        domain: self.domain.clone(),
                        group,
                        sequence: self.sequence,
                    };
                    self.outstanding[index] = Some(ticket.clone());
                    self.in_flight += 1;
                    batch.requests.push(DrainDispatch { ticket, action });
                }
                Ok(_) => (),
                Err(error) => batch.errors.push((group, error)),
            }
        }
        Ok(batch)
    }
}
