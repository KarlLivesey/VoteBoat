// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Preserve original leases while Written sends overlap a terminal continuation.
use super::*;

impl<Q: ReadyScheduler, T: TimerService, E: ElectionEntropy> EffectOwner<Q, T, E> {
    pub(super) fn reserve_replication(
        &mut self,
        event: &WorkerEvent,
    ) -> Result<BTreeSet<GroupIdentity>, EffectOwnerError> {
        let mut selected = BTreeSet::new();
        let WorkerEvent::Written { admissions, .. } = event else {
            return Ok(selected);
        };
        if self.authorization().is_logical() {
            return Ok(selected);
        }
        for (visit, _) in admissions {
            let Some(extra) = self
                .runtime
                .core(visit.group)
                .and_then(Raft::admitted_replication_reservation)
            else {
                continue;
            };
            let active = self
                .active
                .get(&visit.group)
                .ok_or(EffectOwnerError::StaleCompletion)?;
            if active.visit != *visit || active.leased.is_some() || !active.effects.is_empty() {
                return self.fail(EffectOwnerError::ProviderContract);
            }
            let bulk = self
                .active
                .values()
                .filter(|a| !a.control)
                .map(|a| a.reserved)
                .sum::<usize>();
            let available = (self.limits.reserved_bytes - self.reserved).min(if active.control {
                usize::MAX
            } else {
                (self.limits.reserved_bytes - self.limits.control_bytes).saturating_sub(bulk)
            });
            if extra > available {
                continue;
            }
            self.active.get_mut(&visit.group).unwrap().reserved += extra;
            self.reserved += extra;
            selected.insert(visit.group);
        }
        Ok(selected)
    }

    pub(super) fn install_continuation(
        &mut self,
        visit: VisitTicket,
        effects: Vec<Effect>,
    ) -> Result<(), EffectOwnerError> {
        let active = self
            .active
            .get_mut(&visit.group)
            .ok_or(EffectOwnerError::StaleCompletion)?;
        if active.visit != visit {
            return self.fail(EffectOwnerError::ProviderContract);
        }
        if effects.is_empty() {
            return self.refresh(visit.group);
        }
        if active.effects.is_empty() && active.leased.is_none() {
            return self.install(visit, effects);
        }
        // Only immutable early sends can coexist with the worker continuation.
        if active
            .kind
            .is_some_and(|kind| !matches!(kind, LeaseKind::Send(_)))
            || active
                .effects
                .iter()
                .any(|effect| !matches!(effect, Effect::Send(_)))
        {
            return self.fail(EffectOwnerError::ProviderContract);
        }
        if active.effects.try_reserve_exact(effects.len()).is_err() {
            return self.fail(EffectOwnerError::ReservationTooLarge);
        }
        let capacity = active.effects.capacity();
        let retained = effect_bytes(active.effects.make_contiguous(), capacity)
            .and_then(|n| n.checked_add(active.leased_bytes))
            .and_then(|n| n.checked_add(effect_bytes(&effects, effects.capacity())?));
        if retained.is_none_or(|n| n > active.reserved) {
            return self.fail(EffectOwnerError::ReservationTooLarge);
        }
        active.effects.extend(effects);
        self.refresh(visit.group)
    }
}
