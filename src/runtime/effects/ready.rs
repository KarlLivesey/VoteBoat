// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! One owned transition with original per-invocation observations.
use super::*;

impl<Q: ReadyScheduler, T: TimerService, E: ElectionEntropy> EffectOwner<Q, T, E> {
    pub(super) fn fit_ready_reservation(
        &mut self,
        visit: VisitTicket,
        now: MonoTime,
        max_items: usize,
        control: bool,
    ) -> Result<Option<(usize, usize)>, EffectOwnerError> {
        let max_items = max_items.min(
            self.runtime
                .core(visit.group)
                .ok_or(EffectOwnerError::Runtime(RuntimeError::UnknownGroup))?
                .batch_limits()
                .max_proposal_entries,
        );
        let Some(full) = self
            .runtime
            .next_ready_effect_reservation(visit, now, max_items)
            .map_err(EffectOwnerError::Runtime)?
        else {
            return Ok(None);
        };
        let mut room = self.limits.reserved_bytes - self.reserved;
        if !control {
            let bulk = self.bulk_reserved();
            room = room
                .min((self.limits.reserved_bytes - self.limits.control_bytes).saturating_sub(bulk));
        }
        if max_items <= 1 || full <= room {
            return Ok(Some((full, max_items)));
        }
        let single = self
            .runtime
            .next_ready_effect_reservation(visit, now, 1)
            .map_err(EffectOwnerError::Runtime)?
            .ok_or(EffectOwnerError::ProviderContract)?;
        if single > room {
            // Keep the original singleton refusal/backpressure behavior.
            return Ok(Some((single, 1)));
        }
        let mut low = 1;
        let mut high = max_items;
        let mut reserved = single;
        while low + 1 < high {
            let middle = low + (high - low) / 2;
            let bytes = self
                .runtime
                .next_ready_effect_reservation(visit, now, middle)
                .map_err(EffectOwnerError::Runtime)?
                .ok_or(EffectOwnerError::ProviderContract)?;
            if bytes <= room {
                low = middle;
                reserved = bytes;
            } else {
                high = middle;
            }
        }
        Ok(Some((reserved, low)))
    }

    pub(super) fn reserve_ready_visit(
        &mut self,
        visit: VisitTicket,
        reserved: usize,
        control: bool,
    ) -> Result<bool, EffectOwnerError> {
        if reserved > self.limits.reserved_bytes {
            return self.fail(EffectOwnerError::ReservationTooLarge);
        }
        let mut fits = reserved <= self.limits.reserved_bytes - self.reserved;
        if !control {
            if reserved > self.limits.reserved_bytes - self.limits.control_bytes {
                return self.fail(EffectOwnerError::ReservationTooLarge);
            }
            let bulk_count =
                self.visit_slots() - self.active.values().filter(|a| a.control).count();
            let bulk_bytes = self.bulk_reserved();
            fits &= bulk_count < self.limits.active_visits - self.limits.control_visits
                && reserved
                    <= (self.limits.reserved_bytes - self.limits.control_bytes)
                        .saturating_sub(bulk_bytes);
        }
        if !fits {
            self.runtime
                .finish(visit)
                .map_err(EffectOwnerError::Runtime)?;
            return Ok(false);
        }
        self.reserved += reserved;
        self.active.insert(
            visit.sequence,
            Active {
                visit,
                received_from: None,
                reserved,
                continuation_bytes: reserved,
                effects: VecDeque::new(),
                leased: None,
                leased_bytes: 0,
                kind: None,
                waiting: false,
                ready: false,
                control,
                read_captured: false,
                append: None,
            },
        );
        Ok(true)
    }

    pub(super) fn install_ready_steps(
        &mut self,
        visit: VisitTicket,
        mut step: Stepped,
        output: &mut Vec<OwnerStep>,
    ) -> Result<(), EffectOwnerError> {
        let members = std::mem::take(&mut step.batch_members);
        output
            .try_reserve_exact(members.len() + 1)
            .map_err(|_| EffectOwnerError::ReservationTooLarge)?;
        let error = step.result.as_ref().err().cloned();
        output.push(self.install_step(visit, step)?);
        for member in members {
            output.push(OwnerStep {
                admission: member.admission,
                proposed: member.proposed,
                operation: Some(member.operation),
                visit,
                read: None,
                error: member.error.or_else(|| error.clone()),
            });
        }
        Ok(())
    }
}
