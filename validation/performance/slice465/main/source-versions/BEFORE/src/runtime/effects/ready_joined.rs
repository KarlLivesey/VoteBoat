// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Bulk-only optional planning layered over an independently reserved control.
use super::*;
impl<Q: ReadyScheduler, T: TimerService, E: ElectionEntropy> EffectOwner<Q, T, E> {
    pub(super) fn try_joined_ready(
        &mut self,
        visit: VisitTicket,
        now: MonoTime,
        max: usize,
        check: &mut impl FnMut(&Raft, &Event, &[ReadyProposal]) -> Result<(), RaftError>,
        output: &mut Vec<OwnerStep>,
    ) -> Result<bool, EffectOwnerError> {
        let Some((reserved, observations)) = self
            .runtime
            .joined_reservation(visit, now, max)
            .map_err(EffectOwnerError::Runtime)?
        else {
            return Ok(false);
        };
        let original = self.active[&visit.group].reserved;
        let Some(reserved) = output
            .len()
            .checked_mul(size_of::<OwnerStep>())
            .and_then(|bytes| reserved.checked_add(bytes))
        else {
            return Ok(false);
        };
        let reserved = reserved.max(original);
        if !self.joined_bulk_fits(reserved, original) {
            return Ok(false);
        }
        let additional = reserved - original;
        self.reserved += additional;
        {
            let active = self.active.get_mut(&visit.group).unwrap();
            active.reserved = reserved;
            active.continuation_bytes = reserved;
            active.control = false;
        }
        let mut sources = Vec::new();
        let mut expanded = Vec::new();
        let allocated = expanded
            .try_reserve_exact(output.len() + observations)
            .is_ok()
            && sources.try_reserve_exact(observations).is_ok();
        let joined = if allocated {
            self.runtime
                .step_joined_checked(visit, now, max, check)
                .map_err(EffectOwnerError::Runtime)
        } else {
            Ok(None)
        };
        let joined = match joined {
            Ok(Some(joined)) => joined,
            other => {
                self.reserved -= additional;
                let active = self.active.get_mut(&visit.group).unwrap();
                active.reserved = original;
                active.continuation_bytes = original;
                active.control = true;
                return other.map(|_| false);
            }
        };
        expanded.append(output);
        for control in &joined.controls {
            sources.push(control.received_from);
        }
        let first = sources.remove(0);
        let members = joined.controls.len();
        // All owner output and source capacity is already allocated. The joined
        // Core mutation cannot be followed by an allocation-dependent refusal.
        let proposal_offset = expanded.len() + members;
        for control in joined.controls {
            expanded.push(OwnerStep {
                admission: control.admission,
                proposed: None,
                visit,
                operation: None,
                read: None,
                error: None,
            });
        }
        self.install_ready_steps(visit, joined.proposals, &mut expanded)?;
        debug_assert_eq!(expanded[proposal_offset].visit, visit);
        *output = expanded;
        let active = self.active.get_mut(&visit.group).unwrap();
        active.received_from = Some(first);
        active.other_received_from = sources;
        Ok(true)
    }
    fn joined_bulk_fits(&self, reserved: usize, original: usize) -> bool {
        let bulk_ceiling = self.limits.reserved_bytes - self.limits.control_bytes;
        let bulk_count = self.active.values().filter(|a| !a.control).count();
        let bulk = self
            .active
            .values()
            .filter(|a| !a.control)
            .map(|a| a.reserved)
            .sum::<usize>();
        reserved <= bulk_ceiling.saturating_sub(bulk)
            && reserved.saturating_sub(original) <= self.limits.reserved_bytes - self.reserved
            && bulk_count < self.limits.active_visits - self.limits.control_visits
    }
}
