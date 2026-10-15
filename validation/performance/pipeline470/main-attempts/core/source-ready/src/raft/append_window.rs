// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Explicit ordinary-append ownership while earlier units await local durability.
//! Default stepping stays serialized. No accepted position is durability evidence.
use super::*;
use crate::log::transition::LogMetadata;
use std::mem::size_of;

/// Startup-only aggregate ceilings, additional to every existing per-unit limit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AppendWindowLimits {
    pub max_units: usize,
    pub max_originals: usize,
    pub max_retained_bytes: usize,
}
impl AppendWindowLimits {
    pub fn validate(self, log: LogLimits, batch: BatchLimits) -> Result<Self, RaftError> {
        let log = log.validate()?;
        let batch = batch.validate()?;
        if !(2..=64).contains(&self.max_units)
            || self.max_units > log.max_pending_units
            || self.max_originals < self.max_units
            || self.max_originals > batch.max_proposal_entries
            || self.max_retained_bytes == 0
            || self.max_retained_bytes > log.max_batch_bytes
        {
            return Err(StorageError::Rejected("invalid append window limits").into());
        }
        Ok(self)
    }
}
/// Exact volatile Core unit. Host runtime/worker generations are checked separately.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AppendUnitId {
    binding: StoreBinding,
    group: GroupIdentity,
    window: u64,
    ordinal: usize,
}
#[derive(Debug)]
pub struct StagedAppend {
    pub id: AppendUnitId,
    pub update: LogUpdate,
}
#[derive(Debug)]
pub struct AppendRejected {
    pub reason: RaftError,
    pub proposals: Vec<Proposal>,
}
#[derive(Debug)]
pub enum AppendCompletion {
    Waiting,
    /// Core dependencies retired, not application results or host-credit release.
    Retired {
        units: Vec<AppendUnitId>,
        effects: Vec<Effect>,
    },
}
pub(super) struct Unit {
    id: AppendUnitId,
    predecessor: LogMetadata,
    pub(super) next: OwnedNextLog,
    pub(super) ticket: Option<LogTicket>,
    durable: bool,
}
pub(super) struct Window {
    opened: LogMetadata,
    accepted: LogMetadata,
    summary: LogSummary,
    tail: Vec<LogEntry>,
    pub(super) units: Vec<Unit>,
    sealed: bool,
    retained: usize,
}

fn metadata_cost(limits: AppendWindowLimits) -> Option<usize> {
    size_of::<Window>()
        .checked_add(limits.max_units.checked_mul(size_of::<Unit>())?)?
        .checked_add(limits.max_originals.checked_mul(size_of::<LogEntry>())?)
}
fn input_cost(proposals: &[Proposal], capacity: usize) -> Option<usize> {
    proposals.iter().try_fold(
        capacity
            .checked_mul(size_of::<Proposal>())?
            .checked_add(size_of::<Vec<Proposal>>())?,
        |sum, proposal| sum.checked_add(proposal.bytes.capacity()),
    )
}
fn unit_cost(proposals: &[Proposal]) -> Option<usize> {
    let payload = proposals.iter().try_fold(0usize, |sum, proposal| {
        sum.checked_add(CommandBytes::retained_bytes_for_len(proposal.bytes.len())?)
    })?;
    // Independent protected plan, exported effect and complete accepted tail.
    size_of::<StagedAppend>()
        .checked_add(proposals.len().checked_mul(2 * size_of::<LogEntry>())?)?
        .checked_add(payload.checked_mul(3)?)
}
fn window_update(predecessor: LogMetadata, first: u64, proposals: &[Proposal]) -> LogUpdate {
    LogUpdate {
        group: predecessor.group,
        expected_revision: predecessor.revision,
        hard_state: predecessor.hard_state,
        commit_index: predecessor.commit_index,
        suffix: Some(Suffix {
            from: first,
            entries: proposals
                .iter()
                .enumerate()
                .map(|(offset, proposal)| LogEntry {
                    index: first + offset as u64,
                    term: predecessor.hard_state.term,
                    payload: EntryPayload::Command {
                        operation: proposal.operation,
                        bytes: proposal.bytes.as_slice().into(),
                    },
                })
                .collect(),
        }),
        snapshot: None,
        snapshot_membership: None,
    }
}
impl Raft {
    pub fn configure_append_window(
        &mut self,
        limits: Option<AppendWindowLimits>,
    ) -> Result<(), RaftError> {
        if self.fenced {
            return Err(RaftError::Fenced);
        }
        if self.batch_limits_locked {
            return Err(RaftError::Busy);
        }
        self.append_window_limits = limits
            .map(|v| v.validate(self.limits, self.batch_limits))
            .transpose()?;
        Ok(())
    }
    pub fn append_window_limits(&self) -> Option<AppendWindowLimits> {
        self.append_window_limits
    }
    fn window_context_eligible(&self) -> bool {
        !self.fenced
            && self.role == Role::Leader
            && self.local_voter()
            && self.log_summary.is_plain()
            && self.progress_sends_enabled()
            && self.pending.is_none()
            && self.read.is_none()
            && self.ready_read.is_none()
            && self.staged_snapshot.is_none()
            && self.application_install.is_none()
            && self.checkpoint_requested.is_none()
            && self.checkpoint_reconcile.is_none()
            && self.readiness_check.is_none()
            && self.learner_readiness.is_none()
            && self.ready_learner.is_none()
            && self.authority_request.is_none()
            && self.replication_permit.is_none()
    }
    pub fn can_stage_append_window(&self) -> bool {
        let Some(limits) = self.append_window_limits else {
            return false;
        };
        self.window_context_eligible()
            && self.append_window.as_ref().is_none_or(|w| {
                !w.sealed
                    && w.opened == LogMetadata::from_log(&self.durable)
                    && w.units.len() < limits.max_units
                    && w.tail.len() < limits.max_originals
            })
    }
    /// Full Core/export/raw conversion peak. Host input/output/worker/receipt
    /// escrows are additional; this number is never admission authority.
    pub fn append_window_reservation(
        &self,
        proposals: &[Proposal],
        capacity: usize,
    ) -> Option<usize> {
        let limits = self.append_window_limits?;
        if !self.can_stage_append_window()
            || proposals.is_empty()
            || capacity < proposals.len()
            || capacity > self.batch_limits.max_proposal_entries
            || proposals.len() > self.batch_limits.max_proposal_entries
        {
            return None;
        }
        let existing = self.append_window.as_ref().map_or(0, |w| w.tail.len());
        if existing.checked_add(proposals.len())? > limits.max_originals {
            return None;
        }
        let retained = self
            .append_window
            .as_ref()
            .map(|w| w.retained)
            .or_else(|| metadata_cost(limits))?;
        let peak = retained
            .checked_add(unit_cost(proposals)?)?
            .checked_add(input_cost(proposals, capacity)?)?;
        (peak <= limits.max_retained_bytes).then_some(peak)
    }
    /// Staging never applies commands or publishes durable/quorum progress.
    /// Apart from the first-attempt configuration lock, errors return the exact
    /// original input and leave prior staged state and counters unchanged.
    pub fn stage_append_window(
        &mut self,
        proposals: Vec<Proposal>,
    ) -> Result<StagedAppend, AppendRejected> {
        self.batch_limits_locked = true;
        match self.prepare_window_unit(&proposals, proposals.capacity()) {
            Err(reason) => Err(AppendRejected { reason, proposals }),
            Ok(prepared) => {
                drop(proposals);
                Ok(self.install_window_unit(prepared))
            }
        }
    }
    fn prepare_window_unit(
        &self,
        proposals: &[Proposal],
        capacity: usize,
    ) -> Result<PreparedUnit, RaftError> {
        if self.fenced {
            return Err(RaftError::Fenced);
        }
        if !self.can_stage_append_window() {
            return Err(RaftError::Busy);
        }
        let limits = self.append_window_limits.unwrap();
        let (predecessor, summary, view) = if let Some(w) = &self.append_window {
            (
                w.accepted,
                w.summary,
                LogView::overlay(&self.durable, &w.tail, w.accepted),
            )
        } else {
            (
                LogMetadata::from_log(&self.durable),
                self.log_summary,
                LogView::new(&self.durable),
            )
        };
        let first = self.validate_proposal_batch_at(
            proposals,
            capacity,
            predecessor.entries_len,
            predecessor.last_index,
        )?;
        self.validate_conversion_peak(proposals, capacity)?;
        self.append_window_reservation(proposals, capacity)
            .ok_or(StorageError::Rejected("append window ownership budget"))?;
        let window = self
            .append_window
            .as_ref()
            .map(|w| w.units[0].id.window)
            .map(Ok)
            .unwrap_or_else(|| {
                self.append_window_sequence
                    .checked_add(1)
                    .ok_or(RaftError::Exhausted)
            })?;
        let ordinal = self.append_window.as_ref().map_or(0, |w| w.units.len());
        let id = AppendUnitId {
            binding: self.binding,
            group: predecessor.group,
            window,
            ordinal,
        };
        let update = window_update(predecessor, first, proposals);
        let next = OwnedNextLog::plan_copy(&view, &summary, &update, self.limits)?;
        if !next.is_append() {
            return Err(RaftError::WrongCompletion);
        }
        let retained = self
            .append_window
            .as_ref()
            .map(|w| w.retained)
            .unwrap_or(metadata_cost(limits).ok_or(RaftError::Exhausted)?)
            .checked_add(unit_cost(proposals).ok_or(RaftError::Exhausted)?)
            .ok_or(RaftError::Exhausted)?;
        let (fresh, retained) = self.fresh_window(
            limits,
            predecessor,
            summary,
            retained,
            input_cost(proposals, capacity).ok_or(RaftError::Exhausted)?,
        )?;
        Ok(PreparedUnit {
            id,
            predecessor,
            next,
            update,
            retained,
            fresh,
        })
    }
    fn fresh_window(
        &self,
        limits: AppendWindowLimits,
        predecessor: LogMetadata,
        summary: LogSummary,
        mut retained: usize,
        input: usize,
    ) -> Result<(Option<Window>, usize), RaftError> {
        if self.append_window.is_some() {
            return Ok((None, retained));
        }
        let units = Vec::with_capacity(limits.max_units);
        let tail = Vec::with_capacity(limits.max_originals);
        let excess = units
            .capacity()
            .checked_sub(limits.max_units)
            .and_then(|n| n.checked_mul(size_of::<Unit>()))
            .and_then(|n| {
                tail.capacity()
                    .checked_sub(limits.max_originals)?
                    .checked_mul(size_of::<LogEntry>())?
                    .checked_add(n)
            })
            .ok_or(RaftError::Exhausted)?;
        let peak = retained
            .checked_add(excess)
            .and_then(|n| n.checked_add(input))
            .ok_or(RaftError::Exhausted)?;
        if peak > limits.max_retained_bytes {
            return Err(StorageError::Rejected("append window physical capacity").into());
        }
        retained = retained.checked_add(excess).ok_or(RaftError::Exhausted)?;
        let fresh = Window {
            opened: predecessor,
            accepted: predecessor,
            summary,
            tail,
            units,
            sealed: false,
            retained: metadata_cost(limits)
                .and_then(|n| n.checked_add(excess))
                .ok_or(RaftError::Exhausted)?,
        };
        Ok((Some(fresh), retained))
    }
    fn install_window_unit(&mut self, prepared: PreparedUnit) -> StagedAppend {
        let PreparedUnit {
            id,
            predecessor,
            next,
            update,
            retained,
            fresh,
        } = prepared;
        if let Some(fresh) = fresh {
            self.append_window_sequence = id.window;
            self.append_window = Some(fresh);
        }
        let w = self.append_window.as_mut().unwrap();
        w.accepted = next.metadata();
        w.summary = next.summary().expect("ordinary append plan");
        w.tail
            .extend(update.suffix.as_ref().unwrap().entries.iter().cloned());
        w.retained = retained.max(w.retained);
        w.units.push(Unit {
            id,
            predecessor,
            next,
            ticket: None,
            durable: false,
        });
        StagedAppend { id, update }
    }
    pub fn seal_append_window(&mut self) {
        if let Some(w) = &mut self.append_window {
            w.sealed = true;
        }
    }
    fn append_unit(&self, id: AppendUnitId) -> Result<&Unit, RaftError> {
        if self.fenced {
            return Err(RaftError::Fenced);
        }
        self.append_window
            .as_ref()
            .and_then(|w| w.units.get(id.ordinal))
            .filter(|unit| unit.id == id)
            .ok_or(RaftError::WrongCompletion)
    }
    pub fn validate_append_effect(
        &self,
        id: AppendUnitId,
        update: &LogUpdate,
    ) -> Result<(), RaftError> {
        let unit = self.append_unit(id)?;
        if unit.ticket.is_some() || !unit.next.matches_update(update) {
            return Err(RaftError::WrongCompletion);
        }
        Ok(())
    }
    pub fn admitted_append(
        &mut self,
        id: AppendUnitId,
        ticket: LogTicket,
    ) -> Result<(), RaftError> {
        let unit = self.append_unit(id)?;
        let next = unit.next.metadata();
        if unit.ticket.is_some()
            || ticket.binding != self.binding
            || ticket.batch <= self.last_batch
            || ticket.group != next.group
            || ticket.revision != next.revision
            || ticket.generation != next.generation
            || ticket.last_index != next.last_index
            || ticket.term != next.hard_state.term
            || self.append_window.as_ref().unwrap().units[..id.ordinal]
                .iter()
                .any(|u| u.ticket.is_none())
        {
            return Err(RaftError::WrongCompletion);
        }
        self.last_batch = ticket.batch;
        self.append_window.as_mut().unwrap().units[id.ordinal].ticket = Some(ticket);
        Ok(())
    }
    fn window_early_suffix(&self, id: AppendUnitId) -> Option<&Suffix> {
        let unit = self.append_unit(id).ok()?;
        // Preserve the existing durable committed predecessor gate. Descendants
        // do not replace the first unit's outstanding per-peer ordinary request.
        if id.ordinal != 0
            || self.durable.last_index() != self.durable.commit_index
            || self.durable.last_term() != self.durable.hard_state.term
            || !self.window_context_eligible()
        {
            return None;
        }
        let suffix = unit.next.update().suffix.as_ref()?;
        if suffix.entries.len() > self.batch_limits.max_replication_entries {
            return None;
        }
        let bytes = suffix.entries.iter().try_fold(256usize, |sum, entry| {
            sum.checked_add(37)?.checked_add(entry.payload_bytes())
        })?;
        (bytes <= self.limits.max_batch_bytes).then_some(suffix)
    }
    pub fn append_replication_reservation(&self, id: AppendUnitId) -> Option<usize> {
        let suffix = self.window_early_suffix(id)?;
        let peers = self
            .membership()
            .replicas()
            .filter(|(peer, _)| self.early_peer(*peer))
            .count();
        if peers == 0 || self.request_sequence.checked_add(peers as u64).is_none() {
            return None;
        }
        let payload = suffix.entries.iter().try_fold(0usize, |sum, entry| {
            sum.checked_add(entry.retained_payload_bytes())
        })?;
        let message = suffix
            .entries
            .len()
            .checked_mul(size_of::<LogEntry>())?
            .checked_add(payload)?
            .checked_add(size_of::<Message>())?
            .checked_add(4 * size_of::<Effect>())?
            .checked_add(size_of::<AdmittedRange>() + size_of::<EmptyAppendAlias>())?;
        peers.checked_mul(message)
    }
    /// Alternative exact Written admission with separately reserved early output.
    /// An unavailable opportunity emits nothing and retains ordinary durability.
    pub fn replicate_admitted_append(
        &mut self,
        id: AppendUnitId,
        ticket: LogTicket,
    ) -> Result<Vec<Effect>, RaftError> {
        self.admitted_append(id, ticket)?;
        if self.append_replication_reservation(id).is_none() {
            return Ok(Vec::new());
        }
        let peers: Vec<_> = self
            .membership()
            .replicas()
            .map(|(peer, _)| peer)
            .filter(|peer| self.early_peer(*peer))
            .collect();
        let mut effects = Vec::with_capacity(peers.len());
        for peer in peers {
            let suffix = self
                .window_early_suffix(id)
                .ok_or(RaftError::WrongCompletion)?;
            let start = suffix.from;
            let entries = suffix.entries.clone();
            let end = entries.last().ok_or(RaftError::WrongCompletion)?.index;
            let context = self.context()?;
            if let Some(original) = self.requests.get(&peer).copied() {
                self.empty_append_aliases.insert(
                    peer,
                    EmptyAppendAlias {
                        original,
                        successor: context,
                    },
                );
            }
            self.requests.insert(
                peer,
                Replication {
                    context,
                    configuration: self.membership().id(),
                    start,
                    end,
                    snapshot: None,
                    commit_at_start: self.durable.commit_index,
                },
            );
            self.admitted_ranges
                .insert(peer, AdmittedRange { ticket, context });
            effects.push(Effect::Send(self.message(
                peer,
                context,
                Rpc::Append {
                    previous_index: start - 1,
                    previous_term: self.durable.last_term(),
                    entries,
                    leader_commit: self.durable.commit_index,
                },
            )));
        }
        Ok(effects)
    }
    pub fn complete_append_unit(
        &mut self,
        id: AppendUnitId,
        completion: &DurableLog,
    ) -> Result<AppendCompletion, RaftError> {
        let unit = self.append_unit(id)?;
        if unit.durable
            || unit
                .ticket
                .is_none_or(|ticket| !completion.tickets.contains(&ticket))
        {
            return Err(RaftError::WrongCompletion);
        }
        let w = self.append_window.as_mut().unwrap();
        w.sealed = true;
        w.units[id.ordinal].durable = true;
        if w.units.iter().any(|u| !u.durable) {
            return Ok(AppendCompletion::Waiting);
        }
        match self.finish_append_window() {
            Ok(result) => Ok(result),
            Err(error) => {
                self.storage_failed();
                Err(error)
            }
        }
    }
    fn finish_append_window(&mut self) -> Result<AppendCompletion, RaftError> {
        let w = self
            .append_window
            .as_ref()
            .ok_or(RaftError::WrongCompletion)?;
        let mut predecessor = LogMetadata::from_log(&self.durable);
        if predecessor != w.opened || !w.sealed {
            return Err(RaftError::WrongCompletion);
        }
        for unit in &w.units {
            if !unit.durable || unit.predecessor != predecessor {
                return Err(RaftError::WrongCompletion);
            }
            predecessor = unit.next.metadata();
        }
        let ids = w.units.iter().map(|u| u.id).collect();
        self.durable
            .entries
            .try_reserve_exact(w.tail.len())
            .map_err(|_| StorageError::Rejected("append completion allocation"))?;
        let window = self.append_window.take().unwrap();
        for unit in window.units {
            self.log_summary = unit.next.install(&mut self.durable)?;
        }
        self.progress.insert(self.node, self.durable.last_index());
        let mut effects = self.broadcast_append_progress()?;
        effects.extend(self.maybe_commit()?);
        Ok(AppendCompletion::Retired {
            units: ids,
            effects,
        })
    }
}
struct PreparedUnit {
    id: AppendUnitId,
    predecessor: LogMetadata,
    next: OwnedNextLog,
    update: LogUpdate,
    retained: usize,
    fresh: Option<Window>,
}

#[cfg(test)]
mod tests;
