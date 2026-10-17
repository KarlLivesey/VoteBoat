// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Bounded original commands sharing one ordered persistence transition.
use super::*;
use std::mem::size_of;

/// Default original-command count for one direct Core batch.
/// Startup BatchLimits can select a larger bounded envelope.
pub const MAX_PROPOSAL_BATCH_ENTRIES: usize = 64;

/// One original command. Sharing admission does not combine operation identities
/// or turn application results into a cross-command transaction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Proposal {
    pub operation: OperationId,
    pub bytes: Vec<u8>,
}

impl Raft {
    pub(crate) fn proposal_limits(&self) -> LogLimits {
        self.limits
    }

    /// Admit original commands in order under one exact persistence ticket.
    /// The embedding host must perform application admission and reserve input,
    /// effects and individual receipts, as for `Event::Propose`.
    /// This new API uses `LogLimits::max_batch_bytes` as an in-memory staging
    /// ceiling: the proposal Vec header, its full allocated element capacity,
    /// every payload capacity and the staged LogEntry element count are charged.
    /// It is not an encoded frame estimate: selected storage/wire providers
    /// validate their exact encoding independently. Legacy single proposals
    /// retain their existing validation and do not acquire this new ceiling.
    /// For effects, call `effect_reservation` with the complete owned proposal
    /// input size (Vec header + element capacity + payload capacities), retain
    /// that reservation through dependent completions, and separately reserve
    /// individual application receipts. A proposal position is not a receipt.
    /// This direct Core API does not add batching to Node or ClientRouter.
    /// Invalid shape/budgets reject the entire batch before pending log state.
    /// Success returns a persistence request, not commitment or application.
    pub fn propose_batch(&mut self, proposals: Vec<Proposal>) -> Result<Vec<Effect>, RaftError> {
        self.begin_step()?;
        self.check_proposal_role()?;
        let first = self.validate_proposal_batch(&proposals, proposals.capacity())?;
        self.validate_conversion_peak(&proposals, proposals.capacity())?;
        let entries: Vec<LogEntry> = proposals
            .into_iter()
            .enumerate()
            .map(|(offset, proposal)| LogEntry {
                index: first + offset as u64,
                term: self.durable.hard_state.term,
                payload: EntryPayload::Command {
                    operation: proposal.operation,
                    bytes: proposal.bytes.into(),
                },
            })
            .collect();
        // An exact-length source collects tight, so owned planning never has
        // to reallocate the suffix to normalize its retained capacity.
        debug_assert_eq!(entries.capacity(), entries.len());
        let effects = self.persist_proposal_entries(first, entries)?;
        self.finish_step(effects)
    }

    pub(super) fn begin_step(&mut self) -> Result<(), RaftError> {
        self.batch_limits_locked = true;
        if self.fenced {
            return Err(RaftError::Fenced);
        }
        if self.has_pending_dependency() {
            return Err(RaftError::Busy);
        }
        self.refresh_transfer();
        Ok(())
    }

    pub(super) fn finish_step(
        &mut self,
        mut effects: Vec<Effect>,
    ) -> Result<Vec<Effect>, RaftError> {
        effects.extend(self.drive_transfer()?);
        Ok(effects)
    }

    pub(super) fn propose(
        &mut self,
        operation: OperationId,
        bytes: Vec<u8>,
    ) -> Result<Vec<Effect>, RaftError> {
        self.check_proposal_role()?;
        if bytes.len() > self.limits.max_command_bytes {
            return Err(StorageError::Rejected("command byte budget").into());
        }
        let index = self
            .durable
            .last_index()
            .checked_add(1)
            .ok_or(RaftError::Exhausted)?;
        let entry = LogEntry {
            index,
            term: self.durable.hard_state.term,
            payload: EntryPayload::Command {
                operation,
                bytes: bytes.into(),
            },
        };
        self.persist_proposal_entries(index, vec![entry])
    }

    fn validate_proposal_batch(
        &self,
        proposals: &[Proposal],
        capacity: usize,
    ) -> Result<u64, RaftError> {
        let count = proposals.len();
        if count == 0 || count > self.batch_limits.max_proposal_entries {
            return Err(StorageError::Rejected("proposal count budget").into());
        }
        if capacity > self.batch_limits.max_proposal_entries {
            return Err(StorageError::Rejected("proposal allocation count budget").into());
        }
        if self
            .durable
            .entries
            .len()
            .checked_add(count)
            .is_none_or(|n| n > self.limits.max_entries_per_group)
        {
            return Err(StorageError::Rejected("proposal retained entry budget").into());
        }
        let first = self
            .durable
            .last_index()
            .checked_add(1)
            .ok_or(RaftError::Exhausted)?;
        first
            .checked_add(count as u64 - 1)
            .ok_or(RaftError::Exhausted)?;
        let mut owned = capacity
            .checked_mul(size_of::<Proposal>())
            .and_then(|n| n.checked_add(size_of::<Vec<Proposal>>()))
            .and_then(|n| {
                count
                    .checked_mul(size_of::<LogEntry>())
                    .and_then(|entries| n.checked_add(entries))
            })
            .ok_or(RaftError::Exhausted)?;
        for proposal in proposals {
            if proposal.bytes.len() > self.limits.max_command_bytes {
                return Err(StorageError::Rejected("command byte budget").into());
            }
            owned = owned
                .checked_add(proposal.bytes.capacity())
                .ok_or(RaftError::Exhausted)?;
            if owned > self.limits.max_batch_bytes {
                return Err(StorageError::Rejected("proposal batch owned staging budget").into());
            }
        }
        Ok(first)
    }
    fn validate_conversion_peak(
        &self,
        proposals: &[Proposal],
        capacity: usize,
    ) -> Result<(), RaftError> {
        let staged = capacity
            .checked_mul(size_of::<Proposal>())
            .and_then(|n| n.checked_add(size_of::<Vec<Proposal>>()))
            .and_then(|n| {
                proposals
                    .len()
                    .checked_mul(size_of::<LogEntry>())
                    .and_then(|entries| n.checked_add(entries))
            })
            .ok_or(RaftError::Exhausted)?;
        let peak = proposals.iter().try_fold(staged, |sum, proposal| {
            sum.checked_add(proposal.bytes.capacity())
                .and_then(|n| {
                    CommandBytes::retained_bytes_for_len(proposal.bytes.len())
                        .and_then(|shared| n.checked_add(shared))
                })
                .ok_or(RaftError::Exhausted)
        })?;
        if peak > self.limits.max_batch_bytes {
            return Err(StorageError::Rejected("proposal shared conversion peak budget").into());
        }
        Ok(())
    }
    fn persist_proposal_entries(
        &mut self,
        first: u64,
        entries: Vec<LogEntry>,
    ) -> Result<Vec<Effect>, RaftError> {
        self.persist(
            self.durable.hard_state,
            self.durable.commit_index,
            Some(Suffix {
                from: first,
                entries,
            }),
            After::LeaderAppend,
            None,
        )
    }
    fn check_proposal_role(&self) -> Result<(), RaftError> {
        if self.role != Role::Leader || !self.local_voter() {
            return Err(RaftError::NotLeader);
        }
        if self.transfer.is_some() {
            return Err(RaftError::Busy);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
