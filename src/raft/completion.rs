// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Synchronous completion can borrow its canonical committed interval.
use super::*;
use std::ops::Range;

pub(super) struct PreparedCompletion {
    pub committed: Option<Range<usize>>,
    pub following: Vec<Effect>,
}

/// Successful completion effects for an immediate, synchronous host.
///
/// The optional committed interval precedes every following effect. It is not
/// an application result: apply it in order before acknowledging originals.
/// All completion checks and transitions have succeeded before this value is
/// returned. Its borrow prevents another mutable core transition until the
/// interval is released; dropping it does not cancel accepted storage work.
/// Queued or asynchronous hosts can retain the existing owned API or use
/// [`Self::into_owned`] without changing effect order.
#[derive(Debug)]
pub struct CompletionEffects<'a> {
    committed: Option<&'a [LogEntry]>,
    following: Vec<Effect>,
}

impl<'a> CompletionEffects<'a> {
    /// Newly committed contiguous entries, before all following effects.
    pub fn committed(&self) -> Option<&[LogEntry]> {
        self.committed
    }

    /// Owned effects that follow application of the committed interval.
    pub fn following(&self) -> &[Effect] {
        &self.following
    }

    /// Consume the result for immediate application and later effect handling.
    /// The interval continues to borrow the core until its last use.
    pub fn into_parts(self) -> (Option<&'a [LogEntry]>, Vec<Effect>) {
        (self.committed, self.following)
    }

    /// Materialize the legacy owned sequence for queues or asynchronous work.
    pub fn into_owned(self) -> Vec<Effect> {
        let mut effects = self.following;
        if let Some(entries) = self.committed {
            effects.insert(0, Effect::Committed(entries.to_vec()));
        }
        effects
    }
}

impl Raft {
    /// Complete exact admitted durability dependencies without copying committed
    /// entries. All fallible transitions finish before the result borrows the
    /// canonical log. Snapshot installation exposes its installation effect
    /// instead of a committed interval, as in [`Self::complete`].
    pub fn complete_borrowed(
        &mut self,
        completion: &DurableLog,
    ) -> Result<CompletionEffects<'_>, RaftError> {
        let prepared = self.finish_completion(completion)?;
        Ok(self.borrow_completion(prepared))
    }

    fn borrow_completion(&self, prepared: PreparedCompletion) -> CompletionEffects<'_> {
        CompletionEffects {
            committed: prepared.committed.map(|range| &self.durable.entries[range]),
            following: prepared.following,
        }
    }
}

/// Run the same authoritative storage pipeline as [`persist_effect`], returning
/// a committed interval borrowed only after all completion checks succeed.
/// Exact update validation precedes submission; any pipeline error fences the
/// core. Networking and application remain the host's responsibility.
pub fn persist_effect_borrowed<'a, S: LogStore>(
    raft: &'a mut Raft,
    store: &mut S,
    update: LogUpdate,
) -> Result<CompletionEffects<'a>, RaftError> {
    let result = (|| {
        if store.binding() != raft.binding {
            return Err(RaftError::WrongIdentity);
        }
        // A volatile core's log is not a store's to submit.
        if raft.volatile
            || raft
                .pending
                .as_ref()
                .is_none_or(|p| !p.next.matches_update(&update))
        {
            return Err(RaftError::WrongCompletion);
        }
        let tickets = store.append_batch(vec![LogMutation::Update(update)])?;
        if tickets.len() != 1 {
            return Err(RaftError::WrongCompletion);
        }
        // The exact update was checked before submission above.
        raft.admitted(tickets[0])?;
        let completion = store.barrier(&tickets)?;
        raft.finish_completion(&completion)
    })();
    match result {
        Ok(prepared) => Ok(raft.borrow_completion(prepared)),
        Err(error) => {
            raft.storage_failed();
            Err(error)
        }
    }
}

/// Complete one [`Effect::PersistVolatile`] token of a volatile core, returning
/// a committed interval borrowed only after every completion check succeeds.
/// No storage work happens: the core already owns the planned transition and
/// installs it into its own log, the only copy. The binding, the core's
/// incarnation and the exact pending revision and generation are checked
/// first; any error fences the core, whose last completed state stays
/// exportable through `Raft::state`.
pub fn persist_volatile<'a>(
    raft: &'a mut Raft,
    log: &VolatileLog,
    persist: VolatilePersist,
) -> Result<CompletionEffects<'a>, RaftError> {
    let result = if log.binding() != raft.binding {
        Err(RaftError::WrongIdentity)
    } else {
        raft.finish_volatile(persist)
    };
    match result {
        Ok(prepared) => Ok(raft.borrow_completion(prepared)),
        Err(error) => {
            raft.storage_failed();
            Err(error)
        }
    }
}
