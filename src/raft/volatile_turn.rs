// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Checked owner-local completion, after the original event and finish hook.
use super::*;

#[cfg(test)]
mod tests;

/// Synchronous volatile work completed by the core, not an application result.
///
/// Apply `committed()` at `commit_position()` in the returned output queue,
/// before executing later effects or acknowledging originals. A cold preceding
/// effect may need to mutate the core: materialise the interval at that exact
/// position and release this borrow before handling such effects. Dropping the
/// result does not roll back installed state; application replay is the host's
/// responsibility, as with [`CompletionEffects`]. No crash durability is claimed.
#[derive(Debug)]
pub struct VolatileTurn<'a> {
    committed: Option<EntryRange<'a>>,
    commit_position: usize,
    completed_local_units: usize,
}
impl<'a> VolatileTurn<'a> {
    pub fn committed(&self) -> Option<EntryRange<'a>> {
        self.committed
    }
    /// Number of ordinary queued effects that precede application, not an index.
    pub fn commit_position(&self) -> usize {
        self.commit_position
    }
    /// Real checked installations, including both singleton append and commit.
    pub fn completed_local_units(&self) -> usize {
        self.completed_local_units
    }
}

pub(super) enum Delivery {
    Host,
    Local {
        due: Option<VolatilePersist>,
        remaining: u8,
    },
}
impl Delivery {
    pub(super) fn schedule(&mut self, key: VolatilePersist) -> bool {
        match self {
            Self::Local { due, remaining } if *remaining > 0 => {
                assert!(
                    due.is_none(),
                    "one pending completion per turn continuation"
                );
                *due = Some(key);
                *remaining -= 1;
                true
            }
            // A multi-voter LeaderAppend's optional scalar commit is retained
            // as an explicit Host dependency even if all peers are in flight.
            Self::Host | Self::Local { .. } => false,
        }
    }
    fn take_due(&mut self) -> Option<VolatilePersist> {
        match self {
            Self::Host => None,
            Self::Local { due, .. } => due.take(),
        }
    }
}
struct TurnScope<'a>(&'a mut Raft);
impl Drop for TurnScope<'_> {
    fn drop(&mut self) {
        self.0.completion_delivery = Delivery::Host;
    }
}
#[derive(Default)]
struct PreparedTurn {
    committed: Option<Range<usize>>,
    position: usize,
    units: usize,
}

impl Raft {
    /// Execute an event with checked synchronous local volatile completion.
    ///
    /// Nonempty queues and nonvolatile mode are typed pre-admission refusals
    /// leaving state and queue unchanged. Inputs are consumed even on refusal,
    /// as with `step_into`; hosts requiring retry must retain them themselves.
    /// Wrong log binding fences as completion-domain misuse. Ineligible events
    /// execute the existing path exactly once, with ordinary Host effects.
    pub fn step_volatile_into<'a>(
        &'a mut self,
        log: &VolatileLog,
        event: Event,
        out: &mut VecDeque<Effect>,
    ) -> Result<VolatileTurn<'a>, RaftError> {
        let ordinary = match &event {
            Event::Propose { .. } => true,
            Event::Receive(message) => {
                message.term == self.durable.hard_state.term
                    && matches!(message.rpc, Rpc::Append { .. } | Rpc::Appended { .. })
            }
            _ => false,
        };
        self.volatile_turn(log, ordinary, out, |core, out| core.step_into(event, out))
    }

    /// [`Self::propose_entries_into`] with the same selected volatile-turn
    /// contract and consuming refusal policy as [`Self::step_volatile_into`].
    pub fn propose_entries_volatile_into<'a>(
        &'a mut self,
        log: &VolatileLog,
        batch: ProposalBatch,
        out: &mut VecDeque<Effect>,
    ) -> Result<VolatileTurn<'a>, RaftError> {
        self.volatile_turn(log, true, out, |core, out| {
            core.propose_entries_into(batch, out)
        })
    }

    fn local_turn_eligible(&self) -> bool {
        // Only recover_volatile establishes this domain. Its validated static
        // bootstrap forbids configuration ingress (including snapshot members).
        // No policy/peer walk or entry rescan is needed to trust that invariant.
        !self.fenced
            && self.incarnation != 0
            && !self.has_pending_dependency()
            && self.membership.joint().is_none()
            && self.membership.stable().learners().is_empty()
            && self.transfer.is_none()
            && self.learner_readiness.is_none()
            && self.ready_learner.is_none()
            && self.read.is_none()
            && self.ready_read.is_none()
            && self.authority_request.is_none()
            && self.replication_permit.is_none()
    }

    // Only the two public methods above supply these internal continuations;
    // this is not a callback into the host while delivery is active.
    fn volatile_turn<'a>(
        &'a mut self,
        log: &VolatileLog,
        ordinary: bool,
        out: &mut VecDeque<Effect>,
        event: impl FnOnce(&mut Self, &mut VecDeque<Effect>) -> Result<(), RaftError>,
    ) -> Result<VolatileTurn<'a>, RaftError> {
        if !out.is_empty() {
            return Err(RaftError::NonemptyEffects);
        }
        if !self.volatile {
            return Err(RaftError::NotVolatile);
        }
        if log.binding() != self.binding {
            self.storage_failed();
            return Err(RaftError::WrongIdentity);
        }
        if ordinary && self.local_turn_eligible() {
            self.completion_delivery = Delivery::Local {
                due: None,
                remaining: if self.membership.stable().voter_stores().len() == 1 {
                    2
                } else {
                    1
                },
            };
        }
        let prepared = {
            let scope = TurnScope(self);
            let core = &mut *scope.0;
            if let Err(error) = event(core, out) {
                out.clear();
                if core.completion_delivery.take_due().is_some() {
                    core.storage_failed();
                }
                return Err(error);
            }
            let mut prepared = PreparedTurn::default();
            while let Some(key) = core.completion_delivery.take_due() {
                let position = out.len();
                let completed = core.finish_volatile_into(key, out);
                match completed {
                    Ok(committed) => {
                        prepared.units += 1;
                        if let Some(range) = committed {
                            // The selected graph has only Reply/Commit leaves
                            // and LeaderAppend -> Commit, never two intervals.
                            assert!(prepared.committed.is_none());
                            prepared.committed = Some(range);
                            prepared.position = position;
                        }
                    }
                    Err(error) => {
                        out.clear();
                        core.storage_failed();
                        return Err(error);
                    }
                }
            }
            prepared
        }; // Restore Host before the canonical interval can escape borrowed.
        Ok(VolatileTurn {
            committed: prepared.committed.map(|range| {
                self.durable
                    .entries
                    .borrowed(range)
                    .expect("retained commit")
            }),
            commit_position: prepared.position,
            completed_local_units: prepared.units,
        })
    }
}
