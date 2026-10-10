// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use crate::drain::{DrainJournal, DrainJournalError, DrainPhase};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DrainRestoreError {
    Closed,
    WrongOwner,
    Conflict,
    Journal(DrainJournalError),
}
impl<
        S: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
        A: ProposalAdmission + BoundedReadableStateMachine + CheckpointStateMachine,
        W: PersistenceWorker,
        O: OutboundQueue,
        H: SnapshotWorker,
        C: PeerConnector,
        F: PeerTransportFactory<C::Session>,
    > Node<S, T, E, A, W, O, H, C, F>
where
    A::Receipt: ApplicationReceipt,
    C::Endpoint: Clone,
{
    /// Apply a confirmed in-memory journal view before exposing a recovered
    /// Node. Publish outside owner polling first. Active recovery gates every
    /// current assignment even if the recorded manifest has become stale.
    /// Cancellation opens admission only after tracked enables execute.
    /// On a journal/recovery error the host must not expose the recovered Node.
    pub fn restore_drain(&mut self, journal: &impl DrainJournal) -> Result<(), DrainRestoreError> {
        if self.state != NodeState::Running {
            return Err(DrainRestoreError::Closed);
        }
        let owner = journal.owner();
        if owner.store != self.local.owner.identity().store.identity
            || owner.node != self.local.outbound.binding().node
        {
            return Err(DrainRestoreError::WrongOwner);
        }
        let Some(record) = journal.latest().map_err(DrainRestoreError::Journal)? else {
            return if self.drain_history.is_some() {
                Err(DrainRestoreError::Conflict)
            } else {
                Ok(())
            };
        };
        record.validate().map_err(DrainRestoreError::Journal)?;
        if record.owner != owner {
            return Err(DrainRestoreError::WrongOwner);
        }
        if self
            .drain
            .as_ref()
            .is_some_and(|d| d.request != record.request)
            && record.phase == DrainPhase::Cancelled
        {
            return Err(DrainRestoreError::Conflict);
        }
        if let Some(previous) = &self.drain_history {
            if previous == &record {
                return Ok(());
            }
            if record.sequence < previous.sequence
                || (record.sequence == previous.sequence && !record.follows(previous))
            {
                return Err(DrainRestoreError::Conflict);
            }
        }
        self.install_drain_gate(
            record.request.clone(),
            record.phase == DrainPhase::Cancelled,
        );
        self.drain_history = Some(record);
        Ok(())
    }
}
