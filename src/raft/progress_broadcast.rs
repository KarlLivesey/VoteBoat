// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Progress sends avoid replaying ordinary requests; heartbeats still retry.
use super::*;

impl Raft {
    /// Reconstruct the original in-flight scope; a retry's newer commit field
    /// does not upgrade the original request's notification obligation.
    pub(super) fn retry_replication(
        &self,
        peer: NodeId,
        sent: Replication,
    ) -> Result<Effect, RaftError> {
        if let Some(reference) = sent.snapshot {
            return Ok(Effect::SnapshotRequired {
                to: peer,
                context: sent.context,
                reference,
            });
        }
        let entries = if sent.start > sent.end {
            Vec::new()
        } else {
            let start = (sent.start - self.durable.base_index() - 1) as usize;
            let end = (sent.end - self.durable.base_index()) as usize;
            self.durable
                .entries
                .get(start..end)
                .ok_or(RaftError::WrongCompletion)?
                .to_vec()
        };
        Ok(Effect::Send(
            self.scoped_message(
                sent.configuration,
                peer,
                sent.context,
                Rpc::Append {
                    previous_index: sent.start - 1,
                    previous_term: self
                        .durable
                        .term_at(sent.start - 1)
                        .ok_or(RaftError::WrongCompletion)?,
                    entries,
                    leader_commit: self.durable.commit_index,
                },
            ),
        ))
    }

    pub(super) fn progress_sends_enabled(&self) -> bool {
        self.transfer.is_none()
            && self.membership().joint().is_none()
            && self.membership().last_configuration_index() <= self.durable.commit_index
    }

    pub(super) fn broadcast_append_progress(&mut self) -> Result<Vec<Effect>, RaftError> {
        if !self.progress_sends_enabled() {
            return self.broadcast_after_admission();
        }
        self.broadcast_ready_peers()
    }

    pub(super) fn broadcast_commit_progress(&mut self) -> Result<Vec<Effect>, RaftError> {
        if !self.progress_sends_enabled() {
            return self.broadcast();
        }
        self.broadcast_ready_peers()
    }

    fn broadcast_ready_peers(&mut self) -> Result<Vec<Effect>, RaftError> {
        let mut effects = Vec::new();
        for peer in self.peers() {
            // Snapshot requests retain their existing fallback. An ordinary
            // request owns its exact range until ACK or explicit invalidation.
            if self
                .requests
                .get(&peer)
                .is_some_and(|sent| sent.snapshot.is_none())
            {
                continue;
            }
            effects.push(self.append_for(peer)?);
        }
        Ok(effects)
    }
}

#[cfg(test)]
mod tests;
