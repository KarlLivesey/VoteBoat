// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Exact ordinary and historical per-recipient replication boundaries.
use super::*;

impl Raft {
    pub(super) fn replication_window(&self, m: &Message) -> Option<Boundary> {
        let sent = self.requests.get(&m.to)?;
        if !same_request(sent, m) {
            return None;
        }
        match &m.rpc {
            Rpc::Append {
                previous_index,
                previous_term,
                entries,
                leader_commit,
            } if sent.snapshot.is_none()
                && *leader_commit <= self.durable.commit_index
                && self.range_matches(sent, *previous_index, *previous_term, entries) =>
            {
                Some(Boundary::Append {
                    start: sent.start,
                    end: sent.end,
                })
            }
            Rpc::Snapshot { snapshot } if self.snapshot_matches(sent, snapshot) => {
                sent.snapshot.map(Boundary::Snapshot)
            }
            _ => None,
        }
    }
    pub(super) fn repair_window(&self, m: &Message) -> Option<(PeerIdentity, Boundary)> {
        let sent = self.repair_requests.get(&m.to)?;
        if !same_request(sent, m) {
            return None;
        }
        match &m.rpc {
            Rpc::LearnerRepair {
                joint,
                previous_index,
                previous_term,
                entries,
            } => {
                let (expected, _, store) =
                    self.repair_joint(m.to, Some(m.configuration)).ok()??;
                if sent.snapshot.is_some()
                    || **joint != expected
                    || !self.range_matches(sent, *previous_index, *previous_term, entries)
                    || sent.end > joint.index
                {
                    return None;
                }
                Some((
                    PeerIdentity { node: m.to, store },
                    Boundary::Repair {
                        start: sent.start,
                        end: sent.end,
                    },
                ))
            }
            Rpc::LearnerRepairSnapshot { snapshot }
            | Rpc::CommittedLearnerRepairSnapshot { snapshot } => {
                let scope = self.snapshot_repair_scope(m.to)?;
                if scope.configuration != m.configuration
                    || scope.final_checkpoint
                        != matches!(m.rpc, Rpc::CommittedLearnerRepairSnapshot { .. })
                    || !self.snapshot_matches(sent, snapshot)
                {
                    return None;
                }
                Some((
                    PeerIdentity {
                        node: m.to,
                        store: scope.peer_store,
                    },
                    Boundary::RepairSnapshot(sent.snapshot?),
                ))
            }
            _ => None,
        }
    }
    fn range_matches(
        &self,
        sent: &Replication,
        previous: u64,
        term: u64,
        entries: &[LogEntry],
    ) -> bool {
        previous.checked_add(1) == Some(sent.start)
            && self.durable.term_at(previous) == Some(term)
            && entries.last().map_or(previous, |e| e.index) == sent.end
            && entries.len() <= 64
            && entries.iter().enumerate().all(|(offset, e)| {
                previous.checked_add(offset as u64 + 1) == Some(e.index)
                    && self.durable.entry_at(e.index) == Some(e)
            })
    }
    fn snapshot_matches(&self, sent: &Replication, snapshot: &Snapshot) -> bool {
        sent.snapshot.is_some_and(|reference| {
            self.durable.snapshot == Some(reference)
                && reference.index <= self.durable.commit_index
                && reference.matches(snapshot)
                && snapshot.metadata.bootstrap == self.durable.bootstrap
                && snapshot.metadata.membership == self.durable.snapshot_membership
                && !snapshot.application.is_empty()
                && snapshot.application.len() <= self.limits.max_snapshot_bytes
        })
    }
}
fn same_request(sent: &Replication, m: &Message) -> bool {
    sent.context == m.context && sent.configuration == m.configuration
}
impl RequestWindow {
    pub(super) fn current_replication(&self, core: &Raft) -> bool {
        let (map, historical) = match self.boundary {
            Boundary::Append { .. } | Boundary::Snapshot(_) => (&core.requests, false),
            Boundary::Repair { .. } | Boundary::RepairSnapshot(_) => (&core.repair_requests, true),
            _ => return false,
        };
        let Some(sent) = map.get(&self.recipient.node) else {
            return false;
        };
        if sent.context != self.context || sent.configuration != self.configuration {
            return false;
        }
        let shape = match self.boundary {
            Boundary::Append { start, end } | Boundary::Repair { start, end } => {
                sent.snapshot.is_none() && sent.start == start && sent.end == end
            }
            Boundary::Snapshot(reference) | Boundary::RepairSnapshot(reference) => {
                sent.snapshot == Some(reference) && core.durable.snapshot == Some(reference)
            }
            _ => false,
        };
        shape
            && if historical {
                match self.boundary {
                    Boundary::Repair { .. } => core
                        .repair_joint(self.recipient.node, Some(self.configuration))
                        .ok()
                        .flatten()
                        .is_some_and(|(_, _, store)| store == self.recipient.store),
                    Boundary::RepairSnapshot(_) => core
                        .snapshot_repair_scope(self.recipient.node)
                        .is_some_and(|s| {
                            s.configuration == self.configuration
                                && s.peer_store == self.recipient.store
                        }),
                    _ => false,
                }
            } else {
                core.role == Role::Leader
                    && core.local_voter()
                    && core.membership().id() == self.configuration
                    && core.membership().replica_store(self.recipient.node)
                        == Some(self.recipient.store)
            }
    }
}
