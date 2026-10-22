// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
// Unless explicitly acquired and licensed from Licensor under another license,
// the contents of this file are subject to the Reciprocal Public License
// ("RPL") Version 1.5, or subsequent versions as allowed by the RPL, and You may
// not copy or use this file in either source code or executable form, except
// in compliance with the terms and conditions of the RPL.
//
// All software distributed under the RPL is provided strictly on an "AS IS"
// basis, WITHOUT WARRANTY OF ANY KIND, EITHER EXPRESS OR IMPLIED, AND LICENSOR
// HEREBY DISCLAIMS ALL SUCH WARRANTIES, INCLUDING WITHOUT LIMITATION, ANY
// WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, QUIET
// ENJOYMENT, OR NON-INFRINGEMENT. See the RPL for specific language governing
// rights and limitations under the RPL.
//! Exact captured original response against the authoritative parent image.
use super::*;
use crate::raft::{Message, Rpc};
mod authority;
impl<C: LogCodec + Clone, S: SnapshotCodec + Clone, W: WireCodec + Clone>
    LogicalApplication<C, S, W>
{
    pub(super) fn authorize(
        &self,
        c: &LogicalCapsule,
        r: RepresentativeBinding,
    ) -> Result<LogicalOutcome, ApplicationError> {
        let p = self
            .state
            .parent
            .as_ref()
            .ok_or(ApplicationError::InvalidCommand)?;
        if c.expected_revision != p.revision
            || c.expected_generation != p.generation
            || c.local_configuration
                != p.membership()
                    .map_err(|_| ApplicationError::InvalidCommand)?
                    .id()
        {
            return Ok(LogicalOutcome::Refused(LogicalRefusal::StaleImage));
        }
        let valid = match (&c.origin, &c.body) {
            (LogicalCapsuleOrigin::Protocol(e), LogicalCapsuleBody::Protocol(m)) => {
                self.visit(e.visit, r) && e.sequence > 0 && self.protocol(m, p, r)
            }
            (
                LogicalCapsuleOrigin::Client {
                    effect,
                    ticket,
                    position,
                },
                LogicalCapsuleBody::ClientApplied {
                    command,
                    schema,
                    receipt,
                },
            ) => {
                self.visit(effect.visit, r)
                    && effect.sequence > 0
                    && ticket.owner == effect.visit.owner
                    && ticket.group == self.mapping.parent
                    && *schema > 0
                    && command
                        .len()
                        .checked_add(receipt.len())
                        .is_some_and(|n| n <= self.limits.max_capsule_bytes)
                    && position.index <= p.commit_index
                    && position.index > 0
                    && position.term > 0
                    && (p.entry_at(position.index).is_some_and(|e| {
                        e.term == position.term
                            && matches!(&e.payload,
       EntryPayload::Command{operation,bytes} if *operation==ticket.operation&&bytes.as_slice()==command.as_slice())
                    }) || position.index <= p.base_index())
            }
            (
                LogicalCapsuleOrigin::Read {
                    effect,
                    ticket,
                    barrier,
                },
                LogicalCapsuleBody::ReadResult {
                    schema,
                    query,
                    result,
                },
            ) => {
                self.visit(effect.visit, r)
                    && effect.sequence > 0
                    && ticket.owner == effect.visit.owner
                    && ticket.group == self.mapping.parent
                    && barrier.request == ticket.request
                    && barrier.group == self.mapping.parent
                    && barrier.configuration == c.local_configuration
                    && barrier.term == p.hard_state.term
                    && barrier.index <= p.commit_index
                    && *schema > 0
                    && query
                        .len()
                        .checked_add(result.len())
                        .is_some_and(|n| n <= self.limits.max_capsule_bytes)
            }
            _ => false,
        };
        Ok(if valid {
            LogicalOutcome::CapsuleAuthorized
        } else {
            LogicalOutcome::Refused(LogicalRefusal::InvalidResponse)
        })
    }
    fn protocol(&self, m: &Message, p: &GroupLog, r: RepresentativeBinding) -> bool {
        if m.group != self.mapping.parent
            || m.from != self.mapping.voter
            || m.sender != self.binding(r)
            || m.term
                != match m.rpc {
                    Rpc::AuthorityRequest { .. } | Rpc::AuthorityReply { .. } => {
                        p.hard_state.term.max(1)
                    }
                    _ => p.hard_state.term,
                }
            || m.context.sequence == 0
        {
            return false;
        }
        match &m.rpc {
            // Requires child-committed captured emission, unsupported by CPU1.
            Rpc::CommitNotice { .. } => false,
            Rpc::Vote {
                last_index,
                last_term,
            } => *last_index == p.last_index() && *last_term == p.last_term(),
            Rpc::Voted { granted } => {
                !*granted
                    || (p.hard_state.voted_for == Some(m.to)
                        && p.ballot_origin
                            .is_some_and(|o| o.candidate_store == m.context.origin.identity))
            }
            Rpc::Append {
                previous_index,
                previous_term,
                entries,
                leader_commit,
            } => {
                p.term_at(*previous_index) == Some(*previous_term)
                    && *leader_commit <= p.commit_index
                    && entries.iter().enumerate().all(|(n, e)| {
                        previous_index.checked_add(n as u64 + 1) == Some(e.index)
                            && p.entry_at(e.index) == Some(e)
                    })
            }
            Rpc::Appended {
                success,
                matching_index,
            } => {
                !*success
                    || (*matching_index <= p.last_index() && p.term_at(*matching_index).is_some())
            }
            Rpc::ReadProbe | Rpc::ReadAck => true,
            Rpc::Snapshot { snapshot }
            | Rpc::LearnerRepairSnapshot { snapshot }
            | Rpc::CommittedLearnerRepairSnapshot { snapshot } => self
                .state
                .snapshots
                .iter()
                .any(|(_, s)| s == snapshot.as_ref()),
            Rpc::SnapshotAck { index } => snapshot_ack(p, *index),
            Rpc::Compacted { index, term } => *index == p.base_index() && *term == p.base_term(),
            Rpc::TimeoutNow {
                index, log_term, ..
            } => *index == p.last_index() && *log_term == p.last_term(),
            Rpc::LearnerRepaired {
                success,
                matching_index,
                matching_term,
            } => !*success || p.term_at(*matching_index) == Some(*matching_term),
            Rpc::LearnerRepair {
                previous_index,
                previous_term,
                entries,
                joint,
            } => {
                p.term_at(*previous_index) == Some(*previous_term)
                    && p.entry_at(joint.index) == Some(joint.as_ref())
                    && entries.iter().all(|e| p.entry_at(e.index) == Some(e))
            }
            Rpc::AuthorityRequest {
                candidate,
                configuration,
            } => self.authority_request(m, p, *candidate, *configuration),
            Rpc::AuthorityReply {
                candidate,
                configuration,
                committed_index,
                committed_term,
                granted,
            } => self.authority_reply(
                m,
                p,
                (
                    *candidate,
                    *configuration,
                    *committed_index,
                    *committed_term,
                    *granted,
                ),
            ),
            Rpc::LearnerReadinessRequest(request) | Rpc::LearnerReadinessReply { request, .. } => {
                request.group == self.mapping.parent
            }
        }
    }
}

// The core acknowledges an already committed remote snapshot without installing
// it. Compacted historical positions need no retained term; an uncompacted
// position must be in the known committed prefix.
fn snapshot_ack(parent: &GroupLog, index: u64) -> bool {
    index == 0
        || (index <= parent.commit_index
            && (index <= parent.base_index() || parent.term_at(index).is_some()))
}
