// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Pure original receive-source preflight; serialized execution rechecks it.
use super::*;
impl Raft {
    pub(crate) fn authenticated_message_source(&self, m: &Message) -> Result<(), RaftError> {
        // Preserve the existing benign discard of authenticated obsolete repair,
        // before interpreting a historical payload against the newer local head.
        if self.authenticated_repair_source(m) {
            if m.term < self.durable.hard_state.term {
                return Ok(());
            }
            if self.local_voter() && m.configuration < self.membership().id() {
                return Err(RaftError::WrongIdentity);
            }
        }
        match &m.rpc {
            Rpc::LearnerReadinessRequest(_) | Rpc::LearnerReadinessReply { .. } => {
                self.readiness_message_source(m)
            }
            Rpc::AuthorityRequest { .. } | Rpc::AuthorityReply { .. } => {
                self.authority_message_source(m)
            }
            Rpc::LearnerRepair { .. } => self.validate_learner_repair_request(m),
            Rpc::LearnerRepaired { .. } => self.repair_reply_scope(m).map(|_| ()),
            Rpc::LearnerRepairSnapshot { .. } | Rpc::CommittedLearnerRepairSnapshot { .. } => {
                self.validate_repair_snapshot_source(m)
            }
            _ if self.retirement_commit(m).is_some() => Ok(()),
            _ => self.validate_replica_message(m),
        }
    }
}
