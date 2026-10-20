// SPDX-License-Identifier: RPL-1.5
use super::*;
use crate::{
    application::*,
    log::{EntryPayload, LogEntry},
    raft::ReadinessRequirements,
};
use std::mem::{size_of, size_of_val};

impl ApplicationReceipt for ProgramReceipt {
    fn index(&self) -> u64 {
        self.index
    }
    fn operation(&self) -> OperationId {
        self.operation
    }
    fn nested_bytes(&self, _: usize) -> Result<usize, ApplicationError> {
        Ok(0)
    }
}
impl StateMachine for ProgramApplication {
    type Receipt = ProgramReceipt;
    fn validate_group(&self, group: GroupIdentity) -> Result<(), ApplicationError> {
        if self.group == group {
            Ok(())
        } else {
            Err(ApplicationError::InvalidCommand)
        }
    }
    /// Lifetime transaction records until TX1 (audit S6).
    fn retention(&self) -> crate::application::Retention {
        crate::application::Retention::LifetimeBounded
    }
    fn deployment_requirements(&self) -> Option<ReadinessRequirements> {
        use crate::raft::DeploymentPlan;
        // Plan: this group (decides the role), the profile version, the
        // coordinator and participant counts, then each catalog group in
        // order (3 words each). At most 3 + 3 + 16 × 3 = 54 words.
        let (coordinators, participants) =
            (self.profile.coordinators(), self.profile.participants());
        let mut plan = Vec::with_capacity(6 + 3 * (coordinators.len() + participants.len()));
        plan.extend(DeploymentPlan::group_words(self.group));
        plan.extend([
            self.profile.version(),
            coordinators.len() as u64,
            participants.len() as u64,
        ]);
        for group in coordinators.iter().chain(participants) {
            plan.extend(DeploymentPlan::group_words(*group));
        }
        Some(ReadinessRequirements {
            application_schema: PROGRAM_SCHEMA,
            // Words: coordinator transaction and key capacities, each of which
            // decides whether a committed command is recorded.
            identity: crate::raft::LayerIdentity::new(
                crate::raft::ApplicationFamily::PROGRAM_TRANSACTION,
                [
                    self.limits.coordinator_transactions() as u64,
                    self.limits.keys() as u64,
                    0,
                    0,
                ],
            ),
            inner_profile: crate::raft::InnerProfile::LEAF.planned(&plan),
            command_bytes: PROGRAM_COMMAND_BYTES,
            snapshot_bytes: codec::checkpoint_bound(self),
        })
    }
    fn applied_index(&self) -> u64 {
        self.applied
    }
    fn apply_batch(
        &mut self,
        entries: &[LogEntry],
    ) -> Result<Vec<ProgramReceipt>, ApplicationError> {
        let count = entries
            .iter()
            .filter(|e| matches!(e.payload, EntryPayload::Command { .. }))
            .count();
        let mut receipts = Vec::with_capacity(count);
        let mut next = self.clone();
        for entry in entries {
            if next.applied.checked_add(1) != Some(entry.index) {
                return Err(ApplicationError::IndexGap);
            }
            if entry.term == 0 {
                return Err(ApplicationError::InvalidCommand);
            }
            if let EntryPayload::Command { operation, bytes } = &entry.payload {
                let command = codec::decode(bytes)?;
                next.check_command(*operation, &command)?;
                let (outcome, duplicate, first_index) = next.execute(command, entry.index)?;
                receipts.push(ProgramReceipt {
                    index: entry.index,
                    first_index,
                    operation: *operation,
                    outcome,
                    duplicate,
                });
            }
            next.applied = entry.index;
        }
        *self = next;
        Ok(receipts)
    }
}
impl BoundedStateMachine for ProgramApplication {
    fn receipt_bytes_bound(&self, entries: &[LogEntry]) -> Result<usize, ApplicationError> {
        entries
            .iter()
            .filter(|e| matches!(e.payload, EntryPayload::Command { .. }))
            .count()
            .checked_mul(size_of::<ProgramReceipt>())
            .ok_or(ApplicationError::ReceiptBudget)
    }
}
impl ReadableStateMachine for ProgramApplication {
    type Query = ProgramQuery;
    type ReadResult = ProgramRead;
    fn read_at(
        &self,
        required_index: u64,
        query: ProgramQuery,
    ) -> Result<ProgramRead, ApplicationError> {
        if required_index > self.applied {
            return Err(ApplicationError::NotApplied);
        }
        self.query_bytes(&query, usize::MAX)?;
        Ok(match query {
            ProgramQuery::Status(id) => ProgramRead::Status(self.status(id).map(Box::new)),
            ProgramQuery::Keys(keys) => {
                ProgramRead::Keys(keys.iter().map(|id| (*id, self.key(*id))).collect())
            }
        })
    }
}
impl BoundedReadableStateMachine for ProgramApplication {
    fn query_bytes(
        &self,
        query: &ProgramQuery,
        nested_limit: usize,
    ) -> Result<usize, ApplicationError> {
        let bytes = match query {
            ProgramQuery::Status(_) => 0,
            ProgramQuery::Keys(keys) => {
                if keys.is_empty()
                    || keys.len() > self.limits.keys()
                    || !keys.windows(2).all(|p| p[0] < p[1])
                {
                    return Err(ApplicationError::InvalidCommand);
                }
                size_of_val(&**keys)
            }
        };
        if bytes > nested_limit {
            return Err(ApplicationError::ReceiptBudget);
        }
        Ok(bytes)
    }
    fn read_result_bound(&self, query: &ProgramQuery) -> Result<usize, ApplicationError> {
        self.query_bytes(query, usize::MAX)?;
        Ok(size_of::<ProgramRead>()
            + match query {
                ProgramQuery::Status(_) => status_bound(),
                ProgramQuery::Keys(keys) => keys.len() * size_of::<(KeyId, KeyRead)>(),
            })
    }
    fn read_result_bytes(
        &self,
        result: &ProgramRead,
        nested_limit: usize,
    ) -> Result<usize, ApplicationError> {
        let bytes = match result {
            ProgramRead::Status(None) => 0,
            ProgramRead::Status(Some(status)) => size_of::<ProgramStatus>() + status.nested_bytes(),
            ProgramRead::Keys(keys) => size_of_val(&**keys),
        };
        if bytes > nested_limit {
            return Err(ApplicationError::ReceiptBudget);
        }
        Ok(bytes)
    }
}
fn status_bound() -> usize {
    size_of::<ProgramStatus>()
        + 2 * MAX_CATALOG_GROUPS * size_of::<GroupIdentity>()
        + MAX_CATALOG_GROUPS * size_of::<ParticipantProgram>()
        + MAX_PROGRAM_KEYS * size_of::<KeyAccess>()
        + MAX_LOCAL_KEYS * size_of::<PreparedKey>()
        + 4 * MAX_CATALOG_GROUPS * size_of::<PhaseFact>()
}
impl CheckpointStateMachine for ProgramApplication {
    fn schema_version(&self) -> u64 {
        PROGRAM_SCHEMA
    }
    fn checkpoint(&self, max_bytes: usize) -> Result<Vec<u8>, ApplicationError> {
        codec::checkpoint(self, max_bytes)
    }
    fn restore_checkpoint(
        &mut self,
        schema: u64,
        applied_index: u64,
        bytes: &[u8],
    ) -> Result<(), ApplicationError> {
        codec::restore(self, schema, applied_index, bytes)
    }
}
