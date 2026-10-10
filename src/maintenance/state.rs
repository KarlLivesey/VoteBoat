// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

impl<A: StateMachine> Maintenance<A> {
    fn apply_control(
        &mut self,
        entry: &LogEntry,
        command: LeadershipCommand,
    ) -> Result<MaintenanceOutcome, ApplicationError> {
        let intent = command.intent();
        let operation = intent.request.operation;
        let current = self.record(operation);
        if current.is_some_and(|r| r.intent != intent) {
            return Ok(MaintenanceOutcome::Conflict);
        }
        let record = match (current, command) {
            (Some(r), LeadershipCommand::Begin(_)) => r,
            (None, LeadershipCommand::Begin(_)) => {
                if self.pending().is_some() {
                    return Ok(MaintenanceOutcome::Busy);
                }
                if self.records.len() == self.capacity {
                    return Err(ApplicationError::DedupCapacity);
                }
                LeadershipRecord {
                    intent,
                    index: entry.index,
                    term: entry.term,
                    phase: LeadershipPhase::Pending,
                }
            }
            (Some(r), LeadershipCommand::Complete { index, term, .. }) if r.index == index => {
                if r.phase != LeadershipPhase::Pending {
                    r
                } else {
                    if term != entry.term || term <= r.term {
                        return Err(ApplicationError::InvalidCommand);
                    }
                    LeadershipRecord {
                        phase: LeadershipPhase::Completed {
                            index: entry.index,
                            term,
                        },
                        ..r
                    }
                }
            }
            (Some(r), LeadershipCommand::Cancel { index, .. }) if r.index == index => {
                if r.phase != LeadershipPhase::Pending {
                    r
                } else {
                    if entry.term < r.term {
                        return Err(ApplicationError::InvalidCommand);
                    }
                    LeadershipRecord {
                        phase: LeadershipPhase::Cancelled {
                            index: entry.index,
                            term: entry.term,
                        },
                        ..r
                    }
                }
            }
            _ => return Ok(MaintenanceOutcome::Conflict),
        };
        self.records.insert(operation, record);
        Ok(MaintenanceOutcome::Recorded(record))
    }
    fn map_entry(entry: &LogEntry) -> Result<LogEntry, ApplicationError> {
        let mut mapped = entry.clone();
        if let EntryPayload::Command { operation, bytes } = &entry.payload {
            mapped.payload = match decode(bytes)? {
                Envelope::Data(data) => EntryPayload::Command {
                    operation: *operation,
                    bytes: data.to_vec(),
                },
                Envelope::Control(command) if command.intent().request.operation == *operation => {
                    EntryPayload::Noop
                }
                _ => return Err(ApplicationError::InvalidCommand),
            };
        }
        Ok(mapped)
    }
}
impl<A: StateMachine + Clone> StateMachine for Maintenance<A> {
    type Receipt = MaintenanceReceipt<A::Receipt>;
    fn validate_group(&self, group: GroupIdentity) -> Result<(), ApplicationError> {
        self.check_group(group)
    }
    fn deployment_requirements(&self) -> Option<ReadinessRequirements> {
        let inner = self.inner.deployment_requirements()?;
        Some(ReadinessRequirements {
            application_schema: self.schema,
            command_bytes: inner.command_bytes.checked_add(9)?.max(113),
            snapshot_bytes: inner
                .snapshot_bytes
                .checked_add(codec::HEADER_BYTES)?
                .checked_add(self.capacity.checked_mul(codec::RECORD_BYTES)?)?,
        })
    }
    fn applied_index(&self) -> u64 {
        self.inner.applied_index()
    }
    fn apply_batch(
        &mut self,
        entries: &[LogEntry],
    ) -> Result<Vec<Self::Receipt>, ApplicationError> {
        let mut next = self.clone();
        let mut receipts = Vec::with_capacity(
            entries
                .iter()
                .filter(|e| matches!(e.payload, EntryPayload::Command { .. }))
                .count(),
        );
        for entry in entries {
            if entry.term == 0 {
                return Err(ApplicationError::InvalidCommand);
            }
            let mapped = Self::map_entry(entry)?;
            let mut inner_receipts = next.inner.apply_batch(std::slice::from_ref(&mapped))?;
            match &entry.payload {
                EntryPayload::Command { operation, bytes } => match decode(bytes)? {
                    Envelope::Data(_) if inner_receipts.len() == 1 => {
                        receipts.push(MaintenanceReceipt::Data(inner_receipts.remove(0)))
                    }
                    Envelope::Control(command) if inner_receipts.is_empty() => {
                        let outcome = next.apply_control(entry, command)?;
                        receipts.push(MaintenanceReceipt::Administration {
                            operation: *operation,
                            index: entry.index,
                            outcome,
                        });
                    }
                    _ => return Err(ApplicationError::InvalidCommand),
                },
                _ if inner_receipts.is_empty() => (),
                _ => return Err(ApplicationError::InvalidCommand),
            }
            if next.inner.applied_index() != entry.index {
                return Err(ApplicationError::IndexGap);
            }
        }
        *self = next;
        Ok(receipts)
    }
}
impl<A: BoundedStateMachine + Clone> BoundedStateMachine for Maintenance<A>
where
    A::Receipt: ApplicationReceipt,
{
    fn receipt_bytes_bound(&self, entries: &[LogEntry]) -> Result<usize, ApplicationError> {
        let mut bytes = 0usize;
        for entry in entries {
            let mapped = Self::map_entry(entry)?;
            bytes = bytes
                .checked_add(
                    self.inner
                        .receipt_bytes_bound(std::slice::from_ref(&mapped))?,
                )
                .ok_or(ApplicationError::ReceiptBudget)?;
            if matches!(entry.payload, EntryPayload::Command { .. }) {
                bytes = bytes
                    .checked_add(size_of::<Self::Receipt>())
                    .ok_or(ApplicationError::ReceiptBudget)?;
            }
        }
        Ok(bytes)
    }
}
impl<A: ProposalAdmission + Clone> ProposalAdmission for Maintenance<A>
where
    A::Receipt: ApplicationReceipt,
{
    fn validate_proposal_context(
        &self,
        core: &Raft,
        operation: OperationId,
        bytes: &[u8],
    ) -> Result<(), ApplicationError> {
        self.check_group(core.state().bootstrap.group)?;
        match decode(bytes)? {
            Envelope::Data(bytes) => self.inner.validate_proposal_context(core, operation, bytes),
            Envelope::Control(command) if command.intent().request.operation == operation => {
                self.check_control_context(core, command)
            }
            _ => Err(ApplicationError::InvalidCommand),
        }
    }
    fn validate_proposal<'a>(
        &self,
        operation: OperationId,
        bytes: &[u8],
        pending: impl Iterator<Item = (OperationId, &'a [u8])>,
    ) -> Result<usize, ApplicationError> {
        let mut data = Vec::new();
        let mut reserved = BTreeSet::new();
        for (id, bytes) in pending {
            match decode(bytes)? {
                Envelope::Data(bytes) => data.push((id, bytes)),
                Envelope::Control(LeadershipCommand::Begin(i))
                    if !self.records.contains_key(&i.request.operation) =>
                {
                    reserved.insert(i.request.operation);
                }
                _ => (),
            }
        }
        let nested = match decode(bytes)? {
            Envelope::Data(bytes) => {
                self.inner
                    .validate_proposal(operation, bytes, data.into_iter())?
            }
            Envelope::Control(command) => {
                if command.intent().request.operation != operation {
                    return Err(ApplicationError::InvalidCommand);
                }
                if matches!(command, LeadershipCommand::Begin(_))
                    && !self.records.contains_key(&operation)
                {
                    reserved.insert(operation);
                }
                if reserved.len() > self.capacity.saturating_sub(self.records.len()) {
                    return Err(ApplicationError::DedupCapacity);
                }
                0
            }
        };
        size_of::<Self::Receipt>()
            .checked_add(nested)
            .ok_or(ApplicationError::ReceiptBudget)
    }
}
impl<A: CheckpointStateMachine> CheckpointStateMachine for Maintenance<A> {
    fn schema_version(&self) -> u64 {
        self.schema
    }
    fn checkpoint(&self, max_bytes: usize) -> Result<Vec<u8>, ApplicationError> {
        codec::checkpoint(self, max_bytes)
    }
    fn restore_checkpoint(
        &mut self,
        schema: u64,
        applied: u64,
        bytes: &[u8],
    ) -> Result<(), ApplicationError> {
        codec::restore(self, schema, applied, bytes)
    }
}
impl<A: ReadableStateMachine + Clone> ReadableStateMachine for Maintenance<A> {
    type Query = MaintenanceQuery<A::Query>;
    type ReadResult = MaintenanceRead<A::ReadResult>;
    fn read_at(
        &self,
        index: u64,
        query: Self::Query,
    ) -> Result<Self::ReadResult, ApplicationError> {
        if self.applied_index() < index {
            return Err(ApplicationError::NotApplied);
        }
        match query {
            MaintenanceQuery::Data(q) => self.inner.read_at(index, q).map(MaintenanceRead::Data),
            MaintenanceQuery::Leadership(op) => Ok(MaintenanceRead::Leadership(self.record(op))),
        }
    }
}
impl<A: BoundedReadableStateMachine + Clone> BoundedReadableStateMachine for Maintenance<A> {
    fn query_bytes(&self, query: &Self::Query, limit: usize) -> Result<usize, ApplicationError> {
        match query {
            MaintenanceQuery::Data(q) => self.inner.query_bytes(q, limit),
            _ => Ok(0),
        }
    }
    fn read_result_bound(&self, query: &Self::Query) -> Result<usize, ApplicationError> {
        let nested = match query {
            MaintenanceQuery::Data(q) => self.inner.read_result_bound(q)?,
            _ => 0,
        };
        size_of::<Self::ReadResult>()
            .checked_add(nested)
            .ok_or(ApplicationError::ReceiptBudget)
    }
    fn read_result_bytes(
        &self,
        result: &Self::ReadResult,
        limit: usize,
    ) -> Result<usize, ApplicationError> {
        match result {
            MaintenanceRead::Data(r) => self.inner.read_result_bytes(r, limit),
            _ => Ok(0),
        }
    }
}
