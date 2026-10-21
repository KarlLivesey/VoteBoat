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
//! Deterministic counter with exactly-once retries bounded by a session ledger.
use super::*;
use crate::retention::{
    SessionApply, SessionCommand, SessionLedger, SessionLimits, SessionRecord, SessionRefusal,
    SessionRequest,
};

/// Counter checkpoint format (`VBCTR003`, 16-byte commands): the low byte of
/// every Counter application schema. The full schema also encodes the session
/// limits ([`Counter::schema`]), so it never equals an earlier Counter schema
/// (1, or format 4 for `VBCTR002`) or a leadership-maintenance profile schema
/// (2 before DR1; maintenance schemas now use the top four bits).
pub const COUNTER_FORMAT: u8 = 5;
/// Bytes of an operation command: `consumed_through` (u64 LE), delta (i64 LE).
/// A registration command has no bytes.
pub const COUNTER_COMMAND_BYTES: usize = 16;
const MAGIC: &[u8; 8] = b"VBCTR003";
const HEADER_BYTES: usize = 24;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CounterOutcome {
    Value(i64),
    Overflow,
    OperationConflict,
    /// The session allocated by a registration (or retained for its nonce).
    Registered(u64),
    /// A committed command the ledger refused; nothing executed.
    Refused(SessionRefusal),
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CounterReceipt {
    pub index: u64,
    pub operation: OperationId,
    pub outcome: CounterOutcome,
    pub duplicate: bool,
}

/// Answer to "what happened to this operation?" against applied state.
/// `Unknown` is never proof that the operation did not commit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CounterLookup {
    Retained(CounterOutcome),
    Registered(u64),
    Expired,
    Unknown,
}

/// An executed outcome kept for retries.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Executed {
    Value(i64),
    Overflow,
}
impl From<Executed> for CounterOutcome {
    fn from(executed: Executed) -> Self {
        match executed {
            Executed::Value(value) => Self::Value(value),
            Executed::Overflow => Self::Overflow,
        }
    }
}
impl SessionRecord for Executed {
    const BYTES: usize = 9;
    fn encode(&self, out: &mut Vec<u8>) {
        match self {
            Self::Value(value) => {
                out.push(0);
                out.extend(value.to_le_bytes());
            }
            Self::Overflow => out.extend([1; 1].iter().chain(&[0; 8])),
        }
    }
    fn decode(bytes: &[u8]) -> Option<Self> {
        let (tag, value) = bytes.split_first()?;
        match tag {
            0 => Some(Self::Value(i64::from_le_bytes(value.try_into().ok()?))),
            1 if value == [0; 8] => Some(Self::Overflow),
            _ => None,
        }
    }
}

enum Command {
    Register(u64),
    Add(SessionCommand, [u8; 8]),
}
fn decode(operation: OperationId, bytes: &[u8]) -> Result<Command, ApplicationError> {
    match SessionRequest::decode(operation, bytes)? {
        SessionRequest::Register { nonce, inner: [] } => Ok(Command::Register(nonce)),
        SessionRequest::Command { command, inner } => Ok(Command::Add(
            command,
            inner
                .try_into()
                .map_err(|_| ApplicationError::InvalidCommand)?,
        )),
        SessionRequest::Register { .. } => Err(ApplicationError::InvalidCommand),
    }
}

/// A replicated counter. Clients register a session (or use the built-in
/// session [`crate::retention::DEFAULT_SESSION`]) and send numbered additions
/// that acknowledge consumed outcomes; retained retry outcomes never exceed
/// `sessions × window`, whatever the uptime.
#[derive(Clone)]
pub struct Counter {
    value: i64,
    applied: u64,
    ledger: SessionLedger<[u8; 8], Executed>,
}
impl Counter {
    /// One session (the built-in one) with at most `window` unacknowledged
    /// outcomes. A client that never acknowledges can make `window`
    /// operations; acknowledging frees the window. The table holds one
    /// session, so the first registration evicts the built-in session
    /// permanently (its IDs then read `Expired`); use [`Self::with_limits`]
    /// for registered clients.
    pub fn new(window: usize) -> Result<Self, ApplicationError> {
        let window = u32::try_from(window).map_err(|_| ApplicationError::DedupCapacity)?;
        Self::with_limits(SessionLimits::single(window)?)
    }
    /// Limits are part of the application schema ([`Self::schema`]), so every
    /// replica must use the same values: readiness, snapshot install and
    /// restore refuse a replica or checkpoint with different ones.
    pub fn with_limits(limits: SessionLimits) -> Result<Self, ApplicationError> {
        Ok(Self {
            value: 0,
            applied: 0,
            ledger: SessionLedger::with_default_session(limits),
        })
    }
    pub fn limits(&self) -> SessionLimits {
        self.ledger.limits()
    }
    /// The application schema for `limits`: [`COUNTER_FORMAT`] plus the
    /// limits, so replicas that would decide a committed entry differently
    /// have different schemas.
    pub fn schema(limits: SessionLimits) -> u64 {
        limits.schema(COUNTER_FORMAT)
    }
    /// Command bytes for adding `delta` after consuming every outcome up to
    /// `consumed_through` in the same session.
    pub fn command(consumed_through: u64, delta: i64) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(COUNTER_COMMAND_BYTES);
        bytes.extend(consumed_through.to_le_bytes());
        bytes.extend(delta.to_le_bytes());
        bytes
    }
    /// Operation ID of `sequence` in `session` (both at least 1).
    pub fn operation(session: u64, sequence: u64) -> Option<OperationId> {
        SessionCommand {
            session,
            sequence,
            consumed_through: 0,
        }
        .operation()
    }
    /// Diagnostic/local applied read, not a distributed read-index API.
    pub fn read_applied(&self, required_index: u64) -> Result<i64, ApplicationError> {
        if self.applied < required_index {
            return Err(ApplicationError::NotApplied);
        }
        Ok(self.value)
    }
    /// Retained retry outcomes; at most `sessions × window`.
    pub fn retained_receipts(&self) -> usize {
        self.ledger.retained_receipts()
    }
    pub fn live_sessions(&self) -> usize {
        self.ledger.live_sessions()
    }
    pub fn retained_registrations(&self) -> usize {
        self.ledger.retained_registrations()
    }
    pub fn lookup_operation(&self, operation: OperationId) -> CounterLookup {
        match self.ledger.lookup(operation) {
            crate::retention::Lookup::Retained { outcome, .. } => {
                CounterLookup::Retained((*outcome).into())
            }
            crate::retention::Lookup::Registered(session) => CounterLookup::Registered(session),
            crate::retention::Lookup::Expired => CounterLookup::Expired,
            crate::retention::Lookup::Unknown => CounterLookup::Unknown,
        }
    }
    /// Steady-state envelope under the session retention rule: the largest
    /// command and the largest checkpoint the configured limits allow. It is
    /// independent of occupancy and uptime. The schema identifies the limits,
    /// so the family's configuration words are zero.
    pub fn readiness_requirements(&self) -> crate::raft::ReadinessRequirements {
        crate::raft::ReadinessRequirements {
            application_schema: Self::schema(self.limits()),
            identity: crate::raft::LayerIdentity::family(crate::raft::ApplicationFamily::COUNTER),
            inner_profile: crate::raft::InnerProfile::LEAF,
            command_bytes: COUNTER_COMMAND_BYTES,
            snapshot_bytes: HEADER_BYTES
                + SessionLedger::<[u8; 8], Executed>::bound_bytes(self.limits()),
        }
    }
    /// Check an administrator's declaration against enforced application bounds.
    /// Larger byte envelopes are allowed; the schema, which encodes the session
    /// limits, and the family must match exactly, so a replica with other
    /// limits, or another application family, is refused.
    pub fn validate_readiness_requirements(
        &self,
        declared: crate::raft::ReadinessRequirements,
    ) -> Result<(), ApplicationError> {
        let actual = self.readiness_requirements();
        if declared.application_schema != actual.application_schema
            || declared.identity != actual.identity
            || declared.inner_profile != actual.inner_profile
        {
            return Err(ApplicationError::UnsupportedSchema);
        }
        if declared.command_bytes < actual.command_bytes
            || declared.snapshot_bytes < actual.snapshot_bytes
        {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        Ok(())
    }
    fn validate(&self, operation: OperationId, bytes: &[u8]) -> Result<usize, ApplicationError> {
        if let Command::Add(command, _) = decode(operation, bytes)? {
            self.ledger.admit(command)?;
        }
        Ok(std::mem::size_of::<CounterReceipt>())
    }
    /// Apply one decoded command. Never fails: capacity and window decisions
    /// are typed outcomes of the committed entry.
    fn apply_command(&mut self, index: u64, command: Command) -> (CounterOutcome, bool) {
        match command {
            Command::Register(nonce) => match self.ledger.register(index, nonce) {
                Ok((session, duplicate)) => (CounterOutcome::Registered(session), duplicate),
                Err(refusal) => (CounterOutcome::Refused(refusal), false),
            },
            Command::Add(command, delta) => {
                let value = &mut self.value;
                let applied = self.ledger.apply(index, command, delta, || {
                    match value.checked_add(i64::from_le_bytes(delta)) {
                        Some(updated) => {
                            *value = updated;
                            Executed::Value(updated)
                        }
                        None => Executed::Overflow,
                    }
                });
                match applied {
                    SessionApply::Executed(outcome) => (outcome.into(), false),
                    SessionApply::Duplicate(outcome) => (outcome.into(), true),
                    SessionApply::Conflict => (CounterOutcome::OperationConflict, true),
                    SessionApply::Refused(refusal) => (CounterOutcome::Refused(refusal), false),
                }
            }
        }
    }
}

impl ApplicationReceipt for CounterReceipt {
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

impl BoundedStateMachine for Counter {
    fn receipt_bytes_bound(&self, entries: &[LogEntry]) -> Result<usize, ApplicationError> {
        entries
            .iter()
            .filter(|e| matches!(e.payload, EntryPayload::Command { .. }))
            .count()
            .checked_mul(std::mem::size_of::<CounterReceipt>())
            .ok_or(ApplicationError::InvalidCommand)
    }
}

impl ProposalAdmission for Counter {
    fn supports_ready_batch(&self) -> bool {
        true
    }
    /// The window check reads only applied state and the candidate, so pending
    /// commands need no reservation.
    fn prepare_ready_admission<'pending>(
        &self,
        _pending: impl Iterator<Item = (OperationId, &'pending [u8])>,
    ) -> Option<ReadyAdmission<'_>> {
        Some(Box::new(move |operation, bytes| {
            self.validate(operation, bytes)
        }))
    }
    fn validate_proposal_bounded(
        &self,
        operation: OperationId,
        bytes: &[u8],
        _pending_bound: usize,
    ) -> Option<Result<usize, ApplicationError>> {
        Some(self.validate(operation, bytes))
    }
    fn validate_proposal<'a>(
        &self,
        operation: OperationId,
        bytes: &[u8],
        _pending: impl Iterator<Item = (OperationId, &'a [u8])>,
    ) -> Result<usize, ApplicationError> {
        self.validate(operation, bytes)
    }
}

impl StateMachine for Counter {
    type Receipt = CounterReceipt;
    fn deployment_requirements(&self) -> Option<crate::raft::ReadinessRequirements> {
        Some(self.readiness_requirements())
    }
    fn retention(&self) -> Retention {
        Retention::Sessions(self.limits())
    }
    fn applied_index(&self) -> u64 {
        self.applied
    }
    /// `Err` only for an index gap or malformed bytes, both checked for the
    /// whole batch before any mutation.
    fn apply_batch(
        &mut self,
        entries: &[LogEntry],
    ) -> Result<Vec<Self::Receipt>, ApplicationError> {
        let mut expected = self.applied;
        let mut commands = 0;
        for entry in entries {
            expected = expected
                .checked_add(1)
                .filter(|next| *next == entry.index)
                .ok_or(ApplicationError::IndexGap)?;
            if let EntryPayload::Command { operation, bytes } = &entry.payload {
                decode(*operation, bytes)?;
                commands += 1;
            }
        }
        let mut receipts = Vec::with_capacity(commands);
        for entry in entries {
            if let EntryPayload::Command { operation, bytes } = &entry.payload {
                let command = decode(*operation, bytes)?;
                let (outcome, duplicate) = self.apply_command(entry.index, command);
                receipts.push(CounterReceipt {
                    index: entry.index,
                    operation: *operation,
                    outcome,
                    duplicate,
                });
            }
            self.applied = entry.index;
        }
        Ok(receipts)
    }
}

impl ReadableStateMachine for Counter {
    type Query = ();
    type ReadResult = i64;
    fn read_at(&self, required_index: u64, (): ()) -> Result<i64, ApplicationError> {
        self.read_applied(required_index)
    }
}
impl BoundedReadableStateMachine for Counter {
    fn query_bytes(&self, (): &(), _: usize) -> Result<usize, ApplicationError> {
        Ok(0)
    }
    fn read_result_bound(&self, (): &()) -> Result<usize, ApplicationError> {
        Ok(std::mem::size_of::<i64>())
    }
    fn read_result_bytes(&self, _: &i64, _: usize) -> Result<usize, ApplicationError> {
        Ok(0)
    }
}

impl CheckpointStateMachine for Counter {
    fn schema_version(&self) -> u64 {
        Self::schema(self.limits())
    }
    /// `VBCTR003`, applied index, value, then the ledger: limits, the session
    /// allocation mark and its closure record, live sessions with their acknowledged prefixes and
    /// retained receipts, and retained registration nonces.
    fn checkpoint(&self, max_bytes: usize) -> Result<Vec<u8>, ApplicationError> {
        let length = HEADER_BYTES + self.ledger.encoded_len();
        if length > max_bytes {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let mut bytes = Vec::with_capacity(length);
        bytes.extend(MAGIC);
        bytes.extend(self.applied.to_le_bytes());
        bytes.extend(self.value.to_le_bytes());
        self.ledger.encode(&mut bytes);
        Ok(bytes)
    }
    /// Direct decode; no replay. The ledger decode refuses anything that breaks
    /// the retention invariant or is not canonical; nothing is installed on
    /// refusal.
    fn restore_checkpoint(
        &mut self,
        schema: u64,
        applied: u64,
        bytes: &[u8],
    ) -> Result<(), ApplicationError> {
        if schema != self.schema_version() {
            return Err(ApplicationError::UnsupportedSchema);
        }
        let Some((header, rest)) = bytes.split_first_chunk::<HEADER_BYTES>() else {
            return Err(ApplicationError::InvalidCheckpoint);
        };
        let (magic, numbers) = header.split_at(8);
        let index = u64::from_le_bytes(numbers[..8].try_into().unwrap_or_default());
        let value = i64::from_le_bytes(numbers[8..].try_into().unwrap_or_default());
        if magic != MAGIC || index != applied {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let (ledger, used) = self.ledger.decode(rest, index)?;
        if used != rest.len() {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        self.value = value;
        self.applied = index;
        self.ledger = ledger;
        Ok(())
    }
}
