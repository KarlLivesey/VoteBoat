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
//! Bounded retention of client operation outcomes.
//!
//! A [`SessionLedger`] keeps exactly-once outcomes for client operations in a
//! space that does not grow with uptime. Clients register a session (a
//! replicated command with no business effect) and then number their
//! operations `1, 2, …` within it. Each command carries `consumed_through`, the
//! highest sequence whose outcome the client has consumed, all lower ones
//! included. The ledger keeps at most `window` unacknowledged outcomes per
//! session and at most `sessions` sessions; registering a session when the
//! table is full evicts the least recently used one, decided by log order.
//!
//! Retained receipts never exceed `sessions × window`, whatever the uptime.
//! A sequence at or below the acknowledged prefix, or in an evicted session, is
//! answered with [`SessionRefusal::Expired`] and never executed again.
//!
//! The window rule is a function of the command alone: `sequence −
//! consumed_through ≤ window`. Admission refuses a command outside it on every
//! leader, whatever that leader has applied, so an admitted command never
//! meets the rule at apply. A committed command outside it (admission
//! bypassed) is refused [`SessionRefusal::Window`] and closes its session, so
//! that sequence can never execute later under a larger acknowledgement.
//!
//! Registration evicts first a session with nothing retained: no receipt (it
//! never executed) and no registration nonce (the untouched built-in session,
//! or a registration whose nonce was dropped before first use). Then it evicts
//! the least recently used session. Ties go to the lower last-use index, then
//! the lower session ID. A session whose nonce is still retained counts as
//! used, so a client between registering and its first command is not
//! displaced by the next registration. A late copy of a registration whose
//! nonce is no longer retained allocates a new session and can therefore evict
//! a live one; it is indistinguishable from a fresh registration.
//!
//! Every retained receipt keeps the acknowledgement its command carried. The
//! command that last raised a session's floor is above that floor, so it is
//! retained until a later raise supersedes it: the floor always equals the
//! largest retained acknowledgement (0 with none), and restore refuses any
//! other value.
use crate::{application::ApplicationError, identity::OperationId};
use std::collections::{BTreeMap, VecDeque};

/// Largest configurable number of live sessions.
pub const MAX_SESSIONS: u32 = 65_536;
/// Largest configurable window of unacknowledged outcomes per session.
pub const MAX_WINDOW: u32 = 1_000_000;
/// Largest configurable number of retained registration nonces.
pub const MAX_REGISTRATIONS: u32 = 65_536;
/// Largest configurable `sessions × window`.
pub const MAX_RETAINED_RECEIPTS: u64 = 1_000_000;
/// The session that ledgers built with [`SessionLedger::with_default_session`]
/// hold from construction.
pub const DEFAULT_SESSION: u64 = 1;
/// Encoded bytes of the ledger header: limits, `next_session` and counts.
pub const LEDGER_HEADER_BYTES: usize = 28;
/// Encoded bytes of one session header, excluding its receipts.
pub const SESSION_HEADER_BYTES: usize = 28;
/// Encoded bytes of one retained registration nonce.
pub const REGISTRATION_BYTES: usize = 24;

/// Retention limits. They are part of the application schema: every replica
/// must use the same values, and restore refuses a checkpoint written with
/// different ones.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct SessionLimits {
    sessions: u32,
    window: u32,
    registrations: u32,
}

impl SessionLimits {
    /// `sessions` live sessions (S), `window` unacknowledged outcomes per
    /// session (K) and `registrations` retained registration nonces (Rg).
    pub fn new(sessions: u32, window: u32, registrations: u32) -> Result<Self, ApplicationError> {
        if !(1..=MAX_SESSIONS).contains(&sessions)
            || !(1..=MAX_WINDOW).contains(&window)
            || !(1..=MAX_REGISTRATIONS).contains(&registrations)
            || u64::from(sessions) * u64::from(window) > MAX_RETAINED_RECEIPTS
        {
            return Err(ApplicationError::DedupCapacity);
        }
        Ok(Self {
            sessions,
            window,
            registrations,
        })
    }
    /// One session with `window` unacknowledged outcomes.
    pub fn single(window: u32) -> Result<Self, ApplicationError> {
        Self::new(1, window, 1)
    }
    pub fn sessions(self) -> usize {
        self.sessions as usize
    }
    pub fn window(self) -> u64 {
        u64::from(self.window)
    }
    pub fn registrations(self) -> usize {
        self.registrations as usize
    }
    /// Upper bound on retained receipts: `sessions × window`.
    pub fn retained_receipts(self) -> usize {
        self.sessions() * self.window as usize
    }
    /// An application schema that identifies `format` and these limits
    /// exactly: `format` in the low byte, then `sessions − 1` (16 bits),
    /// `window − 1` (20 bits) and `registrations − 1` (16 bits). Replicas
    /// compare schemas by equality at readiness, snapshot install and restore,
    /// so replicas built with different limits refuse each other instead of
    /// deciding the same committed entry differently.
    pub fn schema(self, format: u8) -> u64 {
        u64::from(format)
            | u64::from(self.sessions - 1) << 8
            | u64::from(self.window - 1) << 24
            | u64::from(self.registrations - 1) << 44
    }
}

impl Default for SessionLimits {
    /// 256 sessions, 32 unacknowledged outcomes each, 64 registration nonces.
    fn default() -> Self {
        Self {
            sessions: 256,
            window: 32,
            registrations: 64,
        }
    }
}

/// The operation-ID layout: `(session << 64) | sequence` for commands and
/// `(0 << 64) | nonce` for registrations.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionOperation {
    Register { nonce: u64 },
    Command { session: u64, sequence: u64 },
}

impl SessionOperation {
    pub fn decode(operation: OperationId) -> Result<Self, ApplicationError> {
        let id = operation.get();
        let (session, low) = ((id >> 64) as u64, id as u64);
        match (session, low) {
            // A non-zero ID with a zero high half has a non-zero nonce.
            (0, nonce) => Ok(Self::Register { nonce }),
            (_, 0) => Err(ApplicationError::InvalidCommand),
            (session, sequence) => Ok(Self::Command { session, sequence }),
        }
    }
    /// The operation ID, or `None` for a zero nonce, session or sequence.
    pub fn operation(self) -> Option<OperationId> {
        match self {
            Self::Register { nonce } => OperationId::new(u128::from(nonce)),
            Self::Command { session, sequence } if session > 0 && sequence > 0 => {
                OperationId::new(u128::from(session) << 64 | u128::from(sequence))
            }
            Self::Command { .. } => None,
        }
    }
}

/// One session command's identity and acknowledgement.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SessionCommand {
    pub session: u64,
    pub sequence: u64,
    /// Highest sequence whose outcome the client has consumed, all lower ones
    /// included. Always below `sequence`.
    pub consumed_through: u64,
}

impl SessionCommand {
    /// The operation ID `(session << 64) | sequence`.
    pub fn operation(self) -> Option<OperationId> {
        SessionOperation::Command {
            session: self.session,
            sequence: self.sequence,
        }
        .operation()
    }
}

/// A decoded session request: the operation layout plus the 8-byte
/// `consumed_through` envelope that precedes every command's inner bytes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionRequest<'a> {
    Register {
        nonce: u64,
        inner: &'a [u8],
    },
    Command {
        command: SessionCommand,
        inner: &'a [u8],
    },
}

impl<'a> SessionRequest<'a> {
    /// Decode the layout and envelope. A command must acknowledge less than
    /// its own sequence.
    pub fn decode(operation: OperationId, bytes: &'a [u8]) -> Result<Self, ApplicationError> {
        match SessionOperation::decode(operation)? {
            SessionOperation::Register { nonce } => Ok(Self::Register {
                nonce,
                inner: bytes,
            }),
            SessionOperation::Command { session, sequence } => {
                let (consumed, inner) = bytes
                    .split_first_chunk::<8>()
                    .ok_or(ApplicationError::InvalidCommand)?;
                let consumed_through = u64::from_le_bytes(*consumed);
                if consumed_through >= sequence {
                    return Err(ApplicationError::InvalidCommand);
                }
                Ok(Self::Command {
                    command: SessionCommand {
                        session,
                        sequence,
                        consumed_through,
                    },
                    inner,
                })
            }
        }
    }
}

/// Encode a command envelope: `consumed_through` (u64 LE) then `inner`.
pub fn envelope(consumed_through: u64, inner: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(8 + inner.len());
    bytes.extend(consumed_through.to_le_bytes());
    bytes.extend(inner);
    bytes
}

/// A typed refusal. It is an outcome of a committed entry, never an error.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionRefusal {
    /// The outcome is no longer retained: the sequence is acknowledged or its
    /// session was evicted or closed. It may have executed once; it never
    /// executes again.
    Expired,
    /// The command's sequence is more than `window` above its own
    /// acknowledgement. Admission refuses such a command; a committed one
    /// (admission bypassed) is refused once and closes its session, so the
    /// sequence never executes. Register a new session to continue.
    Window,
    /// The session was never allocated.
    UnknownSession,
    /// No session ID is left (2^64 registrations).
    Exhausted,
}

/// Result of applying one command.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionApply<R> {
    Executed(R),
    /// The retained original outcome of the same command.
    Duplicate(R),
    /// The sequence is retained with different content.
    Conflict,
    Refused(SessionRefusal),
}

/// Answer to "what happened to this operation?" (pack ch08 lookup).
/// Absence is never proof that an operation did not commit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Lookup<'a, C, R> {
    Retained { content: &'a C, outcome: &'a R },
    Registered(u64),
    Expired,
    Unknown,
}

/// A fixed-size record kept in a ledger and its checkpoint.
pub trait SessionRecord: Clone + Eq + Sized {
    /// Exact encoded length.
    const BYTES: usize;
    fn encode(&self, out: &mut Vec<u8>);
    /// Decode exactly `BYTES` bytes; `None` for a non-canonical encoding.
    fn decode(bytes: &[u8]) -> Option<Self>;
}

impl SessionRecord for [u8; 8] {
    const BYTES: usize = 8;
    fn encode(&self, out: &mut Vec<u8>) {
        out.extend(self);
    }
    fn decode(bytes: &[u8]) -> Option<Self> {
        bytes.try_into().ok()
    }
}

/// One retained outcome and the acknowledgement its command carried.
#[derive(Clone, Debug, Eq, PartialEq)]
struct Receipt<C, R> {
    sequence: u64,
    consumed_through: u64,
    content: C,
    outcome: R,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Session<C, R> {
    /// Contiguous acknowledged prefix: every sequence at or below it is
    /// retired. Always the largest retained `consumed_through` (0 with none).
    floor: u64,
    /// Index of the entry that last executed in, or registered, this session.
    last_index: u64,
    /// Registration nonce, kept until the session's first execution.
    nonce: Option<u64>,
    /// Ascending sequences in `(floor, floor + window]`. Never empty once the
    /// session has executed: the latest sequence is above every floor.
    retained: VecDeque<Receipt<C, R>>,
}

impl<C, R> Session<C, R> {
    fn new(last_index: u64, nonce: Option<u64>) -> Self {
        Self {
            floor: 0,
            last_index,
            nonce,
            retained: VecDeque::new(),
        }
    }
    fn position(&self, sequence: u64) -> Result<usize, usize> {
        self.retained
            .binary_search_by_key(&sequence, |r| r.sequence)
    }
}

/// Replicated session table. See the module documentation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionLedger<C, R> {
    limits: SessionLimits,
    default_session: bool,
    /// Allocation high-water mark (a maximum): every session below it was
    /// allocated once; one that is not live has expired.
    next_session: u64,
    sessions: BTreeMap<u64, Session<C, R>>,
    /// Retained registration nonces: nonce → (session, registration index).
    registrations: BTreeMap<u64, (u64, u64)>,
}

impl<C: SessionRecord, R: SessionRecord> SessionLedger<C, R> {
    /// An empty ledger; clients must register before their first command.
    pub fn new(limits: SessionLimits) -> Self {
        Self {
            limits,
            default_session: false,
            next_session: 1,
            sessions: BTreeMap::new(),
            registrations: BTreeMap::new(),
        }
    }
    /// A ledger whose session [`DEFAULT_SESSION`] is live from construction
    /// (last used at index 0, so it is the first evicted).
    pub fn with_default_session(limits: SessionLimits) -> Self {
        let mut ledger = Self::new(limits);
        ledger.default_session = true;
        ledger.next_session = DEFAULT_SESSION + 1;
        ledger
            .sessions
            .insert(DEFAULT_SESSION, Session::new(0, None));
        ledger
    }
    pub fn limits(&self) -> SessionLimits {
        self.limits
    }
    /// Live sessions.
    pub fn live_sessions(&self) -> usize {
        self.sessions.len()
    }
    /// Retained receipts across all sessions; at most `sessions × window`.
    pub fn retained_receipts(&self) -> usize {
        self.sessions.values().map(|s| s.retained.len()).sum()
    }
    /// Retained registration nonces; at most `registrations`.
    pub fn retained_registrations(&self) -> usize {
        self.registrations.len()
    }
    /// The next session ID to allocate.
    pub fn next_session(&self) -> u64 {
        self.next_session
    }
    /// The acknowledged prefix of a live session.
    pub fn floor(&self, session: u64) -> Option<u64> {
        self.sessions.get(&session).map(|s| s.floor)
    }

    /// Admission check. Refuses a command whose sequence is more than
    /// `window` above its own acknowledgement. The rule reads only the
    /// command, so every leader decides it the same way whatever it has
    /// applied (a leader that has not yet applied the session's registration
    /// included), and apply never meets it for an admitted command. Admits
    /// everything else (registrations, duplicates, expired and unknown
    /// sessions) so a client always obtains committed terminal truth; pending
    /// commands need no scan.
    pub fn admit(&self, command: SessionCommand) -> Result<(), ApplicationError> {
        if Self::outside_window(command, self.limits.window()) {
            return Err(ApplicationError::SessionWindow);
        }
        Ok(())
    }
    /// The window rule, a function of the command alone. The command's own
    /// acknowledgement counts, so a client with a full window can always
    /// deliver the acknowledgement that frees it.
    fn outside_window(command: SessionCommand, window: u64) -> bool {
        command.sequence.saturating_sub(command.consumed_through) > window
    }

    /// Apply a registration at `index`. Never fails: the registration path is
    /// always open, except after 2^64 allocations (≈ 584,000 years at 10^6/s).
    pub fn register(&mut self, index: u64, nonce: u64) -> Result<(u64, bool), SessionRefusal> {
        if let Some((session, _)) = self.registrations.get(&nonce) {
            return Ok((*session, true));
        }
        let session = self.next_session;
        if session == u64::MAX {
            return Err(SessionRefusal::Exhausted);
        }
        if self.sessions.len() >= self.limits.sessions() {
            self.evict_least_recent();
        }
        if self.registrations.len() >= self.limits.registrations() {
            self.drop_oldest_registration();
        }
        self.next_session = session + 1;
        self.sessions
            .insert(session, Session::new(index, Some(nonce)));
        self.registrations.insert(nonce, (session, index));
        Ok((session, false))
    }

    /// Apply a command at `index`, executing it with `execute` only when it is
    /// new and fits the window. Checks happen before any mutation. A new
    /// command outside the window is refused and closes its session, so the
    /// refusal is final for that sequence: every later copy is `Expired`.
    pub fn apply(
        &mut self,
        index: u64,
        command: SessionCommand,
        content: C,
        execute: impl FnOnce() -> R,
    ) -> SessionApply<R> {
        let window = self.limits.window();
        let SessionCommand {
            session,
            sequence,
            consumed_through,
        } = command;
        let Some(s) = self.sessions.get_mut(&session) else {
            return SessionApply::Refused(if session >= self.next_session {
                SessionRefusal::UnknownSession
            } else {
                SessionRefusal::Expired
            });
        };
        if sequence <= s.floor {
            return SessionApply::Refused(SessionRefusal::Expired);
        }
        let position = match s.position(sequence) {
            Ok(found) => {
                let kept = &s.retained[found];
                return if kept.content == content {
                    SessionApply::Duplicate(kept.outcome.clone())
                } else {
                    SessionApply::Conflict
                };
            }
            Err(position) => position,
        };
        if Self::outside_window(command, window) {
            // Close the session: no later copy of this sequence, whatever
            // acknowledgement it carries, can execute.
            if let Some(nonce) = self.sessions.remove(&session).and_then(|s| s.nonce) {
                self.registrations.remove(&nonce);
            }
            return SessionApply::Refused(SessionRefusal::Window);
        }
        // `sequence − floor ≤ sequence − consumed_through ≤ window`.
        let floor = s.floor.max(consumed_through);
        let outcome = execute();
        s.retained.insert(
            position,
            Receipt {
                sequence,
                consumed_through,
                content,
                outcome: outcome.clone(),
            },
        );
        s.floor = floor;
        while s.retained.front().is_some_and(|r| r.sequence <= floor) {
            s.retained.pop_front();
        }
        s.last_index = index;
        if let Some(nonce) = s.nonce.take() {
            self.registrations.remove(&nonce);
        }
        SessionApply::Executed(outcome)
    }

    /// Tri-state lookup of an operation against applied state.
    pub fn lookup(&self, operation: OperationId) -> Lookup<'_, C, R> {
        match SessionOperation::decode(operation) {
            Ok(SessionOperation::Register { nonce }) => self
                .registrations
                .get(&nonce)
                .map_or(Lookup::Unknown, |(session, _)| Lookup::Registered(*session)),
            Ok(SessionOperation::Command { session, sequence }) => {
                let Some(s) = self.sessions.get(&session) else {
                    return if session >= self.next_session {
                        Lookup::Unknown
                    } else {
                        Lookup::Expired
                    };
                };
                if sequence <= s.floor {
                    return Lookup::Expired;
                }
                s.position(sequence).map_or(Lookup::Unknown, |found| {
                    let kept = &s.retained[found];
                    Lookup::Retained {
                        content: &kept.content,
                        outcome: &kept.outcome,
                    }
                })
            }
            Err(_) => Lookup::Unknown,
        }
    }

    fn evict_least_recent(&mut self) {
        // Sessions with nothing retained (no receipt, no nonce) go first, so
        // live clients keep their outcomes; then the least recently used. The
        // key is a function of applied entries and unique (last-use indexes
        // are distinct). Registration is rare, so a scan beats an index
        // maintained on every write.
        let Some(victim) = self
            .sessions
            .iter()
            .min_by_key(|(id, s)| {
                let holds = !s.retained.is_empty() || s.nonce.is_some();
                (holds, s.last_index, **id)
            })
            .map(|(id, _)| *id)
        else {
            return;
        };
        if let Some(nonce) = self.sessions.remove(&victim).and_then(|s| s.nonce) {
            self.registrations.remove(&nonce);
        }
    }

    fn drop_oldest_registration(&mut self) {
        let Some(nonce) = self
            .registrations
            .iter()
            .min_by_key(|(nonce, (_, index))| (*index, **nonce))
            .map(|(nonce, _)| *nonce)
        else {
            return;
        };
        if let Some((session, _)) = self.registrations.remove(&nonce) {
            if let Some(s) = self.sessions.get_mut(&session) {
                s.nonce = None;
            }
        }
    }

    /// Largest encoding for these limits.
    pub fn bound_bytes(limits: SessionLimits) -> usize {
        LEDGER_HEADER_BYTES
            + limits.sessions() * SESSION_HEADER_BYTES
            + limits.retained_receipts() * Self::receipt_bytes()
            + limits.registrations() * REGISTRATION_BYTES
    }
    const fn receipt_bytes() -> usize {
        16 + C::BYTES + R::BYTES
    }
    /// Exact encoded length.
    pub fn encoded_len(&self) -> usize {
        LEDGER_HEADER_BYTES
            + self.sessions.len() * SESSION_HEADER_BYTES
            + self.retained_receipts() * Self::receipt_bytes()
            + self.registrations.len() * REGISTRATION_BYTES
    }

    /// Canonical encoding: limits, `next_session`, counts, sessions in
    /// ascending ID with receipts (sequence, acknowledgement, content,
    /// outcome) in ascending sequence, then registrations in ascending nonce.
    /// Nothing below a floor is encoded.
    pub fn encode(&self, out: &mut Vec<u8>) {
        out.extend(self.limits.sessions.to_le_bytes());
        out.extend(self.limits.window.to_le_bytes());
        out.extend(self.limits.registrations.to_le_bytes());
        out.extend(self.next_session.to_le_bytes());
        out.extend((self.sessions.len() as u32).to_le_bytes());
        out.extend((self.registrations.len() as u32).to_le_bytes());
        for (id, s) in &self.sessions {
            out.extend(id.to_le_bytes());
            out.extend(s.floor.to_le_bytes());
            out.extend(s.last_index.to_le_bytes());
            out.extend((s.retained.len() as u32).to_le_bytes());
            for r in &s.retained {
                out.extend(r.sequence.to_le_bytes());
                out.extend(r.consumed_through.to_le_bytes());
                r.content.encode(out);
                r.outcome.encode(out);
            }
        }
        for (nonce, (session, index)) in &self.registrations {
            out.extend(nonce.to_le_bytes());
            out.extend(session.to_le_bytes());
            out.extend(index.to_le_bytes());
        }
    }

    /// Decode a ledger encoded by [`Self::encode`] with this ledger's
    /// configuration, at applied index `applied`. Returns the ledger and the
    /// bytes it used. Refuses (`InvalidCheckpoint`) anything that violates the
    /// retention invariant or is not the canonical encoding.
    pub fn decode(&self, bytes: &[u8], applied: u64) -> Result<(Self, usize), ApplicationError> {
        let mut reader = Reader { bytes, at: 0 };
        let limits = SessionLimits {
            sessions: reader.u32()?,
            window: reader.u32()?,
            registrations: reader.u32()?,
        };
        let mut ledger = Self {
            limits,
            default_session: self.default_session,
            next_session: reader.u64()?,
            sessions: BTreeMap::new(),
            registrations: BTreeMap::new(),
        };
        let first = if self.default_session {
            DEFAULT_SESSION + 1
        } else {
            1
        };
        let session_count = reader.u32()? as usize;
        let registration_count = reader.u32()? as usize;
        if limits != self.limits
            || ledger.next_session < first
            || session_count > limits.sessions()
            || registration_count > limits.registrations()
        {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let mut lru = Lru::default();
        for _ in 0..session_count {
            ledger.decode_session(&mut reader, applied, &mut lru)?;
        }
        for _ in 0..registration_count {
            ledger.decode_registration(&mut reader, applied)?;
        }
        let used = reader.at;
        let mut canonical = Vec::with_capacity(used);
        ledger.encode(&mut canonical);
        if canonical[..] != bytes[..used] {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        Ok((ledger, used))
    }

    fn decode_session(
        &mut self,
        reader: &mut Reader<'_>,
        applied: u64,
        lru: &mut Lru,
    ) -> Result<(), ApplicationError> {
        let invalid = ApplicationError::InvalidCheckpoint;
        let window = self.limits.window();
        let id = reader.u64()?;
        let floor = reader.u64()?;
        let last_index = reader.u64()?;
        let count = reader.u32()? as usize;
        let ascending = self
            .sessions
            .last_key_value()
            .is_none_or(|(last, _)| id > *last);
        if id == 0
            || id >= self.next_session
            || !ascending
            || last_index > applied
            || count as u64 > window
        {
            return Err(invalid);
        }
        let mut session = Session::new(last_index, None);
        session.floor = floor;
        let mut acknowledged = 0;
        for _ in 0..count {
            let sequence = reader.u64()?;
            let consumed_through = reader.u64()?;
            let content = C::decode(reader.take(C::BYTES)?).ok_or(invalid)?;
            let outcome = R::decode(reader.take(R::BYTES)?).ok_or(invalid)?;
            let previous = session.retained.back().map_or(floor, |r| r.sequence);
            let command = SessionCommand {
                session: id,
                sequence,
                consumed_through,
            };
            if sequence <= previous
                || sequence - floor > window
                || consumed_through >= sequence
                || Self::outside_window(command, window)
            {
                return Err(invalid);
            }
            acknowledged = acknowledged.max(consumed_through);
            session.retained.push_back(Receipt {
                sequence,
                consumed_through,
                content,
                outcome,
            });
        }
        // The command that last raised the floor is retained (its sequence
        // is above the floor), so the floor equals the largest retained
        // acknowledgement exactly: a lowered floor would re-execute an
        // acknowledged sequence, a raised one would expire an unacknowledged
        // one. With nothing retained the session never executed: floor 0.
        let built_in = self.default_session && id == DEFAULT_SESSION;
        if floor != acknowledged || !lru.admit(built_in, &session) {
            return Err(invalid);
        }
        self.sessions.insert(id, session);
        Ok(())
    }

    fn decode_registration(
        &mut self,
        reader: &mut Reader<'_>,
        applied: u64,
    ) -> Result<(), ApplicationError> {
        let nonce = reader.u64()?;
        let id = reader.u64()?;
        let index = reader.u64()?;
        let ascending = self
            .registrations
            .last_key_value()
            .is_none_or(|(last, _)| nonce > *last);
        // A nonce is retained only until its session first executes, so the
        // session is live, untouched since registration, and unclaimed.
        let valid = nonce > 0
            && ascending
            && index > 0
            && index <= applied
            && self.sessions.get(&id).is_some_and(|s| {
                s.floor == 0 && s.retained.is_empty() && s.last_index == index && s.nonce.is_none()
            });
        if !valid {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        if let Some(s) = self.sessions.get_mut(&id) {
            s.nonce = Some(nonce);
        }
        self.registrations.insert(nonce, (id, index));
        Ok(())
    }
}

/// Reachable last-use indexes, checked in ascending session order. Each applied
/// entry updates at most one session, so indexes are distinct; only the
/// untouched built-in session has index 0 (a used session's index is the
/// entry that used it); and sessions are allocated in log order, so a session
/// that has not executed since registration (index = registration index) is
/// older than every session with a higher ID.
#[derive(Default)]
struct Lru {
    seen: std::collections::BTreeSet<u64>,
    /// Largest registration index of an unused registered session so far.
    unused_registration: u64,
}

impl Lru {
    fn admit<C, R>(&mut self, built_in: bool, s: &Session<C, R>) -> bool {
        let unused = s.retained.is_empty();
        let valid = if built_in && unused {
            s.last_index == 0
        } else {
            s.last_index > self.unused_registration && self.seen.insert(s.last_index)
        };
        if unused && !built_in {
            self.unused_registration = s.last_index;
        }
        valid
    }
}

struct Reader<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], ApplicationError> {
        let end = self
            .at
            .checked_add(n)
            .filter(|end| *end <= self.bytes.len())
            .ok_or(ApplicationError::InvalidCheckpoint)?;
        let slice = &self.bytes[self.at..end];
        self.at = end;
        Ok(slice)
    }
    fn u32(&mut self) -> Result<u32, ApplicationError> {
        let bytes = self.take(4)?;
        Ok(u32::from_le_bytes(bytes.try_into().unwrap_or_default()))
    }
    fn u64(&mut self) -> Result<u64, ApplicationError> {
        let bytes = self.take(8)?;
        Ok(u64::from_le_bytes(bytes.try_into().unwrap_or_default()))
    }
}

#[cfg(test)]
mod tests;
