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
use super::*;

type Ledger = SessionLedger<[u8; 8], [u8; 8]>;

fn limits(sessions: u32, window: u32, registrations: u32) -> SessionLimits {
    SessionLimits::new(sessions, window, registrations).unwrap()
}
fn command(session: u64, sequence: u64, consumed_through: u64) -> SessionCommand {
    SessionCommand {
        session,
        sequence,
        consumed_through,
    }
}
fn run(ledger: &mut Ledger, index: u64, c: SessionCommand, content: u8) -> SessionApply<[u8; 8]> {
    ledger.apply(index, c, [content; 8], || [content.wrapping_add(1); 8])
}
fn encoded(ledger: &Ledger) -> Vec<u8> {
    let mut bytes = Vec::new();
    ledger.encode(&mut bytes);
    assert_eq!(bytes.len(), ledger.encoded_len());
    bytes
}

#[test]
fn operation_layout_round_trips_and_refuses_zero_halves() {
    let register = SessionOperation::Register { nonce: 7 };
    let id = register.operation().unwrap();
    assert_eq!(id.get(), 7);
    assert_eq!(SessionOperation::decode(id), Ok(register));
    let c = SessionOperation::Command {
        session: 3,
        sequence: 9,
    };
    let id = c.operation().unwrap();
    assert_eq!(id.get(), 3 << 64 | 9);
    assert_eq!(SessionOperation::decode(id), Ok(c));
    assert_eq!(
        SessionOperation::decode(OperationId::new(5 << 64).unwrap()),
        Err(ApplicationError::InvalidCommand)
    );
    assert_eq!(SessionOperation::Register { nonce: 0 }.operation(), None);
    for (session, sequence) in [(0, 1), (1, 0)] {
        assert_eq!(command(session, sequence, 0).operation(), None);
    }
}

#[test]
fn envelope_requires_eight_bytes_and_an_acknowledgement_below_the_sequence() {
    let id = command(2, 5, 0).operation().unwrap();
    let bytes = envelope(4, b"inner");
    assert_eq!(
        SessionRequest::decode(id, &bytes),
        Ok(SessionRequest::Command {
            command: command(2, 5, 4),
            inner: b"inner",
        })
    );
    assert_eq!(
        SessionRequest::decode(id, &envelope(5, b"")),
        Err(ApplicationError::InvalidCommand)
    );
    assert_eq!(
        SessionRequest::decode(id, &[0; 7]),
        Err(ApplicationError::InvalidCommand)
    );
    let register = OperationId::new(9).unwrap();
    assert_eq!(
        SessionRequest::decode(register, b"x"),
        Ok(SessionRequest::Register {
            nonce: 9,
            inner: b"x"
        })
    );
}

#[test]
fn a_ledger_without_a_default_session_requires_registration() {
    let mut ledger = Ledger::new(limits(2, 2, 2));
    assert_eq!(ledger.next_session(), 1);
    assert_eq!(
        run(&mut ledger, 1, command(1, 1, 0), 1),
        SessionApply::Refused(SessionRefusal::UnknownSession)
    );
    assert_eq!(ledger.register(2, 40), Ok((1, false)));
    assert_eq!(
        run(&mut ledger, 3, command(1, 1, 0), 1),
        SessionApply::Executed([2; 8])
    );
    assert_eq!(
        run(&mut ledger, 4, command(1, 1, 0), 1),
        SessionApply::Duplicate([2; 8])
    );
    assert_eq!(
        run(&mut ledger, 5, command(1, 1, 0), 3),
        SessionApply::Conflict
    );
    let mut restored_bytes = encoded(&ledger);
    let (restored, used) = ledger.decode(&restored_bytes, 5).unwrap();
    assert_eq!(used, restored_bytes.len());
    assert_eq!(restored, ledger);
    // The default-session flag is configuration, not data: a ledger that
    // expects one refuses an image whose allocation mark leaves no room for it.
    restored_bytes[12..20].copy_from_slice(&1u64.to_le_bytes());
    assert!(Ledger::with_default_session(limits(2, 2, 2))
        .decode(&restored_bytes, 5)
        .is_err());
}

#[test]
fn admission_reads_the_command_alone() {
    let mut ledger = Ledger::with_default_session(limits(1, 2, 1));
    assert_eq!(ledger.admit(command(1, 2, 0)), Ok(()));
    assert_eq!(
        ledger.admit(command(1, 3, 0)),
        Err(ApplicationError::SessionWindow)
    );
    assert_eq!(ledger.admit(command(1, 3, 1)), Ok(()));
    // Unknown sessions get the same rule as live ones: a leader that has not
    // applied a registration decides exactly as one that has.
    assert_eq!(ledger.admit(command(9, 2, 0)), Ok(()));
    assert_eq!(
        ledger.admit(command(9, 100, 0)),
        Err(ApplicationError::SessionWindow)
    );
    run(&mut ledger, 1, command(1, 1, 0), 1);
    run(&mut ledger, 2, command(1, 3, 1), 1);
    assert_eq!(ledger.floor(1), Some(1));
    // An expired retry admits so the client reads the refusal.
    assert_eq!(ledger.admit(command(1, 1, 0)), Ok(()));
    assert_eq!(
        run(&mut ledger, 3, command(1, 1, 0), 1),
        SessionApply::Refused(SessionRefusal::Expired)
    );
    assert_eq!(ledger.retained_receipts(), 1);
}

#[test]
fn a_committed_window_refusal_closes_the_session_so_the_sequence_never_executes() {
    let mut ledger = Ledger::new(limits(2, 2, 2));
    assert_eq!(ledger.register(1, 10), Ok((1, false)));
    assert_eq!(ledger.register(2, 11), Ok((2, false)));
    assert_eq!(
        run(&mut ledger, 3, command(1, 3, 0), 1),
        SessionApply::Refused(SessionRefusal::Window)
    );
    assert_eq!(ledger.live_sessions(), 1);
    assert_eq!(ledger.retained_registrations(), 1);
    // A later copy with an acknowledgement that would fit never executes.
    assert_eq!(
        run(&mut ledger, 4, command(1, 3, 2), 1),
        SessionApply::Refused(SessionRefusal::Expired)
    );
    assert_eq!(
        ledger.lookup(command(1, 3, 0).operation().unwrap()),
        Lookup::Expired
    );
    // Other sessions are unaffected, and a duplicate of a retained sequence
    // is still its original outcome whatever acknowledgement it carries.
    assert_eq!(
        run(&mut ledger, 5, command(2, 1, 0), 4),
        SessionApply::Executed([5; 8])
    );
    assert_eq!(
        run(&mut ledger, 6, command(2, 1, 0), 4),
        SessionApply::Duplicate([5; 8])
    );
    let bytes = encoded(&ledger);
    assert_eq!(ledger.decode(&bytes, 6).unwrap().0, ledger);
}

/// The default session with receipts 2 (acknowledging nothing) and 3
/// (acknowledging 1, so the floor is 1), and a registered session 2 with its
/// nonce, at applied index 4.
fn populated() -> (Ledger, Vec<u8>) {
    let mut ledger = Ledger::with_default_session(limits(2, 4, 2));
    run(&mut ledger, 1, command(1, 1, 0), 1);
    run(&mut ledger, 2, command(1, 2, 0), 2);
    run(&mut ledger, 3, command(1, 3, 1), 3);
    assert_eq!(ledger.register(4, 50), Ok((2, false)));
    let bytes = encoded(&ledger);
    (ledger, bytes)
}

/// Offsets in `populated()`'s encoding. A receipt is sequence,
/// acknowledgement, content and outcome: 32 bytes here.
mod at {
    pub const SESSIONS: usize = 0;
    pub const NEXT: usize = 12;
    pub const SESSION_COUNT: usize = 20;
    pub const REGISTRATION_COUNT: usize = 24;
    pub const FIRST_ID: usize = 28;
    pub const FIRST_FLOOR: usize = 36;
    pub const FIRST_LAST_INDEX: usize = 44;
    pub const FIRST_COUNT: usize = 52;
    pub const FIRST_RECEIPT: usize = 56;
    pub const FIRST_RECEIPT_ACK: usize = 64;
    pub const SECOND_RECEIPT: usize = 88;
    pub const SECOND_RECEIPT_ACK: usize = 96;
    pub const SECOND_ID: usize = 120;
    pub const SECOND_FLOOR: usize = 128;
    pub const SECOND_LAST_INDEX: usize = 136;
    pub const NONCE: usize = 148;
    pub const NONCE_SESSION: usize = 156;
    pub const NONCE_INDEX: usize = 164;
    pub const END: usize = 172;
}

#[test]
fn restore_refuses_every_state_outside_the_retention_invariant() {
    let (ledger, bytes) = populated();
    assert_eq!(bytes.len(), at::END);
    assert_eq!(ledger.floor(1), Some(1));
    assert_eq!(ledger.decode(&bytes, 4).unwrap().0, ledger);
    assert!(ledger.decode(&bytes, 3).is_err(), "index above applied");
    for (name, image) in mutations(&bytes) {
        assert_eq!(
            ledger.decode(&image, 4).err(),
            Some(ApplicationError::InvalidCheckpoint),
            "{name}"
        );
    }
    for cut in 0..bytes.len() {
        assert!(ledger.decode(&bytes[..cut], 4).is_err(), "cut {cut}");
    }
}

/// Single-field mutations of `populated()`'s encoding, each a state no
/// history produces.
fn mutations(bytes: &[u8]) -> Vec<(&'static str, Vec<u8>)> {
    let put = |at: usize, value: &[u8]| {
        let mut b = bytes.to_vec();
        b[at..at + value.len()].copy_from_slice(value);
        b
    };
    vec![
        ("limits differ", put(at::SESSIONS, &3u32.to_le_bytes())),
        (
            "allocation mark below a live id",
            put(at::NEXT, &2u64.to_le_bytes()),
        ),
        (
            "too many sessions",
            put(at::SESSION_COUNT, &3u32.to_le_bytes()),
        ),
        (
            "too many nonces",
            put(at::REGISTRATION_COUNT, &3u32.to_le_bytes()),
        ),
        ("session zero", put(at::FIRST_ID, &0u64.to_le_bytes())),
        (
            "descending sessions",
            put(at::SECOND_ID, &1u64.to_le_bytes()),
        ),
        (
            "last use after applied",
            put(at::FIRST_LAST_INDEX, &5u64.to_le_bytes()),
        ),
        (
            "index zero on a used built-in session",
            put(at::FIRST_LAST_INDEX, &0u64.to_le_bytes()),
        ),
        (
            "duplicate last-use index",
            put(at::FIRST_LAST_INDEX, &4u64.to_le_bytes()),
        ),
        (
            "floor lowered below the retained acknowledgement",
            put(at::FIRST_FLOOR, &0u64.to_le_bytes()),
        ),
        (
            "floor raised above the retained acknowledgement",
            put(at::SECOND_RECEIPT_ACK, &0u64.to_le_bytes()),
        ),
        (
            "acknowledgement at its own sequence",
            put(at::FIRST_RECEIPT_ACK, &2u64.to_le_bytes()),
        ),
        (
            "index zero on a registered session",
            put(at::SECOND_LAST_INDEX, &0u64.to_le_bytes()),
        ),
        (
            "receipts above the window",
            put(at::FIRST_COUNT, &5u32.to_le_bytes()),
        ),
        (
            "descending receipts",
            put(at::SECOND_RECEIPT, &2u64.to_le_bytes()),
        ),
        (
            "receipt beyond floor plus window",
            put(at::SECOND_RECEIPT, &6u64.to_le_bytes()),
        ),
        (
            "receipt at the floor",
            put(at::FIRST_RECEIPT, &1u64.to_le_bytes()),
        ),
        ("nonce zero", put(at::NONCE, &0u64.to_le_bytes())),
        (
            "nonce for an unknown session",
            put(at::NONCE_SESSION, &7u64.to_le_bytes()),
        ),
        (
            "nonce for a used session",
            put(at::NONCE_SESSION, &1u64.to_le_bytes()),
        ),
        (
            "nonce index mismatch",
            put(at::NONCE_INDEX, &3u64.to_le_bytes()),
        ),
        (
            "nonce on a session with a floor",
            put(at::SECOND_FLOOR, &1u64.to_le_bytes()),
        ),
    ]
}

/// Offsets of each session's `last_index` in a ledger encoding (sessions
/// precede the retained registrations).
fn last_index_offsets(bytes: &[u8]) -> Vec<usize> {
    let read = |at: usize| u64::from_le_bytes(bytes[at..at + 8].try_into().unwrap());
    let count = u32::from_le_bytes(bytes[20..24].try_into().unwrap());
    let mut at = LEDGER_HEADER_BYTES;
    let mut offsets = Vec::new();
    for _ in 0..count {
        offsets.push(at + 16);
        let receipts = read(at + 24) as u32 as usize;
        at += SESSION_HEADER_BYTES + receipts * 32;
    }
    offsets
}

#[test]
fn restore_refuses_last_use_indexes_that_no_history_produces() {
    // Session 2 used at index 2, the built-in session at 3; session 3
    // registered at 4 and never used, its nonce dropped by session 4's
    // registration at 5 (one nonce retained).
    let mut ledger = Ledger::with_default_session(limits(4, 4, 1));
    assert_eq!(ledger.register(1, 10), Ok((2, false)));
    run(&mut ledger, 2, command(2, 1, 0), 1);
    run(&mut ledger, 3, command(1, 1, 0), 1);
    assert_eq!(ledger.register(4, 11), Ok((3, false)));
    assert_eq!(ledger.register(5, 12), Ok((4, false)));
    let bytes = encoded(&ledger);
    assert_eq!(ledger.decode(&bytes, 5).unwrap().0, ledger);
    let at = last_index_offsets(&bytes);
    let put = |session: usize, index: u64| {
        let mut b = bytes.clone();
        b[at[session]..at[session] + 8].copy_from_slice(&index.to_le_bytes());
        b
    };
    for (name, image) in [
        ("two sessions used by one entry", put(1, 3)),
        ("built-in session used but at index zero", put(0, 0)),
        // An unused session's index is its registration, which precedes
        // every later session's registration.
        ("unused session registered after a later one", put(2, 6)),
        ("used session at index zero", put(1, 0)),
    ] {
        assert_eq!(
            ledger.decode(&image, 6).err(),
            Some(ApplicationError::InvalidCheckpoint),
            "{name}"
        );
    }
    // Other reachable orders restore: session 2 used after the built-in one.
    assert!(ledger.decode(&put(1, 6), 6).is_ok());
}

#[test]
fn registration_evicts_a_session_holding_nothing_before_the_least_recent() {
    // Session 1 used at 2; session 2 registered at 3, its nonce dropped by
    // session 3's registration at 4 (one nonce retained), never used;
    // session 3 used at 5. Plain LRU would evict session 1 (oldest use);
    // session 2 holds no outcome and no nonce, so it goes first.
    let mut ledger = Ledger::new(limits(3, 4, 1));
    let kept = Lookup::Retained {
        content: &[1; 8],
        outcome: &[2; 8],
    };
    assert_eq!(ledger.register(1, 10), Ok((1, false)));
    run(&mut ledger, 2, command(1, 1, 0), 1);
    assert_eq!(ledger.register(3, 11), Ok((2, false)));
    assert_eq!(ledger.register(4, 12), Ok((3, false)));
    run(&mut ledger, 5, command(3, 1, 0), 1);
    assert_eq!(ledger.register(6, 13), Ok((4, false)));
    assert_eq!(ledger.lookup(command(1, 1, 0).operation().unwrap()), kept);
    assert_eq!(
        ledger.lookup(command(2, 1, 0).operation().unwrap()),
        Lookup::Expired
    );
    // A session whose nonce is retained (registered, first command not yet
    // applied) counts as holding something: the next registration evicts the
    // least recently used session instead.
    assert_eq!(ledger.register(7, 14), Ok((5, false)));
    assert_eq!(
        ledger.lookup(command(1, 1, 0).operation().unwrap()),
        Lookup::Expired
    );
    assert_eq!(ledger.lookup(command(3, 1, 0).operation().unwrap()), kept);
    assert_eq!(
        ledger.lookup(command(4, 1, 0).operation().unwrap()),
        Lookup::Unknown
    );
    assert_eq!(ledger.live_sessions(), 3);
}

#[test]
fn registration_eviction_drops_the_least_recent_session_and_its_nonce() {
    let mut ledger = Ledger::with_default_session(limits(2, 4, 4));
    assert_eq!(ledger.register(1, 10), Ok((2, false)));
    // Full: the default session (last used at 0) goes first.
    assert_eq!(ledger.register(2, 11), Ok((3, false)));
    assert_eq!(
        ledger.lookup(command(1, 1, 0).operation().unwrap()),
        Lookup::Expired
    );
    // Session 2 is now the least recent and still holds nonce 10.
    assert_eq!(ledger.register(3, 12), Ok((4, false)));
    assert_eq!(
        ledger.lookup(OperationId::new(10).unwrap()),
        Lookup::Unknown
    );
    assert_eq!(ledger.retained_registrations(), 2);
    assert_eq!(ledger.live_sessions(), 2);
    let bytes = encoded(&ledger);
    assert_eq!(ledger.decode(&bytes, 3).unwrap().0, ledger);
    assert_eq!(
        Ledger::bound_bytes(limits(2, 4, 4)),
        LEDGER_HEADER_BYTES + 2 * SESSION_HEADER_BYTES + 8 * 32 + 4 * REGISTRATION_BYTES
    );
}
