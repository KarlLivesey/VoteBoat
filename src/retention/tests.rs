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
fn admission_uses_the_applied_floor_and_the_candidate_acknowledgement_only() {
    let mut ledger = Ledger::with_default_session(limits(1, 2, 1));
    assert_eq!(ledger.admit(command(1, 2, 0)), Ok(()));
    assert_eq!(
        ledger.admit(command(1, 3, 0)),
        Err(ApplicationError::SessionWindow)
    );
    assert_eq!(ledger.admit(command(1, 3, 1)), Ok(()));
    // Unknown and expired sessions admit so the client reads the refusal.
    assert_eq!(ledger.admit(command(9, 100, 0)), Ok(()));
    run(&mut ledger, 1, command(1, 1, 0), 1);
    run(&mut ledger, 2, command(1, 3, 1), 1);
    assert_eq!(ledger.floor(1), Some(1));
    assert_eq!(ledger.admit(command(1, 1, 0)), Ok(()));
    assert_eq!(
        run(&mut ledger, 3, command(1, 1, 0), 1),
        SessionApply::Refused(SessionRefusal::Expired)
    );
    assert_eq!(ledger.retained_receipts(), 1);
}

/// One live session (the default) with receipts 1 and 2, and a registered
/// session 2 with its nonce, at applied index 3.
fn populated() -> (Ledger, Vec<u8>) {
    let mut ledger = Ledger::with_default_session(limits(2, 4, 2));
    run(&mut ledger, 1, command(1, 1, 0), 1);
    run(&mut ledger, 2, command(1, 2, 0), 2);
    assert_eq!(ledger.register(3, 50), Ok((2, false)));
    let bytes = encoded(&ledger);
    (ledger, bytes)
}

/// Offsets in `populated()`'s encoding.
mod at {
    pub const SESSIONS: usize = 0;
    pub const NEXT: usize = 12;
    pub const SESSION_COUNT: usize = 20;
    pub const REGISTRATION_COUNT: usize = 24;
    pub const FIRST_ID: usize = 28;
    pub const FIRST_LAST_INDEX: usize = 44;
    pub const FIRST_COUNT: usize = 52;
    pub const FIRST_RECEIPT: usize = 56;
    pub const SECOND_RECEIPT: usize = 80;
    pub const SECOND_ID: usize = 104;
    pub const SECOND_FLOOR: usize = 112;
    pub const SECOND_LAST_INDEX: usize = 120;
    pub const NONCE: usize = 132;
    pub const NONCE_SESSION: usize = 140;
    pub const NONCE_INDEX: usize = 148;
    pub const END: usize = 156;
}

#[test]
fn restore_refuses_every_state_outside_the_retention_invariant() {
    let (ledger, bytes) = populated();
    assert_eq!(bytes.len(), at::END);
    assert_eq!(ledger.decode(&bytes, 3).unwrap().0, ledger);
    assert!(ledger.decode(&bytes, 2).is_err(), "index above applied");
    let put = |at: usize, value: &[u8]| {
        let mut b = bytes.clone();
        b[at..at + value.len()].copy_from_slice(value);
        b
    };
    let cases = [
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
            put(at::FIRST_LAST_INDEX, &4u64.to_le_bytes()),
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
            put(at::SECOND_RECEIPT, &1u64.to_le_bytes()),
        ),
        (
            "receipt beyond floor plus window",
            put(at::SECOND_RECEIPT, &5u64.to_le_bytes()),
        ),
        (
            "receipt at the floor",
            put(at::FIRST_RECEIPT, &0u64.to_le_bytes()),
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
            put(at::NONCE_INDEX, &2u64.to_le_bytes()),
        ),
        (
            "nonce on a session with a floor",
            put(at::SECOND_FLOOR, &1u64.to_le_bytes()),
        ),
    ];
    for (name, image) in cases {
        assert_eq!(
            ledger.decode(&image, 3).err(),
            Some(ApplicationError::InvalidCheckpoint),
            "{name}"
        );
    }
    for cut in 0..bytes.len() {
        assert!(ledger.decode(&bytes[..cut], 3).is_err(), "cut {cut}");
    }
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
        LEDGER_HEADER_BYTES + 2 * SESSION_HEADER_BYTES + 8 * 24 + 4 * REGISTRATION_BYTES
    );
}
