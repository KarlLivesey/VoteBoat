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
//! The retention bound at exactly `sessions × window`.
use super::*;

#[test]
fn retention_is_exactly_sessions_times_window_at_the_boundary() {
    let (sessions, window, registrations) = (4u64, 8u64, 4u64);
    let limits = SessionLimits::new(sessions as u32, window as u32, registrations as u32).unwrap();
    let mut counter = Counter::with_limits(limits).unwrap();
    let required = counter.readiness_requirements();
    assert_eq!(
        required.snapshot_bytes,
        24 + 28 + 4 * (28 + 8 * 33) + 4 * 24
    );
    let index = std::cell::Cell::new(0);
    let next = |entry: &dyn Fn(u64) -> LogEntry, counter: &mut Counter| {
        index.set(index.get() + 1);
        counter
            .apply_batch(&[entry(index.get())])
            .unwrap()
            .remove(0)
    };
    for nonce in 1..sessions {
        let r = next(&|i| register(i, nonce), &mut counter);
        assert_eq!(r.outcome, CounterOutcome::Registered(nonce + 1));
    }
    // Fill every session's window with unacknowledged outcomes.
    for session in 1..=sessions {
        for sequence in 1..=window {
            let r = next(&|i| add(i, session, sequence, 0, 1), &mut counter);
            assert_eq!(
                r.outcome,
                CounterOutcome::Value(((session - 1) * window + sequence) as i64)
            );
        }
    }
    assert_eq!(counter.retained_receipts(), limits.retained_receipts());
    assert_eq!(counter.retained_registrations(), 0);
    let image = counter.checkpoint(required.snapshot_bytes).unwrap();
    assert_eq!(image.len(), required.snapshot_bytes - 4 * 24);
    // One more outcome in any session is refused at admission.
    for session in 1..=sessions {
        let bytes = Counter::command(0, 1);
        assert_eq!(
            counter.validate_proposal(op(session, window + 1), &bytes, std::iter::empty()),
            Err(ApplicationError::SessionWindow)
        );
    }
    // Acknowledging one outcome admits exactly one more.
    let r = next(&|i| add(i, 2, window + 1, 1, 1), &mut counter);
    assert_eq!(
        r.outcome,
        CounterOutcome::Value((sessions * window + 1) as i64)
    );
    assert_eq!(counter.retained_receipts(), limits.retained_receipts());
    // A new registration evicts the least recently used session and its
    // window; the total never exceeds the bound.
    let r = next(&|i| register(i, 99), &mut counter);
    assert_eq!(r.outcome, CounterOutcome::Registered(sessions + 1));
    assert_eq!(
        counter.retained_receipts(),
        limits.retained_receipts() - window as usize
    );
    assert_eq!(counter.live_sessions(), sessions as usize);
    assert_eq!(counter.lookup_operation(op(1, 1)), CounterLookup::Expired);
    // A command outside its window committed anyway (admission bypassed) is
    // refused once and closes its session: no later copy executes.
    let value = counter.read_applied(index.get()).unwrap();
    let r = next(&|i| add(i, 3, window + 1, 0, 1), &mut counter);
    assert_eq!(r.outcome, CounterOutcome::Refused(SessionRefusal::Window));
    assert_eq!(
        counter.retained_receipts(),
        limits.retained_receipts() - 2 * window as usize
    );
    assert_eq!(counter.live_sessions(), sessions as usize - 1);
    let r = next(&|i| add(i, 3, window + 1, window, 1), &mut counter);
    assert_eq!(r.outcome, CounterOutcome::Refused(SessionRefusal::Expired));
    assert_eq!(counter.read_applied(index.get()), Ok(value));
}

#[test]
fn limits_are_validated_and_capped_at_one_million_retained_receipts() {
    use voteboat::retention::*;
    assert!(SessionLimits::new(0, 1, 1).is_err());
    assert!(SessionLimits::new(1, 0, 1).is_err());
    assert!(SessionLimits::new(1, 1, 0).is_err());
    assert!(SessionLimits::new(MAX_SESSIONS + 1, 1, 1).is_err());
    assert!(SessionLimits::new(1, MAX_WINDOW + 1, 1).is_err());
    assert!(SessionLimits::new(1, 1, MAX_REGISTRATIONS + 1).is_err());
    assert!(SessionLimits::new(1000, 1000, 1).is_ok());
    assert!(SessionLimits::new(1001, 1000, 1).is_err());
    assert!(Counter::new(0).is_err());
    assert!(Counter::new(1_000_001).is_err());
    let service = SessionLimits::default();
    assert_eq!(
        (
            service.sessions(),
            service.window(),
            service.registrations()
        ),
        (256, 32, 64)
    );
    // 24 + 28 + 256 × (28 + 32 × 33) + 64 × 24.
    assert_eq!(
        Counter::with_limits(service)
            .unwrap()
            .readiness_requirements()
            .snapshot_bytes,
        279_092
    );
}
