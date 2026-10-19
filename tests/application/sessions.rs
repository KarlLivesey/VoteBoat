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
//! Session semantics: registration, eviction, acknowledgement, expiry, lookup,
//! checkpoint at any point, and a differential check against an independent
//! model over seeded random histories.
use super::*;
use std::collections::BTreeMap;

fn limits(sessions: u32, window: u32, registrations: u32) -> SessionLimits {
    SessionLimits::new(sessions, window, registrations).unwrap()
}
fn outcomes(receipts: &[CounterReceipt]) -> Vec<(CounterOutcome, bool)> {
    receipts.iter().map(|r| (r.outcome, r.duplicate)).collect()
}

#[test]
fn registration_allocates_sessions_and_a_retried_registration_returns_the_same_one() {
    let mut counter = Counter::with_limits(limits(4, 2, 2)).unwrap();
    let receipts = counter
        .apply_batch(&[register(1, 77), register(2, 78), register(3, 77)])
        .unwrap();
    assert_eq!(
        outcomes(&receipts),
        [
            (CounterOutcome::Registered(2), false),
            (CounterOutcome::Registered(3), false),
            (CounterOutcome::Registered(2), true),
        ]
    );
    assert_eq!(
        counter.lookup_operation(OperationId::new(77).unwrap()),
        CounterLookup::Registered(2)
    );
    // The first command drops the nonce: a later retry of the registration
    // allocates a spare session with no business effect.
    counter.apply_batch(&[add(4, 2, 1, 0, 5)]).unwrap();
    assert_eq!(counter.retained_registrations(), 1);
    assert_eq!(
        counter.lookup_operation(OperationId::new(77).unwrap()),
        CounterLookup::Unknown
    );
    let spare = counter.apply_batch(&[register(5, 77)]).unwrap();
    assert_eq!(spare[0].outcome, CounterOutcome::Registered(4));
    assert_eq!(counter.read_applied(5), Ok(5));
    assert_eq!(counter.live_sessions(), 4);
}

#[test]
fn least_recently_used_session_is_evicted_at_registration_and_its_ids_expire() {
    let mut counter = Counter::with_limits(limits(3, 4, 4)).unwrap();
    counter
        .apply_batch(&[
            register(1, 1),      // session 2
            register(2, 2),      // session 3; default session 1 is least recent
            add(3, 2, 1, 0, 10), // session 2 used at 3
            add(4, 3, 1, 0, 20), // session 3 used at 4
        ])
        .unwrap();
    // A full table evicts the least recently used: the default session (index 0).
    let r = counter.apply_batch(&[register(5, 3)]).unwrap();
    assert_eq!(r[0].outcome, CounterOutcome::Registered(4));
    let late = counter.apply_batch(&[command(6, 1, 1)]).unwrap();
    assert_eq!(
        late[0].outcome,
        CounterOutcome::Refused(SessionRefusal::Expired)
    );
    // Session 2 (last used at 3) goes next, taking its retained receipt.
    counter
        .apply_batch(&[add(7, 3, 2, 1, 1), register(8, 4)])
        .unwrap();
    assert_eq!(counter.lookup_operation(op(2, 1)), CounterLookup::Expired);
    let retry = counter.apply_batch(&[add(9, 2, 1, 0, 10)]).unwrap();
    assert_eq!(
        retry[0].outcome,
        CounterOutcome::Refused(SessionRefusal::Expired)
    );
    assert!(!retry[0].duplicate);
    // Never executed twice: 10 + 20 + 1 only.
    assert_eq!(counter.read_applied(9), Ok(31));
    // A session that was never allocated is unknown, not expired.
    let unknown = counter.apply_batch(&[add(10, 9, 1, 0, 1)]).unwrap();
    assert_eq!(
        unknown[0].outcome,
        CounterOutcome::Refused(SessionRefusal::UnknownSession)
    );
    assert_eq!(counter.lookup_operation(op(9, 1)), CounterLookup::Unknown);
    assert_eq!(counter.read_applied(10), Ok(31));
    assert_eq!(counter.live_sessions(), 3);
}

#[test]
fn a_consumed_sequence_retried_late_is_expired_and_never_executes_again() {
    let mut counter = Counter::new(4).unwrap();
    counter
        .apply_batch(&[command(1, 1, 5), command(2, 2, 6)])
        .unwrap();
    assert_eq!(
        counter.lookup_operation(op(1, 1)),
        CounterLookup::Retained(CounterOutcome::Value(5))
    );
    // Sequence 3 acknowledges through 2: both receipts retire.
    counter.apply_batch(&[add(3, 1, 3, 2, 7)]).unwrap();
    assert_eq!(counter.retained_receipts(), 1);
    for (index, sequence, delta) in [(4, 1, 5), (5, 2, 6), (6, 2, 99)] {
        let r = counter
            .apply_batch(&[command(index, sequence, delta)])
            .unwrap();
        assert_eq!(
            r[0].outcome,
            CounterOutcome::Refused(SessionRefusal::Expired)
        );
        assert_eq!(
            counter.lookup_operation(op(1, sequence)),
            CounterLookup::Expired
        );
    }
    assert_eq!(counter.read_applied(6), Ok(18));
    // A retry inside the window still returns the original outcome.
    let r = counter.apply_batch(&[add(7, 1, 3, 1, 7)]).unwrap();
    assert_eq!(
        (r[0].outcome, r[0].duplicate),
        (CounterOutcome::Value(18), true)
    );
}

#[test]
fn registration_nonces_are_bounded_oldest_first() {
    let mut counter = Counter::with_limits(limits(8, 2, 2)).unwrap();
    let r = counter
        .apply_batch(&[register(1, 10), register(2, 11), register(3, 12)])
        .unwrap();
    assert_eq!(r[2].outcome, CounterOutcome::Registered(4));
    assert_eq!(counter.retained_registrations(), 2);
    // Nonce 10 was dropped (oldest); its session stays live and usable.
    assert_eq!(
        counter.lookup_operation(OperationId::new(10).unwrap()),
        CounterLookup::Unknown
    );
    let r = counter.apply_batch(&[add(4, 2, 1, 0, 3)]).unwrap();
    assert_eq!(r[0].outcome, CounterOutcome::Value(3));
    // Its retried registration only allocates a spare session.
    let r = counter.apply_batch(&[register(5, 10)]).unwrap();
    assert_eq!(r[0].outcome, CounterOutcome::Registered(5));
    assert_eq!(counter.read_applied(5), Ok(3));
}

#[test]
fn session_exhaustion_is_a_typed_refusal_and_commands_keep_working() {
    let counter = Counter::new(2).unwrap();
    let mut image = counter.checkpoint(1024).unwrap();
    // Next session u64::MAX: 2^64 registrations at 10^6/s take 584,000 years.
    image[36..44].copy_from_slice(&u64::MAX.to_le_bytes());
    let mut exhausted = Counter::new(2).unwrap();
    exhausted
        .restore_checkpoint(COUNTER_SCHEMA, 0, &image)
        .unwrap();
    let r = exhausted
        .apply_batch(&[register(1, 5), command(2, 1, 4)])
        .unwrap();
    assert_eq!(
        r[0].outcome,
        CounterOutcome::Refused(SessionRefusal::Exhausted)
    );
    assert_eq!(r[1].outcome, CounterOutcome::Value(4));
    let unknown = exhausted.apply_batch(&[add(3, u64::MAX, 1, 0, 1)]).unwrap();
    assert_eq!(
        unknown[0].outcome,
        CounterOutcome::Refused(SessionRefusal::UnknownSession)
    );
}

/// A history exercising every transition: registration, duplicates,
/// conflicts, overflow, window refusals, acknowledgements, eviction, expiry
/// and unknown sessions.
fn representative() -> Vec<LogEntry> {
    let mut entries = Vec::new();
    let mut index = 0;
    let mut push = |make: &dyn Fn(u64) -> LogEntry| {
        index += 1;
        entries.push(make(index));
    };
    push(&|i| register(i, 100));
    push(&|i| command(i, 1, i64::MAX));
    push(&|i| command(i, 2, 1));
    push(&|i| add(i, 2, 1, 0, 3));
    push(&|i| register(i, 100));
    push(&|i| add(i, 2, 2, 0, -4));
    push(&|i| add(i, 2, 3, 0, 5));
    push(&|i| add(i, 2, 2, 1, -4));
    push(&|i| add(i, 2, 2, 1, 9));
    push(&|i| noop(i));
    push(&|i| register(i, 101));
    push(&|i| add(i, 3, 1, 0, 2));
    push(&|i| add(i, 2, 4, 3, 1));
    push(&|i| add(i, 2, 1, 0, 3));
    push(&|i| register(i, 102));
    push(&|i| command(i, 1, i64::MAX));
    push(&|i| add(i, 3, 2, 1, 2));
    push(&|i| add(i, 9, 1, 0, 2));
    push(&|i| register(i, 103));
    push(&|i| add(i, 3, 3, 2, 1));
    push(&|i| add(i, 4, 1, 0, 1));
    entries
}

#[test]
fn checkpoint_between_every_apply_restores_to_the_replayed_state() {
    let entries = representative();
    let limits = limits(3, 3, 2);
    let mut replayed = Counter::with_limits(limits).unwrap();
    let mut receipts = Vec::new();
    for entry in &entries {
        receipts.extend(replayed.apply_batch(std::slice::from_ref(entry)).unwrap());
    }
    // Every outcome kind appears.
    let kinds: std::collections::HashSet<_> = receipts
        .iter()
        .map(|r| std::mem::discriminant(&r.outcome))
        .collect();
    assert_eq!(kinds.len(), 5);
    let bound = replayed.readiness_requirements().snapshot_bytes;
    for split in 0..=entries.len() {
        let mut prefix = Counter::with_limits(limits).unwrap();
        prefix.apply_batch(&entries[..split]).unwrap();
        let image = prefix.checkpoint(bound).unwrap();
        let mut restored = Counter::with_limits(limits).unwrap();
        restored
            .restore_checkpoint(COUNTER_SCHEMA, split as u64, &image)
            .unwrap();
        assert_eq!(restored.checkpoint(bound).unwrap(), image);
        let tail = restored.apply_batch(&entries[split..]).unwrap();
        assert_eq!(
            tail,
            receipts[receipts.len() - tail.len()..],
            "split {split}"
        );
        assert_eq!(
            restored.checkpoint(bound).unwrap(),
            replayed.checkpoint(bound).unwrap()
        );
    }
}

/// (floor, last_index, nonce, sequence → (delta, outcome))
type ModelSession = (u64, u64, Option<u64>, BTreeMap<u64, (i64, CounterOutcome)>);

/// Independent model of the session rule, written without the ledger.
struct Model {
    sessions: usize,
    window: u64,
    registrations: usize,
    value: i64,
    next: u64,
    live: BTreeMap<u64, ModelSession>,
    /// nonce → (session, index)
    nonces: BTreeMap<u64, (u64, u64)>,
}
impl Model {
    fn new(sessions: usize, window: u64, registrations: usize) -> Self {
        Self {
            sessions,
            window,
            registrations,
            value: 0,
            next: 2,
            live: BTreeMap::from([(1, (0, 0, None, BTreeMap::new()))]),
            nonces: BTreeMap::new(),
        }
    }
    fn register(&mut self, index: u64, nonce: u64) -> (CounterOutcome, bool) {
        if let Some((session, _)) = self.nonces.get(&nonce) {
            return (CounterOutcome::Registered(*session), true);
        }
        if self.live.len() == self.sessions {
            let mut by_age: Vec<_> = self.live.iter().map(|(s, v)| (v.1, *s)).collect();
            by_age.sort();
            let victim = by_age[0].1;
            if let Some(nonce) = self.live.remove(&victim).unwrap().2 {
                self.nonces.remove(&nonce);
            }
        }
        if self.nonces.len() == self.registrations {
            let mut by_age: Vec<_> = self.nonces.iter().map(|(n, (_, i))| (*i, *n)).collect();
            by_age.sort();
            let (_, oldest) = by_age[0];
            let (session, _) = self.nonces.remove(&oldest).unwrap();
            self.live.get_mut(&session).unwrap().2 = None;
        }
        let session = self.next;
        self.next += 1;
        self.live
            .insert(session, (0, index, Some(nonce), BTreeMap::new()));
        self.nonces.insert(nonce, (session, index));
        (CounterOutcome::Registered(session), false)
    }
    fn add(
        &mut self,
        index: u64,
        session: u64,
        sequence: u64,
        ack: u64,
        delta: i64,
    ) -> (CounterOutcome, bool) {
        let refused = |r| (CounterOutcome::Refused(r), false);
        let Some((floor, last, nonce, kept)) = self.live.get_mut(&session) else {
            return refused(if session >= self.next {
                SessionRefusal::UnknownSession
            } else {
                SessionRefusal::Expired
            });
        };
        if sequence <= *floor {
            return refused(SessionRefusal::Expired);
        }
        if let Some((original, outcome)) = kept.get(&sequence) {
            return if *original == delta {
                (*outcome, true)
            } else {
                (CounterOutcome::OperationConflict, true)
            };
        }
        let new_floor = (*floor).max(ack);
        if sequence > new_floor + self.window {
            return refused(SessionRefusal::Window);
        }
        let outcome = match self.value.checked_add(delta) {
            Some(v) => {
                self.value = v;
                CounterOutcome::Value(v)
            }
            None => CounterOutcome::Overflow,
        };
        kept.insert(sequence, (delta, outcome));
        kept.retain(|q, _| *q > new_floor);
        *floor = new_floor;
        *last = index;
        if let Some(n) = nonce.take() {
            self.nonces.remove(&n);
        }
        (outcome, false)
    }
    fn admit(&self, session: u64, sequence: u64, ack: u64) -> Result<usize, ApplicationError> {
        match self.live.get(&session) {
            Some((floor, ..)) if sequence > (*floor).max(ack) + self.window => {
                Err(ApplicationError::SessionWindow)
            }
            _ => Ok(std::mem::size_of::<CounterReceipt>()),
        }
    }
    fn retained(&self) -> usize {
        self.live.values().map(|v| v.3.len()).sum()
    }
}

struct Random(u64);
impl Random {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

#[test]
fn seeded_histories_match_an_independent_model_through_checkpoints() {
    let (sessions, window, registrations) = (3, 4, 2);
    let limits = limits(sessions, window, registrations);
    let bound = Counter::with_limits(limits)
        .unwrap()
        .readiness_requirements()
        .snapshot_bytes;
    for seed in 1..=300u64 {
        let mut random = Random(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15));
        let mut counter = Counter::with_limits(limits).unwrap();
        let mut model = Model::new(sessions as usize, u64::from(window), registrations as usize);
        for index in 1..=200u64 {
            let (entry, expected) = if random.below(6) == 0 {
                let nonce = 1 + random.below(5);
                (register(index, nonce), model.register(index, nonce))
            } else {
                let session = 1 + random.below(model.next + 1);
                let sequence = 1 + random.below(12);
                let ack = random.below(sequence);
                let delta = if random.below(40) == 0 {
                    i64::MAX
                } else {
                    random.below(7) as i64 - 3
                };
                assert_eq!(
                    counter.validate_proposal(
                        op(session, sequence),
                        &Counter::command(ack, delta),
                        std::iter::empty()
                    ),
                    model.admit(session, sequence, ack),
                    "seed {seed} index {index}"
                );
                (
                    add(index, session, sequence, ack, delta),
                    model.add(index, session, sequence, ack, delta),
                )
            };
            let receipts = counter.apply_batch(std::slice::from_ref(&entry)).unwrap();
            assert_eq!(
                (receipts[0].outcome, receipts[0].duplicate),
                expected,
                "seed {seed} index {index}"
            );
            assert_eq!(counter.read_applied(index), Ok(model.value));
            assert_eq!(counter.retained_receipts(), model.retained());
            assert!(counter.retained_receipts() <= limits.retained_receipts());
            assert!(counter.live_sessions() <= limits.sessions());
            if random.below(10) == 0 {
                let image = counter.checkpoint(bound).unwrap();
                let mut restored = Counter::with_limits(limits).unwrap();
                restored
                    .restore_checkpoint(COUNTER_SCHEMA, index, &image)
                    .unwrap();
                counter = restored;
            }
        }
    }
}
