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
//! Steady-state soak: 1,000,000 counter writes from 1,000 rotating clients
//! under the counter service limits (256 sessions × 32 outcomes, 64 nonces),
//! with lost replies, retries, acknowledgement lag and session eviction.
//! Retention stays within `sessions × window` at every checkpoint, each
//! checkpoint restores into the instance that continues, and an oracle checks
//! that no operation executes twice and every duplicate returns the original.
//!
//! On the base (`Counter::new(10000)`, the old service table) the same stream
//! of distinct writes is refused from write 10,001 with `DedupCapacity`.
use super::*;
use std::collections::{BTreeMap, BTreeSet, HashSet};

struct Random(u64);
impl Random {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn chance(&mut self, percent: u64) -> bool {
        self.next() % 100 < percent
    }
}

#[derive(Default)]
struct Client {
    session: Option<u64>,
    registering: Option<u64>,
    registrations: u64,
    next: u64,
    /// Sent, reply not yet received: sequence → delta.
    outstanding: BTreeMap<u64, i64>,
    /// Replies received but not yet consumed by the client.
    received: BTreeSet<u64>,
    consumed: BTreeSet<u64>,
    /// Contiguous consumed prefix, sent as `consumed_through`.
    prefix: u64,
}
impl Client {
    fn consume(&mut self) {
        self.consumed.append(&mut self.received);
        while self.consumed.remove(&(self.prefix + 1)) {
            self.prefix += 1;
        }
    }
    /// Expired above our own acknowledgement means the session was evicted:
    /// register again and drop unacknowledged outcomes (they may have executed
    /// once; they never execute again).
    fn reset(&mut self) {
        *self = Self {
            registrations: self.registrations,
            ..Self::default()
        };
    }
}

#[derive(Debug, Default, Eq, PartialEq)]
struct Stats {
    writes: u64,
    entries: u64,
    registrations: u64,
    evicted_sessions: u64,
    duplicates: u64,
    expired: u64,
    window: u64,
    lost_replies: u64,
    checkpoints: u64,
    max_retained: usize,
    max_checkpoint_bytes: usize,
    value: i64,
}

struct Soak {
    limits: SessionLimits,
    counter: Counter,
    random: Random,
    clients: Vec<Client>,
    owner: BTreeMap<u64, usize>,
    nonces: BTreeMap<u64, usize>,
    executed: HashSet<(u64, u64)>,
    originals: BTreeMap<(u64, u64), CounterOutcome>,
    received: Vec<(u64, u64)>,
    value: i64,
    index: u64,
    stats: Stats,
}

impl Soak {
    fn new(seed: u64, clients: usize) -> Self {
        let limits = SessionLimits::default();
        Self {
            limits,
            counter: Counter::with_limits(limits).unwrap(),
            random: Random(seed),
            clients: (0..clients).map(|_| Client::default()).collect(),
            owner: BTreeMap::new(),
            nonces: BTreeMap::new(),
            executed: HashSet::new(),
            originals: BTreeMap::new(),
            received: Vec::new(),
            value: 0,
            index: 0,
            stats: Stats::default(),
        }
    }

    /// One proposal from client `c`, or `None` if it must first consume.
    fn propose(&mut self, c: usize) -> Option<(OperationId, Vec<u8>)> {
        let client = &mut self.clients[c];
        if self.random.chance(50) {
            client.consume();
        }
        let Some(session) = client.session else {
            let nonce = match client.registering {
                Some(nonce) => nonce,
                None => {
                    client.registrations += 1;
                    let nonce = c as u64 * 1_000_000 + client.registrations;
                    client.registering = Some(nonce);
                    self.nonces.insert(nonce, c);
                    nonce
                }
            };
            return Some((OperationId::new(u128::from(nonce)).unwrap(), Vec::new()));
        };
        let retry = client
            .outstanding
            .iter()
            .next()
            .filter(|_| self.random.chance(30))
            .map(|(q, d)| (*q, *d));
        let (sequence, delta) = retry.unwrap_or_else(|| {
            client.next += 1;
            let delta = (self.random.next() % 9 + 1) as i64;
            client.outstanding.insert(client.next, delta);
            (client.next, delta)
        });
        let operation = op(session, sequence);
        let bytes = Counter::command(client.prefix, delta);
        match self
            .counter
            .validate_proposal(operation, &bytes, std::iter::empty())
        {
            Ok(_) => Some((operation, bytes)),
            Err(ApplicationError::SessionWindow) => {
                // Backpressure: the client consumes before it may continue.
                self.stats.window += 1;
                self.clients[c].consume();
                None
            }
            Err(other) => panic!("unexpected admission refusal {other:?}"),
        }
    }

    fn deliver(&mut self, receipt: &CounterReceipt, bytes: &[u8]) {
        let id = receipt.operation.get();
        let (session, sequence) = ((id >> 64) as u64, id as u64);
        let lost = self.random.chance(10);
        self.stats.lost_replies += u64::from(lost);
        match receipt.outcome {
            CounterOutcome::Registered(allocated) => {
                if !receipt.duplicate {
                    self.stats.registrations += 1;
                }
                // A late duplicate reply after the client adopted a session.
                let Some(&c) = self.nonces.get(&sequence) else {
                    return;
                };
                let client = &mut self.clients[c];
                if !lost && client.registering == Some(sequence) {
                    client.registering = None;
                    client.session = Some(allocated);
                    self.nonces.remove(&sequence);
                    self.owner.insert(allocated, c);
                }
            }
            CounterOutcome::Value(value) => {
                let delta = i64::from_le_bytes(bytes[8..].try_into().unwrap());
                if receipt.duplicate {
                    self.stats.duplicates += 1;
                    assert_eq!(
                        self.originals.get(&(session, sequence)),
                        Some(&receipt.outcome)
                    );
                } else {
                    assert!(self.executed.insert((session, sequence)), "executed twice");
                    self.value += delta;
                    assert_eq!(value, self.value);
                    self.originals.insert((session, sequence), receipt.outcome);
                    self.stats.writes += 1;
                }
                let c = self.owner[&session];
                let client = &mut self.clients[c];
                if !lost
                    && client.session == Some(session)
                    && client.outstanding.remove(&sequence).is_some()
                {
                    client.received.insert(sequence);
                    // A retry sent in the same batch may still follow.
                    self.received.push((session, sequence));
                }
            }
            CounterOutcome::Refused(SessionRefusal::Expired) => {
                self.stats.expired += 1;
                self.originals.remove(&(session, sequence));
                let c = self.owner[&session];
                let client = &mut self.clients[c];
                if !lost && client.session == Some(session) && sequence > client.prefix {
                    client.reset();
                }
            }
            CounterOutcome::Refused(SessionRefusal::Window) => self.stats.window += 1,
            other => panic!("unexpected outcome {other:?} for {session}:{sequence}"),
        }
    }

    fn checkpoint(&mut self) {
        let required = self.counter.readiness_requirements();
        let image = self.counter.checkpoint(required.snapshot_bytes).unwrap();
        assert!(self.counter.retained_receipts() <= self.limits.retained_receipts());
        assert!(self.counter.live_sessions() <= self.limits.sessions());
        assert!(self.counter.retained_registrations() <= self.limits.registrations());
        let mut restored = Counter::with_limits(self.limits).unwrap();
        restored
            .restore_checkpoint(COUNTER_SCHEMA, self.index, &image)
            .unwrap();
        assert_eq!(restored.checkpoint(image.len()).unwrap(), image);
        self.counter = restored;
        self.stats.checkpoints += 1;
        self.stats.max_checkpoint_bytes = self.stats.max_checkpoint_bytes.max(image.len());
    }

    fn run(mut self, writes: u64) -> Stats {
        let clients = self.clients.len() as u64;
        let mut next_checkpoint = 10_000;
        let mut step = 0u64;
        while self.stats.writes < writes {
            let mut batch = Vec::new();
            while batch.len() < 64 {
                step += 1;
                // A working set of 300 clients that rotates through all of them.
                let c = ((step / 64 + self.random.next() % 300) % clients) as usize;
                if let Some((operation, bytes)) = self.propose(c) {
                    self.index += 1;
                    batch.push(entry(self.index, operation, bytes));
                }
            }
            let before = self.counter.live_sessions();
            let receipts = self.counter.apply_batch(&batch).unwrap();
            self.stats.entries += batch.len() as u64;
            for (receipt, entry) in receipts.iter().zip(&batch) {
                let EntryPayload::Command { bytes, .. } = &entry.payload else {
                    unreachable!()
                };
                self.deliver(receipt, bytes);
            }
            for key in self.received.drain(..) {
                self.originals.remove(&key);
            }
            let registered = receipts
                .iter()
                .filter(|r| matches!(r.outcome, CounterOutcome::Registered(_)) && !r.duplicate)
                .count();
            self.stats.evicted_sessions +=
                (before + registered).saturating_sub(self.counter.live_sessions()) as u64;
            self.stats.max_retained = self
                .stats
                .max_retained
                .max(self.counter.retained_receipts());
            assert_eq!(self.counter.read_applied(self.index), Ok(self.value));
            if self.stats.writes >= next_checkpoint {
                next_checkpoint += 10_000;
                self.checkpoint();
            }
        }
        self.stats.value = self.value;
        self.stats
    }
}

#[test]
fn one_million_writes_from_rotating_clients_keep_retention_within_the_bound() {
    let stats = Soak::new(0x5eed_d1e5, 1000).run(1_000_000);
    eprintln!("{stats:?}");
    let limits = SessionLimits::default();
    assert!(stats.writes >= 1_000_000);
    assert!(stats.max_retained <= limits.retained_receipts());
    assert!(
        stats.max_checkpoint_bytes
            <= Counter::with_limits(limits)
                .unwrap()
                .readiness_requirements()
                .snapshot_bytes
    );
    assert!(stats.checkpoints >= 100);
    // The run exercised eviction, expiry, duplicates, backpressure and loss.
    assert!(stats.evicted_sessions > 1000);
    assert!(stats.expired > 0 && stats.duplicates > 0 && stats.window > 0);
    assert!(stats.lost_replies > 0);
}

#[test]
fn soak_traces_are_identical_on_two_runs() {
    let first = Soak::new(77, 1000).run(20_000);
    let second = Soak::new(77, 1000).run(20_000);
    assert_eq!(first, second);
    assert!(first.evicted_sessions > 0);
}
