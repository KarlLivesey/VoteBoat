import assert from 'node:assert/strict';

// Independent ordinal model, not native provider code or a byte/storage proof.
// A valid old/new record survives whole; an interrupted overwrite may be invalid.
// CRC-valid arbitrary forgery/replay is outside the current CRC format's guarantee.
// Data units represent complete valid transition frames; partial final frames
// recover the preceding unit. Corruption at/below the mandatory boundary errors.
const counts = { ordinary_crash: 0, corruption_truncation: 0, restart_repair: 0, mismatch: 0 };
const witness = (n, session = 1, overrides = {}) => ({
  counter: n, length: n, session, identity: 1, generation: 0, ...overrides,
});
const equal = (a, b) => JSON.stringify(a) === JSON.stringify(b);
function select(a, b) {
  for (const r of [a, b]) {
    if (r && (r.identity !== 1 || r.generation !== 0 || r.counter <= 0
      || r.session <= 0 || r.length < 0)) throw Error('identity/generation/record');
  }
  if (!a && !b) throw Error('both invalid');
  if (!a || !b) return a ?? b;
  if (a.counter === b.counter) {
    if (!equal(a, b)) throw Error('same counter, contradictory record');
    return b;
  }
  // Several interrupted recoveries can widen the candidate-counter gap.
  if (a.counter < b.counter || a.length < b.length || a.session < b.session
    || a.session > b.session + 1) throw Error('unreachable ordered pair');
  return b; // A ahead is NOT an acknowledged boundary.
}
function recover(a, b, completeFrames) {
  const required = select(a, b);
  if (completeFrames < required.length) throw Error('acknowledged data missing');
  return { required, recovered: completeFrames };
}
function succeeds(a, b, data) {
  try { return recover(a, b, data); } catch { return null; }
}
for (let old = 1; old <= 4; old++) {
  const before = witness(old);
  const candidate = witness(old + 1);
  // First write/sync can persist primary header and appended data independently.
  // Prior second witness is untouched, and preserves the acknowledged boundary.
  for (const a of [before, candidate, null]) {
    for (const tail of ['absent', 'partial', 'complete']) {
      const data = tail === 'complete' ? old + 1 : old;
      const out = recover(a, before, data);
      assert(out.required.length >= old && out.recovered >= old);
      counts.ordinary_crash++;
    }
  }
  // Second overwrite starts only after first sync succeeds: primary+data durable.
  for (const b of [before, candidate, null]) {
    const out = recover(candidate, b, old + 1);
    assert(out.required.length >= old && out.recovered === old + 1);
    counts.ordinary_crash++;
  }
  // After success BOTH witnesses hold the same acknowledged boundary. Compose
  // arbitrary WAL truncation with independent invalidation of either/both records.
  for (const a of [candidate, null]) for (const b of [candidate, null]) {
    for (let data = 0; data <= old + 1; data++) {
      const out = succeeds(a, b, data);
      if (data < old + 1 || (!a && !b)) assert.equal(out, null);
      else assert.equal(out.recovered, old + 1);
      counts.corruption_truncation++;
    }
  }
  // A candidate can coexist with B after an interrupted recovery publication.
  // Invalid B plus missing candidate data fails closed; do not retreat to B/old.
  for (let data = 0; data <= old + 1; data++) {
    const out = succeeds(candidate, null, data);
    if (data < old + 1) assert.equal(out, null);
    counts.corruption_truncation++;
  }
  // Cold repair must copy the sole survivor into the damaged record BEFORE
  // overwriting the survivor for the next publication. Every repair cut keeps it.
  for (const sole of ['primary', 'secondary']) {
    for (const repaired of [null, before]) {
      const a = sole === 'primary' ? before : repaired;
      const b = sole === 'secondary' ? before : repaired;
      assert.equal(recover(a, b, old).required.length, old);
      counts.restart_repair++;
    }
    // After repair both equal, any number of interrupted first phases is safe.
    for (let attempt = 1; attempt <= 5; attempt++) {
      const next = witness(old + attempt, 2, { length: old + 1 });
      for (const a of [before, next, null]) {
        assert.equal(recover(a, before, old).required.length, old);
        counts.restart_repair++;
      }
    }
  }
}

const old = witness(2), next = witness(3);
// Counterexample: max(A,B) makes a legitimate first-sync power cut falsely fail.
assert.equal(recover(next, old, 2).recovered, 2);
assert.throws(() => {
  const unsafeRequired = next.length;
  if (2 < unsafeRequired) throw Error('unsafe max falsely faults ordinary crash');
});
// Counterexample: historical marker fallback silently loses acknowledged N=3.
const unsafeHistoricalRecovery = 2; // WAL truncated to prior valid marker, B bad.
assert(unsafeHistoricalRecovery < 3);
assert.equal(succeeds(next, null, 2), null); // Fixed primary N rejects this loss.
// Counterexample: overwriting the sole survivor without repair can tear both.
assert.equal(succeeds(null, null, 2), null);

for (const [a, b] of [
  [old, next],
  [next, witness(3, 2)],
  [witness(3, 1, { identity: 2 }), old],
  [next, witness(2, 1, { identity: 2 })],
  [witness(3, 1, { generation: 1 }), old],
  [witness(3, 0), old],
  [witness(3, 3), old],
  [witness(3, 1, { length: 1 }), old],
]) {
  assert.throws(() => select(a, b));
  counts.mismatch++;
}
assert.equal(select(witness(10, 2, { length: 3 }), old), old);
console.log(JSON.stringify({
  status: 'PASS', counts,
  counterexamples: ['max-primary-secondary', 'historical-marker-fallback', 'overwrite-sole-survivor'],
  scope: 'Finite ordinal model only. Does not model byte CRC framing, actual synchronization, failures inside directory publication, initialization, reclamation, counters at u64 exhaustion, codec transition validity, runtime tickets, or actual power loss.',
}, null, 2));
