# Fresh read authority in fault schedules

Extend the actual-core/native WAL-model driver with checked read IDs, cancellation
and Counter reads through the public read_at_barrier contract. Serve only actual
issued barriers; missing barriers produce a harness NoBarrier outcome, without
inventing a core error or value. Replay retains canonical barrier identity and
actual outcomes while excluding opaque recovery sessions. Requests and tokens
remain bounded by the existing512-action limit.

Four new checks cover:

- Fresh quorum and one-use value9. An isolated cached leader cannot issue a new
  barrier while another quorum advances the Counter to12; the current leader's
  fresh read returns12.
- Pending and ready cancellation. Old replies do not satisfy another request,
  ready barriers cannot be reused after cancellation, and request IDs remain
  monotonic within the original core lifetime.
- An unapplied Counter returns NotApplied without consuming authority; its
  subsequent applied read succeeds. Process restart, re-election and actual
  failed-sync fencing reject old issued tokens.
- A [27-action schedule](composed.vbsim) combines read, cancellation, partition,
  process crash and stale-token attempts, replaying identical canonical barriers
  and outcomes. Focused single-test captures preserve exact schedule bytes.

| Scope | Linux passed/failed | macOS arm64 passed/failed |
| --- | ---: | ---: |
| Replay/codec/reducer checks |21/0|21/0|
| Focused composed-schedule capture |1/0|1/0|
| Complete Raft target, all features |62/0|62/0|
| Complete Raft target, no default features |29/0|29/0|
| Complete Raft target, native only |62/0|62/0|
| Complete Raft target, default |62/0|62/0|

Formatting and four strict Clippy profiles have zero diagnostics on both hosts.
All661 Rust inputs verify before/after the full-target runs. Profiles execute
sequentially in independent checkouts; live lifecycle sources remain immutable.
The grammar parser separates unary and structured cases without weakening bounds
or malformed-input rejection. No production, dependency, protocol or public-seam
changes. Linux stable1.98.1 and Mac stable1.98.0 retain their existing conditions.

This is finite ordinary-majority device-model evidence using the actual native
store/core and public Counter. It does not certify physical disk faults, general
worker composition, snapshots, timers, reference-Raft comparison or full-platform
acceptance. Raw.sha256 hashes captured bytes; published.sha256 hashes normalized
paths and text without trailing whitespace.
