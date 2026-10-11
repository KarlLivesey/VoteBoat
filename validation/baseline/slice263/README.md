# Actual-core fault replay and reduction

The existing Raft test cluster now accepts a bounded, versioned text schedule
that preserves proposal IDs/bytes and explicit delivery, partition, election,
heartbeat and power-loss choices. Thirty-two 256-action schedules reproduce
the same logical logs, roles, applied prefixes and queue counts on fresh native
WAL model instances. Malformed and oversized schedules are rejected.

A deliberately defective test-only JournalIo reports sync success without
syncing. The real native WAL rejects its power-loss recovery with the exact
missing-durable-prefix error. Reduction retains that failure stage, node2 and
original operation7001; the saved [six-action schedule](minimized.vbsim) still
fails, and removing any one action loses that exact target. Removing the faulty
sync behavior permits recovery. Exhausted budgets never claim minimality.

| Scope | Linux passed/failed | macOS arm64 passed/failed |
| --- | ---: | ---: |
| New replay/codec/reducer checks |5/0|5/0|
| Complete Raft target, all features |46/0|46/0|
| Complete Raft target, no default features |29/0|29/0|
| Complete Raft target, native only |46/0|46/0|
| Complete Raft target, default |46/0|46/0|

Formatting and default/all/core/native strict Clippy have zero diagnostics on
both hosts. Feature profiles ran sequentially. All657 Rust build inputs verify
at terminal on both hosts and before Mac execution. Source differs fromd6adc44
only in the Raft test module and three new private test files; no production,
dependency, public contract or protocol changes. Linux stable1.98.1 and Mac
stable1.98.0 retain their existing conditions. Live lifecycle runs use separate
immutable checkouts.

The initial3/2 result is preserved as an unhandled recovery rejection during
harness setup. The correction recognizes only the precise existing native
recovery guard, with a distinct tag from recovered-but-short prefixes. Other
storage errors and core assertions remain loud failures, never reducer targets.

This is synchronous-persistence, ordinary-majority, in-memory WAL-model evidence.
It does not cover reordered persistence completions, snapshots, timer runtime,
reference-Raft comparison, arbitrary compositions or general linearizability.
Opaque new store sessions are recreated; comparisons concern logical states.
Raw.sha256 hashes captured input bytes; published.sha256 hashes text normalized
for checkout paths and trailing whitespace.
