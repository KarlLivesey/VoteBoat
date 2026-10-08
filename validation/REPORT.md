# Validation report — slice 35

The independent Rust model in `tests/ballot_model.rs` explores the complete
reachable closure within explicit finite bounds: seven configuration phases,
four candidate/store identities (including one reused NodeId), two local replica
identities, terms 0–2, and at most one restart. No search-depth cutoff is used;
a three-million-state resource cap fails the test if reached.

Actions separate accepted state, write completion, synchronization, exact
completion, durable crash recovery and recovery of a complete unsynchronized
tail. They include learner staging, two joint/final transitions, store replacement,
uncommitted rollback, committed-prefix compaction and higher-term observation.
Commit advancement is an external quorum oracle. Stale completion is ignored;
restart does not produce a vote reply.

The correct model explores **237,556 states and 665,097 edges** with no invariant
violation. It reaches all seven phases, historical origins ahead of a rolled-back
head and behind a compacted base, and a replacement-store ballot. Invariants check
prefix/term bounds, original candidate eligibility, retained per-term durable
promises, receipt durability/incarnation, and quiescent core/disk agreement.

Three negative controls produce counterexamples:

| Deliberate defect | States to counterexample | Observed violation |
| --- | ---: | --- |
| Erase vote when rollback removes candidate | 5,835 | Durable promise erased in its term after joint rollback |
| Reply at Write | 44 | Reply precedes recoverable promise |
| Compare candidate NodeId without store identity | 75,905 | Replacement store changes a durable same-term promise |

Run `cargo +stable test --locked --offline --no-default-features --test ballot_model -- --nocapture`
for exploration counts and exact counterexample traces. The implementation's host,
native, actual-file and actual-core tests are separate evidence; the model does
not import production helpers or establish their refinement automatically.

This finite local model is not a full distributed membership proof. Network
partitions, log matching/leader completeness, distributed quorum commitment,
unbounded terms/restarts, liveness and resource fairness remain outside it.
Online reconfiguration stays gated pending its distributed model and faulted
actual network histories. Linux local execution does not establish macOS or
hardware power-failure behavior. CI is background feedback, never a required
merge gate or a reason to stop local implementation.
