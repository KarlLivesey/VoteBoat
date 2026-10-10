# Slice162 — bounded recovery admission

Base `d36fc717a20782bfe434d29f27de2370c115a08d` plus this commit. Linux local
validation; public router policy changes, no persistent or wire format changes.

## Commands and results

```sh
cargo +stable test --locked --offline --all-features --test effect_owner
cargo +stable test --locked --offline --all-features --test learners
cargo +stable test --locked --offline --all-features --example native_benchmark
cargo +stable test --locked --offline --all-features --test connect
cargo +stable clippy --locked --offline --keep-going --all-targets --all-features -- -D warnings
cargo +stable clippy --locked --offline --keep-going --all-targets --no-default-features -- -D warnings
cargo +stable fmt --all -- --check
RUSTDOCFLAGS='-D warnings' cargo +stable doc --locked --offline --no-deps --all-features
node validation/check-inventory.mjs
```

Effect owner:136/136; learners:24/24; native benchmark:20/20. Connect results
are in connect.log. Both strict Clippy profiles return success with zero
diagnostics; formatting and warnings-denied API docs pass. Inventory metadata
passes89 records; this metadata check is not a consensus proof.

## Contract and exercised histories

`SnapshotRecoveryLimits` selects accepted job count and aggregate declared
image capacity before preparation. Existing constructors default to at most
four jobs and twice the individual image maximum. Readiness charges two image
allowances (loaded anchor and application checkpoint), other recovery one.
A job larger than the budget returns TooLarge; occupancy returns Overloaded.
Original leases survive rejection. Failed provider admission consumes no slot.
Credits survive completed-but-undelivered provider work and stale completions,
then release on exact delivery or explicit failed-owner discard. No cancellation
is treated as rollback of accepted I/O.

Five new host tests cover independent request/byte exhaustion, exact retry,
unchanged reservation before refusal, retained completion credit, stale tickets,
provider refusal, uncertain failure/fenced cleanup and foreground append plus
checkpoint publication/reconciliation while recovery is capped. Existing
shutdown tests still drain the required WAL and snapshot dependencies.

Native learner tests reject a one-image budget for readiness before extra
reservation or worker submission, then complete with exactly two allowances.
They retain stale-completion rejection, other-group progress and owner-close
cases. All24 learner tests pass.

Two new native TCP/QUIC histories force snapshot repair for eight stale groups
by compacting surviving replicas beyond the stopped follower's last indexes,
closing every transport and reopening files. An explicit one-job/128MiB quota
is observed on every replica. A zero-delta write is admitted while recovery is
active and returns an applied receipt; every group recovers its state. Existing
quorum reads and duplicate receipts pass, including another complete restart.
All20 tests in the native benchmark example pass, including original recovery
histories with default limits. This is admission/progress evidence for selected
histories, not a benchmark or latency bound. Blocking provider I/O remains
non-preemptible and the independent worker/owner limits still apply.

## Corrections and platform scope

The initial test helper incorrectly assumed the one-request host WAL could
accept multiple persists. It now retains only overloaded original leases and
retries after worker completion. Initial Clippy caught a103-line benchmark
constructor; optional quota selection moved to its existing Workers assembly.
No lint allowance or production retry weakening was added.

Previous commitd36fc71's macOS job114107977833 passed the previously corrected
fragmented-preface case, then failed the invalid-hint fixture after four
immediate polls (anonymous1 instead of0). The retained excerpt records that
failure. The fixture now waits up to two seconds for the actual socket close,
continuously checking that the authorized request stays pending and no TLS
handshake begins. Virtual time remains0, all three invalid hints remain, and
the original cancellation outcome stays checked. No production change.
Fresh macOS execution remains required; background Linux CI was still running
when inspected. The old full-suite process handles are absent; their retained
partial logs do not establish a complete run and are not counted here.

Full P0–P7 remains active. No physical power-loss, arbitrary failure, sustainable
throughput, fixed-p99 or complete-platform claim is made by this slice.
