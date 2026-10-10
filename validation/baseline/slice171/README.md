# Slice171 — recorded creation decision races

Base: `286c2c00a6349412853d7a779dd04e573117d23b`, plus this commit.

Two native integration tests each execute eight explicitly recorded schedules:
first decision is publication or cancellation; its owner is aborted either
before any poll or after two voters apply it with the client receipt unread;
the recovered winner is checked with WAL recovery or another checkpoint/reopen.
The matrix runs on TCP/TLS and QUIC. No production protocol or format changes.

Each schedule reopens actual native logs/snapshots, prefers a different freshest
voter for the new campaign, and resolves with the original operation identity.
The unpolled operation must be absent from all logs. Quorum-applied outcomes
must survive, and the exact retry must be a duplicate. The opposite command is
rejected and the parent manifest/control reservation remain consistent. Canceled
targets reopen non-serving; published targets activate from recovered metadata,
serve while metadata is stopped, and recover original data retries.

## Checks

```sh
cargo +stable test --locked --offline --all-features --test routed namespace_race -- --nocapture
cargo +stable test --locked --offline --all-features --test routed native::creation -- --nocapture
cargo +stable clippy --locked --offline --keep-going --all-targets --all-features -- -D warnings
cargo +stable clippy --locked --offline --keep-going --all-targets --no-default-features -- -D warnings
cargo +stable fmt --all -- --check
RUSTDOCFLAGS='-D warnings' cargo +stable doc --locked --offline --all-features --no-deps
node validation/check-inventory.mjs
```

`namespace-race-final.log` passes both test entry points and all16 schedules in
12.98s. Both strict Clippy profiles, formatting, warning-denied documentation and
92-record inventory validation pass. Source hashes identify the tested changes.
The final matrix with exact original status comparisons also passes all16
schedules in12.68s (`namespace-race-exact-status.log`).
The complete14-test native creation/created-source regression selection passes
in210.16s (`creation-regressions.log`), including the existing TCP/QUIC split
phase, unread-receipt, WAL-only and checkpoint recovery histories.

## Failures retained and limits

`namespace-race-first.log` records initial test-only visibility and borrow errors.
`namespace-race-second.log` records a failed QUIC pre-activation read while the
TCP matrix passed. `quic-race-trace.log` reproduces the same schedule and points
to `Service::activate`. The one-shot read converted any of its three explicit
retryable authority refusals into failure. That fixture call now uses the existing
four-attempt helper, which retries only NotLeader, ReadNotReady or
LeadershipChanged and still requires a fresh quorum-backed NotActive result.
No consensus behavior or success assertion was weakened. Canceled targets now
also reopen after the metadata decision before their negative service checks.

These are selected process-owner cuts and real native-file recovery, not physical
power loss or exhaustive message schedules. Written-but-not-quorum-applied cuts,
concurrent membership changes, disk corruption and wider recursive transitions
remain outside this matrix. The schema16 byte-cut model evidence is separately
retained under slice169; this run does not relabel that model as a physical test.

Prior ecaa2ab CI run38019357736 was cancelled. Current base286c2c0 CI run38019594984
had passed formatting/strict lint with Linux and macOS tests still running at
inspection. Neither is a new platform-success claim. The earlier macOS shutdown
failure remains unverified; the full P0–P7 goal stays active.
