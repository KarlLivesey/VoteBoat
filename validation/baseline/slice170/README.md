# Slice170 — service snapshot refusal and explicit retry evidence

Base: `ecaa2ab5331b5ffa7ff96d2e8ba30fa6e6a3f8fc`, plus this commit.

The core emits a zero-index SnapshotAck when a snapshot's term is older than its
durable term. The native codec incorrectly rejected it and terminated a QUIC
service during automatic checkpoint/reclaim. This slice accepts that existing
refusal in formats1–7; no framing, persistent state or quorum semantics change.
The real-core regression checks the same-term zero reply remains invalid and a
higher-term reply only causes durable term advancement, not replication credit.

The local CLI retries exact read-not-ready refusals under its original deadline.
Unknown writes remain caller-visible. Service test callers explicitly retry the
same write/configuration identity after LeadershipChanged. Failed shutdowns and
checkpoint deadlines now expose the actual last-start child log in CI output.

## Executed checks

```sh
cargo +stable test --locked --offline --all-features --test raft --test snapshot --test wire -- --nocapture
cargo +stable test --locked --offline --all-features --test counter_service -- --nocapture
cargo +stable test --locked --offline --no-default-features --test raft --test snapshot --test wire -- --nocapture
cargo +stable test --locked --offline --all-features --test snapshot delayed_snapshot_refusal -- --nocapture
cargo +stable clippy --locked --offline --keep-going --all-targets --all-features -- -D warnings
cargo +stable clippy --locked --offline --keep-going --all-targets --no-default-features -- -D warnings
cargo +stable fmt --all -- --check
RUSTDOCFLAGS='-D warnings' cargo +stable doc --locked --offline --all-features --no-deps
node validation/check-inventory.mjs
```

All-feature core targets pass22 Raft,22 snapshot and14 wire tests. Core-only
targets pass10 Raft,10 snapshot and1 wire test. All48 service tests pass in41.17s.
The final deterministic refusal check also passes after the Clippy correction.
The all-target/all-feature and no-default strict Clippy runs pass with zero
diagnostics. Final formatting, documentation and inventory results are retained
in `final-checks.log`; source hashes are in `source-sha256.txt`.

## Retained failures and scope

`service-first.log` records47 successes and the QUIC checkpoint timeout.
`failed-maintenance-quic-node3.log` contains the actual codec error from that
child. The deterministic refusal test failed at the same codec validation before
the fix; its output was observed in the task, not retained as a raw file.
`clippy-new-test-first.log` records an unnecessary reference clone in the new
assertion, corrected using `slice::from_ref`; lint levels are unchanged.
While adding checkpoint diagnostics, an owned stdout was initially borrowed
after conversion and failed to compile; changing the conversion to borrow fixes
that diagnostic. Its tool output was observed but not retained as a raw file.

The final service run preserves successful drain/join assertions. It does not
explain the earlier macOS nonzero shutdown exit: that remote run lacked a child
log, and this local Linux run did not reproduce it. Base ecaa2ab CI run38019357736
had completed its lint job successfully but both platform test jobs were still
running when inspected. No remote green result is inferred.

These are finite selected loopback and modeled-storage histories. They do not
establish physical power-loss behavior, arbitrary fault schedules, multi-host
deployment or complete macOS/P0–P7 acceptance. The broader goal stays active.
