# Slice165 — automatic physical WAL scheduling

Base f8e903d05a1280b5cda60043f18cb7146bd25c22 plus this commit. Production
Node policy/worker capability and opt-in executable support; no wire/disk format
or Raft protocol changes.

## Commands and results

```sh
cargo +stable test --locked --offline --test effect_owner --all-features node_facade
cargo +stable test --locked --offline --no-default-features --test effect_owner node_facade
cargo +stable test --locked --offline --no-default-features --lib runtime::maintenance
cargo +stable test --locked --offline --all-features --test effect_owner native::owning_native_facade_checkpoints_restarts_and_retries_hundred_groups
cargo +stable test --locked --offline --all-features --test log_reclaim --test maintenance
cargo +stable test --locked --offline --all-features --test counter_service maintenance -- --nocapture
cargo +stable test --locked --offline --all-features --test counter_service
cargo +stable clippy --locked --offline --keep-going --all-targets --all-features -- -D warnings
cargo +stable clippy --locked --offline --keep-going --all-targets --no-default-features -- -D warnings
cargo +stable fmt --all -- --check
node validation/check-inventory.mjs
```

- host-node.log and host-core.log:41/41 each, including6 new policy tests.
- clock.log:1/1 core-only deadline exhaustion test.
- native-node.log:1/1 native three-node100-group history,21.82s.
- reclaim.log:8/8 log-reclaim and5/5 worker-maintenance tests.
- service.log:3/3 new tests, including TCP/QUIC and preflight;0.92s.
- service-all.log:42/42 executable tests;42.07s.
- clippy-all-final.log, clippy-core.log and format.log:zero diagnostics.
- inventory.log:90 contract records; this verifies metadata, not behavior.

Native socket tests ran with local-socket permission. Linux local evidence;
ci-observation.json records the prior f8e903d run still active when inspected.
No new macOS or separate-host execution is claimed.

## Acceptance and limits

Downstream HostWorker injection covers disabled/unsupported configuration,
transactional invalid-policy refusal, bounded poll validation, late deadline
coalescing, exact automatic/manual ticket isolation, busy and safe-refusal
backoff, permanent refusal, held completion, shutdown, abort, uncertain results
and wrong receipts. Recovery retains the accepted ticket/diagnostics with the
original driver/providers. The clock check verifies overflow stops scheduling
instead of wrapping to an immediately repeated request.

The native100-group history retains its manual maintenance check and adds an
automatic pass after further writes. Actual positive byte reduction, zero driver
reclaim leases, explicit shutdown/join, changed store sessions, historical
idempotent retries and new writes are checked. Existing native/model crash tests
continue to cover the unchanged replacement operation.

The service histories require distinct successive completed worker sequences,
then graceful drain, reopening, original operation retry and a new write/read.
They prove periodic integration and recovery, not physical byte reduction at
every interval; a no-op replacement is valid. The native history independently
requires byte reduction. Automatic policy deadlines/results are local volatile
state and are rebuilt after ordinary recovery.

Physical reclaim preserves all current live state. It neither creates an
application checkpoint nor authorizes deleting any live suffix, ownership fence
or tombstone. Automatic checkpoint selection, incremental cleaning and general
retention remain work. The worker may delay foreground work while replacing a
full image; no rate or latency target is asserted.

## Initial corrections

The first host run passed4/6. Its overflow fixture drove the entire Raft timer
system to u64::MAX and hit its existing Exhausted guard before the maintenance
assertion. Overflow now has an isolated policy boundary check; the Node test
still checks regression/invalid timing before work. The other fixture assumed
one poll drained election work after a long time jump; it now waits within an
explicit100-poll bound. No production guard was relaxed.

clippy-all.log retains the initial service-loop110/100-line failure. The final
code puts startup policy construction on StartupOptions, uses that typed
options object in serve, and keeps all lint levels unchanged. Final strict logs
are separate from that initial failure.
